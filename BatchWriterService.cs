// BatchWriterService — переработанная версия.
//
// Ключевые изменения относительно исходника:
//
// 1. Убран volatile CancellationTokenSource + Interlocked-танцы в Dispose().
//    Это была гонка TOCTOU: `if (_cts is null) ... _cts.TryReset()` мог словить NRE,
//    если Dispose() обнулил поле между проверкой и использованием.
//    Linger-CTS теперь живёт строго внутри ExecuteAsync (single consumer),
//    синхронизация не нужна вообще.
//
// 2. Исправлен фатальный баг DrainRemainingAsync: линковка финального CTS
//    на stoppingToken, который К ЭТОМУ МОМЕНТУ УЖЕ ОТМЕНЁН, означала, что
//    финальный flush умирал мгновенно и логи терялись при каждом shutdown.
//    Теперь — независимый токен с собственным дедлайном (< HostOptions.ShutdownTimeout).
//
// 3. Исправлен потенциальный hot-spin в FillBatchAsync: возврат false из
//    WaitToReadAsync (канал completed) не обрабатывался -> бесконечный цикл
//    TryRead=false / WaitToReadAsync=false на 100% CPU.
//
// 4. Убран linked CTS на каждую итерацию (аллокация на каждый батч).
//    Linger реализован через монотонный дедлайн Environment.TickCount64
//    + один переиспользуемый CTS (TryReset). Новая аллокация CTS происходит
//    только когда linger-таймер реально сработал (~1 объект в FlushInterval).
//
// 5. Polly v7 AsyncRetryPolicy -> Polly v8 ResiliencePipeline:
//    строится один раз, ExecuteAsync со static-лямбдой и state-кортежем —
//    ноль замыканий, ноль аллокаций на успешном пути.
//
// 6. Логирование через [LoggerMessage] source generator:
//    нет боксинга int в object[], нет аллокации params-массива,
//    проверка IsEnabled встроена.
//
// 7. Отмена flush при shutdown больше не считается «FATAL -> DLQ»:
//    OCE пробрасывается наверх, батч сохраняется и дописывается
//    в DrainRemainingAsync со свежим токеном.
//
// NuGet: Polly.Core (>= 8.0), Microsoft.Data.Sqlite.

using System.Net.Http;
using System.Net.Sockets;
using System.Threading.Channels;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

public sealed partial class BatchWriterService : BackgroundService
{
    private readonly LogChannel _channel;
    private readonly ILogRepository _repository;
    private readonly ILogger<BatchWriterService> _logger; // нужен source-генератору LoggerMessage
    private readonly LogCollectorOptions _options;
    private readonly ResiliencePipeline _retryPipeline;
    private readonly long _flushIntervalMs;

    // Linger-таймер. Один на весь сервис, переиспользуется через TryReset().
    // Доступ — только из потока ExecuteAsync, поэтому ни volatile, ни Interlocked не нужны.
    private CancellationTokenSource _lingerCts = null!;

    public BatchWriterService(
        LogChannel channel,
        ILogRepository repository,
        ILogger<BatchWriterService> logger,
        IOptions<LogCollectorOptions> options)
    {
        _channel = channel;
        _repository = repository;
        _logger = logger;
        _options = options.Value;
        _flushIntervalMs = (long)_options.FlushInterval.TotalMilliseconds;

        // Пайплайн строится один раз; сам по себе immutable и thread-safe.
        _retryPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                ShouldHandle = static args => ValueTask.FromResult(IsTransient(args.Outcome.Exception)),
                MaxRetryAttempts = 3,
                BackoffType = DelayBackoffType.Exponential,
                Delay = TimeSpan.FromSeconds(2), // 2s -> 4s -> 8s
                OnRetry = args =>
                {
                    LogRetry(args.Outcome.Exception!, args.AttemptNumber + 1, args.RetryDelay);
                    return default;
                }
            })
            .Build();
    }

    private static bool IsTransient(Exception? ex) => ex switch
    {
        // Отмена — это не сбой, ретраить бессмысленно.
        OperationCanceledException => false,

        // SQLite: 5 = SQLITE_BUSY (база занята другим соединением),
        //         6 = SQLITE_LOCKED (конфликт внутри соединения).
        // Оба разрешаются повтором. Остальные коды (corrupt, full, readonly) — нет.
        SqliteException s => s.SqliteErrorCode is 5 or 6,

        // Сетевые сбои актуальны, если репозиторий когда-нибудь станет удалённым.
        SocketException or HttpRequestException or TimeoutException => true,

        _ => false
    };

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // Батч аллоцируется один раз и переиспользуется: Clear() сохраняет capacity,
        // внутренний массив не пересоздаётся.
        var batch = new List<LogEntry>(_options.BatchSize);

        _lingerCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);

        try
        {
            var channelAlive = true;
            while (channelAlive && !stoppingToken.IsCancellationRequested)
            {
                channelAlive = await FillBatchAsync(batch, stoppingToken);

                if (batch.Count > 0)
                    await FlushWithRetryAsync(batch, stoppingToken);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Graceful shutdown. Недописанный batch остаётся в списке —
            // его дольёт DrainRemainingAsync вместе с остатками канала.
        }
        finally
        {
            _lingerCts.Dispose();
        }

        await DrainRemainingAsync(batch);
    }

    /// <summary>
    /// Собирает батч: блокируется до первого элемента, затем добирает до BatchSize
    /// либо до истечения linger-окна (FlushInterval), отсчитываемого от первого элемента.
    /// </summary>
    /// <returns>false — канал закрыт, продолжать цикл бессмысленно.</returns>
    private async Task<bool> FillBatchAsync(List<LogEntry> batch, CancellationToken stoppingToken)
    {
        LogEntry? entry;

        try
        {
            // Ждём первый элемент без таймаута: пустой канал не должен
            // будить сервис вхолостую каждые FlushInterval.
            entry = await _channel.Reader.ReadAsync(stoppingToken);
        }
        catch (ChannelClosedException)
        {
            return false;
        }

        batch.Add(entry);

        // Монотонный дедлайн вместо CancelAfter-на-итерацию:
        // TickCount64 не зависит от перевода системных часов и ничего не аллоцирует.
        var deadline = Environment.TickCount64 + _flushIntervalMs;

        while (batch.Count < _options.BatchSize)
        {
            // Горячий путь: элементы уже в канале — забираем синхронно, без await.
            if (_channel.Reader.TryRead(out entry))
            {
                batch.Add(entry);
                continue;
            }

            var remaining = deadline - Environment.TickCount64;
            if (remaining <= 0)
                break; // linger истёк — отдаём неполный батч

            // Переиспользуем CTS. TryReset() возвращает true, если предыдущий
            // CancelAfter не успел сработать (мы вышли из ожидания по данным) —
            // тогда таймер просто перевзводится без аллокаций.
            // false — таймер сработал или прилетел stoppingToken: пересоздаём.
            if (!_lingerCts.TryReset())
            {
                _lingerCts.Dispose();
                _lingerCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            }
            _lingerCts.CancelAfter(TimeSpan.FromMilliseconds(remaining));

            try
            {
                // ВАЖНО: false означает «канал completed и пуст».
                // В исходнике это не проверялось -> бесконечный spin на 100% CPU.
                if (!await _channel.Reader.WaitToReadAsync(_lingerCts.Token))
                    return false;
            }
            catch (OperationCanceledException) when (!stoppingToken.IsCancellationRequested)
            {
                break; // сработал именно linger-таймер
            }
            // OCE от stoppingToken пробрасывается в ExecuteAsync:
            // там его перехватит фильтрованный catch, batch уцелеет.
            catch (ChannelClosedException)
            {
                return false;
            }
        }

        return true;
    }

    private async Task FlushWithRetryAsync(List<LogEntry> batch, CancellationToken ct)
    {
        try
        {
            // static-лямбда + state-кортеж (value type) = ноль замыканий.
            // ResiliencePipeline v8 на успешном пути не аллоцирует.
            await _retryPipeline.ExecuteAsync(
                static (state, token) => new ValueTask(state.Repository.InsertBatchAsync(state.Batch, token)),
                (Repository: _repository, Batch: batch),
                ct);

            LogFlushed(batch.Count);
            batch.Clear();
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // Shutdown посреди записи — это НЕ повод хоронить батч в DLQ.
            // Пробрасываем наверх, batch не очищаем: DrainRemainingAsync
            // сделает финальную попытку со свежим токеном.
            throw;
        }
        catch (Exception ex)
        {
            LogBatchFailed(ex, batch.Count);
            await MoveToDeadLetterAsync(batch);
            batch.Clear();
        }
    }

    private async Task DrainRemainingAsync(List<LogEntry> batch)
    {
        // Синхронно выгребаем всё, что осталось в канале (писатели уже остановлены хостом).
        while (_channel.Reader.TryRead(out var entry))
            batch.Add(entry);

        if (batch.Count == 0)
            return;

        // НЕ линкуемся на stoppingToken — он уже отменён, и linked-токен
        // родился бы «мёртвым»: InsertBatchAsync отменился бы мгновенно,
        // а финальные логи терялись бы при каждом рестарте сервиса.
        // Независимый дедлайн; должен укладываться в HostOptions.ShutdownTimeout.
        using var cts = new CancellationTokenSource(_options.ShutdownFlushTimeout);

        try
        {
            await _repository.InsertBatchAsync(batch, cts.Token);
            LogFinalFlush(batch.Count);
        }
        catch (OperationCanceledException)
        {
            LogFinalFlushTimeout(batch.Count);
            await MoveToDeadLetterAsync(batch);
        }
        catch (Exception ex)
        {
            LogBatchFailed(ex, batch.Count);
            await MoveToDeadLetterAsync(batch);
        }
        finally
        {
            batch.Clear();
        }
    }

    private Task MoveToDeadLetterAsync(List<LogEntry> batch)
    {
        // Точка для реальной DLQ-реализации (append-only файл / отдельная таблица).
        LogDeadLetterStub(batch.Count);
        return Task.CompletedTask;
    }

    // Dispose() намеренно не переопределён: единственный disposable-ресурс
    // (_lingerCts) освобождается в finally ExecuteAsync, гонок с хостом нет.

    // --- LoggerMessage source generator: zero-allocation логирование ---
    // Нет боксинга аргументов, нет params object[], IsEnabled-проверка встроена.

    [LoggerMessage(EventId = 1, Level = LogLevel.Warning,
        Message = "Transient failure, retry {Attempt} in {Delay}")]
    private partial void LogRetry(Exception exception, int attempt, TimeSpan delay);

    [LoggerMessage(EventId = 2, Level = LogLevel.Debug,
        Message = "Flushed {Count} entries")]
    private partial void LogFlushed(int count);

    [LoggerMessage(EventId = 3, Level = LogLevel.Critical,
        Message = "Batch of {Count} entries failed after retries, moving to dead-letter store")]
    private partial void LogBatchFailed(Exception exception, int count);

    [LoggerMessage(EventId = 4, Level = LogLevel.Information,
        Message = "Final flush: {Count} entries")]
    private partial void LogFinalFlush(int count);

    [LoggerMessage(EventId = 5, Level = LogLevel.Warning,
        Message = "Final flush of {Count} entries timed out, moving to dead-letter store")]
    private partial void LogFinalFlushTimeout(int count);

    [LoggerMessage(EventId = 6, Level = LogLevel.Critical,
        Message = "DLQ not implemented: {Count} entries will be lost")]
    private partial void LogDeadLetterStub(int count);
}

/// <summary>
/// In-process шина. BoundedChannel + FullMode.Wait = backpressure:
/// при перегрузке писатели await-ят WriteAsync вместо роста кучи. Защита от OOM.
/// </summary>
public sealed class LogChannel
{
    private readonly Channel<LogEntry> _channel;

    public LogChannel(IOptions<LogCollectorOptions> options)
    {
        _channel = Channel.CreateBounded<LogEntry>(new BoundedChannelOptions(options.Value.ChannelCapacity)
        {
            // SingleReader = true — читает только BatchWriterService.
            // Channel выбирает облегчённую реализацию без лишней синхронизации на чтении.
            SingleReader = true,
            SingleWriter = false, // пишут несколько сетевых слушателей
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    public ChannelWriter<LogEntry> Writer => _channel.Writer;
    public ChannelReader<LogEntry> Reader => _channel.Reader;
}

/// <summary>
/// Намеренно плоская сущность: без вложенных объектов и коллекций.
/// Одна аллокация на событие — неизбежная плата за Channel&lt;T&gt; с reference type;
/// настоящий zero-alloc живёт уровнем ниже, в парсинге (Span/Pipelines).
/// </summary>
public sealed class LogEntry
{
    public required string Source { get; init; }
    public required DateTime Timestamp { get; init; }
    public required string Level { get; init; }
    public required string Message { get; init; }
    public string? SourceIp { get; init; }
    public string? Hostname { get; init; }
}

public sealed class LogCollectorOptions
{
    public int BatchSize { get; set; } = 500;
    public int ChannelCapacity { get; set; } = 10_000;
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Дедлайн финального flush при shutdown. Держать меньше HostOptions.ShutdownTimeout.</summary>
    public TimeSpan ShutdownFlushTimeout { get; set; } = TimeSpan.FromSeconds(10);
}
