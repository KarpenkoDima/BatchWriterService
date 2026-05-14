void Main()
{
	/*
using Polly;
using Polly.Retry;
*/

public sealed class BatchWriterSevice : BackgroundService
{

	private readonly LogChannel _channel;
	private readonly ILogRepository _repository;
	private readonly ILogger<BatchWriterService> _logger;

	private const int BatchSize = 500;

	public BatchWriterSevice(
		LogChannel channel,
		ILogRepository repository,
		ILogger<BatchWriterService> logger)
	{
		_channel = channel;
		_logger = logger;
		_repository = repository;
	}

	private static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(5);

	// Inside your class
	private readonly AsyncRetryPolicy _retryPolicy = Policy
		.Handle<Exception>(ex => IsTransient(ex)) // Only retry on "fixable" errors
		.WaitAndRetryAsync(3, retryAttempt =>
			TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), // Exponential backoff: 2s, 4s, 8s
			(exception, timeSpan, retryCount, context) =>
			{
				// Log a warning so you know retries are happening
				// _logger.LogWarning("Retry {Count} due to {Message}", retryCount, exception.Message);
			});

	private static bool IsTransient(Exception ex)
	{
		// You'd typically check for SQL timeout or Network errors here
		// For now, let's assume all exceptions except critical ones are retryable
		return ex is not InvalidOperationException;
	}

	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		var batch = new List<LogEntry>(BatchSize);

		while (false == stoppingToken.IsCancellationRequested)
		{
			using var timeoutCts = new CancellationTokenSource(FlushInterval);
			using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, timeoutCts.Token);
			try
			{
				await FillBatchAsync(batch, timeoutCts.Token);
			}
			catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
			{
				break; ;
			}
			catch (OperationCanceledException) {/*Timer hit, proceed to flush */}

			if (batch.Count > 0)
			{
				try
				{
					// Wrap the call in our Polly policy
					await _retryPolicy.ExecuteAsync(async (ct) =>
					{
						await _repository.InsertBatchAsync(batch, ct);
					}, linkedCts.Token);

					_logger.LogInformation("Successfully flushed {Count} entries", batch.Count);
				}
				catch (Exception ex)
				{
					_logger.LogCritical(ex, "FATAL: Batch failed after retries. Moving to Dead Letter Store.");
					await MoveToDeadLetterAsync(batch);
				}
				finally
				{
					//batch.Clear();
				}
			}
		}
		// Финальный flush при graceful shutdown
		await DrainRemainingAsync(batch, stoppingToken);

	}

	private async Task DrainRemainingAsync(List<LogEntry> batch, CancellationToken stoppingToken)
	{

		while (_channel.Reader.TryRead(out var entry))
		{
			batch.Add(entry);
		}

		if (batch.Count > 0)
		{
			// чтобі не зависла на всегда запись в хранилище
			using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
			cts.CancelAfter(FlushInterval);
			await _repository.InsertBatchAsync(batch, cts.Token);
			_logger.LogInformation("Final flush {Count} entries", batch.Count);
		}
	}

	async Task FillBatchAsync(List<LogEntry> batch, CancellationToken token)
	{
		// Ждём хотя бы один элемент, чтобы не крутить цил в холостую
		var entry = await _channel.Reader.ReadAsync(token);
		batch.Add(entry);

		using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
		cts.CancelAfter(FlushInterval);

		while (batch.Count < BatchSize && false == cts.Token.IsCancellationRequested)
		{
			if (false == _channel.Reader.TryRead(out entry))
			{
				// Канал пуст прямо сейчас — ждём немного или до таймаута  
				try
				{
					await _channel.Reader.WaitToReadAsync(cts.Token);
				}
				catch (OperationCanceledException ex)
				{
					break; // FlushInterval истёк — отправляем неполный батч  
				}
			}
			else
			{
				batch.Add(entry);
			}
		}

	}

	Task MoveToDeadLetterAsync(List<LogEntry> batch)
	{
		// В случае неудач записываем либо в лог либо в файл несохраненные 
		// после всех попыток очищаем
		batch.Clear();
		throw new NotImplementedException();
	}
}


/// <summary>
/// Централизованная точка доступа к in-process шине сообщений.
///
/// BoundedChannel + FullMode.Wait = backpressure:
/// если воркер не успевает писать в БД, сетевой слой начнёт await-ить WriteAsync
/// вместо того чтобы накапливать объекты в памяти. Защита от OOM при всплеске трафика.
/// </summary>
public sealed class LogChannel
{
	// SingleReader = true — читает только один BatchWriterService.
	// Подсказка позволяет Channel использовать более лёгкую внутреннюю структуру без лишних lock-ов.
	private readonly Channel<LogEntry> _channel = Channel.CreateBounded<LogEntry>(
		new BoundedChannelOptions(capacity: 10_000)
		{
			SingleReader = true,
			SingleWriter = false,   // пишут несколько сетевых слушателей
			FullMode = BoundedChannelFullMode.Wait
		});

	public ChannelWriter<LogEntry> Writer => _channel.Writer;
	public ChannelReader<LogEntry> Reader => _channel.Reader;
}

/// <summary>
/// Единственная доменная сущность сервиса.
/// Намеренно плоская — никаких вложенных объектов, никаких коллекций.
/// Минимизирует аллокации при создании и упрощает маппинг в БД.
/// </summary>
public sealed class LogEntry
{
	/// <summary>Источник: "winlogbeat", "mikrotik".</summary>
	public required string Source { get; init; }

	/// <summary>Временная метка события. Никогда не null — подставляем UtcNow если не распарсили.</summary>
	public required DateTime Timestamp { get; init; }

	/// <summary>Severity: "info", "warning", "error", "critical" и т.д.</summary>
	public required string Level { get; init; }

	/// <summary>Тело сообщения. Единственное поле где большая строка неизбежна.</summary>
	public required string Message { get; init; }

	/// <summary>IP-адрес источника.</summary>
	public string? SourceIp { get; init; }

	/// <summary>Имя хоста из лога (host.name у Winlogbeat, HOSTNAME у Syslog).</summary>
	public string? Hostname { get; init; }
}

}

// You can define other methods, fields, classes and namespaces here