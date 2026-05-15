void Main()
{ }
/*using Polly;
using Polly.Retry;
*/

using System.Threading.Tasks
;
public sealed class BatchWriterService : BackgroundService
{

	private readonly LogChannel _channel;
	private readonly ILogRepository _repository;
	private readonly ILogger<BatchWriterService> _logger;
	private readonly LogCollectorOptions _options; // Добалвяем опции
	private readonly AsyncRetryPolicy _retryPolicy;

	private CancellationTokenSource _cts = new CancellationTokenSource();

	public BatchWriterService(
		LogChannel channel,
		ILogRepository repository,
		ILogger<BatchWriterService> logger,
		IOptions<LogCollectorOptions> options)
	{
		_channel = channel;
		_logger = logger;
		_repository = repository;
		_options = options.Value;

		_retryPolicy = Policy
			.Handle<Exception>(ex => IsTransient(ex)) // Only retry on "fixable" errors
			.WaitAndRetryAsync(
			retryCount: 3,
			sleepDurationProvider: retryAttempt =>
			TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), // Exponential backoff: 2s, 4s, 8s
			onRetry: (exception, timeSpan, retryCount, context) =>
			{
				// Log a warning so you know retries are happening
				_logger.LogWarning("Retry {Count} due to {Message}", retryCount, exception.Message);
			});
	}


	private static bool IsTransient(Exception ex)
	{
		// Более детальная проверка на временные ошибки, например для SQL Server и сетевых ошибок.
		// Здесь можно добавить другие типы исключений, характерные для вашего хранилища данных.
		if (ex is SqlException sqlEx)
		{
			// Transient SQL error codes (e.g., timeout, deadlock victim, network issues)
			// https://docs.microsoft.com/en-us/azure/azure-sql/database/troubleshoot-vnet-connectivity#transient-errors
			var transientSqlErrorCodes = new[] { 4060, 40197, 40501, 40613, 49918, 49919, 49920, 11001 };
			if (transientSqlErrorCodes.Contains(sqlEx.Number))
			{
				return true;
			}
		}

		if (ex is SocketException || ex is HttpRequestException)
		{
			return true; // Временные ошибки сети
		}
		if (ex is OperationCanceledException)
		{
			return false; // Отмена операции не является временной ошибкой для ретрая
		}
		// По умолчанию не считаем ошибку временной, если нет явного указания
		return false;
	}


	protected override async Task ExecuteAsync(CancellationToken stoppingToken)
	{
		var batch = new List<LogEntry>(_options.BatchSize);

		if (false == _cts.TryReset())
		{
			_cts.Dispose();
			_cts = new CancellationTokenSource();
		}

		_cts.CancelAfter(TimeSpan.FromMilliseconds(_options.FlushInterval));
		var token = _cts.Token;

		while (false == stoppingToken.IsCancellationRequested)
		{
			// Создаем CTS для таймаута сброса (Linger) для каждой итерации
			//using var timeoutCts = new CancellationTokenSource(TimeSpan.FromSeconds(_options.FlushInterval));
			using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken, token);
			try
			{
				await FillBatchAsync(batch, linkedCts.Token); // Используем linkedCts.Token для сбора
			}
			catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
			{
				// Приложение завершает работу, выходим из цикла
				break;
			}
			catch (OperationCanceledException)
			{
				// Сработал таймаут сбора батча (linger timeout), продолжаем к попытке сброса того, что собрали
			}

			if (batch.Count > 0)
			{
				try
				{
					// Wrap the call in our Polly policy
					await _retryPolicy.ExecuteAsync(async (ct) =>
					{
						await _repository.InsertBatchAsync(batch, ct);
					}, stoppingToken);// Запись в БД с Retry-политикой, используя ГЛОБАЛЬНЫЙ stoppingToken

					_logger.LogInformation("Successfully flushed {Count} entries", batch.Count);
					batch.Clear(); // Очищаем батч только после успешной записи
				}
				catch (Exception ex)
				{
					_logger.LogCritical(ex, "FATAL: Batch failed after retries. Moving to Dead Letter Store.");
					await MoveToDeadLetterAsync(batch);
					batch.Clear(); // Очищаем батч после попытки перемещения в DLQ
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
			// чтобы не зависла на всегда запись в хранилище
			using var cts = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
			cts.CancelAfter(TimeSpan.FromSeconds(_options.FlushInterval));
			try
			{
				await _repository.InsertBatchAsync(batch, cts.Token);
				_logger.LogInformation("Final flush {Count} entries", batch.Count);
			}
			catch (OperationCanceledException)
			{
				_logger.LogWarning("Финальный сьрос был отменён");
			}
			catch (Exception ex)
			{
				_logger.LogError(ex, "Ошибка при финальном сбросе логов. Перемещение в DLQ");
				await MoveToDeadLetterAsync(batch);
			}
			finally
			{
				batch.Clear(); // Очищаем батч после финального сброса или DLQ
			}
		}
	}

	async Task FillBatchAsync(List<LogEntry> batch, CancellationToken token)
	{
		try
		{
			// Ждём хотя бы один элемент, чтобы не крутить цил в холостую
			var entry = await _channel.Reader.ReadAsync(token);
			batch.Add(entry);

			using var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
			cts.CancelAfter(TimeSpan.FromSeconds(_options.FlushInterval));

			while (batch.Count < _options.BatchSize && false == cts.Token.IsCancellationRequested)
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
						_logger.LogDebug("Таймаут сбора батча истек. Отправляем неполный батч.");
						break; // FlushInterval истёк — отправляем неполный батч  
					}
				}
				else
				{
					batch.Add(entry);
				}
			}
		}
		catch (OperationCanceledException)
		{
			// Нормальное поведение при отмене: батч, собранный на данный момент, остается в списке
		}
		catch (ChannelClosedException ex)
		{
			_logger.LogWarning("Канал был закрыт во время чтения.");
		}
	}

	Task MoveToDeadLetterAsync(List<LogEntry> batch)
	{
		_logger.LogCritical("Реализуйте логику сохранения {Count} логов в Dead Letter Store.", batch.Count);
		// В случае неудач записываем либо в лог, либо в файл несохраненные логи
		// Это место для вашей реальной реализации DLQ
		return Task.CompletedTask; // Заглушка
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

// пример класса опций (нужно создать в проекте)
public class LogCollectorOptions
{
	public int BatchSize { get; set; }=500;
	public double FlushInterval { get; set; }=5; // seconds
}