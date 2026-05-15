# LogCollector — BatchWriterService

High-performance background service for batched log ingestion in .NET.  
Accepts logs from multiple sources (Winlogbeat, Syslog/MikroTik, etc.), buffers them in-process via a bounded channel, and writes to a repository in configurable batches with retry logic and graceful shutdown.

---

## Architecture

```
[Network listeners] ──WriteAsync──▶ [LogChannel (BoundedChannel)]
                                              │
                                    [BatchWriterService]
                                              │
                          ┌───────────────────┼───────────────────┐
                          ▼                   ▼                   ▼
                   FillBatchAsync     FlushWithRetryAsync   DrainRemainingAsync
                  (linger timeout)     (Polly + DLQ)        (graceful shutdown)
                                              │
                                      [ILogRepository]
                                       (SQL / other)
```

### Key design decisions

| Decision | Reason |
|---|---|
| `BoundedChannel` with `FullMode.Wait` | Backpressure: writers block instead of causing OOM on traffic spikes |
| `SingleReader = true` | Lighter internal structure, no unnecessary locks |
| Linger timeout (`FlushInterval`) | Sends partial batches after timeout, avoids data sitting in memory too long |
| Polly exponential backoff (2s / 4s / 8s) | Retries only transient errors (SQL codes, network), not cancellations |
| Dead Letter Queue on final failure | Prevents silent data loss after exhausted retries |
| `volatile` + `Interlocked.Exchange` on `_cts` | Safe disposal without lock when host shuts down concurrently |

---

## Getting started

### Requirements

- .NET 8+
- `Polly` NuGet package
- An `ILogRepository` implementation (SQL Server, ClickHouse, etc.)

### Install dependencies

```bash
dotnet add package Polly
dotnet add package Microsoft.Extensions.Hosting
```

### Register services (`Program.cs`)

```csharp
builder.Services.AddSingleton<LogChannel>();
builder.Services.AddScoped<ILogRepository, YourLogRepository>();
builder.Services.Configure<LogCollectorOptions>(
    builder.Configuration.GetSection("LogCollector"));
builder.Services.AddHostedService<BatchWriterService>();
```

### Configuration (`appsettings.json`)

```json
{
  "LogCollector": {
    "BatchSize": 500,
    "FlushInterval": "00:00:05"
  }
}
```

| Option | Default | Description |
|---|---|---|
| `BatchSize` | `500` | Max entries per DB write |
| `FlushInterval` | `00:00:05` | Max wait before flushing a partial batch |

---

## Components

### `BatchWriterService`
`BackgroundService` that reads from `LogChannel`, assembles batches, and writes them to `ILogRepository`. Handles graceful shutdown by draining remaining entries before stopping.

### `LogChannel`
Singleton wrapper around `Channel<LogEntry>`. Single reader, multiple writers. Capacity: 10 000 entries.

### `LogEntry`
Flat domain entity. No nested objects — minimises allocations and simplifies DB mapping.

| Field | Required | Description |
|---|---|---|
| `Source` | ✅ | e.g. `"winlogbeat"`, `"mikrotik"` |
| `Timestamp` | ✅ | UTC event time |
| `Level` | ✅ | `"info"`, `"warning"`, `"error"`, `"critical"` |
| `Message` | ✅ | Raw log body |
| `SourceIp` | ❌ | Source IP address |
| `Hostname` | ❌ | `host.name` (Winlogbeat) / `HOSTNAME` (Syslog) |

### `ILogRepository`
Implement this interface to target your storage:

```csharp
public interface ILogRepository
{
    Task InsertBatchAsync(IReadOnlyList<LogEntry> batch, CancellationToken ct);
}
```

---

## Retry policy

Retries fire **only** on transient errors:

- SQL Server error codes: `4060`, `40197`, `40501`, `40613`, `49918`, `49919`, `49920`, `11001`
- `SocketException`, `HttpRequestException`

`OperationCanceledException` is **never** retried.  
After 3 failed attempts the batch is forwarded to `MoveToDeadLetterAsync` — implement this method to persist failed entries (file, secondary DB, etc.).

---

## Extending

**Dead Letter Store** — replace the stub in `MoveToDeadLetterAsync`:
```csharp
Task MoveToDeadLetterAsync(List<LogEntry> batch)
{
    // Write to file, secondary DB, Azure Service Bus, etc.
}
```

**Custom transient errors** — extend `IsTransient()` with your storage-specific exception types.

---

## License

MIT
