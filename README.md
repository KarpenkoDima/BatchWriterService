# LogCollector — BatchWriterService

High-performance background service for batched log ingestion in .NET.
Accepts log entries from multiple sources (Winlogbeat, Syslog/MikroTik, etc.) via a bounded in-process channel and writes them to a repository in configurable batches, with Polly-based retry, dead-letter fallback, and graceful shutdown.

---

## Architecture

```
[Network listeners] ──WriteAsync──▶ [LogChannel (BoundedChannel<LogEntry>)]
                                              │
                                    [BatchWriterService]
                                              │
                          ┌───────────────────┼───────────────────┐
                          ▼                   ▼                   ▼
                   FillBatchAsync     FlushWithRetryAsync   DrainRemainingAsync
                  (linger timeout)    (Polly v8 + DLQ)      (graceful shutdown)
                                              │
                                      [ILogRepository]
                                       (SQLite by default)
```

### Key design decisions

| Decision | Reason |
|---|---|
| `BoundedChannel<LogEntry>` with `FullMode.Wait` | Backpressure: writers `await` `WriteAsync` instead of unbounded heap growth |
| `SingleReader = true` | Lighter internal Channel implementation, no unnecessary read-side locking |
| Linger timeout (`FlushInterval`) | Flushes partial batches after a timeout so data doesn't sit in memory indefinitely |
| Reusable `CancellationTokenSource` via `TryReset()` | Linger timer doesn't allocate a new CTS on every wait — only when the timer actually fires |
| Polly v8 `ResiliencePipeline`, static lambda + state tuple | Retries transient errors with exponential backoff, zero allocations on the happy path |
| Independent timeout for `DrainRemainingAsync` | The final flush must NOT be linked to `stoppingToken` — it's already cancelled by the time drain runs |
| Dead-letter stub on exhausted retries | Prevents *silent* data loss; currently logs at `Critical` — replace with real persistence |

> **Note on the previous design:** an earlier revision used a `volatile CancellationTokenSource` with `Interlocked` access from `Dispose()`. This was a TOCTOU race and has been **removed**. The linger CTS now lives entirely inside `ExecuteAsync` (single-consumer access), so no synchronization primitives are needed.

---

## ⚠️ Operational trade-off: retry latency vs. channel capacity

`FlushWithRetryAsync` can retry up to 3 times with delays of **2s → 4s → 8s** (≈14s worst case) before falling back to the dead-letter stub. During that time `FillBatchAsync` is not called, so the channel is **not drained**.

Consequences at default settings (`ChannelCapacity = 10_000`):

- If incoming throughput exceeds roughly **700 entries/sec** sustained over ~14s, the channel fills up.
- **TCP listeners** (`TcpLogListener`) will simply block on `WriteAsync` — this is the intended backpressure and is safe.
- **UDP listeners** (`UdpLogListener`) will also block on `WriteAsync`, but UDP datagrams arriving while `ReceiveFromAsync` is not being called are dropped **by the OS socket buffer**, not by the channel. Backpressure does not protect UDP sources.

If your `ILogRepository` implementation can stall for tens of seconds under load (e.g. `SQLITE_BUSY` contention), consider:

- splitting UDP into its own channel with `FullMode.DropOldest`,
- reducing `MaxRetryAttempts` / backoff for latency-sensitive deployments,
- or sizing `ChannelCapacity` against your real worst-case throughput × retry duration.

This is a deliberate design trade-off, not a bug — but it must be sized for your traffic profile.

---

## Getting started

### Requirements

- .NET 9
- `Polly.Core` (>= 8.0)
- `Microsoft.Extensions.Hosting`
- An `ILogRepository` implementation (SQLite reference implementation included)

### Install dependencies

```bash
dotnet add package Polly.Core
dotnet add package Microsoft.Extensions.Hosting
dotnet add package Microsoft.Data.Sqlite   # if using the SQLite repository
```

### Register services (`Program.cs`)

`BatchWriterService` is a `BackgroundService` resolved as a **singleton**. Its dependencies — including `ILogRepository` — must therefore also be registered as **singletons** (or resolved via `IServiceScopeFactory` inside the repository itself). Registering `ILogRepository` as `Scoped` will throw `InvalidOperationException` ("Cannot consume scoped service from singleton") when the host starts.

```csharp
builder.Services.AddSingleton<LogChannel>();

builder.Services.Configure<LogCollectorOptions>(
    builder.Configuration.GetSection("BatchWriter"));

// ILogRepository MUST be singleton-compatible.
// The reference SqliteLogRepository opens/closes a connection per batch,
// so it is safe to register as a singleton.
builder.Services.AddSingleton<ILogRepository>(_ =>
    new SqliteLogRepository(connectionString));

// Two-step registration: one singleton instance, exposed both as the
// hosted service AND as an injectable type if needed elsewhere.
builder.Services.AddSingleton<BatchWriterService>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<BatchWriterService>());
```

### Configuration (`appsettings.json`)

```json
{
  "BatchWriter": {
    "BatchSize": 500,
    "ChannelCapacity": 10000,
    "FlushInterval": "00:00:05",
    "ShutdownFlushTimeout": "00:00:10"
  }
}
```

| Option | Default | Description |
|---|---|---|
| `BatchSize` | `500` | Max entries per DB write |
| `ChannelCapacity` | `10000` | Bounded channel size; see trade-off section above |
| `FlushInterval` | `00:00:05` | Max wait before flushing a partial batch (linger timeout) |
| `ShutdownFlushTimeout` | `00:00:10` | Deadline for the final drain on shutdown. **Must be smaller than `HostOptions.ShutdownTimeout`**, or the host will kill the process before the final flush completes |

---

## Components

### `BatchWriterService`

`BackgroundService` that reads from `LogChannel`, assembles batches via `FillBatchAsync`, flushes them with retry via `FlushWithRetryAsync`, and on shutdown drains any remaining entries via `DrainRemainingAsync` using an independent timeout (not linked to the cancelled `stoppingToken`).

### `LogChannel`

Singleton wrapper around `Channel<LogEntry>`. Single reader (the writer service), multiple writers (network listeners). Capacity configured via `ChannelCapacity`.

### `LogEntry`

Flat domain entity — no nested objects or collections, minimizing per-entry allocation and simplifying repository mapping.

| Field | Required | Description |
|---|---|---|
| `Source` | ✅ | e.g. `"winlogbeat"`, `"mikrotik"` |
| `Timestamp` | ✅ | UTC event time |
| `Level` | ✅ | `"info"`, `"warning"`, `"error"`, `"critical"`, etc. |
| `Message` | ✅ | Raw log body |
| `SourceIp` | ❌ | Source IP address |
| `Hostname` | ❌ | `host.name` (Winlogbeat) / hostname field (Syslog) |

### `ILogRepository`

```csharp
public interface ILogRepository
{
    Task<int> InsertBatchAsync(IReadOnlyList<LogEntry> entries, CancellationToken ct = default);
}
```

The `int` return value (rows actually affected) is currently **not inspected** by `BatchWriterService` — `FlushWithRetryAsync` wraps the call in `new ValueTask(...)`, discarding the count. If your repository uses upsert-style semantics (e.g. `ON CONFLICT DO NOTHING`, as the SQLite reference implementation does), `affected` may legitimately be less than `entries.Count`. If you need to detect/log partial conflicts, this is the place to add that check.

---

## Retry policy

Retries fire **only** for errors classified as transient by `IsTransient(Exception?)`:

| Exception | Condition | Retried? |
|---|---|---|
| `OperationCanceledException` | always | ❌ never |
| `SqliteException` | `SqliteErrorCode` is `5` (`SQLITE_BUSY`) or `6` (`SQLITE_LOCKED`) | ✅ |
| `SqliteException` | any other code (e.g. corrupt, full, readonly) | ❌ |
| `SocketException`, `HttpRequestException`, `TimeoutException` | always (for remote repositories) | ✅ |
| anything else | — | ❌ |

Retry schedule: **3 attempts**, exponential backoff `2s → 4s → 8s` (see the [latency trade-off](#️-operational-trade-off-retry-latency-vs-channel-capacity) above).

After retries are exhausted (or on a non-transient error), the batch is passed to `MoveToDeadLetterAsync` and logged at `Critical`.

> If you swap in a different `ILogRepository` (SQL Server, ClickHouse, Postgres, ...), extend `IsTransient()` with the relevant transient error codes for that backend — the current list is SQLite-specific.

---

## Dead-letter handling

`MoveToDeadLetterAsync` is currently a **stub** that only logs at `Critical`:

```csharp
private Task MoveToDeadLetterAsync(List<LogEntry> batch)
{
    LogDeadLetterStub(batch.Count);
    return Task.CompletedTask;
}
```

**Contract for a real implementation:** the caller calls `batch.Clear()` immediately after this method returns (or, on the synchronous path, right after `await`). Any real implementation **must fully read or copy the contents of `batch` before returning** — do not buffer the `List<LogEntry>` reference itself for later/background processing, since it will be cleared and reused on the next iteration.

Suggested implementations:
- append-only NDJSON file per failed batch,
- secondary lightweight store (e.g. a separate SQLite table or file-based queue),
- external message broker (if available).

If `Critical`-level logs feed an alerting pipeline, ensure the alert has dedup/rate-limiting — a prolonged outage will otherwise emit one `Critical` log roughly every `FlushInterval` + retry duration.

---

## Graceful shutdown

1. The host signals cancellation via `stoppingToken`.
2. `ExecuteAsync`'s main loop exits on `OperationCanceledException`; any partially-filled `batch` is preserved.
3. `DrainRemainingAsync` synchronously drains all remaining entries from the channel into `batch`.
4. A **new, independent** `CancellationTokenSource` (timeout = `ShutdownFlushTimeout`) is used for the final `InsertBatchAsync` — it is intentionally **not** linked to `stoppingToken`, which is already cancelled at this point.
5. On success: logged via `LogFinalFlush`. On timeout or error: forwarded to `MoveToDeadLetterAsync`.

Make sure `ShutdownFlushTimeout` < `HostOptions.ShutdownTimeout`, otherwise the host may terminate the process before step 4 completes.

---

## License

MIT
