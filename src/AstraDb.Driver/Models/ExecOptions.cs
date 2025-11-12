using Cassandra;

namespace AstraDb.Driver.Models;

public sealed class ExecOptions
{
    public ConsistencyLevel? Consistency { get; init; }
    public bool Tracing { get; init; }
    public bool Idempotent { get; init; }
    public int? PageSize { get; init; }
    public int? TimeoutMs { get; init; }
    public int? TtlSeconds { get; init; }
    public DateTimeOffset? UsingTimestamp { get; init; }
    public CancellationToken Cancellation { get; init; }
}
