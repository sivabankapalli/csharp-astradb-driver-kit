using Cassandra;

namespace AstraDb.Driver.Models;

/// <summary>
/// Per-call execution options for AstraDB read/write operations.
/// Can override environment defaults defined in <see cref="Options.AstraDbRequestDefaults"/>.
/// </summary>
public sealed class ExecOptions
{
    /// <summary>
    /// Consistency level for the request. If null, defaults are used.
    /// </summary>
    public ConsistencyLevel? Consistency { get; init; }

    /// <summary>
    /// Enable/disable tracing. If null, defaults are used.
    /// </summary>
    public bool? Tracing { get; init; }

    /// <summary>
    /// Whether the statement is idempotent. If null, defaults are used.
    /// </summary>
    public bool? Idempotent { get; init; }

    /// <summary>
    /// Page size for queries. If null, defaults are used.
    /// </summary>
    public int? PageSize { get; init; }

    /// <summary>
    /// Read timeout in milliseconds. If null, defaults are used.
    /// </summary>
    public int? TimeoutMs { get; init; }

    /// <summary>
    /// TTL in seconds for writes. Not yet applied in CQL, reserved for future use.
    /// </summary>
    public int? TtlSeconds { get; init; }

    /// <summary>
    /// Client-side timestamp for writes. Not yet applied in CQL, reserved for future use.
    /// </summary>
    public DateTimeOffset? UsingTimestamp { get; init; }

    /// <summary>
    /// Optional cancellation token (mainly for future extensibility).
    /// </summary>
    public CancellationToken Cancellation { get; init; }
}
