using Cassandra;

namespace AstraDb.Driver.Options;

/// <summary>
/// Environment-level defaults for AstraDB read/write operations.
/// Typically bound from configuration (appsettings.json, environment variables).
/// </summary>
public sealed class AstraDbRequestDefaults
{
    public RequestProfile Read { get; set; } = new();
    public RequestProfile Write { get; set; } = new();

    public sealed class RequestProfile
    {
        public ConsistencyLevel? Consistency { get; set; }
        public bool? Idempotent { get; set; }
        public bool? Tracing { get; set; }
        public int? PageSize { get; set; }
        public int? TimeoutMs { get; set; }
    }
}
