using AstraDb.Driver.Abstractions;
using Cassandra;

namespace AstraDb.Driver.Implementations;

/// <summary>
/// Production-ready AstraDB client that manages Cassandra connection lifecycle via DI.
/// Read/Write operations are intentionally unimplemented until the Mapping or Data API layers are added.
/// </summary>
public sealed class AstraDbCqlClient : IAstraDbClient, IAsyncDisposable
{
    private readonly ISession _session;
    private readonly ICluster _cluster;

    public AstraDbCqlClient(ICluster cluster, ISession session)
    {
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _session = session ?? throw new ArgumentNullException(nameof(session));
    }

    /// <summary>
    /// Placeholder for future read operations (implemented in Mapping package).
    /// </summary>
    public Task<IEnumerable<TDocument>> ReadAsync<TDocument>(
        string keyspace, string table, IDictionary<string, object> filters)
    {
        throw new NotImplementedException("Read operation is not yet implemented in AstraDb.Driver.");
    }

    /// <summary>
    /// Placeholder for future write operations (implemented in Mapping package).
    /// </summary>
    public Task WriteAsync<TDocument>(
        string keyspace, string table, TDocument document)
    {
        throw new NotImplementedException("Write operation is not yet implemented in AstraDb.Driver.");
    }

    /// <summary>
    /// Gracefully shuts down Cassandra connections when the DI container disposes this service.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        await _cluster.ShutdownAsync();
    }
}
