using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Internal;
using Cassandra;
using Serilog;

namespace AstraDb.Driver.Implementations;

/// <summary>
/// Production-ready AstraDB client that manages Cassandra connection lifecycle via DI.
/// Read operations are intentionally unimplemented until the Mapping/Data API layers are added.
/// </summary>
public sealed class AstraDbCqlClient : IAstraDbClient, IAsyncDisposable
{
    private readonly ISession _session;
    private readonly ICluster _cluster;

    private static readonly PreparedStatementCache _prepCache = new PreparedStatementCache();

    public AstraDbCqlClient(ICluster cluster, ISession session)
    {
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _session = session ?? throw new ArgumentNullException(nameof(session));
    }

    /// <summary>
    /// Placeholder for future read operations (implemented in Mapping/Data API packages).
    /// </summary>
    public Task<IEnumerable<TDocument>> ReadAsync<TDocument>(
        string keyspace, string table, IDictionary<string, object> filters) =>
        throw new NotImplementedException("Read operation is not yet implemented in AstraDb.Driver.");

    // ----------------- Public Write API (no options, no LWT) -----------------

    /// <summary>
    /// Plain INSERT (upsert semantics): inserts if missing, overwrites existing columns if present.
    /// </summary>
    public Task<WriteResult> WriteAsync(
        string keyspace,
        string table,
        IReadOnlyDictionary<string, object?> fields)
        => ExecuteWriteAsync(keyspace, table, fields);

    /// <summary>
    /// Plain INSERT (upsert semantics) for a POCO via a field-mapper delegate.
    /// </summary>
    public Task<WriteResult> WriteAsync<T>(
        string keyspace,
        string table,
        T document,
        Func<T, IReadOnlyDictionary<string, object?>> toFields)
    {
        if (document is null) throw new ArgumentNullException(nameof(document));
        if (toFields is null) throw new ArgumentNullException(nameof(toFields));

        var fields = toFields(document)
            ?? throw new ArgumentException("toFields returned null.", nameof(toFields));

        return ExecuteWriteAsync(keyspace, table, fields);
    }

    // ----------------- Core execution (shared) -----------------

    private async Task<WriteResult> ExecuteWriteAsync(
        string keyspace,
        string table,
        IReadOnlyDictionary<string, object?> fields)
    {
        if (string.IsNullOrWhiteSpace(keyspace)) throw new ArgumentException("keyspace required", nameof(keyspace));
        if (string.IsNullOrWhiteSpace(table)) throw new ArgumentException("table required", nameof(table));
        if (fields is null || fields.Count == 0)
            throw new ArgumentException("At least one column/value is required.", nameof(fields));

        var ks = KeyspaceResolver.Resolve(_session, keyspace);

        // Build CQL for a plain INSERT (no IF NOT EXISTS, no TTL, no TIMESTAMP)
        var cols = fields.Keys.ToList();
        var cql = CqlInsertBuilder.BuildInsert(
            ks, table, cols,
            ifNotExists: false,
            ttlSeconds: null,
            usingTimestamp: null);

        // Cache key mirrors the CQL signature
        var cacheKey = CqlInsertBuilder.BuildCacheKey(
            ks, table, cols,
            ifNotExists: false,
            ttlSeconds: null,
            usingTimestamp: null);

        // Prepare (cached)
        var ps = await _prepCache.GetOrPrepareAsync(_session, cql, cacheKey).ConfigureAwait(false);

        // Bind values (columns only). Use UNSET for nulls to avoid tombstones on optional columns.
        // NOTE: Primary key columns must NOT be UNSET; callers must provide them.
        var paramValues = new List<object>(cols.Count);
        foreach (var c in cols)
        {
            fields.TryGetValue(c, out var v);
            paramValues.Add(v ?? Unset.Value);
        }

        var bs = ps.Bind(paramValues.ToArray());

        // Sensible defaults (configured once here)
        bs.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        bs.SetIdempotence(true); // safe for plain INSERT upsert (no LWT/counters)

        try
        {
            await _session.ExecuteAsync(bs).ConfigureAwait(false);
            return new WriteResult(true);
        }
        catch (DriverException ex)
        {
            Log.Warning(ex, "Cassandra write failed (ks={Keyspace}, table={Table})", ks, table);
            return new WriteResult(false);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Unexpected error during WriteAsync (ks={Keyspace}, table={Table})", ks, table);
            return new WriteResult(false);
        }
    }

    /// <summary>
    /// Gracefully shuts down Cassandra connections when DI disposes this service.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        (_session as IDisposable)?.Dispose();
        await _cluster.ShutdownAsync().ConfigureAwait(false);
    }
}
