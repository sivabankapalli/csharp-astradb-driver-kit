using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Helpers;
using AstraDb.Driver.Internal;
using AstraDb.Driver.Models;
using AstraDb.Driver.Options;
using Cassandra;
using Cassandra.Mapping;
using Microsoft.Extensions.Options;
using Serilog;

namespace AstraDb.Driver.Implementations;

/// <summary>
/// Production-ready AstraDB client that manages Cassandra connection lifecycle via DI.
/// Read operations are intentionally unimplemented until the Mapping/Data API layers are added.
/// </summary>
public sealed class AstraDbCqlClient : IAstraDbClient, IAsyncDisposable
{
    /// <summary>
    /// The active Cassandra session used for executing queries.
    /// </summary>
    private readonly ISession _session;

    /// <summary>
    /// The Cassandra cluster instance used for managing connections.
    /// </summary>
    private readonly ICluster _cluster;

    /// <summary>
    /// Cassandra Mapper for POCO mapping (optional, may be null).
    /// </summary>
    private readonly IMapper _mapper;

    /// <summary>
    /// AstraDB request defaults from configuration.
    /// </summary>
    private readonly AstraDbRequestDefaults _defaults;

    /// <summary>
    /// Shared cache for prepared statements to optimize query execution.
    /// </summary>
    private static readonly PreparedStatementCache _prepCache = new PreparedStatementCache();

    /// <summary>
    /// Initializes a new instance of the <see cref="AstraDbCqlClient"/> class with the specified cluster and session.
    /// </summary>
    /// <param name="cluster">The Cassandra cluster instance.</param>
    /// <param name="session">The Cassandra session instance.</param>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="cluster"/> or <paramref name="session"/> is null.</exception>
    public AstraDbCqlClient(
        ICluster cluster, 
        ISession session,
        IMapper mapper,
        IOptions<AstraDbRequestDefaults> defaults)
    {
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _session = session ?? throw new ArgumentNullException(nameof(session));
        _mapper = mapper ?? throw new ArgumentNullException(nameof(mapper));
        _defaults = defaults?.Value ?? new AstraDbRequestDefaults();
    }

    /// <summary>
    /// Placeholder for future read operations (implemented in Mapping/Data API packages).
    /// </summary>
    /// <typeparam name="TDocument">The document type to read.</typeparam>
    /// <param name="keyspace">The keyspace to query.</param>
    /// <param name="table">The table to query.</param>
    /// <param name="filters">The filter criteria for the query.</param>
    /// <returns>Throws <see cref="NotImplementedException"/> as read is not yet supported.</returns>
    public Task<IEnumerable<TDocument>> ReadAsync<TDocument>(
        string keyspace, string table, IDictionary<string, object> filters) =>
        throw new NotImplementedException("Read operation is not yet implemented in AstraDb.Driver.");

    // ----------------- Public Write API (no options, no LWT) -----------------

    /// <summary>
    /// Executes a plain INSERT (upsert semantics): inserts if missing, overwrites existing columns if present.
    /// </summary>
    /// <param name="keyspace">The keyspace to write to.</param>
    /// <param name="table">The table to write to.</param>
    /// <param name="fields">The column-value pairs to insert or update.</param>
    /// <returns>A <see cref="WriteResult"/> indicating success or failure.</returns>
    public Task<WriteResult> WriteAsync(
        string keyspace,
        string table,
        IReadOnlyDictionary<string, object?> fields,
        ExecOptions? options = null)
        => ExecuteWriteAsync(keyspace, table, fields, options);

    /// <summary>
    /// Executes a plain INSERT (upsert semantics) for a POCO via a field-mapper delegate.
    /// </summary>
    /// <typeparam name="T">The type of the document to write.</typeparam>
    /// <param name="keyspace">The keyspace to write to.</param>
    /// <param name="table">The table to write to.</param>
    /// <param name="document">The document instance to map and write.</param>
    /// <param name="toFields">Delegate to map the document to column-value pairs.</param>
    /// <returns>A <see cref="WriteResult"/> indicating success or failure.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="document"/> or <paramref name="toFields"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown if <paramref name="toFields"/> returns null.</exception>
    public Task<WriteResult> WriteAsync<T>(
        string keyspace,
        string table,
        T document,
        Func<T, IReadOnlyDictionary<string, object?>> toFields,
        ExecOptions? options = null)
    {
        if (document is null) throw new ArgumentNullException(nameof(document));
        if (toFields is null) throw new ArgumentNullException(nameof(toFields));

        var fields = toFields(document)
            ?? throw new ArgumentException("toFields returned null.", nameof(toFields));

        return ExecuteWriteAsync(keyspace, table, fields, options);
    }

    // ----------------- Core execution (shared) -----------------

    /// <summary>
    /// Executes the core logic for a plain INSERT operation, including CQL generation, statement preparation, and execution.
    /// </summary>
    /// <param name="keyspace">The keyspace to write to.</param>
    /// <param name="table">The table to write to.</param>
    /// <param name="fields">The column-value pairs to insert or update.</param>
    /// <returns>A <see cref="WriteResult"/> indicating success or failure.</returns>
    /// <exception cref="ArgumentException">Thrown if keyspace, table, or fields are invalid.</exception>
    private async Task<WriteResult> ExecuteWriteAsync(
        string keyspace,
        string table,
        IReadOnlyDictionary<string, object?> fields,
        ExecOptions? options)
    {
        if (string.IsNullOrWhiteSpace(keyspace)) throw new ArgumentException("keyspace required", nameof(keyspace));
        if (string.IsNullOrWhiteSpace(table)) throw new ArgumentException("table required", nameof(table));
        if (fields is null || fields.Count == 0)
            throw new ArgumentException("At least one column/value is required.", nameof(fields));

        var execOptions = ExecOptionsApplier.EffectiveWrite(options, _defaults);

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
        ExecOptionsApplier.ApplyToStatement(bs, execOptions);

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
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous dispose operation.</returns>
    public async ValueTask DisposeAsync()
    {
        (_session as IDisposable)?.Dispose();
        await _cluster.ShutdownAsync().ConfigureAwait(false);
    }

    public async Task<IEnumerable<T>> ReadAsync<T>(
        IDictionary<string, object> filters = null,
         ExecOptions? options = null,
         CancellationToken ct = default)
    {
        try
        {
            if (filters is null || filters.Count == 0)
                return await _mapper.FetchAsync<T>();

            var (whereCql, keys) = WhereBuilder.BuildWhereFromDict(filters);
            var args = keys.Select(k => filters[k]).ToArray();
            var cql = new Cql(whereCql, args);
            cql.WithOptions(options =>
            {
                options.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            });

            return await _mapper.FetchAsync<T>(cql);
        }
        catch (DriverException ex)
        {
            Log.Warning(ex, "Cassandra read failed for {Type}", typeof(T).Name);
            return Array.Empty<T>();
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Unexpected error during ReadAsync<{Type}>", typeof(T).Name);
            return Array.Empty<T>();
        }
    }

    public async Task<WriteResult> WriteAsync<T>(T document, CancellationToken ct = default)
    {
        try
        {
            await _mapper.InsertAsync(document);
            return new WriteResult(true);
        }
        catch (DriverException ex)
        {
            Log.Warning(ex, "Insert failed for {Type}", typeof(T).Name);
            return new WriteResult(false);
        }
        catch (Exception ex)
        {
            Log.Error(ex, "Unexpected error during WriteAsync<{Type}>", typeof(T).Name);
            return new WriteResult(false);
        }
    }

}
