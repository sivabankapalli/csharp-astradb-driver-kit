using System.Collections.Concurrent;
using Cassandra;

namespace AstraDb.Driver.Internal;

internal sealed class PreparedStatementCache
{
    private readonly ConcurrentDictionary<string, PreparedStatement> _cache =
        new ConcurrentDictionary<string, PreparedStatement>(StringComparer.Ordinal);

    public async Task<PreparedStatement> GetOrPrepareAsync(ISession session, string cql, string key)
    {
        if (_cache.TryGetValue(key, out var ps)) return ps;
        var prepared = await session.PrepareAsync(cql).ConfigureAwait(false); // 3.22 API
        _cache[key] = prepared;
        return prepared;
    }
}