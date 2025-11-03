using Cassandra;

namespace AstraDb.Driver.Abstractions;

public sealed record WriteResult(bool Success);

public interface IAstraDbClient : IAsyncDisposable
{
    Task<IEnumerable<TDocument>> ReadAsync<TDocument>(
        string keyspace,
        string table,
        IDictionary<string, object> filters);

    Task<WriteResult> WriteAsync(
        string keyspace,
        string table,
        IReadOnlyDictionary<string, object?> fields);

    Task<WriteResult> WriteAsync<T>(
        string keyspace,
        string table,
        T document,
        Func<T, IReadOnlyDictionary<string, object?>> toFields);
}