using AstraDb.Driver.Models;

namespace AstraDb.Driver.Abstractions;

public interface IAstraDbClient : IAsyncDisposable
{
    Task<IEnumerable<TDocument>> ReadAsync<TDocument>(
        string keyspace,
        string table,
        IDictionary<string, object> filters);

    Task<IEnumerable<T>> ReadAsync<T>(
        IDictionary<string, object> filters = null!,
        ExecOptions? options = null,
        CancellationToken ct = default);

    Task<WriteResult> WriteAsync<T>(
        T document,
        CancellationToken ct = default);

    Task<WriteResult> WriteAsync(
        string keyspace,
        string table,
        IReadOnlyDictionary<string, object?> fields,
        ExecOptions? options = null);

    Task<WriteResult> WriteAsync<T>(
        string keyspace,
        string table,
        T document,
        Func<T, IReadOnlyDictionary<string, object?>> toFields,
        ExecOptions? options = null);
}