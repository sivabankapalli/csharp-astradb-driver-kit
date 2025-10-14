namespace AstraDb.Driver.Abstractions
{
    public interface IAstraDbClient
    {
        Task<IEnumerable<TDocument>> ReadAsync<TDocument>(string keyspace, string table, IDictionary<string, object> filters);
        Task WriteAsync<TDocument>(string keyspace, string table, TDocument document);
    }
}
