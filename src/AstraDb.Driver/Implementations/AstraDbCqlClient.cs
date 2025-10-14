using AstraDb.Driver.Abstractions;

namespace AstraDb.Driver.Implementations
{
    public class AstraDbCqlClient : IAstraDbClient
    {
        public Task<IEnumerable<TDocument>> ReadAsync<TDocument>(string keyspace, string table, IDictionary<string, object> filters)
        {
            throw new NotImplementedException();
        }

        public Task WriteAsync<TDocument>(string keyspace, string table, TDocument document)
        {
            throw new NotImplementedException();
        }
    }
}
