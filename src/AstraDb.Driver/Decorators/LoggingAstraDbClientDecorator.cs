using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Helpers;
using Microsoft.Extensions.Logging;

namespace AstraDb.Driver.Decorators
{
    public class LoggingAstraDbClientDecorator : IAstraDbClient
    {
        private readonly IAstraDbClient _inner;
        private readonly ILogger<LoggingAstraDbClientDecorator> _logger;

        public LoggingAstraDbClientDecorator(IAstraDbClient inner, ILogger<LoggingAstraDbClientDecorator> logger)
        {
            _inner = inner;
            _logger = logger;
        }

        public async Task<IEnumerable<TDocument>> ReadAsync<TDocument>(string keyspace, string table, IDictionary<string, object> filters)
        {
            _logger.LogInformation("Reading from {Keyspace}.{Table} with filters: {Keys}",
                keyspace, table, FilterFormatter.SummarizeFilterKeys(filters));
            return await _inner.ReadAsync<TDocument>(keyspace, table, filters);
        }

        public async Task WriteAsync<TDocument>(string keyspace, string table, TDocument document)
        {
            _logger.LogInformation("Writing to {Keyspace}.{Table} DocumentType={Type}",
                keyspace, table, typeof(TDocument).Name);
            await _inner.WriteAsync(keyspace, table, document);
        }
    }
}
