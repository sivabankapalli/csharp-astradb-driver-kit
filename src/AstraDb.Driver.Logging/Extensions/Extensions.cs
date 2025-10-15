using AstraDb.Driver.Logging.Enrichers;
using Serilog;
using Serilog.Configuration;

namespace AstraDb.Driver.Logging.Extensions
{
    /// <summary>
    /// Extension for adding AstraDB exception context to Serilog enrichment pipeline.
    /// </summary>
    public static class Extensions
    {
        public static LoggerConfiguration WithAstraDbExceptionContext(
            this LoggerEnrichmentConfiguration enrich)
            => enrich.With(new AstraDbExceptionEnricher());
    }
}
