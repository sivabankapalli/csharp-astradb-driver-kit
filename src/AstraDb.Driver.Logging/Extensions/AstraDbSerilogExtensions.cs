using AstraDb.Driver.Logging.Enrichers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Serilog;

namespace AstraDb.Driver.Logging.Extensions
{
    /// <summary>
    /// Extension for adding AstraDB Serilog configuration to .NET logging.
    /// </summary>
    public static class AstraDbSerilogExtensions
    {
        /// <summary>
        /// Adds and configures Serilog with AstraDB enrichment using the given configuration.
        /// </summary>
        public static void AddAstraDbSerilog(
            this ILoggingBuilder loggingBuilder,
            IConfiguration configuration)
        {
            var logger = new LoggerConfiguration()
                .ReadFrom.Configuration(configuration)
                //.Enrich.FromLogContext()
                .Enrich.With<AstraDbExceptionEnricher>()
                .CreateLogger();

            Log.Logger = logger;
            loggingBuilder.AddSerilog(logger, dispose: true);
        }
    }
}
