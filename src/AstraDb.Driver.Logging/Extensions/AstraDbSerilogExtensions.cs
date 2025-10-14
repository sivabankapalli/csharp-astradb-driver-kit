using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Serilog;

namespace AstraDb.Driver.Logging.Extensions
{
    /// <summary>
    /// Adds Serilog configured for AstraDB diagnostics.
    /// </summary>
    public static class AstraDbSerilogExtensions
    {
        public static void AddAstraDbSerilog(
            this ILoggingBuilder loggingBuilder,
            IConfiguration configuration)
        {
            var logger = new LoggerConfiguration()
                .ReadFrom.Configuration(configuration)
                .Enrich.WithAstraDbExceptionContext()
                .CreateLogger();

            Log.Logger = logger;
            loggingBuilder.AddSerilog(logger, dispose: true);
        }
    }
}
