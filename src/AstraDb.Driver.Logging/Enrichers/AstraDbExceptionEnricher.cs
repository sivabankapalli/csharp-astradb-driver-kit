using System;
using Cassandra;
using Serilog.Core;
using Serilog.Events;

namespace AstraDb.Driver.Logging.Enrichers
{
    /// <summary>
    /// Enriches Serilog events with AstraDB exception metadata.
    /// Only activates for exceptions from the DataStax Cassandra driver namespace.
    /// </summary>
    public sealed class AstraDbExceptionEnricher : ILogEventEnricher
    {
        public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
        {
            var exception = logEvent.Exception;
            if (exception is null)
                return;

            var exceptionNamespace = exception.GetType().Namespace ?? string.Empty;
            if (!exceptionNamespace.StartsWith("Cassandra", StringComparison.OrdinalIgnoreCase))
                return;

            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("AstraDbExceptionType", exception.GetType().Name));
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("AstraDbMessage", exception.Message));
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("AstraDbInnerMessage", exception.InnerException?.Message ?? string.Empty));
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("AstraDbStackTrace", exception.StackTrace ?? string.Empty));
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("AstraDbSource", exception.Source ?? "unknown"));
        }
    }
}
