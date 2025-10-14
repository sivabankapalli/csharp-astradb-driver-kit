using Cassandra;
using Serilog.Core;
using Serilog.Events;

namespace AstraDb.Driver.Logging.Enrichers
{
    /// <summary>
    /// Enriches Serilog log events with diagnostic information from DataStax Driver exceptions.
    /// </summary>
    public class AstraDbExceptionEnricher : ILogEventEnricher
    {
        public void Enrich(LogEvent logEvent, ILogEventPropertyFactory propertyFactory)
        {
            if (logEvent?.Exception is not DriverException driverEx)
                return;

            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty(
                "AstraDbExceptionType", driverEx.GetType().Name));
            logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty(
                "AstraDbMessage", driverEx.Message));

            // Host list for NoHostAvailableException
            if (driverEx is NoHostAvailableException nhex && nhex.Errors?.Any() == true)
            {
                var hosts = nhex.Errors.Keys.Select(h => h.Address.ToString());
                logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty(
                    "AstraDbTriedHosts", string.Join(",", hosts)));
            }

            // Timeout diagnostics
            if (driverEx is ReadTimeoutException rtex)
            {
                logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("AstraDbConsistency", rtex.ConsistencyLevel.ToString()));
                logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("AstraDbReceived", rtex.ReceivedAcknowledgements));
                logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("AstraDbRequired", rtex.RequiredAcknowledgements));
            }

            if (driverEx is WriteTimeoutException wtex)
            {
                logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("AstraDbConsistency", wtex.ConsistencyLevel.ToString()));
                logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty("AstraDbWriteType", wtex.WriteType.ToString()));
            }

            // Inner exception for transport / protocol details
            if (driverEx.InnerException != null)
                logEvent.AddPropertyIfAbsent(propertyFactory.CreateProperty(
                    "AstraDbInnerMessage", driverEx.InnerException.Message));
        }
    }
}
