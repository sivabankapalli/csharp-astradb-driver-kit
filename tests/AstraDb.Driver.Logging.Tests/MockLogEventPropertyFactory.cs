using Serilog.Core;
using Serilog.Events;

namespace AstraDb.Driver.Logging.Tests
{
    /// <summary>
    /// Simple test implementation of ILogEventPropertyFactory
    /// used for verifying enricher behavior.
    /// </summary>
    public class MockLogEventPropertyFactory : ILogEventPropertyFactory
    {
        public LogEventProperty CreateProperty(string name, object value, bool destructureObjects = false)
        {
            return new LogEventProperty(name, new ScalarValue(value));
        }
    }
}
