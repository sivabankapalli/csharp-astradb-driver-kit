using System;
using System.Linq;
using AstraDb.Driver.Logging.Enrichers;
using Cassandra;
using Serilog;
using Serilog.Sinks.InMemory;
using Xunit;

namespace AstraDb.Driver.Logging.Tests
{
    public class AstraDbExceptionEnricherTests
    {
        private readonly ILogger _logger;

        public AstraDbExceptionEnricherTests()
        {
            InMemorySink.Instance.Dispose();
            _logger = new LoggerConfiguration()
                .Enrich.With<AstraDbExceptionEnricher>()
                .WriteTo.InMemory()
                .CreateLogger();
        }

        [Fact]
        public void Should_Enrich_With_Generic_Properties_For_DriverException()
        {
            var driverException = new DriverInternalError("Mock internal driver error");
            _logger.Error(driverException, "Query failed with driver exception");

            var logEvent = InMemorySink.Instance.LogEvents.FirstOrDefault();
            Assert.NotNull(logEvent);
            Assert.NotNull(logEvent.Exception);

            Assert.True(logEvent.Properties.ContainsKey("AstraDbExceptionType"), "AstraDbExceptionType missing");
            Assert.Equal("DriverInternalError", logEvent.Properties["AstraDbExceptionType"].ToString().Trim('"'));
            Assert.True(logEvent.Properties.ContainsKey("AstraDbMessage"));
            Assert.Contains("Mock internal driver error", logEvent.Properties["AstraDbMessage"].ToString());
            Assert.True(logEvent.Properties.ContainsKey("AstraDbStackTrace"));
        }

        [Fact]
        public void Should_Ignore_Non_Driver_Exception()
        {
            var invalidOperationException = new InvalidOperationException("Invalid op");
            _logger.Error(invalidOperationException, "Generic failure");

            var logEvent = InMemorySink.Instance.LogEvents.FirstOrDefault();
            Assert.NotNull(logEvent);
            Assert.NotNull(logEvent.Exception);

            Assert.False(logEvent.Properties.ContainsKey("AstraDbExceptionType"));
            Assert.False(logEvent.Properties.ContainsKey("AstraDbMessage"));
        }

        [Fact]
        public void Should_Add_Inner_Exception_Message()
        {
            var driverException = new DriverInternalError("Wrapper error", new Exception("Inner issue"));
            _logger.Error(driverException, "Error occurred");

            var logEvent = InMemorySink.Instance.LogEvents.FirstOrDefault();
            Assert.NotNull(logEvent);
            Assert.NotNull(logEvent.Exception);

            Assert.True(logEvent.Properties.ContainsKey("AstraDbInnerMessage"));
            Assert.Contains("Inner issue", logEvent.Properties["AstraDbInnerMessage"].ToString());
        }
    }
}
