using System;
using System.Collections.Generic;
using System.Net;
using AstraDb.Driver.Logging.Enrichers;
using Cassandra;
using Serilog.Events;
using Xunit;

namespace AstraDb.Driver.Logging.Tests
{
    public class AstraDbExceptionEnricherTests
    {
        private readonly AstraDbExceptionEnricher _enricher = new();
        private readonly MockLogEventPropertyFactory _factory = new();

        private LogEvent CreateLogEvent(Exception ex = null) => new(
                DateTimeOffset.Now,
                LogEventLevel.Error,
                ex,
                new MessageTemplate("Test", []),
                []);

        [Fact]
        public void Should_Not_Add_Anything_When_Exception_Is_Null()
        {
            var logEvent = CreateLogEvent();
            _enricher.Enrich(logEvent, _factory);

            Assert.Empty(logEvent.Properties);
        }

        [Fact]
        public void Should_Add_Type_And_Message_For_Generic_DriverException()
        {
            var ex = new DriverException("Generic driver failure");
            var logEvent = CreateLogEvent(ex);

            _enricher.Enrich(logEvent, _factory);

            Assert.Contains("AstraDbExceptionType", logEvent.Properties.Keys);
            Assert.Contains("AstraDbMessage", logEvent.Properties.Keys);

            var typeValue = logEvent.Properties["AstraDbExceptionType"].ToString();
            Assert.Contains("DriverException", typeValue);

            var messageValue = logEvent.Properties["AstraDbMessage"].ToString();
            Assert.Contains("Generic driver failure", messageValue);
        }

        [Fact]
        public void Should_Add_ReadTimeout_Details()
        {
            var ex = new ReadTimeoutException(ConsistencyLevel.LocalQuorum, 2, 3, false);
            var logEvent = CreateLogEvent(ex);

            _enricher.Enrich(logEvent, _factory);

            Assert.Contains("AstraDbConsistency", logEvent.Properties.Keys);
            Assert.Contains("AstraDbReceived", logEvent.Properties.Keys);
            Assert.Contains("AstraDbRequired", logEvent.Properties.Keys);

            var consistency = logEvent.Properties["AstraDbConsistency"].ToString();
            Assert.Contains("LocalQuorum", consistency);
        }

        [Fact]
        public void Should_Add_WriteTimeout_Details()
        {
            var ex = new WriteTimeoutException(
                ConsistencyLevel.EachQuorum,
                received: 1,
                required: 3,
                writeType:"BatchLog");
            var logEvent = CreateLogEvent(ex);

            _enricher.Enrich(logEvent, _factory);

            Assert.Contains("AstraDbConsistency", logEvent.Properties.Keys);
            Assert.Contains("AstraDbWriteType", logEvent.Properties.Keys);

            var consistency = logEvent.Properties["AstraDbConsistency"].ToString();
            Assert.Contains("EachQuorum", consistency);

            var writeType = logEvent.Properties["AstraDbWriteType"].ToString();
            Assert.Contains("BatchLog", writeType);
        }

        [Fact]
        public void Should_Add_TriedHosts_For_NoHostAvailableException()
        {
            var host = new IPEndPoint(IPAddress.Parse("10.1.2.3"), 9042);
            var ex = new NoHostAvailableException(new Dictionary<IPEndPoint, Exception>
            {
                { host, new Exception("unreachable") }
            });

            var logEvent = CreateLogEvent(ex);
            _enricher.Enrich(logEvent, _factory);

            Assert.Contains("AstraDbTriedHosts", logEvent.Properties.Keys);

            var triedHosts = logEvent.Properties["AstraDbTriedHosts"].ToString();
            Assert.Contains("10.1.2.3", triedHosts);
        }

        [Fact]
        public void Should_Add_Inner_Exception_Message()
        {
            var inner = new InvalidOperationException("Network layer failed");
            var ex = new DriverException("Wrapper error", inner);
            var logEvent = CreateLogEvent(ex);

            _enricher.Enrich(logEvent, _factory);

            Assert.Contains("AstraDbInnerMessage", logEvent.Properties.Keys);

            var innerValue = logEvent.Properties["AstraDbInnerMessage"].ToString();
            Assert.Contains("Network layer failed", innerValue);
        }

        [Fact]
        public void Should_Not_Add_For_Unrelated_Exception()
        {
            var ex = new InvalidOperationException("Business rule error");
            var logEvent = CreateLogEvent(ex);

            _enricher.Enrich(logEvent, _factory);

            Assert.Empty(logEvent.Properties);
        }

        [Fact]
        public void Should_Not_Overwrite_Existing_Properties()
        {
            var ex = new DriverException("New message");
            var logEvent = CreateLogEvent(ex);

            // Add existing property before enrichment
            logEvent.AddPropertyIfAbsent(new LogEventProperty("AstraDbMessage", new ScalarValue("Existing")));

            _enricher.Enrich(logEvent, _factory);

            var existingValue = logEvent.Properties["AstraDbMessage"].ToString();
            Assert.Contains("Existing", existingValue);
        }
    }
}
