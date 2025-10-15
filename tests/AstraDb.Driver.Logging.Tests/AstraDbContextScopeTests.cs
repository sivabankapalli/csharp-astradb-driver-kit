using System.Linq;
using AstraDb.Driver.Logging.Scopes;
using Serilog;
using Serilog.Sinks.InMemory;
using Xunit;

namespace AstraDb.Driver.Logging.Tests
{
    public class AstraDbContextScopeTests
    {
        public AstraDbContextScopeTests()
        {
            InMemorySink.Instance.Dispose();
            Log.Logger = new LoggerConfiguration()
                .Enrich.FromLogContext()
                .WriteTo.InMemory()
                .CreateLogger();
        }

        [Fact]
        public void Should_Add_Keyspace_Table_Operation_To_Log()
        {
            using (AstraDbContextScope.Push("dev_ks", "users", "READ"))
            {
                Log.Information("Fetching data");
            }

            var logEvent = InMemorySink.Instance.LogEvents.FirstOrDefault();
            Assert.NotNull(logEvent);

            Assert.True(logEvent.Properties.ContainsKey("AstraDbKeyspace"));
            Assert.True(logEvent.Properties.ContainsKey("AstraDbTable"));
            Assert.True(logEvent.Properties.ContainsKey("AstraDbOperation"));

            Assert.Equal("dev_ks", logEvent.Properties["AstraDbKeyspace"].ToString().Trim('"'));
            Assert.Equal("users", logEvent.Properties["AstraDbTable"].ToString().Trim('"'));
            Assert.Equal("READ", logEvent.Properties["AstraDbOperation"].ToString().Trim('"'));
        }

        [Fact]
        public void Should_Handle_Empty_Scope_Safely()
        {
            using (AstraDbContextScope.Push())
            {
                Log.Information("Executing query without context");
            }

            var logEvent = InMemorySink.Instance.LogEvents.FirstOrDefault();
            Assert.NotNull(logEvent);

            Assert.False(logEvent.Properties.ContainsKey("AstraDbKeyspace"));
            Assert.False(logEvent.Properties.ContainsKey("AstraDbTable"));
            Assert.False(logEvent.Properties.ContainsKey("AstraDbOperation"));
        }

        [Fact]
        public void Should_Dispose_Without_Exception()
        {
            var contextScope = AstraDbContextScope.Push("ks", "tbl", "WRITE");
            contextScope.Dispose();
        }
    }
}
