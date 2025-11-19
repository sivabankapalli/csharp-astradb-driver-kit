using AstraDb.Driver.Options;
using Microsoft.Extensions.Configuration;
using Xunit;
using System.Collections.Generic;

namespace AstraDb.Driver.Tests;

public sealed class AstraDbRequestDefaultsTests
{
    [Fact]
    public void Should_Bind_Defaults_From_Configuration()
    {
        var settings = new Dictionary<string, string>
        {
            ["AstraDb:Defaults:Read:Consistency"] = "LocalOne",
            ["AstraDb:Defaults:Read:Tracing"] = "true",
            ["AstraDb:Defaults:Write:PageSize"] = "150"
        };

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(settings!)
            .Build();

        var defaults = new AstraDbRequestDefaults();
        config.GetSection("AstraDb:Defaults").Bind(defaults);

        Assert.Equal(Cassandra.ConsistencyLevel.LocalOne, defaults.Read.Consistency);
        Assert.True(defaults.Read.Tracing);
        Assert.Equal(150, defaults.Write.PageSize);
    }
}
