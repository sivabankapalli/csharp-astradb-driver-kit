using System;
using System.Collections.Generic;
using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Config;
using AstraDb.Driver.Extensions;
using AstraDb.Driver.Implementations;
using Cassandra;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using Xunit;

namespace AstraDb.Driver.Tests;

public class AstraDbCqlClientTests
{
    [Fact]
    public void AddAstraDbDriver_Should_Throw_When_Invalid_Config()
    {
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();
        Assert.Throws<InvalidOperationException>(() =>
            services.AddAstraDbDriver(config.GetSection("Astra:Driver")));
    }

    [Fact]
    public void AddAstraDbDriver_Should_Register_Dependencies()
    {
        var inMemory = new Dictionary<string, string>
        {
            ["Astra:Driver:SecureConnectBundlePath"] = "secure-connect.zip",
            ["Astra:Driver:Token"] = "AstraCS:xxxxxx",
            ["Astra:Driver:Keyspace"] = "demo"
        };

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(inMemory)
            .Build();

        var services = new ServiceCollection();
        services.AddAstraDbDriver(config.GetSection("Astra:Driver"));

        var provider = services.BuildServiceProvider();

        Assert.NotNull(provider.GetRequiredService<AstraDbConnectionOptions>());
        Assert.NotNull(provider.GetRequiredService<ICluster>());
        Assert.NotNull(provider.GetRequiredService<ISession>());
        Assert.NotNull(provider.GetRequiredService<IAstraDbClient>());
    }

    [Fact]
    public async System.Threading.Tasks.Task AstraDbCqlClient_Should_Throw_NotImplemented()
    {
        var mockCluster = new Mock<ICluster>();
        var mockSession = new Mock<ISession>();

        var client = new AstraDbCqlClient(mockCluster.Object, mockSession.Object);

        await Assert.ThrowsAsync<NotImplementedException>(() =>
            client.ReadAsync<object>("ks", "tbl", new Dictionary<string, object> { { "id", 1 } }));
    }
}
