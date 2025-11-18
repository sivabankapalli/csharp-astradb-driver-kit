using System;
using System.Collections.Generic;
using System.Linq;
using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Config;
using AstraDb.Driver.Extensions;
using Cassandra;
using Cassandra.Mapping;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AstraDb.Driver.Tests;

public class AstraDbDriverExtensionsTests
{
    [Fact]
    public void AddAstraDbDriver_Should_Throw_When_ConfigSection_Null() // NEW TEST
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        Action act = () => AstraDbDriverExtensions.AddAstraDbDriver(services, null!);

        // Assert
        Assert.Throws<ArgumentNullException>(act);
    }

    [Fact]
    public void AddAstraDbDriver_Should_Throw_When_Options_Invalid() // NEW TEST
    {
        // Arrange
        var data = new Dictionary<string, string?>
        {
            // Missing SecureConnectBundlePath
            ["Astra:Driver:Token"] = "AstraCS:xxx",
            ["Astra:Driver:Keyspace"] = "demo"
        };

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(data!)
            .Build();

        var section = config.GetSection("Astra:Driver");
        var services = new ServiceCollection();

        // Act
        Action act = () => services.AddAstraDbDriver(section);

        // Assert
        Assert.Throws<ArgumentException>(act);
    }

    [Fact]
    public void AddAstraDbDriver_WithMappings_Should_Register_MappingConfiguration_And_IMapper()
    {
        // Arrange
        var data = new Dictionary<string, string?>
        {
            ["Astra:Driver:SecureConnectBundlePath"] = "secure-connect.zip",
            ["Astra:Driver:Token"] = "AstraCS:xxx",
            ["Astra:Driver:Keyspace"] = "demo"
        };

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(data!)
            .Build();

        var section = config.GetSection("Astra:Driver");
        var services = new ServiceCollection();
        var configureCalled = false;

        // Act
        services.AddAstraDbDriver(section, reg =>
        {
            configureCalled = true;
        });

        // We just inspect descriptors; we don't want to actually build the real cluster.
        var optionsDescriptor = services.SingleOrDefault(s => s.ServiceType == typeof(AstraDbConnectionOptions));
        var clusterDescriptor = services.SingleOrDefault(s => s.ServiceType == typeof(ICluster));
        var sessionDescriptor = services.SingleOrDefault(s => s.ServiceType == typeof(ISession));
        var clientDescriptor = services.SingleOrDefault(s => s.ServiceType == typeof(IAstraDbClient));
        var mappingDescriptor = services.SingleOrDefault(s => s.ServiceType == typeof(MappingConfiguration));
        var mapperDescriptor = services.SingleOrDefault(s => s.ServiceType == typeof(IMapper));

        // Assert basic registrations
        Assert.NotNull(optionsDescriptor);
        Assert.NotNull(clusterDescriptor);
        Assert.NotNull(sessionDescriptor);
        Assert.NotNull(clientDescriptor);
        Assert.NotNull(mappingDescriptor);
        Assert.NotNull(mapperDescriptor);

        // ✅ NOW trigger the MappingConfiguration factory so configureMappings runs
        Assert.NotNull(mappingDescriptor!.ImplementationFactory);

        // Mapping factory doesn’t use the service provider, so null is safe here.
        var cfg = (MappingConfiguration)mappingDescriptor.ImplementationFactory!(null!)!;

        Assert.NotNull(cfg);
        Assert.True(configureCalled);
    }

    [Fact]
    public void AddAstraDbDriver_WithoutMappings_Should_Use_Global_MappingConfiguration() // NEW TEST
    {
        // Arrange
        var data = new Dictionary<string, string?>
        {
            ["Astra:Driver:SecureConnectBundlePath"] = "secure-connect.zip",
            ["Astra:Driver:Token"] = "AstraCS:xxx",
            ["Astra:Driver:Keyspace"] = "demo"
        };

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(data!)
            .Build();

        var section = config.GetSection("Astra:Driver");
        var services = new ServiceCollection();

        // Act
        services.AddAstraDbDriver(section); // configureMappings = null

        var mappingDescriptor = services.SingleOrDefault(s => s.ServiceType == typeof(MappingConfiguration));
        var mapperDescriptor = services.SingleOrDefault(s => s.ServiceType == typeof(IMapper));

        // Assert
        // No explicit MappingConfiguration registered in this branch
        Assert.Null(mappingDescriptor);
        Assert.NotNull(mapperDescriptor);
    }

}
