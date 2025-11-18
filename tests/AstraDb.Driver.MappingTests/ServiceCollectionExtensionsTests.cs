using System;
using System.Collections.Generic;
using System.Linq;
using AstraDb.Driver.Mapping;
using Cassandra.Mapping;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace AstraDb.Driver.MappingTests;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddAstraMapping_ShouldRegister_MappingConfiguration_And_IMapper_AsSingletons()
    {
        // Arrange
        var services = new ServiceCollection();

        // Act
        services.AddAstraMapping(reg => { /* no-op */ });

        // We DO NOT build the provider or resolve Mapper here.
        // We only inspect the service descriptors.
        var mappingDescriptor = services.SingleOrDefault(
            s => s.ServiceType == typeof(MappingConfiguration));

        var mapperDescriptor = services.SingleOrDefault(
            s => s.ServiceType == typeof(IMapper));

        // Assert
        Assert.NotNull(mappingDescriptor);
        Assert.Equal(ServiceLifetime.Singleton, mappingDescriptor!.Lifetime);
        Assert.NotNull(mappingDescriptor.ImplementationFactory);

        Assert.NotNull(mapperDescriptor);
        Assert.Equal(ServiceLifetime.Singleton, mapperDescriptor!.Lifetime);
        Assert.NotNull(mapperDescriptor.ImplementationFactory);
    }

    [Fact]
    public void AddAstraMapping_ShouldInvoke_ConfigureRegistry_WhenMappingConfigurationFactoryRuns()
    {
        // Arrange
        var services = new ServiceCollection();
        var configureCalled = false;

        services.AddAstraMapping(reg =>
        {
            configureCalled = true;
            // you could add some registry steps here, but the flag is enough
        });

        // Grab the MappingConfiguration descriptor
        var mappingDescriptor = services.Single(
            s => s.ServiceType == typeof(MappingConfiguration));

        Assert.NotNull(mappingDescriptor.ImplementationFactory);

        // Act
        // Call the factory manually. It does not use IServiceProvider,
        // so passing null is safe.
        var cfg = (MappingConfiguration)mappingDescriptor.ImplementationFactory!(null!)!;

        // Assert
        Assert.NotNull(cfg);
        Assert.True(configureCalled, "configureRegistry should have been invoked.");
    }

    // ---------- BindOverrides tests ----------

    [Fact]
    public void BindOverrides_ShouldBind_Overrides_From_DefaultSection()
    {
        // Arrange
        var data = new Dictionary<string, string?>
        {
            // First override with keyspace, table and columns
            ["AstraMapping:Overrides:0:ClrType"] = typeof(OverrideEntity).AssemblyQualifiedName,
            ["AstraMapping:Overrides:0:Keyspace"] = "dev_ks",
            ["AstraMapping:Overrides:0:Table"] = "override_table",
            ["AstraMapping:Overrides:0:Columns:Id"] = "id_col",
            ["AstraMapping:Overrides:0:Columns:Name"] = "name_col",

            // Second override with only type
            ["AstraMapping:Overrides:1:ClrType"] = typeof(OverrideEntity2).AssemblyQualifiedName,
        };

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(data!)
            .Build();

        // Act
        var overrides = ServiceCollectionExtensions.BindOverrides(config).ToList();

        // Assert
        Assert.Equal(2, overrides.Count);

        var first = overrides[0];
        Assert.Equal(typeof(OverrideEntity), first.ClrType);
        Assert.Equal("dev_ks", first.Keyspace);
        Assert.Equal("override_table", first.Table);
        Assert.NotNull(first.Columns);
        Assert.Equal("id_col", first.Columns!["Id"]);
        Assert.Equal("name_col", first.Columns!["Name"]);

        var second = overrides[1];
        Assert.Equal(typeof(OverrideEntity2), second.ClrType);
        Assert.Null(second.Keyspace);
        Assert.Null(second.Table);
        Assert.Null(second.Columns);
    }

    [Fact]
    public void BindOverrides_ShouldSkip_Items_WithMissingClrType()
    {
        // Arrange
        var data = new Dictionary<string, string?>
        {
            // This item should be skipped (no ClrType)
            ["AstraMapping:Overrides:0:Keyspace"] = "dev_ks",
            ["AstraMapping:Overrides:0:Table"] = "ignored_table",

            // Proper item
            ["AstraMapping:Overrides:1:ClrType"] = typeof(OverrideEntity).AssemblyQualifiedName,
            ["AstraMapping:Overrides:1:Keyspace"] = "ks2",
            ["AstraMapping:Overrides:1:Table"] = "tbl2"
        };

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(data!)
            .Build();

        // Act
        var overrides = ServiceCollectionExtensions.BindOverrides(config).ToList();

        // Assert
        Assert.Single(overrides);
        Assert.Equal(typeof(OverrideEntity), overrides[0].ClrType);
        Assert.Equal("ks2", overrides[0].Keyspace);
        Assert.Equal("tbl2", overrides[0].Table);
    }

    [Fact]
    public void BindOverrides_ShouldUse_CustomSectionPath_WhenProvided()
    {
        // Arrange
        var data = new Dictionary<string, string?>
        {
            ["Custom:Overrides:0:ClrType"] = typeof(OverrideEntity).AssemblyQualifiedName,
            ["Custom:Overrides:0:Keyspace"] = "custom_ks",
            ["Custom:Overrides:0:Table"] = "custom_table"
        };

        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(data!)
            .Build();

        // Act
        var overrides = ServiceCollectionExtensions.BindOverrides(config, "Custom:Overrides").ToList();

        // Assert
        Assert.Single(overrides);
        var ov = overrides[0];
        Assert.Equal(typeof(OverrideEntity), ov.ClrType);
        Assert.Equal("custom_ks", ov.Keyspace);
        Assert.Equal("custom_table", ov.Table);
    }
}
