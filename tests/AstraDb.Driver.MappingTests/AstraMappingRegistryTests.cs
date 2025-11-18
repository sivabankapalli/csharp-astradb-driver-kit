using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Cassandra.Mapping;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Xunit;
using AstraDb.Driver.Mapping;

namespace AstraDb.Driver.MappingTests;

public class AstraMappingRegistryTests
{
    [Fact]
    public void AddMappings_ShouldInstantiateMappingsType()
    {
        // Arrange
        SampleMappings.Reset();
        var registry = new AstraMappingRegistry();

        // Act
        registry.AddMappings<SampleMappings>()
                .Build();

        // Assert
        Assert.True(SampleMappings.Constructed);
    }

    [Fact]
    public void AddMappingsFromAssembly_ShouldInstantiateAllConcreteMappings()
    {
        // Arrange
        SampleMappings.Reset();
        AnotherMappings.Reset();

        var registry = new AstraMappingRegistry();
        var assembly = typeof(SampleMappings).Assembly;

        // Act
        registry.AddMappingsFromAssembly(assembly)
                .Build();

        // Assert
        Assert.True(SampleMappings.Constructed);
        Assert.True(AnotherMappings.Constructed);
    }

    [Fact]
    public void AddConventionMaps_ShouldUseCustomTableAndColumnNameFunctions()
    {
        // Arrange
        var registry = new AstraMappingRegistry();
        var tableNameCalls = new List<string>();
        var columnNameCalls = new List<string>();

        // custom naming functions that also capture calls
        string TableNameFunc(string typeName)
        {
            tableNameCalls.Add(typeName);
            return typeName.ToLowerInvariant();
        }

        string ColumnNameFunc(string propName)
        {
            columnNameCalls.Add(propName);
            return propName.ToLowerInvariant();
        }

        // Act
        registry.AddConventionMaps(
                    new[] { typeof(ConventionEntity) },
                    keyspace: "dev_ks",
                    columnName: ColumnNameFunc,
                    tableName: TableNameFunc)
                .Build();

        // Assert
        // TableNameFunc should be called once for the type name
        Assert.Single(tableNameCalls);
        Assert.Equal(nameof(ConventionEntity), tableNameCalls[0]);

        // ColumnNameFunc should be called once per public read/write property
        Assert.Contains(nameof(ConventionEntity.Id), columnNameCalls);
        Assert.Contains(nameof(ConventionEntity.Name), columnNameCalls);
    }

    [Fact]
    public void AddConventionMaps_ShouldSkipTypesWithTableAttribute()
    {
        // Arrange
        var registry = new AstraMappingRegistry();
        var tableNameCalls = new List<string>();
        var columnNameCalls = new List<string>();

        string TableNameFunc(string typeName)
        {
            tableNameCalls.Add(typeName);
            return typeName.ToLowerInvariant();
        }

        string ColumnNameFunc(string propName)
        {
            columnNameCalls.Add(propName);
            return propName.ToLowerInvariant();
        }

        // One attributed type, one convention-based type
        var types = new[] { typeof(AttributedEntity), typeof(ConventionEntity) };

        // Act
        registry.AddConventionMaps(
                    types,
                    keyspace: "dev_ks",
                    columnName: ColumnNameFunc,
                    tableName: TableNameFunc)
                .Build();

        // Assert: table-name function should only be called for ConventionEntity
        Assert.Single(tableNameCalls);
        Assert.Equal(nameof(ConventionEntity), tableNameCalls[0]);

        // Both types have Id + Name, but only the non-attributed type should be processed.
        // So we expect exactly 2 column-name calls.
        Assert.Equal(2, columnNameCalls.Count);
        Assert.Contains(nameof(ConventionEntity.Id), columnNameCalls);
        Assert.Contains(nameof(ConventionEntity.Name), columnNameCalls);
    }


    [Theory]
    [InlineData("UserName", "user_name")]
    [InlineData("userName", "user_name")]
    [InlineData("URL", "u_r_l")]
    [InlineData("X", "x")]
    [InlineData("", "")]
    [InlineData(null, null)]
    public void DefaultSnakeCase_ShouldConvertProperly(string input, string expected)
    {
        // DefaultSnakeCase is private; call via reflection
        var method = typeof(AstraMappingRegistry)
            .GetMethod("DefaultSnakeCase", BindingFlags.NonPublic | BindingFlags.Static);

        Assert.NotNull(method);

        var result = (string)method!.Invoke(null, new object[] { input })!;

        Assert.Equal(expected, result);
    }

    [Fact]
    public void AddAstraMapping_ShouldRegisterMappingConfigurationAndMapperInDi()
    {
        // Arrange
        var services = new ServiceCollection();

        // We only want to check registrations; actual resolution of Mapper
        // will happen in integration tests when ISession is available.
        services.AddAstraMapping(reg =>
        {
            // No-op registry configuration is fine here
        });

        // Act
        var mappingDescriptor = services.SingleOrDefault(s => s.ServiceType == typeof(MappingConfiguration));
        var mapperDescriptor = services.SingleOrDefault(s => s.ServiceType == typeof(IMapper));

        // Assert
        Assert.NotNull(mappingDescriptor);
        Assert.Equal(ServiceLifetime.Singleton, mappingDescriptor!.Lifetime);
        Assert.NotNull(mappingDescriptor.ImplementationFactory);

        Assert.NotNull(mapperDescriptor);
        Assert.Equal(ServiceLifetime.Singleton, mapperDescriptor!.Lifetime);
        Assert.NotNull(mapperDescriptor.ImplementationFactory);
    }

    [Fact]
    public void BindOverrides_ShouldBindTypeOverridesFromConfiguration()
    {
        // Arrange
        var data = new Dictionary<string, string>
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
}

// ---------- Helper entities / mappings for tests ----------

public class ConventionEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
