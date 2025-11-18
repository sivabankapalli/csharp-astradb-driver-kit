using System.Collections.Generic;
using AstraDb.Driver.Mapping;
using Xunit;

namespace AstraDb.Driver.MappingTests;

public class TypeOverrideTests
{
    [Fact]
    public void CanCreate_WithAllProperties()
    {
        // Arrange
        var columns = new Dictionary<string, string>
        {
            ["Id"] = "id_col",
            ["Name"] = "name_col"
        };

        // Act
        var ov = new TypeOverride
        {
            ClrType = typeof(SampleEntity),
            Keyspace = "dev_ks",
            Table = "sample_table",
            Columns = columns
        };

        // Assert
        Assert.Equal(typeof(SampleEntity), ov.ClrType);
        Assert.Equal("dev_ks", ov.Keyspace);
        Assert.Equal("sample_table", ov.Table);
        Assert.Same(columns, ov.Columns); // should hold same reference
        Assert.Equal("id_col", ov.Columns!["Id"]);
        Assert.Equal("name_col", ov.Columns!["Name"]);
    }

    [Fact]
    public void OptionalProperties_CanBeNull()
    {
        // Act
        var ov = new TypeOverride
        {
            ClrType = typeof(SampleEntity),
            Keyspace = null,
            Table = null,
            Columns = null
        };

        // Assert
        Assert.Equal(typeof(SampleEntity), ov.ClrType);
        Assert.Null(ov.Keyspace);
        Assert.Null(ov.Table);
        Assert.Null(ov.Columns);
    }

    [Fact]
    public void ColumnsDictionary_IsMutableByCaller()
    {
        // Arrange
        var columns = new Dictionary<string, string>
        {
            ["Id"] = "id_col"
        };

        var ov = new TypeOverride
        {
            ClrType = typeof(SampleEntity),
            Columns = columns
        };

        // Act
        columns["Name"] = "name_col"; // mutate after assignment

        // Assert
        Assert.NotNull(ov.Columns);
        Assert.Equal("id_col", ov.Columns!["Id"]);
        Assert.Equal("name_col", ov.Columns!["Name"]);
    }
}

// Simple test entity
public class SampleEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
