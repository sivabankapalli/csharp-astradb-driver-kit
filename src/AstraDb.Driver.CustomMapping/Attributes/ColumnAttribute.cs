using System;

namespace AstraDb.Driver.Mapping.Attributes;

/// <summary>
/// Overrides the column name for a property.
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false)]
public sealed class ColumnAttribute(string name) : Attribute
{
    /// <summary>
    /// Name of the column in the database.
    /// </summary>
    public string Name { get; } = name!;
}