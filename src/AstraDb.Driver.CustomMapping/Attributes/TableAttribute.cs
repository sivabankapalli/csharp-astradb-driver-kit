using System;

namespace AstraDb.Driver.Mapping.Attributes;

/// <summary>
/// Marks a class as mapped to a Cassandra table.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class TableAttribute(string name) : Attribute
{
    /// <summary>
    /// Name of the table in the database.
    /// </summary>
    public string Name { get; } = name;
}
