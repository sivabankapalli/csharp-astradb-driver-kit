using Cassandra.Mapping.Attributes;

namespace AstraDb.Driver.MappingTests;

[Table(Name = "attrib_users", Keyspace = "attrib_ks")]
public class AttributedEntity
{
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}
