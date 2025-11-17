using Cassandra.Mapping.Attributes;

namespace AstraDb.Driver.Mapping.Examples.Models;

[Table(Name = "users", Keyspace = "dev_ks")]
public class User
{
    [PartitionKey] public Guid UserId { get; set; }
    [Column("email")] public string Email { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
}
