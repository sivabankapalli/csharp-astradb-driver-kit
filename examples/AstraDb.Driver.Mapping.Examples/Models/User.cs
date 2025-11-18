using Cassandra.Mapping.Attributes;

namespace AstraDb.Driver.Mapping.Examples.Models;

[Table(Name = "users", Keyspace = "dev_cdk_ks")]
public class User
{
    [PartitionKey]
    [Column("user_id")]
    public Guid UserId { get; set; }

    [Column("created_at")]
    public DateTimeOffset CreatedAt { get; set; }

    [Column("email")]
    public string Email { get; set; } = string.Empty;

    [Column("name")]
    public string Name { get; set; } = string.Empty;
}
