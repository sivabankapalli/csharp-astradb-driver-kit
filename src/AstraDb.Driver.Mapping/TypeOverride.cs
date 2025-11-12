namespace AstraDb.Driver.Mapping;

public sealed class TypeOverride
{
    public required Type ClrType { get; init; }
    public string? Keyspace { get; init; }
    public string? Table { get; init; }
    public IDictionary<string, string>? Columns { get; init; }
}
