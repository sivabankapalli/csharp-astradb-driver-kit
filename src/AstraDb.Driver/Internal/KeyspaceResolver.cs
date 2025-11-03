using Cassandra;

namespace AstraDb.Driver.Internal;

internal static class KeyspaceResolver
{
    public static string Resolve(ISession session, string? keyspace)
    {
        var ks = string.IsNullOrWhiteSpace(keyspace) ? session.Keyspace : keyspace;
        if (string.IsNullOrWhiteSpace(ks))
            throw new ArgumentException("Keyspace is required (explicitly or via ISession.Keyspace).", nameof(keyspace));
        return ks!;
    }
}