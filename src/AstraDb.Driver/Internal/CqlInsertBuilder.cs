namespace AstraDb.Driver.Internal;

internal static class CqlInsertBuilder
{
    public static string BuildInsert(
        string keyspace, string table, IReadOnlyList<string> cols,
        bool ifNotExists, int? ttlSeconds, DateTimeOffset? usingTimestamp)
    {
        // 3.22: use positional bind markers (?,?,...) for broadest compatibility
        var colList = string.Join(", ", cols.Select(QuoteId));
        var valList = string.Join(", ", Enumerable.Repeat("?", cols.Count));

        var usingParts = new List<string>();
        if (usingTimestamp.HasValue) usingParts.Add("TIMESTAMP ?");
        if (ttlSeconds is > 0) usingParts.Add("TTL ?");

        var usingClause = usingParts.Count > 0
            ? " USING " + string.Join(" AND ", usingParts)
            : string.Empty;

        var lwt = ifNotExists ? " IF NOT EXISTS" : string.Empty;

        return $"INSERT INTO {QuoteId(keyspace)}.{QuoteId(table)} ({colList}) VALUES ({valList}){usingClause}{lwt}";
    }

    public static string BuildCacheKey(
        string keyspace, string table, IReadOnlyList<string> cols,
        bool ifNotExists, int? ttlSeconds, DateTimeOffset? usingTimestamp)
        => string.Join("|",
        [
                keyspace, table,
                string.Join(",", cols),
                ifNotExists ? "IFNE" : "NOIF",
                usingTimestamp.HasValue ? "TS" : "NOTS",
                (ttlSeconds is > 0) ? "TTL" : "NOTTL"
        ]);

    public static string QuoteId(string id)
        => id.All(ch => char.IsLetterOrDigit(ch) || ch == '_')
            ? id
            : $"\"{id.Replace("\"", "\"\"")}\"";
}
