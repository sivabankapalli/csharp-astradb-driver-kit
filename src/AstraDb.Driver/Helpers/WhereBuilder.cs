namespace AstraDb.Driver.Helpers;

public static class WhereBuilder
{
    public static (string whereCql, List<string> orderKeys) BuildWhereFromDict(IDictionary<string, object> filters)
    {
        if (filters is null || filters.Count == 0)
            return (string.Empty, new List<string>());
        var parts = new List<string>();
        var order = new List<string>();
        foreach (var kv in filters)
        {
            if (kv.Value is null) throw new ArgumentException($"Filter '{kv.Key}' cannot be null");
            parts.Add($"{kv.Key} = ?");
            order.Add(kv.Key);
        }
        return ("WHERE " + string.Join(" AND ", parts), order);
    }
}
