namespace AstraDb.Driver.Helpers
{
    public static class FilterFormatter
    {
        public static string SummarizeFilterKeys(IDictionary<string, object> filters)
        {
            return (filters == null || filters.Count == 0)
                ? "None"
                : string.Join(", ", filters.Keys);
        }

        public static string BuildWhereClause(IDictionary<string, object> filters)
        {
            if (filters == null || filters.Count == 0) return string.Empty;
            return "WHERE " + string.Join(" AND ", filters.Select(k => k.Key + " = ?"));
        }
    }
}
