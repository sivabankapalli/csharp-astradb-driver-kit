using System;
using System.Collections.Generic;
using System.Linq;
using AstraDb.Driver.Mapping.Contracts;
using Cassandra;

namespace AstraDb.Driver.Mapping.Internals;

public sealed class AstraDbMapper : IMapper
{
    private readonly MappingRegistry reg;
    private readonly MappingMetadataCache cache;

    internal AstraDbMapper(MappingRegistry reg, MappingMetadataCache cache)
    {
        this.reg = reg;
        this.cache = cache;
    }

    public IDictionary<string, object?> MapToFields<T>(T entity)
    {
        if (entity is null) throw new ArgumentNullException(nameof(entity));
        var meta = cache.GetOrAdd(typeof(T));
        var dict = new SortedDictionary<string, object?>(StringComparer.Ordinal);

        foreach (var c in meta.Columns)
        {
            var colName = reg.ResolveColumn(typeof(T), c.Property.Name, c.Column);
            var raw = c.Getter(entity!);
            dict[colName] = raw is null ? null : reg.ConvertToDb(c.Property.PropertyType, raw);
        }

        return dict;
    }

    // ------------------------------------------------------
    // 👇 Public interface member (kept for compatibility)
    // ------------------------------------------------------
    public T MapFromRow<T>(Row row)
    {
        if (row == null) throw new ArgumentNullException(nameof(row));

        // Try to access RowSet metadata via the internal reference
        // Unfortunately, Row doesn’t expose RowSet directly,
        // so this fallback uses the exception-safe pattern.
        return MapFromRowInternal<T>(row, null);
    }

    // ------------------------------------------------------
    // 👇 Internal overload with optional RowSet
    // ------------------------------------------------------
    internal T MapFromRowInternal<T>(Row row, RowSet? rowSet)
    {
        var meta = cache.GetOrAdd(typeof(T));
        var inst = Activator.CreateInstance<T>()!;

        // If RowSet is available, use its Columns for presence check
        HashSet<string>? available = null;
        if (rowSet != null)
        {
            available = new HashSet<string>(
                rowSet.Columns.Select(c => c.Name),
                StringComparer.OrdinalIgnoreCase);
        }

        foreach (var c in meta.Columns)
        {
            var colName = reg.ResolveColumn(typeof(T), c.Property.Name, c.Column);

            if (available != null && !available.Contains(colName))
                continue;

            try
            {
                if (row.IsNull(colName))
                {
                    c.Setter(inst, null);
                    continue;
                }

                var dbVal = row.GetValue<object>(colName);
                var val = reg.ConvertFromDb(c.Property.PropertyType, dbVal);
                c.Setter(inst, val);
            }
            catch (ArgumentException)
            {
                // Column not present in this RowSet projection
                continue;
            }
        }

        return inst;
    }
}
