using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using AstraDb.Driver.Mapping.Attributes;

namespace AstraDb.Driver.Mapping.Internals;

internal sealed class MappingMetadataCache
{
    private readonly ConcurrentDictionary<Type, MappingMetadata> _cache = new();

    public MappingMetadata GetOrAdd(Type type) => _cache.GetOrAdd(type, Build);

    private static MappingMetadata Build(Type t)
    {
        var table = t.GetCustomAttribute<TableAttribute>()?.Name ?? ToSnake(t.Name);
        var props = t.GetProperties(BindingFlags.Instance | BindingFlags.Public)
                     .Where(p => p.CanRead && p.CanWrite).ToArray();

        var cols = new List<PropertyBinding>(props.Length);
        foreach (var p in props)
        {
            var name = p.GetCustomAttribute<ColumnAttribute>()?.Name ?? ToSnake(p.Name);

            // (object x) => (object?)((T)x).Prop
            var x = Expression.Parameter(typeof(object), "x");
            var cast = Expression.Convert(x, t);
            var prop = Expression.Property(cast, p);
            var box = Expression.Convert(prop, typeof(object));
            var getter = Expression.Lambda<Func<object, object?>>(box, x).Compile();

            // (object x, object? v) => ((T)x).Prop = (PropType)v
            var v = Expression.Parameter(typeof(object), "v");
            var castV = Expression.Convert(v, p.PropertyType);
            var assign = Expression.Assign(prop, castV);
            var setter = Expression.Lambda<Action<object, object?>>(assign, x, v).Compile();

            cols.Add(new PropertyBinding(p, name, getter, setter));
        }

        // Deterministic order (alphabetical column)
        cols.Sort((a, b) => StringComparer.Ordinal.Compare(a.Column, b.Column));

        return new MappingMetadata(table, cols);
    }

    private static string ToSnake(string s)
    {
        if (string.IsNullOrEmpty(s)) return s;
        var list = new List<char>(s.Length + 8);
        for (int i = 0; i < s.Length; i++)
        {
            var c = s[i];
            if (char.IsUpper(c))
            {
                if (i > 0) list.Add('_');
                list.Add(char.ToLowerInvariant(c));
            }
            else list.Add(c);
        }
        return new string(list.ToArray());
    }
}

internal sealed record MappingMetadata(string TableName, IReadOnlyList<PropertyBinding> Columns);
internal sealed record PropertyBinding(PropertyInfo Property, string Column,
    Func<object, object?> Getter, Action<object, object?> Setter);
