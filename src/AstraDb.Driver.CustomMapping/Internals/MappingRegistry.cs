using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using AstraDb.Driver.Mapping.Contracts;

namespace AstraDb.Driver.Mapping.Internals;

internal sealed class MappingRegistry
{
    private readonly ConcurrentDictionary<Type, string> _table = new();
    private readonly ConcurrentDictionary<(Type, string), string> _column = new();

    private readonly ConcurrentDictionary<Type, List<Conv>> _toDb = new();
    private readonly ConcurrentDictionary<Type, List<Conv>> _fromDb = new();

    public void MapTable(Type t, string name) => _table[t] = name;
    public void MapColumn(Type t, string prop, string col) => _column[(t, prop)] = col;

    public string ResolveTable(Type t, string @default) =>
        _table.TryGetValue(t, out var n) ? n : @default;

    public string ResolveColumn(Type t, string prop, string @default) =>
        _column.TryGetValue((t, prop), out var n) ? n : @default;

    public void AddConverter(Type src, Type dst, object converter)
    {
        var iface = converter.GetType().GetInterfaces()
          .FirstOrDefault(i => i.IsGenericType &&
              i.GetGenericTypeDefinition() == typeof(ITypeConverter<,>) &&
              i.GenericTypeArguments[0] == src &&
              i.GenericTypeArguments[1] == dst);
        if (iface is null)
            throw new ArgumentException($"Converter must implement ITypeConverter<{src.Name},{dst.Name}>");

        var entry = Conv.Create(converter, iface);
        _toDb.AddOrUpdate(src, _ => new() { entry }, (_, list) => { lock (list) list.Add(entry); return list; });
        _fromDb.AddOrUpdate(src, _ => new() { entry }, (_, list) => { lock (list) list.Add(entry); return list; });
    }

    public object? ConvertToDb(Type sourceType, object? value)
    {
        if (value is null) return null;
        if (_toDb.TryGetValue(sourceType, out var list))
            foreach (var c in list) { try { return c.ToDb(value); } catch { } }
        return value;
    }

    public object? ConvertFromDb(Type targetType, object? db)
    {
        if (db is null) return null;
        if (targetType.IsInstanceOfType(db)) return db;

        if (_fromDb.TryGetValue(targetType, out var list))
        {
            var dbType = db.GetType();
            foreach (var c in list)
            {
                if (c.TargetType.IsAssignableFrom(dbType))
                    try { return c.FromDb(db); } catch { }
            }
            foreach (var c in list) { try { return c.FromDb(db); } catch { } }
        }

        try { return Convert.ChangeType(db, targetType); }
        catch (Exception ex) { throw new MappingException($"ConvertFromDb failure to {targetType.Name}", ex); }
    }

    private sealed class Conv
    {
        private readonly MethodInfo _to, _from; public readonly Type TargetType; private readonly object _inst;
        private Conv(object inst, Type src, Type tgt, MethodInfo to, MethodInfo from) { _inst = inst; TargetType = tgt; _to = to; _from = from; }
        public static Conv Create(object inst, Type iface)
        {
            var args = iface.GetGenericArguments();
            return new Conv(inst, args[0], args[1], iface.GetMethod("ToDb")!, iface.GetMethod("FromDb")!);
        }
        public object? ToDb(object? v) => _to.Invoke(_inst, new[] { v });
        public object? FromDb(object? v) => _from.Invoke(_inst, new[] { v });
    }
}

internal sealed class MappingException : Exception
{
    public MappingException(string message, Exception? inner = null) : base(message, inner) { }
}
