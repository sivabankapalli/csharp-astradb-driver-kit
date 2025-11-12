using System.Reflection;
using Cassandra.Mapping;

namespace AstraDb.Driver.Mapping;

public sealed class AstraMappingRegistry
{
    private readonly List<Func<MappingConfiguration, MappingConfiguration>> _steps = new();

    public AstraMappingRegistry AddMappings<TMappings>() where TMappings : Mappings, new()
    {
        _steps.Add(cfg => { cfg.Define(new TMappings()); return cfg; });
        return this;
    }

    public AstraMappingRegistry AddMappingsFromAssembly(Assembly asm)
    {
        var types = asm.GetTypes()
            .Where(t => !t.IsAbstract && typeof(Mappings).IsAssignableFrom(t));
        foreach (var t in types)
        {
            _steps.Add(cfg =>
            {
                var instance = (Mappings)Activator.CreateInstance(t)!;
                cfg.Define(instance);
                return cfg;
            });
        }
        return this;
    }

    public AstraMappingRegistry AddConventionMaps(IEnumerable<Type> entityTypes, string? keyspace = null, Func<string, string>? columnName = null, Func<string, string>? tableName = null)
    {
        columnName ??= DefaultSnakeCase;
        tableName ??= DefaultSnakeCase;

        _steps.Add(cfg =>
        {
            foreach (var t in entityTypes)
            {
                var mapType = typeof(Map<>).MakeGenericType(t);
                dynamic map = Activator.CreateInstance(mapType)!;
                if (!string.IsNullOrWhiteSpace(keyspace))
                    map = map.KeyspaceName(keyspace);
                map = map.TableName(tableName(t.Name));

                foreach (var p in t.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance))
                {
                    if (!p.CanRead || !p.CanWrite) continue;
                    var col = columnName(p.Name);
                    map = map.Column(p, (Func<ColumnMap, ColumnMap>)(cm => cm.WithName(col)));
                }
                cfg.Define(map);
            }
            return cfg;
        });
        return this;
    }

    public AstraMappingRegistry AddOverridesFrom(IEnumerable<TypeOverride> overrides)
    {
        _steps.Add(cfg =>
        {
            foreach (var ov in overrides)
            {
                var mapType = typeof(Map<>).MakeGenericType(ov.ClrType);
                dynamic map = Activator.CreateInstance(mapType)!;
                if (!string.IsNullOrWhiteSpace(ov.Keyspace))
                    map = map.KeyspaceName(ov.Keyspace);
                if (!string.IsNullOrWhiteSpace(ov.Table))
                    map = map.TableName(ov.Table);

                if (ov.Columns is not null)
                {
                    var props = ov.ClrType.GetProperties(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);
                    foreach (var kv in ov.Columns)
                    {
                        var p = props.FirstOrDefault(x => string.Equals(x.Name, kv.Key, StringComparison.Ordinal));
                        if (p == null) continue;
                        map = map.Column(p, (Func<ColumnMap, ColumnMap>)(cm => cm.WithName(kv.Value)));
                    }
                }
                cfg.Define(map);
            }
            return cfg;
        });
        return this;
    }

    public MappingConfiguration Build()
    {
        var cfg = new MappingConfiguration();
        foreach (var step in _steps) cfg = step(cfg);
        return cfg;
    }

    private static string DefaultSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name)) return name;
        var chars = new List<char>(name.Length * 2);
        for (int i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0) chars.Add('_');
                chars.Add(char.ToLowerInvariant(c));
            }
            else chars.Add(c);
        }
        return new string(chars.ToArray());
    }
}