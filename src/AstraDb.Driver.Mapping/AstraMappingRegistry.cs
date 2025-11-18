using System.Linq.Expressions;
using System.Reflection;
using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;

namespace AstraDb.Driver.Mapping;

public sealed class AstraMappingRegistry
{
    private readonly List<Func<MappingConfiguration, MappingConfiguration>> _steps = new();

    public AstraMappingRegistry AddMappings<TMappings>() where TMappings : Mappings, new()
    {
        _steps.Add(cfg => { cfg.Define(new TMappings()); return cfg; });
        return this;
    }

    /// <summary>
    /// Registers fluent mappings discovered in a given assembly.
    /// Supports types that implement Mappings.
    /// </summary>
    public AstraMappingRegistry AddMappingsFromAssembly(Assembly assembly)
    {
        _steps.Add(cfg =>
        {
            foreach (var type in assembly.GetTypes())
            {
                if (typeof(Mappings).IsAssignableFrom(type) && !type.IsAbstract)
                {
                    var mappingsInstance = (Mappings)Activator.CreateInstance(type)!;
                    cfg.Define(mappingsInstance);
                }
            }
            return cfg;
        });

        return this;
    }

    /// <summary>
    /// Automatically applies convention-based mapping for types WITHOUT [Table] attributes.
    /// For types with [Table], attributes take precedence and conventions are skipped.
    /// </summary>
    public AstraMappingRegistry AddConventionMaps(
        IEnumerable<Type> entityTypes,
        string? keyspace = null,
        Func<string, string>? columnName = null,
        Func<string, string>? tableName = null)
    {
        columnName ??= DefaultSnakeCase;
        tableName ??= DefaultSnakeCase;

        _steps.Add(cfg =>
        {
            foreach (var t in entityTypes)
            {
                // Skip types already using [Table] attribute
                var tableAttr = t.GetCustomAttribute<TableAttribute>();
                if (tableAttr != null)
                {
                    continue;
                }

                // Create Map<T>
                var mapType = typeof(Map<>).MakeGenericType(t);
                var map = Activator.CreateInstance(mapType)
                          ?? throw new InvalidOperationException($"Failed to create Map<{t.Name}>");

                // ----- Apply keyspace -----
                if (!string.IsNullOrWhiteSpace(keyspace))
                {
                    mapType.GetMethod("KeyspaceName")!
                           .Invoke(map, new object[] { keyspace });
                }

                // ----- Apply table -----
                mapType.GetMethod("TableName")!
                       .Invoke(map, [tableName(t.Name)]);

                // Get Column<TProp>() method definition
                var columnMethod = mapType
                    .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                    .Single(m =>
                        m.Name == "Column" &&
                        m.IsGenericMethodDefinition &&
                        m.GetParameters().Length == 2);

                // Build property mappings
                foreach (var p in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    if (!p.CanRead || !p.CanWrite)
                        continue;

                    var colName = columnName(p.Name);

                    // Build expression: x => (object)x.Property
                    var param = Expression.Parameter(t, "x");
                    var body = Expression.Property(param, p);
                    var cast = Expression.Convert(body, typeof(object));

                    var funcType = typeof(Func<,>).MakeGenericType(t, typeof(object));
                    var expr = Expression.Lambda(funcType, cast, param);

                    // Make Column<object>(expr, action)
                    var genericColumn = columnMethod.MakeGenericMethod(typeof(object));

                    genericColumn.Invoke(
                        map,
                        [
                                expr,
                                (Action<ColumnMap>)(cm => cm.WithName(colName))
                        ]);
                }

                // Register mapping
                cfg.Define((dynamic)map);
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