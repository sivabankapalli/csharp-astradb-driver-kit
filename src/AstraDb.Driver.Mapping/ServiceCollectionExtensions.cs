using System;
using System.Collections.Generic;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Cassandra.Mapping;

namespace AstraDb.Driver.Mapping;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddAstraMapping(this IServiceCollection services, Action<AstraMappingRegistry> configureRegistry)
    {
        services.AddSingleton(provider =>
        {
            var reg = new AstraMappingRegistry();
            configureRegistry(reg);
            return reg.Build();
        });

        services.AddSingleton<IMapper>(sp =>
        {
            var session = sp.GetRequiredService<Cassandra.ISession>();
            var cfg = sp.GetRequiredService<MappingConfiguration>();
            return new Mapper(session, cfg);
        });

        return services;
    }

    public static IEnumerable<TypeOverride> BindOverrides(IConfiguration cfg, string sectionPath = "AstraMapping:Overrides")
    {
        var list = new List<TypeOverride>();
        var section = cfg.GetSection(sectionPath);
        foreach (var item in section.GetChildren())
        {
            var typeName = item.GetValue<string>("ClrType");
            if (string.IsNullOrWhiteSpace(typeName)) continue;
            var clr = Type.GetType(typeName, throwOnError: true);
            var columns = new Dictionary<string, string>(StringComparer.Ordinal);
            var colSec = item.GetSection("Columns");
            foreach (var c in colSec.GetChildren())
                columns[c.Key] = c.Value ?? c.Key;

            list.Add(new TypeOverride
            {
                ClrType = clr!,
                Keyspace = item.GetValue<string>("Keyspace"),
                Table = item.GetValue<string>("Table"),
                Columns = columns.Count == 0 ? null : columns
            });
        }
        return list;
    }
}
