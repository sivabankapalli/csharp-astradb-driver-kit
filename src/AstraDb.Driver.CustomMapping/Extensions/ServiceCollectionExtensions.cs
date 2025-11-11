using System;
using AstraDb.Driver.Mapping.Configuration;
using AstraDb.Driver.Mapping.Contracts;
using AstraDb.Driver.Mapping.Internals;
using Microsoft.Extensions.DependencyInjection;
using IMapper = AstraDb.Driver.Mapping.Contracts.IMapper;

namespace AstraDb.Driver.Mapping.Extensions;


public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddAstraDbMapping(this IServiceCollection services,
        Action<IMappingConfigurator>? configure = null)
    {
        services.AddSingleton<MappingMetadataCache>();
        services.AddSingleton<MappingRegistry>();
        services.AddSingleton<IMapper, AstraDbMapper>(sp =>
        {
            var reg = sp.GetRequiredService<MappingRegistry>();
            var cache = sp.GetRequiredService<MappingMetadataCache>();
            var cfg = new MappingConfigurator(reg);
            configure?.Invoke(cfg);

            // Register a couple of useful defaults (next step)
            return new AstraDbMapper(reg, cache);
        });
        return services;
    }
}
