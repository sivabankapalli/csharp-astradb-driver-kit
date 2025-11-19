using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Config;
using AstraDb.Driver.Implementations;
using AstraDb.Driver.Mapping;
using AstraDb.Driver.Options;
using Cassandra;
using Cassandra.Mapping;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace AstraDb.Driver.Extensions;

/// <summary>
/// Registers AstraDB driver components in the DI container using configuration from appsettings or environment variables.
/// </summary>
public static class AstraDbDriverExtensions
{
    public static IServiceCollection AddAstraDbDriver(
            this IServiceCollection services,
            IConfigurationSection configSection,
            Action<AstraMappingRegistry>? configureMappings = null)
    {
        if (configSection == null)
            throw new ArgumentNullException(nameof(configSection), "Configuration section cannot be null.");

        var options = configSection.Get<AstraDbConnectionOptions>()
            ?? throw new InvalidOperationException("AstraDB configuration section is missing or invalid.");

        ValidateOptions(options);

        // Options
        services.AddSingleton(options);
        var defaults = configSection.Get<AstraDbConnectionOptions>()
            ?? throw new InvalidOperationException("AstraDB connection options section is missing or invalid.");

        services.AddSingleton(defaults);

        // Cluster (singleton)
        services.AddSingleton<ICluster>(_ =>
            Cluster.Builder()
                .WithCloudSecureConnectionBundle(options.SecureConnectBundlePath)
                .WithCredentials("token", options.Token)
                .Build());

        // Session (singleton)
        services.AddSingleton<ISession>(sp =>
        {
            var cluster = sp.GetRequiredService<ICluster>();
            return cluster.Connect(options.Keyspace);
        });

        // Optional Mapper
        if (configureMappings is not null)
        {
            services.AddSingleton<MappingConfiguration>(sp =>
            {
                var reg = new AstraMappingRegistry();
                configureMappings(reg);
                return reg.Build();
            });

            services.AddSingleton<IMapper>(sp =>
                new Mapper(sp.GetRequiredService<ISession>(), sp.GetRequiredService<MappingConfiguration>()));
        }
        else
        {
            // If not configured, try to use MappingConfiguration.Global only if someone asks for IMapper
            services.TryAddSingleton<IMapper>(sp =>
                new Mapper(sp.GetRequiredService<ISession>(), MappingConfiguration.Global));
        }

        // Client
        services.AddSingleton<IAstraDbClient, AstraDbCqlClient>();

        return services;
    }


    private static void ValidateOptions(AstraDbConnectionOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.SecureConnectBundlePath))
            throw new ArgumentException("Secure Connect Bundle path must be provided.", nameof(options.SecureConnectBundlePath));

        if (string.IsNullOrWhiteSpace(options.Token))
            throw new ArgumentException("Token must be provided.", nameof(options.Token));

        if (string.IsNullOrWhiteSpace(options.Keyspace))
            throw new ArgumentException("Keyspace must be provided.", nameof(options.Keyspace));
    }
}
