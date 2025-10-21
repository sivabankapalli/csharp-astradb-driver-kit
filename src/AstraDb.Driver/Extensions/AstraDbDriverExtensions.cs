using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Config;
using AstraDb.Driver.Implementations;
using Cassandra;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace AstraDb.Driver.Extensions;

/// <summary>
/// Registers AstraDB driver components in the DI container using configuration from appsettings or environment variables.
/// </summary>
public static class AstraDbDriverExtensions
{
    public static IServiceCollection AddAstraDbDriver(
        this IServiceCollection services,
        IConfigurationSection configSection)
    {
        if (configSection == null)
            throw new ArgumentNullException(nameof(configSection), "Configuration section cannot be null.");

        var options = configSection.Get<AstraDbConnectionOptions>()
            ?? throw new InvalidOperationException("AstraDB configuration section is missing or invalid.");

        ValidateOptions(options);

        // Register options as singleton
        services.AddSingleton(options);

        // Register Cluster (singleton)
        services.AddSingleton<ICluster>(_ =>
            Cluster.Builder()
                .WithCloudSecureConnectionBundle(options.SecureConnectBundlePath)
                .WithCredentials("token", options.Token)
                .Build());

        // Register Session (singleton, per cluster)
        services.AddSingleton<ISession>(sp =>
        {
            var cluster = sp.GetRequiredService<ICluster>();
            return cluster.Connect(options.Keyspace);
        });

        // Register AstraDB client
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
