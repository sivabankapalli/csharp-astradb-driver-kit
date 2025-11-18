using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Extensions;
using AstraDb.Driver.Mapping.Examples;
using AstraDb.Driver.Mapping.Examples.Mappings;
using AstraDb.Driver.Mapping.Examples.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using AstraDb.Driver.Logging.Extensions;
using Microsoft.Extensions.Logging;

// using AstraDb.Driver;                     // where IAstraDbClient lives
// using AstraDb.Driver.Logging;             // for AddAstraDbSerilog if you use it
// using AstraDb.Driver.DependencyInjection; // where AddAstraDbDriver extension lives
// using YourNamespace.For.Mapping;          // DomainMappings, User
// using YourNamespace.For.Samples;          // SampleRunner

public class Program
{
    public static async Task Main(string[] args)
    {
        // -------------------------------
        //  CONFIGURATION
        // -------------------------------
        var configuration = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();

        // -------------------------------
        //  SERVICE REGISTRATION
        // -------------------------------
        var services = new ServiceCollection();

        // Logging – either use your AstraDb-specific Serilog helper or plain console
        services.AddLogging(logging =>
        {
            logging.ClearProviders();
            logging.AddAstraDbSerilog(configuration);
        });

        // Determine mode (Raw vs Mapped)
        var mode = configuration["Mode"] ?? "Mapped";

        if (string.Equals(mode, "Raw", StringComparison.OrdinalIgnoreCase))
        {
            // RAW-ONLY: no mapper registration
            services.AddAstraDbDriver(configuration.GetSection("AstraDb"));
        }
        else
        {
            // MAPPED: driver + mappings in one call
            services.AddAstraDbDriver(configuration.GetSection("AstraDb"), reg =>
            {
                // Domain mappings assembly
                reg.AddMappingsFromAssembly(typeof(DomainMappings).Assembly);

                // Optional convention maps
                reg.AddConventionMaps(new[] { typeof(User) }, keyspace: "dev_cdk_ks");
            });
        }

        // Register SampleRunner
        services.AddScoped<SampleRunner>(sp =>
            new SampleRunner(
                sp.GetRequiredService<IAstraDbClient>(),
                sp.GetRequiredService<ILogger<SampleRunner>>(),
                mode));

        // -------------------------------
        //  BUILD PROVIDER & RUN
        // -------------------------------
        await using var serviceProvider = services.BuildServiceProvider();

        // Root logger (for Program-level errors)
        var rootLogger = serviceProvider.GetRequiredService<ILogger<Program>>();

        using (var scope = serviceProvider.CreateScope())
        {
            var runner = scope.ServiceProvider.GetRequiredService<SampleRunner>();

            try
            {
                await runner.RunAsync();
            }
            catch (Exception ex)
            {
                var log = scope.ServiceProvider.GetRequiredService<ILogger<Program>>();
                log.LogError(ex, "Sample failed");
            }
        }

        rootLogger.LogInformation("Sample completed. Shutting down.");
    }
}
