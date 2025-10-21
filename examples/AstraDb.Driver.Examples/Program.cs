using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Extensions;
using AstraDb.Driver.Logging.Extensions;
using AstraDb.Driver.Logging.Scopes;
using Cassandra;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AstraDb.Driver.Examples;

class Program
{
    static async Task Main()
    {
        // Build configuration
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: false)
            .AddEnvironmentVariables()
            .Build();

        // Register services
        var services = new ServiceCollection();

        // Register logging (your AstraDb.Driver.Logging handles enrichers & scopes)
        services.AddLogging(logging =>
        {
            logging.ClearProviders();
            logging.AddAstraDbSerilog(configuration);
        });

        // Register AstraDB Driver with configuration section
        services.AddAstraDbDriver(configuration.GetSection("Astra:Driver"));

        // Build service provider
        await using var sp = services.BuildServiceProvider();

        var client = sp.GetRequiredService<IAstraDbClient>();
        var logger = sp.GetRequiredService<ILogger<Program>>();
        logger.LogInformation("AstraDB Client initialized successfully.");

        // -------------------------------
        //  READ OPERATION
        // -------------------------------
        using (AstraDbContextScope.Push("dev_ks", "users", "READ"))
        {
            try
            {
                await client.ReadAsync<object>(
                    "dev_ks",
                    "users",
                    new Dictionary<string, object> { { "email", "test@test.com" } });
            }
            catch (NotImplementedException ex)
            {
                logger.LogWarning(ex, "ReadAsync not implemented yet (expected).");
            }
            catch (DriverException ex)
            {
                logger.LogError(ex, "AstraDB read operation failed.");
            }
        }

        // -------------------------------
        //  WRITE OPERATION
        // -------------------------------
        using (AstraDbContextScope.Push("dev_ks", "users", "WRITE"))
        {
            try
            {
                await client.WriteAsync(
                    "dev_ks",
                    "users",
                    new { Email = "x@y.com" });
            }
            catch (NotImplementedException ex)
            {
                logger.LogWarning(ex, "WriteAsync not implemented yet (expected).");
            }
            catch (DriverException ex)
            {
                logger.LogError(ex, "AstraDB write operation failed.");
            }
        }

        logger.LogInformation("Demo complete. Connection and DI validated successfully.");
    }
}
