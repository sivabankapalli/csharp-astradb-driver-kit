using Cassandra;
using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Extensions;
using AstraDb.Driver.Logging.Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

class Program
{
    static async Task Main()
    {
        var configuration = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json", optional: true)
            .Build();

        var services = new ServiceCollection();

        // Add AstraDB Serilog integration
        services.AddLogging(logging =>
        {
            logging.ClearProviders();
            logging.AddAstraDbSerilog(configuration);
        });

        services.AddAstraDbDriver();
        var sp = services.BuildServiceProvider();

        var client = sp.GetRequiredService<IAstraDbClient>();
        var logger = sp.GetRequiredService<ILogger<Program>>();
        logger.LogInformation("AstraDB Client initialized.");

        try
        {
            await client.ReadAsync<object>(
                "dev_ks", 
                "users",
                new Dictionary<string, object> { { "email", "test@test.com" } });
        }
        catch (NotImplementedException ex)
        {
            logger.LogWarning(ex, "ReadAsync not implemented yet.");
        }
        catch (DriverException ex)
        {
            logger.LogError(ex, "AstraDB read operation failed.");
        }

        try
        {
            await client.WriteAsync("dev_ks", "users", new { Email = "x@y.com" });
        }
        catch (NotImplementedException ex)
        {
            logger.LogWarning(ex, "WriteAsync not implemented yet.");
        }
        catch (DriverException ex)
        {
            logger.LogError(ex, "AstraDB write operation failed.");
        }
    }
}
