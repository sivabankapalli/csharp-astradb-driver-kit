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
        using (AstraDbContextScope.Push("dev_cdk_ks", "users", "READ"))
        {
            try
            {
                await client.ReadAsync<object>(
                    "dev_cdk_ks",
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
        //  WRITE OPERATION - Using Dictionary based write
        // -------------------------------
        using (AstraDbContextScope.Push("dev_cdk_ks", "users", "WRITE"))
        {
            try
            {
                var result = await client.WriteAsync(
                    "dev_cdk_ks",
                    "users",
                    new Dictionary<string, object?>
                    {
                        ["user_id"] = Guid.NewGuid(),
                        ["email"] = "x@y.com",
                        ["name"] = "Siva",
                        ["created_at"] = DateTimeOffset.UtcNow
                    });

                if (result.Success)
                    logger.LogInformation("Write succeeded.");
                else
                    logger.LogWarning("Write failed or not applied.");
            }
            catch (DriverException ex)
            {
                logger.LogError(ex, "AstraDB write operation failed.");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Unexpected error during AstraDB write.");
            }
        }

        // -------------------------------
        //  WRITE OPERATION - Using POCO + mapping delegate
        // -------------------------------
        var user = new
        {
            User_Id = Guid.NewGuid(),
            Email = "x@y.com",
            FirstName = "Siva",
            CreatedAt = DateTimeOffset.UtcNow
        };

        try
        {
            var result = await client.WriteAsync(
                keyspace: "dev_cdk_ks",
                table: "users",
                document: user,
                toFields: u => new Dictionary<string, object?>
                {
                    ["user_id"] = u.User_Id,
                    ["email"] = u.Email,
                    ["name"] = u.FirstName,
                    ["created_at"] = u.CreatedAt
                });

            if (result.Success)
                logger.LogInformation("Write succeeded.");
            else
                logger.LogWarning("Write failed or not applied.");
        }
        catch (DriverException ex)
        {
            logger.LogError(ex, "AstraDB write operation failed.");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Unexpected error during AstraDB write.");
        }

        logger.LogInformation("Demo complete. Connection and DI validated successfully.");
    }
}
