using Microsoft.Extensions.Logging;
using AstraDb.Driver.Mapping.Examples.Models;
using AstraDb.Driver.Abstractions;

namespace AstraDb.Driver.Mapping.Examples;

public sealed class SampleRunner(IAstraDbClient client, ILogger<SampleRunner> log, string mode)
{
    public async Task RunAsync()
    {
        log.LogInformation("Sample mode: {Mode}", mode);

        if (string.Equals(mode, "Raw", StringComparison.OrdinalIgnoreCase))
        {
            // RAW ONLY
            var fields = new Dictionary<string, object?>
            {
                ["userid"] = Guid.NewGuid(),
                ["email"] = "raw@demo.com",
                ["name"] = "Raw Demo"
            };
            var wr = await client.WriteAsync("dev_cdk_ks", "users", fields);
            log.LogInformation("Raw write success: {Success}", wr.Success);

            await client.ReadAsync<object>(
                "dev_cdk_ks",
                "users",
                new Dictionary<string, object> { { "email", "test@test.com" } });
        }
        else
        {
            var userID = Guid.NewGuid();

            // MAPPED (POCO)
            var user = new User { UserId = userID, Email = "mapped@demo.com", Name = "Mapped Demo" };
            var wr = await client.WriteAsync(user);
            log.LogInformation("POCO write success: {Success}", wr.Success);

            var users = await client.ReadAsync<User>(new Dictionary<string, object> { ["user_id"] = userID });
            foreach (var u in users)
                log.LogInformation("Fetched (POCO): {Id} {Email} {Name}", u.UserId, u.Email, u.Name);
        }
    }
}
