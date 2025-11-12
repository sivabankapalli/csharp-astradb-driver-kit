using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

var builder = Host.CreateDefaultBuilder(args)
    .ConfigureAppConfiguration((ctx, cfg) =>
    {
        cfg.SetBasePath(Directory.GetCurrentDirectory());
        cfg.AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
        cfg.AddEnvironmentVariables();
    })
    .UseSerilog((ctx, conf) => conf.ReadFrom.Configuration(ctx.Configuration))
    .ConfigureServices((ctx, services) =>
    {
        var mode = ctx.Configuration["Mode"] ?? "Mapped";

        if (string.Equals(mode, "Raw", StringComparison.OrdinalIgnoreCase))
        {
            // RAW-ONLY: no mapper registration
            services.AddAstraDbDriver(ctx.Configuration.GetSection("AstraDb"));
        }
        else
        {
            // MAPPED: driver + mappings in one call
            services.AddAstraDbDriver(ctx.Configuration.GetSection("AstraDb"), reg =>
            {
                reg.AddMappingsFromAssembly(typeof(DomainMappings).Assembly);
                reg.AddConventionMaps(new[] { typeof(User) }, keyspace: "dev_ks");
            });
        }

        services.AddLogging();
        services.AddScoped<SampleRunner>(sp =>
            new SampleRunner(
                sp.GetRequiredService<IAstraDbClient>(),
                sp.GetRequiredService<ILogger<SampleRunner>>(),
                mode));
    });

var app = builder.Build();

using (var scope = app.Services.CreateScope())
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

await app.StopAsync();
