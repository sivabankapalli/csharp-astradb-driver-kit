using AstraDb.Driver.Abstractions;
using AstraDb.Driver.Decorators;
using AstraDb.Driver.Implementations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace AstraDb.Driver.Extensions
{
    public static class AstraDbDriverExtensions
    {
        public static IServiceCollection AddAstraDbDriver(this IServiceCollection services)
        {
            services.AddScoped<AstraDbCqlClient>();
            services.AddScoped<IAstraDbClient>(sp =>
            {
                var inner = sp.GetRequiredService<AstraDbCqlClient>();
                var logger = sp.GetRequiredService<ILogger<LoggingAstraDbClientDecorator>>();
                return new LoggingAstraDbClientDecorator(inner, logger);
            });
            return services;
        }
    }
}
