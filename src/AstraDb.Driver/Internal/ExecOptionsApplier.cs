using AstraDb.Driver.Models;
using AstraDb.Driver.Options;
using Cassandra;
using Cassandra.Mapping;

namespace AstraDb.Driver.Internal;

/// <summary>
/// Central mapping between CDK execution options and the Cassandra C# driver.
/// </summary>
internal static class ExecOptionsApplier
{
    // Hardcoded safety nets if both config + per-call options are absent.
    private static readonly ExecOptions HardcodedWriteDefaults = new()
    {
        Consistency = ConsistencyLevel.LocalQuorum,
        Idempotent = true,
        Tracing = false
    };

    private static readonly ExecOptions HardcodedReadDefaults = new()
    {
        Consistency = ConsistencyLevel.LocalQuorum,
        Idempotent = true,
        Tracing = false
    };

    public static ExecOptions EffectiveWrite(
        ExecOptions? perCall,
        AstraDbRequestDefaults defaults)
        => Merge(perCall, defaults?.Write, HardcodedWriteDefaults);

    public static ExecOptions EffectiveRead(
        ExecOptions? perCall,
        AstraDbRequestDefaults defaults)
        => Merge(perCall, defaults?.Read, HardcodedReadDefaults);

    private static ExecOptions Merge(
        ExecOptions? perCall,
        AstraDbRequestDefaults.RequestProfile? fromConfig,
        ExecOptions hardDefaults)
    {
        return new ExecOptions
        {
            Consistency = perCall?.Consistency
                          ?? fromConfig?.Consistency
                          ?? hardDefaults.Consistency,

            Idempotent = perCall?.Idempotent
                         ?? fromConfig?.Idempotent
                         ?? hardDefaults.Idempotent,

            Tracing = perCall?.Tracing
                      ?? fromConfig?.Tracing
                      ?? hardDefaults.Tracing,

            PageSize = perCall?.PageSize
                       ?? fromConfig?.PageSize
                       ?? hardDefaults.PageSize,

            TimeoutMs = perCall?.TimeoutMs
                        ?? fromConfig?.TimeoutMs
                        ?? hardDefaults.TimeoutMs,

            // reserved for future write features
            TtlSeconds = perCall?.TtlSeconds,
            UsingTimestamp = perCall?.UsingTimestamp,
            Cancellation = perCall?.Cancellation ?? default
        };
    }

    public static void ApplyToStatement(IStatement stmt, ExecOptions options)
    {
        if (options is null) return;

        if (options.Consistency.HasValue)
            stmt.SetConsistencyLevel(options.Consistency.Value);

        if (options.Idempotent.HasValue)
            stmt.SetIdempotence(options.Idempotent.Value);

        if (options.PageSize.HasValue)
            stmt.SetPageSize(options.PageSize.Value);

        if (options.TimeoutMs.HasValue)
            stmt.SetReadTimeoutMillis(options.TimeoutMs.Value);

        if (options.Tracing.HasValue)
        {
            if (options.Tracing.Value)
                stmt.EnableTracing();
            else
                stmt.DisableTracing();
        }
    }


    public static void ApplyToQueryOptions(CqlQueryOptions queryOptions, ExecOptions options)
    {
        if (options is null) return;

        if (options.Consistency.HasValue)
            queryOptions.SetConsistencyLevel(options.Consistency.Value);

        if (options.PageSize.HasValue)
            queryOptions.SetPageSize(options.PageSize.Value);
    }
}
