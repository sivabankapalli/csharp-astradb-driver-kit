using System;
using AstraDb.Driver.Mapping.Contracts;

namespace AstraDb.Driver.Mapping.Converters.Primitives;

/// <summary>
/// For converting DateTimeOffset to/from Unix epoch milliseconds.
/// </summary>
public sealed class DateTimeOffsetEpochConverter : ITypeConverter<DateTimeOffset, long>
{
    /// <summary>
    /// To database representation (Unix epoch milliseconds).
    /// </summary>
    /// <param name="source"></param>
    /// <returns></returns>
    public long ToDb(DateTimeOffset source) => source.ToUnixTimeMilliseconds();

    /// <summary>
    /// From database representation (Unix epoch milliseconds).
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public DateTimeOffset FromDb(long value) => DateTimeOffset.FromUnixTimeMilliseconds(value);
}
