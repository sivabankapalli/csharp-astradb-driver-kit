using System.Text.Json;
using AstraDb.Driver.Mapping.Contracts;

namespace AstraDb.Driver.Mapping.Converters.Primitives;

/// <summary>
/// JSON string converter.
/// </summary>
/// <typeparam name="T"></typeparam>
public sealed class JsonStringConverter<T> : ITypeConverter<T, string>
{
    /// <summary>
    /// To database representation (JSON string).
    /// </summary>
    /// <param name="source"></param>
    /// <returns></returns>
    public string ToDb(T source) => JsonSerializer.Serialize(source);

    /// <summary>
    /// From database representation (JSON string).
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    public T FromDb(string value) => JsonSerializer.Deserialize<T>(value)!;
}
