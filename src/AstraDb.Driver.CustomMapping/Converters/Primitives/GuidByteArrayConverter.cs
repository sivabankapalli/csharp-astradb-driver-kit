using System;
using AstraDb.Driver.Mapping.Contracts;

namespace AstraDb.Driver.Mapping.Converters.Primitives;

/// <summary>
/// Guid to byte array converter.
/// </summary>
public sealed class GuidByteArrayConverter : ITypeConverter<Guid, byte[]>
{
    /// <summary>
    /// To database representation (byte array).
    /// </summary>
    /// <param name="source"></param>
    /// <returns></returns>
    public byte[] ToDb(Guid source) => source.ToByteArray();

    /// <summary>
    /// From database representation (byte array).
    /// </summary>
    public Guid FromDb(byte[] value) => new(value ?? throw new ArgumentNullException(nameof(value)));
}