namespace AstraDb.Driver.Mapping.Contracts;

/// <summary>
/// Type converter for converting between source type and target type (e.g., database type).
/// </summary>
/// <typeparam name="TSource"></typeparam>
/// <typeparam name="TTarget"></typeparam>
public interface ITypeConverter<TSource, TTarget>
{
    /// <summary>
    /// Transforms the source value to the target database value.
    /// </summary>
    /// <param name="source"></param>
    /// <returns></returns>
    TTarget? ToDb(TSource? source);

    /// <summary>
    /// Transforms the database value back to the source value.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    TSource? FromDb(TTarget? value);
}