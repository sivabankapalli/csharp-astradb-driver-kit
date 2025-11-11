using System.Collections.Generic;
using Cassandra;

namespace AstraDb.Driver.Mapping.Contracts;

/// <summary>
/// Defines methods for mapping entities to dictionaries of field names and values,  and for mapping database rows to
/// entities.
/// </summary>
/// <remarks>This interface is typically used to facilitate data transformation between  application-level
/// entities and database representations, such as rows or fields.</remarks>
public interface IMapper
{
    /// <summary>
    /// Maps an entity to a dictionary of field names and values.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="entity"></param>
    /// <returns></returns>
    IDictionary<string, object?> MapToFields<T>(T entity);

    /// <summary>
    /// Maps a Cassandra row to an entity of type T.
    /// </summary>
    T MapFromRow<T>(Row row);
}
