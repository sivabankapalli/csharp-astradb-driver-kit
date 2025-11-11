using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace AstraDb.Driver.Mapping.Contracts;

/// <summary>
/// Mapping configurator interface for defining custom mappings between entities and database structures.
/// </summary>
public interface IMappingConfigurator
{
    /// <summary>
    /// Maps an entity type T to a specific table name in the database.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="tableName"></param>
    /// <returns></returns>
    IMappingConfigurator MapTable<T>(string tableName);

    /// <summary>
    /// Maps a property of an entity type T to a specific column name in the database.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TProp"></typeparam>
    /// <param name="prop"></param>
    /// <param name="column"></param>
    /// <returns></returns>
    IMappingConfigurator MapColumn<T, TProp>(Expression<Func<T, TProp>> prop, string column);

    /// <summary>
    /// Maps a custom type converter for converting between source and target types.
    /// </summary>
    /// <typeparam name="TSource"></typeparam>
    /// <typeparam name="TTarget"></typeparam>
    /// <param name="converter"></param>
    /// <returns></returns>
    IMappingConfigurator AddConverter<TSource, TTarget>(ITypeConverter<TSource, TTarget> converter);
}
