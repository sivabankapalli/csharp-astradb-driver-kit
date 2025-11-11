using System;
using System.Linq.Expressions;
using AstraDb.Driver.Mapping.Contracts;
using AstraDb.Driver.Mapping.Internals;

namespace AstraDb.Driver.Mapping.Configuration;

public sealed class MappingConfigurator : IMappingConfigurator
{
    private readonly MappingRegistry _reg;
    internal MappingConfigurator(MappingRegistry reg) => _reg = reg;

    public IMappingConfigurator MapTable<T>(string tableName)
    { _reg.MapTable(typeof(T), tableName); return this; }

    public IMappingConfigurator MapColumn<T, TProp>(Expression<Func<T, TProp>> prop, string column)
    {
        var member = prop.Body is MemberExpression m ? m.Member :
                     prop.Body is UnaryExpression u && u.Operand is MemberExpression um ? um.Member : null;
        if (member is null) throw new ArgumentException("Expression must be a property accessor.", nameof(prop));
        _reg.MapColumn(typeof(T), member.Name, column);
        return this;
    }

    public IMappingConfigurator AddConverter<TSource, TTarget>(ITypeConverter<TSource, TTarget> converter)
    {
        _reg.AddConverter(typeof(TSource), typeof(TTarget), converter);
        return this;
    }
}
