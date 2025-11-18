using Cassandra.Mapping;

namespace AstraDb.Driver.MappingTests;

public class AnotherMappings : Mappings
{
    public static bool Constructed { get; private set; }

    public AnotherMappings()
    {
        Constructed = true;

        For<AttributedEntity>()
            .TableName("attrib_entities");
    }

    public static void Reset() => Constructed = false;
}
