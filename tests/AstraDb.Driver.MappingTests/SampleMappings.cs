using Cassandra.Mapping;

namespace AstraDb.Driver.MappingTests;

public class SampleMappings : Mappings
{
    public static bool Constructed { get; private set; }

    public SampleMappings()
    {
        Constructed = true;

        // Minimal definition – not important for the test,
        // we just want the ctor called.
        For<ConventionEntity>()
            .TableName("convention_entities");
    }

    public static void Reset() => Constructed = false;
}
