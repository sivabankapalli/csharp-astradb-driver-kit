using AstraDb.Driver.Mapping.Examples.Models;

namespace AstraDb.Driver.Mapping.Examples.Mappings;

public sealed class DomainMappings : Mappings
{
    public DomainMappings()
    {
        For<Invoice>()
            .KeyspaceName("dev_ks")
            .TableName("invoices")
            .PartitionKey(i => i.InvoiceId)
            .Column(i => i.CustomerId, cm => cm.WithName("customer_id"))
            .Column(i => i.Total, cm => cm.WithName("total_amount"));
    }
}
