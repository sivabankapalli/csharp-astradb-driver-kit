using System;

namespace AstraDb.Driver.Mapping.Examples.Models;

public class Invoice
{
    public Guid InvoiceId { get; set; }
    public Guid CustomerId { get; set; }
    public decimal Total { get; set; }
}
