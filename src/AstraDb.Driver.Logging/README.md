# AstraDB.Driver.Logging

The **AstraDB.Driver.Logging** package makes it easier to understand what’s happening inside your AstraDB operations.  
It plugs into your existing Serilog setup and automatically captures important details from the DataStax C# Driver — like consistency level, host and exception type — whenever something goes wrong.

Use this package when you want clear, structured AstraDB logs without adding extra code in your services.

### Key Features
- Consistent and structured AstraDB exception logging  
- Enriches logs automatically for `DriverException` types  
- Works seamlessly with Serilog and .NET logging  
- Lightweight and safe — never logs query text or sensitive data  

### Quick Setup
```csharp
services.AddLogging(lb => lb.AddAstraDbSerilog(configuration));
```

### Example
```csharp
try
{
    await client.ReadAsync<object>("dev_ks", "users", new { Email = "test@test.com" });
}
catch (DriverException ex)
{
    Log.Error(ex, "AstraDB read failed");
}
```

### Example Output
```json
{
  "Message": "AstraDB read failed",
  "AstraDbExceptionType": "ReadTimeoutException",
  "AstraDbConsistency": "LOCAL_QUORUM"
}
```
