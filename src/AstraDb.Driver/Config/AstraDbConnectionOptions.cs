namespace AstraDb.Driver.Config;

/// <summary>
/// Configuration options required to connect to AstraDB using SCB (Secure Connect Bundle) and Token.
/// </summary>
public sealed class AstraDbConnectionOptions
{
    /// <summary>Path to the secure-connect bundle ZIP file.</summary>
    public string SecureConnectBundlePath { get; set; } = default!;

    /// <summary>Authentication token (AstraCS:...)</summary>
    public string Token { get; set; } = default!;

    /// <summary>Default keyspace to connect to.</summary>
    public string Keyspace { get; set; } = default!;
}
