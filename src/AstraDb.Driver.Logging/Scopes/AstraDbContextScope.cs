using Serilog.Context;

namespace AstraDb.Driver.Logging.Scopes
{
    /// <summary>
    /// Provides a scoped helper to enrich log events with AstraDB context such as keyspace, table, and operation.
    /// </summary>
    public static class AstraDbContextScope
    {
        /// <summary>
        /// Pushes AstraDB context properties into Serilog's LogContext for structured logging.
        /// Use within a <c>using</c> block to ensure automatic cleanup.
        /// </summary>
        /// <param name="keyspace">Optional keyspace name.</param>
        /// <param name="table">Optional table name.</param>
        /// <param name="operation">Optional operation (e.g., SELECT, INSERT).</param>
        /// <returns>An IDisposable scope that removes properties when disposed.</returns>
        public static IDisposable Push(
            string? keyspace = null,
            string? table = null,
            string? operation = null)
        {
            var propertyScopes = new List<IDisposable>();

            AddPropertyIfNotEmpty(propertyScopes, "AstraDbKeyspace", keyspace);
            AddPropertyIfNotEmpty(propertyScopes, "AstraDbTable", table);
            AddPropertyIfNotEmpty(propertyScopes, "AstraDbOperation", operation);

            return propertyScopes.Count == 0
                ? NoopDisposable.Instance
                : new CompositeDisposable(propertyScopes);
        }

        /// <summary>
        /// Adds a property to the log context if the specified property value is not null, empty, or consists only of
        /// whitespace.
        /// </summary>
        /// <param name="propertyScopes">The list to which the property scope will be added. This list is used to manage disposable log context
        /// properties.</param>
        /// <param name="propertyName">The name of the property to add to the log context.</param>
        /// <param name="propertyValue">The value of the property to add. If null, empty, or whitespace, the property will not be added.</param>
        private static void AddPropertyIfNotEmpty(List<IDisposable> propertyScopes, string propertyName, string? propertyValue)
        {
            if (!string.IsNullOrWhiteSpace(propertyValue))
                propertyScopes.Add(LogContext.PushProperty(propertyName, propertyValue));
        }

        /// <summary>
        /// Represents a collection of <see cref="IDisposable"/> objects that are disposed together.
        /// </summary>
        /// <remarks>This class is used to manage multiple <see cref="IDisposable"/> instances as a single
        /// unit.  When the <see cref="Dispose"/> method is called, all contained disposables are disposed in the order
        /// they were added. Any exceptions thrown during the disposal of individual objects are suppressed.</remarks>
        private sealed class CompositeDisposable : IDisposable
        {
            private readonly List<IDisposable> _propertyScopes;
            public CompositeDisposable(List<IDisposable> propertyScopes) => _propertyScopes = propertyScopes;

            public void Dispose()
            {
                foreach (var scope in _propertyScopes)
                {
                    try { scope.Dispose(); } catch { }
                }
            }
        }

        /// <summary>
        /// Represents a no-operation implementation of the <see cref="IDisposable"/> interface.
        /// </summary>
        /// <remarks>This class provides a singleton instance, <see cref="Instance"/>, that can be used in
        /// scenarios where a disposable object is required but no actual resource cleanup is necessary. The <see
        /// cref="Dispose"/>  method performs no action.</remarks>
        private sealed class NoopDisposable : IDisposable
        {
            public static readonly NoopDisposable Instance = new();
            private NoopDisposable() { }
            public void Dispose() { }
        }
    }
}
