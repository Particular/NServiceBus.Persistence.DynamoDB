namespace NServiceBus.TransactionalSession
{
    using Features;
    using Configuration.AdvancedExtensibility;

    /// <summary>
    /// Extension methods for the DynamoDB transactional session support.
    /// </summary>
    public static class DynamoTransactionalSessionExtensions
    {
        /// <summary>
        /// Enables transactional session for this endpoint.
        /// </summary>
        public static PersistenceExtensions<DynamoPersistence> EnableTransactionalSession(
            this PersistenceExtensions<DynamoPersistence> persistenceExtensions)
        {
            persistenceExtensions.GetSettings().EnableFeatureByDefault<DynamoTransactionalSession>();
            return persistenceExtensions;
        }
    }
}