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
        public static PersistenceExtensions<DynamoDBPersistence> EnableTransactionalSession(
            this PersistenceExtensions<DynamoDBPersistence> persistenceExtensions)
        {
            persistenceExtensions.GetSettings().EnableFeatureByDefault<DynamoTransactionalSession>();
            return persistenceExtensions;
        }
    }
}