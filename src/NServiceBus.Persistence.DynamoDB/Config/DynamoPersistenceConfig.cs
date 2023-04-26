namespace NServiceBus
{
    using Amazon.DynamoDBv2;
    using Configuration.AdvancedExtensibility;
    using Persistence.DynamoDB;

    /// <summary>
    /// Configuration extensions for DynamoDB Core API Persistence
    /// </summary>
    public static class DynamoPersistenceConfig
    {
        /// <summary>
        /// Override the default AmazonDynamoDBClient creation by providing a pre-configured AmazonDynamoDBClient
        /// </summary>
        /// <remarks>The lifetime of the provided client is assumed to be controlled by the caller of this method and thus the client will not be disposed.</remarks>
        public static PersistenceExtensions<DynamoPersistence> DynamoClient(this PersistenceExtensions<DynamoPersistence> persistenceExtensions, IAmazonDynamoDB dynamoClient)
        {
            Guard.AgainstNull(nameof(persistenceExtensions), persistenceExtensions);
            Guard.AgainstNull(nameof(dynamoClient), dynamoClient);

            persistenceExtensions.GetSettings().Set<IDynamoClientProvider>(new DynamoClientProvidedByConfigurationProvider(dynamoClient));
            return persistenceExtensions;
        }

        /// <summary>
        /// Uses the provided table configuration for both Saga and Outbox storage settings.
        /// </summary>
        public static PersistenceExtensions<DynamoPersistence> UseSharedTable(this PersistenceExtensions<DynamoPersistence> persistenceExtensions, TableConfiguration sharedTableConfiguration)
        {
            Guard.AgainstNull(nameof(persistenceExtensions), persistenceExtensions);
            Guard.AgainstNull(nameof(sharedTableConfiguration), sharedTableConfiguration);

            persistenceExtensions.Sagas().Table = sharedTableConfiguration with { };
            persistenceExtensions.Outbox().Table = sharedTableConfiguration with { };
            return persistenceExtensions;
        }

        /// <summary>
        /// Disables the tables creation.
        /// </summary>
        public static void DisableTablesCreation(this PersistenceExtensions<DynamoPersistence> persistenceExtensions)
        {
            Guard.AgainstNull(nameof(persistenceExtensions), persistenceExtensions);
            persistenceExtensions.Sagas().CreateTable = false;
            persistenceExtensions.Outbox().CreateTable = false;
        }

        /// <summary>
        /// Obtains the outbox persistence configuration options.
        /// </summary>
        /// <param name="persistenceExtensions"></param>
        /// <returns></returns>
        public static OutboxPersistenceConfiguration Outbox(this PersistenceExtensions<DynamoPersistence> persistenceExtensions) =>
            persistenceExtensions.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>();

        /// <summary>
        /// Obtains the saga persistence configuration options.
        /// </summary>
        public static SagaPersistenceConfiguration Sagas(this PersistenceExtensions<DynamoPersistence> persistenceExtensions) =>
            persistenceExtensions.GetSettings().GetOrCreate<SagaPersistenceConfiguration>();
    }
}