namespace NServiceBus
{
    using Amazon.DynamoDBv2;
    using Configuration.AdvancedExtensibility;
    using Persistence.DynamoDB;

    /// <summary>
    /// Configuration extensions for DynamoDB Core API Persistence
    /// </summary>
    public static class DynamoDBPersistenceConfig
    {
        /// <summary>
        /// Override the default AmazonDynamoDBClient creation by providing a pre-configured AmazonDynamoDBClient
        /// </summary>
        /// <remarks>The lifetime of the provided client is assumed to be controlled by the caller of this method and thus the client will not be disposed.</remarks>
        public static PersistenceExtensions<DynamoDBPersistence> DynamoDBClient(this PersistenceExtensions<DynamoDBPersistence> persistenceExtensions, IAmazonDynamoDB dynamoDBClient)
        {
            Guard.AgainstNull(nameof(persistenceExtensions), persistenceExtensions);
            Guard.AgainstNull(nameof(dynamoDBClient), dynamoDBClient);

            persistenceExtensions.GetSettings().Set<IDynamoDBClientProvider>(new DynamoDBClientProvidedByConfigurationProvider(dynamoDBClient));
            return persistenceExtensions;
        }

        /// <summary>
        /// Uses the provided table configuration for both Saga and Outbox storage settings.
        /// </summary>
        public static PersistenceExtensions<DynamoDBPersistence> UseSharedTable(this PersistenceExtensions<DynamoDBPersistence> persistenceExtensions, DynamoTableConfiguration sharedTableConfiguration)
        {
            Guard.AgainstNull(nameof(persistenceExtensions), persistenceExtensions);
            Guard.AgainstNull(nameof(sharedTableConfiguration), sharedTableConfiguration);

            persistenceExtensions.Sagas().Table = sharedTableConfiguration;
            persistenceExtensions.Outbox().Table = sharedTableConfiguration;
            return persistenceExtensions;
        }

        /// <summary>
        /// Disables the tables creation.
        /// </summary>
        public static void DisableTablesCreation(this PersistenceExtensions<DynamoDBPersistence> persistenceExtensions)
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
        public static OutboxPersistenceConfiguration Outbox(this PersistenceExtensions<DynamoDBPersistence> persistenceExtensions) =>
            persistenceExtensions.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>();

        /// <summary>
        /// Obtains the saga persistence configuration options.
        /// </summary>
        public static SagaPersistenceConfiguration Sagas(this PersistenceExtensions<DynamoDBPersistence> persistenceExtensions) =>
            persistenceExtensions.GetSettings().GetOrCreate<SagaPersistenceConfiguration>();
    }
}