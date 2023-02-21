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
        //TODO any reason to not use a static table across all endpoints?
        internal const string SharedTableName = "NServiceBus.Storage";
        internal const string DefaultPartitionKeyName = "PK";
        internal const string DefaultSortKeyName = "SK";

        /// <summary>
        /// Override the default AmazonDynamoDBClient creation by providing a pre-configured AmazonDynamoDBClient
        /// </summary>
        /// <remarks>The lifetime of the provided client is assumed to be controlled by the caller of this method and thus the client will not be disposed.</remarks>
        public static PersistenceExtensions<DynamoDBPersistence> DynamoDBClient(this PersistenceExtensions<DynamoDBPersistence> persistenceExtensions, AmazonDynamoDBClient dynamoDBClient)
        {
            Guard.AgainstNull(nameof(persistenceExtensions), persistenceExtensions);
            Guard.AgainstNull(nameof(dynamoDBClient), dynamoDBClient);

            persistenceExtensions.GetSettings().Set<IProvideDynamoDBClient>(new DynamoDBClientProvidedByConfiguration { Client = dynamoDBClient });
            return persistenceExtensions;
        }

        /// <summary>
        /// Sets the table name for the outbox and the saga storage
        /// </summary>
        public static PersistenceExtensions<DynamoDBPersistence> TableName(this PersistenceExtensions<DynamoDBPersistence> persistenceExtensions, string tableName)
        {
            Guard.AgainstNullAndEmpty(nameof(tableName), tableName);

            persistenceExtensions.Outbox().TableName = tableName;
            persistenceExtensions.Sagas().TableName = tableName;

            return persistenceExtensions;
        }

        /// <summary>
        /// Disables the tables creation.
        /// </summary>
        public static void DisableTablesCreation(this PersistenceExtensions<DynamoDBPersistence> persistenceExtensions)
        {
            Guard.AgainstNull(nameof(persistenceExtensions), persistenceExtensions);

            var installerSettings = persistenceExtensions.GetSettings().GetOrCreate<InstallerSettings>();
            installerSettings.Disabled = true;
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

        /// <summary>
        /// Obtains the transaction information configuration options.
        /// </summary>
        public static TransactionInformationConfiguration TransactionInformation(this PersistenceExtensions<DynamoDBPersistence> persistenceExtensions)
        {
            Guard.AgainstNull(nameof(persistenceExtensions), persistenceExtensions);

            return persistenceExtensions.GetSettings().GetOrCreate<TransactionInformationConfiguration>();
        }

        // TODO Add ability to customize PK and SK names
    }
}