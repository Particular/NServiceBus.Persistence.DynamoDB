namespace NServiceBus;

using Amazon.DynamoDBv2;
using Configuration.AdvancedExtensibility;
using Persistence.DynamoDB;

/// <summary>
/// Configuration extensions for DynamoDB Core API Persistence
/// </summary>
public static class DynamoPersistenceConfigExtensions
{
    /// <summary>
    /// Override the default AmazonDynamoDBClient creation by providing a pre-configured AmazonDynamoDBClient
    /// </summary>
    /// <remarks>The lifetime of the provided client is assumed to be controlled by the caller of this method and thus the client will not be disposed.</remarks>
    public static PersistenceExtensions<DynamoPersistence> DynamoClient(this PersistenceExtensions<DynamoPersistence> persistenceExtensions, IAmazonDynamoDB dynamoClient)
    {
        Guard.ThrowIfNull(persistenceExtensions);
        Guard.ThrowIfNull(dynamoClient);

        persistenceExtensions.GetSettings().Set<IDynamoClientProvider>(new DynamoClientProvidedByConfigurationProvider(dynamoClient));
        return persistenceExtensions;
    }

    /// <summary>
    /// Uses the provided table configuration for both Saga and Outbox storage settings.
    /// </summary>
    public static PersistenceExtensions<DynamoPersistence> UseSharedTable(this PersistenceExtensions<DynamoPersistence> persistenceExtensions, TableConfiguration sharedTableConfiguration)
    {
        Guard.ThrowIfNull(persistenceExtensions);
        Guard.ThrowIfNull(sharedTableConfiguration);

        persistenceExtensions.GetSettings().GetOrCreate<SagaPersistenceConfiguration>().Table = sharedTableConfiguration with { };
        persistenceExtensions.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>().Table = sharedTableConfiguration with { };
        return persistenceExtensions;
    }

    /// <summary>
    /// Disables the tables creation.
    /// </summary>
    public static void DisableTablesCreation(this PersistenceExtensions<DynamoPersistence> persistenceExtensions)
    {
        Guard.ThrowIfNull(persistenceExtensions);

        persistenceExtensions.GetSettings().GetOrCreate<SagaPersistenceConfiguration>().CreateTable = false;
        persistenceExtensions.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>().CreateTable = false;
    }
}