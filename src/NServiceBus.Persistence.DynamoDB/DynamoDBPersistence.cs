namespace NServiceBus
{
    using Features;
    using Persistence;
    using Persistence.DynamoDB;

    /// <summary>
    /// Used to configure NServiceBus to use DynamoDB persistence.
    /// </summary>
    public class DynamoDBPersistence : PersistenceDefinition
    {
        internal DynamoDBPersistence()
        {
            Defaults(s =>
            {
                s.SetDefault(new OutboxPersistenceConfiguration());
                s.SetDefault(new SagaPersistenceConfiguration());
                s.SetDefault<IDynamoDBClientProvider>(new DefaultDynamoDbClientProviderProvider());
                s.EnableFeatureByDefault<InstallerFeature>();
            });

            Supports<StorageType.Sagas>(s => s.EnableFeatureByDefault<SagaStorage>());
            Supports<StorageType.Outbox>(s => s.EnableFeatureByDefault<OutboxStorage>());
        }
    }
}