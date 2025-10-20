namespace NServiceBus;

using Features;
using Persistence;
using Persistence.DynamoDB;

/// <summary>
/// Used to configure NServiceBus to use DynamoDB persistence.
/// </summary>
public class DynamoPersistence : PersistenceDefinition, IPersistenceDefinitionFactory<DynamoPersistence>
{
    DynamoPersistence()
    {
        Defaults(s =>
        {
            s.SetDefault(new OutboxPersistenceConfiguration());
            s.SetDefault(new SagaPersistenceConfiguration());
            s.SetDefault<IDynamoClientProvider>(new DefaultDynamoClientProviderProvider());
            s.EnableFeatureByDefault<InstallerFeature>();
        });

        Supports<StorageType.Sagas, SagaStorage>();
        Supports<StorageType.Outbox, OutboxStorage>();
    }

    static DynamoPersistence IPersistenceDefinitionFactory<DynamoPersistence>.Create() => new();
}