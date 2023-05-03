namespace NServiceBus;

using Configuration.AdvancedExtensibility;

/// <summary>
/// Saga configuration extensions for DynamoDB persistence.
/// </summary>
public static class DynamoSagaConfigurationExtensions
{
    /// <summary>
    /// Obtains the saga persistence configuration options.
    /// </summary>
    public static SagaPersistenceConfiguration Sagas(this PersistenceExtensions<DynamoPersistence> persistenceExtensions) =>
        persistenceExtensions.GetSettings().GetOrCreate<SagaPersistenceConfiguration>();
}