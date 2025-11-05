namespace NServiceBus.TransactionalSession;

using System;
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
        this PersistenceExtensions<DynamoPersistence> persistenceExtensions) =>
        EnableTransactionalSession(persistenceExtensions, new TransactionalSessionOptions());

    /// <summary>
    /// Enables the transactional session for this endpoint using the specified TransactionalSessionOptions.
    /// </summary>
    public static PersistenceExtensions<DynamoPersistence> EnableTransactionalSession(this PersistenceExtensions<DynamoPersistence> persistenceExtensions,
        TransactionalSessionOptions transactionalSessionOptions)
    {
        ArgumentNullException.ThrowIfNull(persistenceExtensions);
        ArgumentNullException.ThrowIfNull(transactionalSessionOptions);

        var settings = persistenceExtensions.GetSettings();

        settings.Set(transactionalSessionOptions);
        if (!string.IsNullOrWhiteSpace(transactionalSessionOptions.ProcessorEndpoint))
        {
            settings.GetOrCreate<OutboxPersistenceConfiguration>().ProcessorEndpoint = transactionalSessionOptions.ProcessorEndpoint;
        }

        settings.EnableFeature<DynamoTransactionalSession>();

        return persistenceExtensions;
    }
}