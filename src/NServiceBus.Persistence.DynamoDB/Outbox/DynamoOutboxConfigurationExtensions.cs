namespace NServiceBus;

using System;
using Configuration.AdvancedExtensibility;
using Outbox;

/// <summary>
/// Outbox configuration extensions for DynamoDB persistence.
/// </summary>
public static class DynamoOutboxConfigurationExtensions
{
    /// <summary>
    /// Customize the configuration of the table used by the outbox persistence.
    /// </summary>
    public static OutboxSettings UseTable(this OutboxSettings outboxSettings, TableConfiguration tableConfiguration)
    {
        ArgumentNullException.ThrowIfNull(outboxSettings);
        ArgumentNullException.ThrowIfNull(tableConfiguration);

        outboxSettings.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>().Table = tableConfiguration;
        return outboxSettings;
    }

    /// <summary>
    /// Sets the time to live for outbox deduplication records. The default value is <value>7 days</value>.
    /// </summary>
    public static OutboxSettings SetTimeToKeepDeduplicationData(this OutboxSettings outboxSettings, TimeSpan timeToKeepDeduplicationData)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeToKeepDeduplicationData, TimeSpan.Zero);

        outboxSettings.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>().TimeToKeepDeduplicationData = timeToKeepDeduplicationData;
        return outboxSettings;
    }

    /// <summary>
    /// Determines whether the NServiceBus installer should create the Outbox table when enabled. The default value is <value>true</value>.
    /// </summary>
    public static OutboxSettings CreateTable(this OutboxSettings outboxSettings, bool createTable)
    {
        ArgumentNullException.ThrowIfNull(outboxSettings);

        outboxSettings.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>().CreateTable = createTable;
        return outboxSettings;
    }

    /// <summary>
    /// Sets a custom endpoint name for the persister to use when storing and querying for outbox records.
    /// </summary>
    public static OutboxSettings EndpointName(this OutboxSettings outboxSettings, string endpointName)
    {
        ArgumentNullException.ThrowIfNull(outboxSettings);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointName);

        outboxSettings.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>().CustomEndpointName = endpointName;
        return outboxSettings;
    }
}