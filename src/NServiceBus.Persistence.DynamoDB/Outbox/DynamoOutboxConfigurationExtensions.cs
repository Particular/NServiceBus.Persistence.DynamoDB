namespace NServiceBus;

using System;
using Configuration.AdvancedExtensibility;
using Outbox;
using Persistence.DynamoDB;

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
        Guard.ThrowIfNull(outboxSettings);
        Guard.ThrowIfNull(tableConfiguration);

        outboxSettings.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>().Table = tableConfiguration;
        return outboxSettings;
    }

    /// <summary>
    /// Sets the time to live for outbox deduplication records. The default value is <value>7 days</value>.
    /// </summary>
    public static OutboxSettings SetTimeToKeepDeduplicationData(this OutboxSettings outboxSettings, TimeSpan timeToKeepDeduplicationData)
    {
        Guard.ThrowIfNegativeOrZero(timeToKeepDeduplicationData);

        outboxSettings.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>().TimeToKeepDeduplicationData = timeToKeepDeduplicationData;
        return outboxSettings;
    }

    /// <summary>
    /// Determines whether the NServiceBus installer should create the Outbox table when enabled. The default value is <value>true</value>.
    /// </summary>
    public static OutboxSettings CreateTable(this OutboxSettings outboxSettings, bool createTable)
    {
        Guard.ThrowIfNull(outboxSettings);

        outboxSettings.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>().CreateTable = createTable;
        return outboxSettings;
    }
}