﻿namespace NServiceBus.Persistence.DynamoDB;

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
    public static OutboxSettings UseTable(this OutboxSettings outboxSettings, Func<TableConfiguration, TableConfiguration> tableConfiguration)
    {
        OutboxPersistenceConfiguration outboxConfiguration = outboxSettings.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>();
        outboxConfiguration.Table = tableConfiguration(outboxConfiguration.Table);
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
    public static OutboxSettings CreateTableWithInstaller(this OutboxSettings outboxSettings, bool createTable)
    {
        outboxSettings.GetSettings().GetOrCreate<OutboxPersistenceConfiguration>().CreateTable = createTable;
        return outboxSettings;
    }
}