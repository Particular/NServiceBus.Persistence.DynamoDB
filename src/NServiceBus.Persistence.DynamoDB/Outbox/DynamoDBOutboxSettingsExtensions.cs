namespace NServiceBus
{
    using System;
    using Configuration.AdvancedExtensibility;
    using Outbox;
    using Persistence.DynamoDB;

    /// <summary>
    /// DynamoDB outbox settings
    /// </summary>
    public static class DynamoDBOutboxSettingsExtensions
    {
        /// <summary>
        /// Sets the time to live for outbox deduplication records
        /// </summary>
        public static void TimeToKeepOutboxDeduplicationData(this OutboxSettings outboxSettings, TimeSpan timeToKeepOutboxDeduplicationData)
        {
            Guard.AgainstNegativeAndZero(nameof(timeToKeepOutboxDeduplicationData), timeToKeepOutboxDeduplicationData);

            outboxSettings.GetSettings().Set(SettingsKeys.OutboxTimeToLive, (int)timeToKeepOutboxDeduplicationData.TotalSeconds);
        }

        // TODO Add ability to override the outbox table name
        // TODO Add ability to customize PK and SK names
    }
}