using System;

namespace NServiceBus.Persistence.DynamoDB
{
    /// <summary>
    /// The Outbox persistence configuration options.
    /// </summary>
    public class OutboxPersistenceConfiguration
    {
        /// <summary>
        /// The name of the table used to store outbox information
        /// </summary>
        public string TableName { get; set; } = DynamoDBPersistenceConfig.SharedTableName;

        /// <summary>
        /// The name of the partition key
        /// </summary>
        public string PartitionKeyName { get; set; } = DynamoDBPersistenceConfig.DefaultPartitionKeyName;
    
        /// <summary>
        /// The name of the sort key
        /// </summary>
        public string SortKeyName { get; set; } = DynamoDBPersistenceConfig.DefaultSortKeyName;

        /// <summary>
        /// The Time to Live for outbox records.
        /// </summary>
        public TimeSpan TimeToLive { get; set; } = TimeSpan.FromDays(7);

        /// <summary>
        /// The attribute name for the Time to Live setting.
        /// </summary>
        public string TimeToLiveAttributeName { get; set; } = "ttl";
    }
}