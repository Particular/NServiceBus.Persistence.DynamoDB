namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using Amazon.DynamoDBv2.Model;
    using Amazon.DynamoDBv2;

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
        public string TimeToLiveAttributeName { get; set; } = "ExpireAt";

        /// <summary>
        /// The billing mode for this table
        /// </summary>
        public BillingMode BillingMode { get; set; } = DynamoDBPersistenceConfig.DefaultBillingMode;

        /// <summary>
        /// The provisioned throughput for this table if using <code>BillingMode.PROVISIONED</code>.
        /// </summary>
        public ProvisionedThroughput ProvisionedThroughput { get; set; }

        /// <summary>
        /// Determines whether the NServiceBus installer should create the Outbox table when enabled.
        /// </summary>
        internal bool CreateTable { get; set; } = true;
    }
}