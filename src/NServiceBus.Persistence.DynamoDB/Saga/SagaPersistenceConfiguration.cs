namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using Amazon.DynamoDBv2.Model;
    using Amazon.DynamoDBv2;

    /// <summary>
    /// The saga persistence configuration options.
    /// </summary>
    public class SagaPersistenceConfiguration
    {
        /// <summary>
        /// The name of the table used to store saga data
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
        /// The billing mode for this table
        /// </summary>
        public BillingMode BillingMode { get; set; } = DynamoDBPersistenceConfig.DefaultBillingMode;

        /// <summary>
        /// The provisioned throughput for this table if using <code>BillingMode.PROVISIONED</code>.
        /// </summary>
        public ProvisionedThroughput ProvisionedThroughput { get; set; }

        /// <summary>
        /// Enables pessimistic locking mode to avoid concurrent modifications to the same saga. Enable this mode to reduce retries due to optimistic concurrency control violations.
        /// </summary>
        public bool UsePessimisticLocking { get; set; } = false;

        /// <summary>
        /// Determines whether the NServiceBus installer should create the Outbox table when enabled.
        /// </summary>
        internal bool CreateTable { get; set; } = true;

        //TODO should we make this publicly configurable?
        internal TimeSpan LeaseDuration = TimeSpan.FromSeconds(30); // based on SQS visibility timeout
        internal TimeSpan LeaseAcquistionTimeout { get; set; } = TimeSpan.FromSeconds(10);
    }
}