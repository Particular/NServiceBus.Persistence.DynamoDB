#nullable enable
namespace NServiceBus.Persistence.DynamoDB
{
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;


    /// <summary>
    /// Describes a DynamoDB table.
    /// </summary>
    public record DynamoTableConfiguration
    {
        /// <summary>
        /// The name of the table used to store outbox information
        /// </summary>
        public string TableName { get; set; } = "NServiceBus.Storage";

        /// <summary>
        /// The name of the partition key
        /// </summary>
        public string PartitionKeyName { get; set; } = "PK";

        /// <summary>
        /// The name of the sort key
        /// </summary>
        public string SortKeyName { get; set; } = "SK";

        /// <summary>
        /// The attribute name for the Time to Live setting.
        /// </summary>
        public string? TimeToLiveAttributeName { get; set; } = "ExpiresAt";

        /// <summary>
        /// The billing mode for this table
        /// </summary>
        public BillingMode BillingMode { get; set; } = BillingMode.PAY_PER_REQUEST;

        /// <summary>
        /// The provisioned throughput for this table if using <code>BillingMode.PROVISIONED</code>.
        /// </summary>
        public ProvisionedThroughput? ProvisionedThroughput { get; set; } = null;
    }
}