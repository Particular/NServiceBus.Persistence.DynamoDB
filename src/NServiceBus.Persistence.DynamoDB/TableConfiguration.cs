namespace NServiceBus.Persistence.DynamoDB;

using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;


/// <summary>
/// Describes a DynamoDB table.
/// </summary>
public record TableConfiguration
{
    /// <summary>
    /// The name of the table used to store outbox information
    /// </summary>
    /// <remarks>
    /// The default value is <value>"NServiceBus.Storage"</value>
    /// </remarks>
    public string TableName { get; set; } = "NServiceBus.Storage";

    /// <summary>
    /// The name of the partition key
    /// </summary>
    /// <remarks>
    /// The default value is <value>"PK"</value>
    /// </remarks>
    public string PartitionKeyName { get; set; } = "PK";

    /// <summary>
    /// The name of the sort key
    /// </summary>
    /// <remarks>
    /// The default value is <value>"SK"</value>
    /// </remarks>
    public string SortKeyName { get; set; } = "SK";

    /// <summary>
    /// The attribute name for the Time to Live setting.
    /// </summary>
    /// <remarks>
    /// The default value is <value>"ExpiresAt"</value>
    /// </remarks>
    public string? TimeToLiveAttributeName { get; set; } = "ExpiresAt";

    /// <summary>
    /// The billing mode for this table
    /// </summary>
    /// <remarks>
    /// The default value is <value>"PAY_PER_REQUEST"</value>
    /// </remarks>
    public BillingMode BillingMode { get; set; } = BillingMode.PAY_PER_REQUEST;

    /// <summary>
    /// The provisioned throughput for this table if using <code>BillingMode.PROVISIONED</code>.
    /// </summary>
    /// <remarks>
    /// The default value is <code>null</code>
    /// </remarks>
    public ProvisionedThroughput? ProvisionedThroughput { get; set; } = null;
}