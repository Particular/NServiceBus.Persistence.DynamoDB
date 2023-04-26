namespace NServiceBus.Persistence.DynamoDB
{
    using Amazon.DynamoDBv2;

    /// <summary>
    /// Provides a AmazonDynamoDBClient via dependency injection. A custom implementation can be registered on the container and will be picked up by the persistence.
    /// <remarks>
    /// The client provided will not be disposed by the persistence. It is the responsibility of the provider to take care of proper resource disposal if necessary.
    /// </remarks>
    /// </summary>
    public interface IDynamoClientProvider
    {
        /// <summary>
        /// The DynamoDB client to use.
        /// </summary>
        IAmazonDynamoDB Client { get; }
    }
}
