namespace NServiceBus.Persistence.DynamoDB
{
    using Amazon.DynamoDBv2;

    class DynamoDBClientProvidedByConfiguration : IProvideDynamoDBClient
    {
        public AmazonDynamoDBClient Client { get; set; }
    }
}