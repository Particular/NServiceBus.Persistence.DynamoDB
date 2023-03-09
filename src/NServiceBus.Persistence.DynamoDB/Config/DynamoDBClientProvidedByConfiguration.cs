namespace NServiceBus.Persistence.DynamoDB
{
    using Amazon.DynamoDBv2;

    sealed class DynamoDBClientProvidedByConfiguration : IProvideDynamoDBClient
    {
        public DynamoDBClientProvidedByConfiguration(IAmazonDynamoDB client)
        {
            Guard.AgainstNull(nameof(client), client);

            Client = client;
        }

        public IAmazonDynamoDB Client { get; }
    }
}