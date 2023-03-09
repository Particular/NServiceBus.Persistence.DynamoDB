namespace NServiceBus.Persistence.DynamoDB
{
    using Amazon.DynamoDBv2;

    sealed class DynamoDBClientProvidedByConfigurationProvider : IDynamoDBClientProvider
    {
        public DynamoDBClientProvidedByConfigurationProvider(IAmazonDynamoDB client)
        {
            Guard.AgainstNull(nameof(client), client);

            Client = client;
        }

        public IAmazonDynamoDB Client { get; }
    }
}