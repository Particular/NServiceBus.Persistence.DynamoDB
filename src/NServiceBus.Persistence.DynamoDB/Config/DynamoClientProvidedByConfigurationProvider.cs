namespace NServiceBus.Persistence.DynamoDB
{
    using Amazon.DynamoDBv2;

    sealed class DynamoClientProvidedByConfigurationProvider : IDynamoClientProvider
    {
        public DynamoClientProvidedByConfigurationProvider(IAmazonDynamoDB client)
        {
            Guard.AgainstNull(nameof(client), client);

            Client = client;
        }

        public IAmazonDynamoDB Client { get; }
    }
}