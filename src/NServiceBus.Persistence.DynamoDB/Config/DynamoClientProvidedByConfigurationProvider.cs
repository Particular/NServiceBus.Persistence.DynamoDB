namespace NServiceBus.Persistence.DynamoDB;

using System;
using Amazon.DynamoDBv2;

sealed class DynamoClientProvidedByConfigurationProvider : IDynamoClientProvider
{
    public DynamoClientProvidedByConfigurationProvider(IAmazonDynamoDB client)
    {
        ArgumentNullException.ThrowIfNull(client);

        Client = client;
    }

    public IAmazonDynamoDB Client { get; }
}