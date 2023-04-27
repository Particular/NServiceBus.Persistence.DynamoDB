namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.Threading;
using Amazon.DynamoDBv2;

// This type will be tracked by the container and therefore disposed by the container
sealed class DefaultDynamoClientProviderProvider : IDynamoClientProvider, IDisposable
{
    public IAmazonDynamoDB Client => client.Value;

    public void Dispose()
    {
        if (client.IsValueCreated)
        {
            client.Value.Dispose();
        }
    }

    static IAmazonDynamoDB CreateDynamoDBClient() => new AmazonDynamoDBClient();

    readonly Lazy<IAmazonDynamoDB> client = new(CreateDynamoDBClient, LazyThreadSafetyMode.ExecutionAndPublication);
}