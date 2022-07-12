namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using Amazon.DynamoDBv2;

    class ThrowIfNoDynamoDBClientIsProvided : IProvideDynamoDBClient
    {
        public IAmazonDynamoDB Client => throw new Exception($"No AmazonDynamoDBClient has been configured. Either use `persistence.DynamoDBClient(client)` or register an implementation of `{nameof(IProvideDynamoDBClient)}` in the container.");
    }
}