namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using Amazon.DynamoDBv2;

    interface ICleanupAction
    {
        Guid Id { get; }
        AmazonDynamoDBRequest CreateRequest();
    }
}