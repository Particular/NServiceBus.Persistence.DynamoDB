﻿namespace NServiceBus.Persistence.DynamoDB;

using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Extensibility;
using Outbox;

class DynamoOutboxTransaction : IOutboxTransaction
{
    public StorageSession StorageSession { get; }

    public DynamoOutboxTransaction(IAmazonDynamoDB dynamoDbClient, ContextBag context)
    {
        StorageSession = new StorageSession(dynamoDbClient, context);
    }

    public Task Commit(CancellationToken cancellationToken = default) => StorageSession.Commit(cancellationToken);

    public void Dispose() => StorageSession.Dispose();
}