namespace NServiceBus.Persistence.DynamoDB;

using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Extensibility;
using Outbox;

class DynamoOutboxTransaction(IAmazonDynamoDB dynamoDbClient, ContextBag context) : IOutboxTransaction
{
    public StorageSession StorageSession { get; } = new(dynamoDbClient, context);

    public Task Commit(CancellationToken cancellationToken = default) => StorageSession.Commit(cancellationToken);

    public void Dispose() => StorageSession.Dispose();
    public ValueTask DisposeAsync() => StorageSession.DisposeAsync();
}