namespace NServiceBus.Persistence.DynamoDB
{
    using System.Threading;
    using System.Threading.Tasks;
    using Extensibility;
    using Outbox;

    class DynamoDBOutboxTransaction : IOutboxTransaction
    {
        public StorageSession StorageSession { get; }

        public DynamoDBOutboxTransaction(ContextBag context)
        {
            StorageSession = new StorageSession(context);
        }

        public Task Commit(CancellationToken cancellationToken = default) => StorageSession.Commit(cancellationToken);

        public void Dispose() => StorageSession.Dispose();
    }
}