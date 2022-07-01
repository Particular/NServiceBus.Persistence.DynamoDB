namespace NServiceBus.Persistence.DynamoDB
{
    using System.Threading;
    using System.Threading.Tasks;
    using Extensibility;
    using Outbox;

    class DynamoDBOutboxTransaction : IOutboxTransaction
    {
        public StorageSession StorageSession { get; }
        public PartitionKey? PartitionKey { get; set; }

        // By default, store and commit are enabled
        public bool AbandonStoreAndCommit { get; set; }

        public DynamoDBOutboxTransaction(ContextBag context) => StorageSession = new StorageSession(context);

        public Task Commit(CancellationToken cancellationToken = default) =>
            AbandonStoreAndCommit ? Task.CompletedTask : StorageSession.Commit(cancellationToken);

        public void Dispose() => StorageSession.Dispose();
    }
}