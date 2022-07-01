namespace NServiceBus.Persistence.DynamoDB
{
    using System.Threading;
    using System.Threading.Tasks;
    using Extensibility;
    using Outbox;

    class OutboxPersister : IOutboxStorage
    {
        public OutboxPersister(int ttlInSeconds) => this.ttlInSeconds = ttlInSeconds;

        public Task<IOutboxTransaction> BeginTransaction(ContextBag context, CancellationToken cancellationToken = default)
        {
            var cosmosOutboxTransaction = new DynamoDBOutboxTransaction(context);

            if (context.TryGet<PartitionKey>(out var partitionKey))
            {
                cosmosOutboxTransaction.PartitionKey = partitionKey;
            }

            return Task.FromResult((IOutboxTransaction)cosmosOutboxTransaction);
        }

        public Task<OutboxMessage> Get(string messageId, ContextBag context, CancellationToken cancellationToken = default)
        {
            if (!context.TryGet<PartitionKey>(out var _))
            {
                // we return null here to enable outbox work at logical stage
                return null;
            }

            // var outboxRecord = await setAsDispatchedHolder.ContainerHolder.Container.ReadOutboxRecord(messageId, partitionKey, serializer, context, cancellationToken: cancellationToken)
            //     .ConfigureAwait(false);

            return Task.FromResult<OutboxMessage>(null);
            // return outboxRecord != null ? new OutboxMessage(outboxRecord.Id, outboxRecord.TransportOperations?.Select(op => op.ToTransportType()).ToArray()) : null;
        }

        public Task Store(OutboxMessage message, IOutboxTransaction transaction, ContextBag context, CancellationToken cancellationToken = default)
        {
            var cosmosTransaction = (DynamoDBOutboxTransaction)transaction;

            if (cosmosTransaction == null || cosmosTransaction.AbandonStoreAndCommit || cosmosTransaction.PartitionKey == null)
            {
                return Task.CompletedTask;
            }

            return Task.CompletedTask;
        }

        public Task SetAsDispatched(string messageId, ContextBag context, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

#pragma warning disable IDE0052
        readonly int ttlInSeconds;
#pragma warning restore IDE0052

        internal static readonly string SchemaVersion = "1.0.0";
    }
}