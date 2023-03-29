namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Net;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
    using Extensibility;
    using Logging;

    class StorageSession
    {
        static readonly ILog Logger = LogManager.GetLogger<StorageSession>();

        public HashSet<Guid> SagaLocksReleased = new();

        //TODO optimize allocations by avoiding creation of the dictionary on OOC settings. Expect 1 saga to be the default.
        public Dictionary<Guid, Func<IAmazonDynamoDB, CancellationToken, Task>> CleanupActions { get; } = new();

        public StorageSession(IAmazonDynamoDB dynamoDbClient, ContextBag context)
        {
            this.dynamoDbClient = dynamoDbClient;
            CurrentContextBag = context;
        }

        public void Add(TransactWriteItem writeItem)
        {
            batch.Add(writeItem);
            CheckCapacity();
        }

        public void AddRange(IEnumerable<TransactWriteItem> writeItems)
        {
            batch.AddRange(writeItems);
            CheckCapacity();
        }

        void CheckCapacity()
        {
            // The error on exceeded transaction items raised by the service is extremely convoluted and hard to understand. Until this is improved, we prevent an invalid request on the client side and throw a more meaningful exception to help users understand the limitations.
            if (batch.Count > 100)
            {
                throw new AmazonDynamoDBException(
                    "Transactional writes are limited to 100 items. Each saga counts as one item. Outbox, if enabled, counts as one item plus one additional item for each outgoing message.");
            }
        }

        public async Task Commit(CancellationToken cancellationToken = default)
        {
            if (batch.Count == 0)
            {
                return;
            }

            var transactItemsRequest = new TransactWriteItemsRequest { TransactItems = batch };
            var response = await dynamoDbClient.TransactWriteItemsAsync(transactItemsRequest, cancellationToken).ConfigureAwait(false);
            batch.Clear();
            // TODO: Check response
            // TODO: Add retries
            // TODO: Do we need to verify streams are disposed or is the SDK doing this?
            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new InvalidOperationException("Unable to complete transaction. Retrying");
            }

            // The transaction operations already released any lock, don't clean them up explicitly
            foreach (var sagaId in SagaLocksReleased)
            {
                CleanupActions.Remove(sagaId);
            }
        }

        public void Dispose()
        {

            // release lock as fire & forget
            _ = ReleaseLocksAsync();

            async Task ReleaseLocksAsync()
            {
                // release any outstanding lock

                // Batches only support put/delete operations, no updates, therefore we execute all cleanups separately
                foreach (var action in CleanupActions.Values)
                {
                    try
                    {
                        await action(dynamoDbClient, CancellationToken.None).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        // ignore failures and let the lock release naturally due to the max lock duration
                        Logger.Warn("Failed to cleanup saga locks", e);
                    }
                }
            }
        }

        public ContextBag CurrentContextBag { get; set; }

        List<TransactWriteItem> batch = new List<TransactWriteItem>();
        readonly IAmazonDynamoDB dynamoDbClient;
    }
}