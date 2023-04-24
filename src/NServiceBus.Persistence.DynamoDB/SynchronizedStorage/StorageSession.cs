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

    class StorageSession : IDynamoDBStorageSessionInternal
    {
        static readonly ILog Logger = LogManager.GetLogger<StorageSession>();

        public HashSet<Guid> SagaLocksReleased = new();

        public StorageSession(IAmazonDynamoDB dynamoDbClient, ContextBag context)
        {
            this.dynamoDbClient = dynamoDbClient;
            CurrentContextBag = context;
        }

        public void Add(TransactWriteItem writeItem)
        {
            ThrowIfDisposed();
            batch.Add(writeItem);
            CheckCapacity();
        }

        public void AddRange(IEnumerable<TransactWriteItem> writeItems)
        {
            ThrowIfDisposed();
            batch.AddRange(writeItems);
            CheckCapacity();
        }

        public void Add(ILockCleanup lockCleanup)
        {
            ThrowIfDisposed();
            lockCleanups ??= new Dictionary<Guid, ILockCleanup>();
            lockCleanups.Add(lockCleanup.Id, lockCleanup);
        }

        public void MarkAsNoLongerNecessaryWhenSessionCommitted(Guid lockCleanupId)
        {
            ThrowIfDisposed();
            if (lockCleanups?.TryGetValue(lockCleanupId, out var lockCleanup) ?? false)
            {
                lockCleanup.NoLongerNecessaryWhenSessionCommitted = true;
            }
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

            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new InvalidOperationException($"Unable to complete transaction (status code: {response.HttpStatusCode}.");
            }

            committed = true;
        }

        public void Dispose()
        {
            if (disposed)
            {
                return;
            }

            // release lock as fire & forget
            _ = ReleaseLocksAsync(CancellationToken.None);
            disposed = true;
        }

        async Task ReleaseLocksAsync(CancellationToken cancellationToken)
        {
            if (lockCleanups is null or { Count: 0 })
            {
                return;
            }

            // release any outstanding lock
            // Batches only support put/delete operations, no updates, therefore we execute all cleanups separately
            foreach (var action in lockCleanups.Values)
            {
                if (committed && action.NoLongerNecessaryWhenSessionCommitted)
                {
                    continue;
                }

                try
                {
                    await action.Cleanup(dynamoDbClient, cancellationToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    // ignored
                }
                catch (Exception e)
                {
                    // ignore failures and let the lock release naturally due to the max lock duration
                    Logger.Warn("Failed to cleanup saga locks", e);
                }
            }
        }

        void ThrowIfDisposed()
        {
            if (disposed)
            {
                throw new ObjectDisposedException(
                    $"The storage session has already been disposed. Make sure to retrieve the current storage session from the `{nameof(IMessageHandlerContext)}.{nameof(IMessageHandlerContext.SynchronizedStorageSession)}` or by injecting `{nameof(ICompletableSynchronizedStorageSession)}`.");
            }
        }

        public ContextBag CurrentContextBag { get; set; }

        List<TransactWriteItem> batch = new();
        Dictionary<Guid, ILockCleanup>? lockCleanups;
        readonly IAmazonDynamoDB dynamoDbClient;
        bool disposed;
        bool committed;
    }
}