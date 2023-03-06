﻿namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
    using Extensibility;
    using Outbox;
    using Transport;

    class DynamoDBSynchronizedStorageSession : ICompletableSynchronizedStorageSession, IDynamoDBStorageSession
    {
        StorageSession storageSession;
        bool commitOnComplete;
        bool disposed;
        readonly IAmazonDynamoDB client;
        bool sessionSuccessfullyCommitted;

        public UpdateItemRequest TransactionCleanup { get; set; }

        public DynamoDBSynchronizedStorageSession(IDynamoDBClientProvider dynamoDbClientProvider)
            => client = dynamoDbClientProvider.Client;

        public ValueTask<bool> TryOpen(IOutboxTransaction transaction, ContextBag context,
            CancellationToken cancellationToken = default)
        {
            if (transaction is DynamoDBOutboxTransaction dynamoOutboxTransaction)
            {
                storageSession = dynamoOutboxTransaction.StorageSession;
                // because the synchronized storage session acts as decorator that forwards operations to the storage session
                // and we require access to the current context bag we need to make sure to assign the current context bag
                // to the storage session that was created as part of the outbox seam.
                CurrentContextBag = context;
                commitOnComplete = false;
                return new ValueTask<bool>(true);
            }

            return new ValueTask<bool>(false);
        }

        public ValueTask<bool> TryOpen(TransportTransaction transportTransaction, ContextBag context,
            CancellationToken cancellationToken = default) =>
            new ValueTask<bool>(false);

        public Task Open(ContextBag contextBag, CancellationToken cancellationToken = default)
        {
            // Creating the storage session already sets the correct context bag so there is no need to assign
            // CurrentContextBag here
            storageSession = new StorageSession(client, contextBag);
            commitOnComplete = true;
            return Task.CompletedTask;
        }

        //TODO also release locks if no update/complete was made
        //TODO move to storage session
        public async Task CompleteAsync(CancellationToken cancellationToken = default)
        {
            var commitTask = commitOnComplete ? storageSession.Commit(cancellationToken) : Task.CompletedTask;
            await commitTask.ConfigureAwait(false);
            sessionSuccessfullyCommitted = true;
        }

        public void Dispose()
        {
            if (!commitOnComplete || disposed)
            {
                return;
            }

            // release lock as fire & forget
            _ = ReleaseLocksAsync();

            storageSession.Dispose();
            disposed = true;

            async Task ReleaseLocksAsync()
            {
                if (!sessionSuccessfullyCommitted && TransactionCleanup != null)
                {
                    // release any outstanding lock
                    try
                    {

                        await client.UpdateItemAsync(TransactionCleanup).ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        // ignore failures and let the lock release naturally due to the max lock duration
                        Console.WriteLine(e);
                    }
                }
            }
        }

        public ContextBag CurrentContextBag
        {
            get => storageSession.CurrentContextBag;
            set => storageSession.CurrentContextBag = value;
        }

        public void Add(TransactWriteItem writeItem) => storageSession.Add(writeItem);

        public void AddRange(IEnumerable<TransactWriteItem> writeItems) => storageSession.AddRange(writeItems);
    }
}