namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2.Model;
    using Extensibility;

    class StorageSession
    {
        public StorageSession(ContextBag context) => CurrentContextBag = context;

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
            if (batch.Count > 25)
            {
                throw new Exception(
                    "Transactional writes are limited to 25 items. Each saga counts as one item. Outbox, if enabled, counts as one item plus one additional item for each outgoing message.");
            }
        }

        public Task Commit(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void Dispose()
        {
        }

        public ContextBag CurrentContextBag { get; set; }

        List<TransactWriteItem> batch = new List<TransactWriteItem>();
    }
}