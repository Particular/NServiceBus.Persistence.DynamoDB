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

    class StorageSession
    {
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
            if (batch.Count > 25)
            {
                throw new Exception(
                    "Transactional writes are limited to 25 items. Each saga counts as one item. Outbox, if enabled, counts as one item plus one additional item for each outgoing message.");
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
            if (response.HttpStatusCode != HttpStatusCode.OK)
            {
                throw new InvalidOperationException("Unable to complete transaction. Retrying");
            }
        }

        public void Dispose()
        {
        }

        public ContextBag CurrentContextBag { get; set; }

        List<TransactWriteItem> batch = new List<TransactWriteItem>();
        readonly IAmazonDynamoDB dynamoDbClient;
    }
}