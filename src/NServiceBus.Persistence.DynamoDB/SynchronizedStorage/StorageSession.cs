namespace NServiceBus.Persistence.DynamoDB
{
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2.Model;
    using Extensibility;

    class StorageSession
    {
        public StorageSession(ContextBag context) => CurrentContextBag = context;

        public void Add(TransactWriteItem writeItem)
        {

        }

        public Task Commit(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void Dispose()
        {
        }

        public ContextBag CurrentContextBag { get; set; }
    }
}