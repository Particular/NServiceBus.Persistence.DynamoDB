namespace NServiceBus.Persistence.DynamoDB
{
    using System.Threading;
    using System.Threading.Tasks;
    using Extensibility;

    class StorageSession
    {
        public StorageSession(ContextBag context) => CurrentContextBag = context;

        public Task Commit(CancellationToken cancellationToken = default) => Task.CompletedTask;

        public void Dispose()
        {
        }

        public ContextBag CurrentContextBag { get; set; }
    }
}