namespace NServiceBus.Testing
{
    using Extensibility;
    using Persistence;
    using Persistence.DynamoDB;

    /// <summary>
    /// A fake implementation for <see cref="SynchronizedStorageSession"/> for testing purposes.
    /// </summary>
    public class TestableDynamoDBSynchronizedStorageSession : ISynchronizedStorageSession
    {
        /// <summary>
        /// Initializes a new TestableCosmosSynchronizedStorageSession with a partition key.
        /// </summary>
        public TestableDynamoDBSynchronizedStorageSession(PartitionKey partitionKey)
        {
            var contextBag = new ContextBag();
            contextBag.Set(partitionKey);
        }
    }
}