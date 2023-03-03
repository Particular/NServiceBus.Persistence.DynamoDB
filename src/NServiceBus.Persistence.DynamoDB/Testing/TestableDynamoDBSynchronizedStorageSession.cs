namespace NServiceBus.Testing
{
    using System.Collections.Generic;
    using Amazon.DynamoDBv2.Model;
    using Persistence;

    /// <summary>
    /// A fake implementation for <see cref="SynchronizedStorageSession"/> for testing purposes.
    /// </summary>
    public class TestableDynamoDBSynchronizedStorageSession : ISynchronizedStorageSession, IDynamoDBStorageSession
    {
        readonly List<TransactWriteItem> transactWriteItems = new List<TransactWriteItem>();

        /// <summary>
        /// Initializes a new TestableCosmosSynchronizedStorageSession with a partition key.
        /// </summary>
        public TestableDynamoDBSynchronizedStorageSession()
        {
        }

        /// <summary>
        /// Provides access to the added <see cref="TransactWriteItem"/>.
        /// </summary>
        public IReadOnlyCollection<TransactWriteItem> TransactWriteItems => transactWriteItems;

        /// <inheritdoc />
        void IDynamoDBStorageSession.Add(TransactWriteItem writeItem) => transactWriteItems.Add(writeItem);

        /// <inheritdoc />
        void IDynamoDBStorageSession.AddRange(IEnumerable<TransactWriteItem> writeItems) => transactWriteItems.AddRange(writeItems);
    }
}