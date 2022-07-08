namespace NServiceBus
{
    using System.Collections.Generic;
    using Amazon.DynamoDBv2.Model;

    /// <summary>
    /// </summary>
    public interface IDynamoDBStorageSession
    {
        /// <summary>
        /// Adds operation to the list of operations to be executed in a transaction
        /// </summary>
        void Add(TransactWriteItem writeItem);
        /// <summary>
        /// Adds operations to the list of operations to be executed in a transaction
        /// </summary>
        void AddRange(IEnumerable<TransactWriteItem> writeItems);
    }
}
