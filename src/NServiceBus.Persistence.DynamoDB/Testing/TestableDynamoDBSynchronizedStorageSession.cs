namespace NServiceBus.Testing;

using System;
using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;
using Persistence;
using Persistence.DynamoDB;

/// <summary>
/// A fake implementation for <see cref="ISynchronizedStorageSession"/> for testing purposes.
/// </summary>
public class TestableDynamoSynchronizedStorageSession : ISynchronizedStorageSession, IDynamoStorageSessionInternal
{
    readonly List<TransactWriteItem> transactWriteItems = new List<TransactWriteItem>();

    /// <summary>
    /// Initializes a new <see cref="TestableDynamoSynchronizedStorageSession"/>.
    /// </summary>
    public TestableDynamoSynchronizedStorageSession()
    {
    }

    /// <summary>
    /// Provides access to the added <see cref="TransactWriteItem"/>.
    /// </summary>
    public IReadOnlyCollection<TransactWriteItem> TransactWriteItems => transactWriteItems;

    /// <inheritdoc />
    void IDynamoStorageSession.Add(TransactWriteItem writeItem) => transactWriteItems.Add(writeItem);

    /// <inheritdoc />
    void IDynamoStorageSession.AddRange(IEnumerable<TransactWriteItem> writeItems) => transactWriteItems.AddRange(writeItems);

    void IDynamoStorageSessionInternal.AddToBeExecutedWhenSessionDisposes(ILockCleanup lockCleanup)
    {
    }

    void IDynamoStorageSessionInternal.MarkAsNoLongerNecessaryWhenSessionCommitted(Guid lockCleanupId)
    {
    }
}