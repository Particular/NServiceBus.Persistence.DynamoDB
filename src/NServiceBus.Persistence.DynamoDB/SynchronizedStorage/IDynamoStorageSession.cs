﻿namespace NServiceBus;

using System.Collections.Generic;
using Amazon.DynamoDBv2.Model;

/// <summary>
/// The DynamoDB storage session.
/// </summary>
public interface IDynamoStorageSession
{
    /// <summary>
    /// Adds an operation to the list of operations to be executed in a transaction
    /// </summary>
    void Add(TransactWriteItem writeItem);

    /// <summary>
    /// Adds operations to the list of operations to be executed in a transaction
    /// </summary>
    void AddRange(IEnumerable<TransactWriteItem> writeItems);
}