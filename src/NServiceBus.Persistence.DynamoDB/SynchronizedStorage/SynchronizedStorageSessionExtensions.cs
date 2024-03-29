﻿namespace NServiceBus;

using System;
using Persistence;

/// <summary>
/// DynamoDB persistence specific extension methods for the <see cref="ISynchronizedStorageSession"/>.
/// </summary>
public static class SynchronizedStorageSessionExtensions
{
    /// <summary>
    /// Retrieves the shared <see cref="IDynamoStorageSession"/> from the <see cref="ISynchronizedStorageSession"/>.
    /// </summary>
    public static IDynamoStorageSession DynamoPersistenceSession(this ISynchronizedStorageSession session)
    {
        ArgumentNullException.ThrowIfNull(session);

        if (session is not IDynamoStorageSession dynamoSession)
        {
            throw new Exception($"Cannot access the synchronized storage session. Either this endpoint has not been configured to use the DynamoDB persistence or a different persistence type is used for sagas. Ensure that 'EndpointConfiguration.UsePersistence<{nameof(DynamoPersistence)}>()' is used both for Sagas and Outbox.");
        }
        return dynamoSession;
    }
}