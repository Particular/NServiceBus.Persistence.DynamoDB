namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.Runtime.Serialization;

/// <summary>
/// Exception that can occur when the outbox records are queried immediately after the outbox transaction committed due to the read-committed transaction isolation levels between transactional write items and query operations.
///
/// Retrying should resolve this problem.
/// </summary>
[Serializable]
public class PartialOutboxResultException : Exception
{
    /// <summary>
    /// Initializes a new instance of <see cref="PartialOutboxResultException" />.
    /// </summary>
    public PartialOutboxResultException(string messageId, int transportOperationsRetrieved, int expectedNumberOfTransportOperations) : base(
        $"Partial outbox results retrieved with {transportOperationsRetrieved} instead of {expectedNumberOfTransportOperations} transport operation while attempting to load the outbox records for the message id '{messageId}'. This problem can occur due to read-committed isolation levels between transactional write items and query operations. Retrying this message should resolve the issue.")
    {
    }

    /// <summary>
    /// Initializes a new instance of <see cref="PartialOutboxResultException" />.
    /// </summary>
    protected PartialOutboxResultException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }
}