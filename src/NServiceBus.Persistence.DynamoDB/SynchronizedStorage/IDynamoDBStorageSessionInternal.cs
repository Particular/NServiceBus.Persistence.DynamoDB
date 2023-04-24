namespace NServiceBus.Persistence.DynamoDB
{
    using System;

    interface IDynamoDBStorageSessionInternal : IDynamoDBStorageSession
    {
        void Add(ILockCleanup lockCleanup);

        void MarkAsNoLongerNecessaryWhenSessionCommitted(Guid lockCleanupId);
    }
}