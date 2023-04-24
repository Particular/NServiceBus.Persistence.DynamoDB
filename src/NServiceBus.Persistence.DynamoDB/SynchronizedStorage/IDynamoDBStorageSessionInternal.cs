namespace NServiceBus.Persistence.DynamoDB
{
    using System;

    interface IDynamoDBStorageSessionInternal : IDynamoDBStorageSession
    {
        void AddToBeExecutedWhenSessionDisposes(ILockCleanup lockCleanup);

        void MarkAsNoLongerNecessaryWhenSessionCommitted(Guid lockCleanupId);
    }
}