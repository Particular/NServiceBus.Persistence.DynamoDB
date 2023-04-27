namespace NServiceBus.Persistence.DynamoDB;

using System;

interface IDynamoStorageSessionInternal : IDynamoStorageSession
{
    void AddToBeExecutedWhenSessionDisposes(ILockCleanup lockCleanup);

    void MarkAsNoLongerNecessaryWhenSessionCommitted(Guid lockCleanupId);
}