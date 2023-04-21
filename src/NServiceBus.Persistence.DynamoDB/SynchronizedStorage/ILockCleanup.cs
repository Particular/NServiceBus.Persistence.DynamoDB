namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;

    interface ILockCleanup
    {
        Guid Id { get; }

        bool PotentiallyNoLongerNecessary { get; set; }

        bool Deactivated { get; set; }

        Task Cleanup(IAmazonDynamoDB client, CancellationToken cancellationToken = default);
    }
}