namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
    using static SagaMetadataAttributeNames;

    sealed class UpdateSagaLock : ILockCleanup
    {
        public UpdateSagaLock(Guid sagaId, SagaPersistenceConfiguration configuration, string sagaPartitionKey, string sagaSortKey, string currentLease, string currentVersion)
        {
            this.configuration = configuration;
            this.sagaPartitionKey = sagaPartitionKey;
            this.sagaSortKey = sagaSortKey;
            this.currentLease = currentLease;
            this.currentVersion = currentVersion;
            Id = sagaId;
        }
        public Guid Id { get; }
        public bool PotentiallyNoLongerNecessary { get; set; }
        public bool Deactivated { get; set; }
        public Task Cleanup(IAmazonDynamoDB client, CancellationToken cancellationToken = default) =>
            client.UpdateItemAsync(new UpdateItemRequest
            {
                Key = new Dictionary<string, AttributeValue>
                {
                    { configuration.Table.PartitionKeyName, new AttributeValue { S = sagaPartitionKey } },
                    { configuration.Table.SortKeyName, new AttributeValue { S = sagaSortKey } }
                },
                UpdateExpression = "SET #lease = :released_lease",
                ConditionExpression =
                    "#lease = :current_lease AND #metadata.#version = :current_version", // only if the lock is still the same that we acquired.
                ExpressionAttributeNames =
                    new Dictionary<string, string>
                    {
                        { "#metadata", SagaMetadataAttributeName },
                        { "#lease", SagaLeaseAttributeName },
                        { "#version", SagaDataVersionAttributeName }
                    },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":current_lease", new AttributeValue { N = currentLease } },
                    { ":released_lease", new AttributeValue { N = "-1" } },
                    { ":current_version", new AttributeValue { N = currentVersion} }
                },
                ReturnValues = ReturnValue.NONE,
                TableName = configuration.Table.TableName
            }, cancellationToken);

        readonly SagaPersistenceConfiguration configuration;
        readonly string sagaPartitionKey;
        readonly string sagaSortKey;
        readonly string currentLease;
        readonly string currentVersion;
    }
}