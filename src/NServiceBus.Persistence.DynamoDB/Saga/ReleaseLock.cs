namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;

    sealed class ReleaseLock : ICleanupAction
    {
        public ReleaseLock(SagaPersistenceConfiguration configuration, Guid sagaId, string currentLease, string currentVersion)
        {
            this.currentVersion = currentVersion;
            this.currentLease = currentLease;
            this.configuration = configuration;
            Id = sagaId;
        }
        public Guid Id { get; }

        public AmazonDynamoDBRequest CreateRequest() =>
            new UpdateItemRequest
            {
                Key = new Dictionary<string, AttributeValue>
                {
                    { configuration.Table.PartitionKeyName, new AttributeValue { S = $"SAGA#{Id}" } },
                    { configuration.Table.SortKeyName, new AttributeValue { S = $"SAGA#{Id}" } }
                },
                UpdateExpression = "SET #lease = :released_lease",
                ConditionExpression =
                    "#lease = :current_lease AND #metadata.#version = :current_version", // only if the lock is still the same that we acquired.
                ExpressionAttributeNames =
                    new Dictionary<string, string>
                    {
                        { "#metadata", SagaPersister.SagaMetadataAttributeName },
                        { "#lease", SagaPersister.SagaLeaseAttributeName },
                        { "#version", SagaPersister.SagaDataVersionAttributeName }
                    },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":current_lease", new AttributeValue { N = currentVersion } },
                    { ":released_lease", new AttributeValue { N = "-1" } },
                    { ":current_version", new AttributeValue { N = currentVersion } }
                },
                ReturnValues = ReturnValue.NONE,
                TableName = configuration.Table.TableName
            };

        readonly SagaPersistenceConfiguration configuration;
        private string currentLease;
        private string currentVersion;
    }
}