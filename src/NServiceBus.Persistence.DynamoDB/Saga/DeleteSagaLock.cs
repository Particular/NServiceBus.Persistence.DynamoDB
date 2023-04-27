namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using static SagaMetadataAttributeNames;

sealed class DeleteSagaLock : ILockCleanup
{
    public DeleteSagaLock(Guid sagaId, SagaPersistenceConfiguration configuration, string sagaPartitionKey, string sagaSortKey, string currentLease)
    {
        this.configuration = configuration;
        this.sagaPartitionKey = sagaPartitionKey;
        this.sagaSortKey = sagaSortKey;
        this.currentLease = currentLease;
        Id = sagaId;
    }

    public Guid Id { get; }

    public bool NoLongerNecessaryWhenSessionCommitted { get; set; }

    public Task Cleanup(IAmazonDynamoDB client, CancellationToken cancellationToken = default) =>
        client.DeleteItemAsync(new DeleteItemRequest
        {
            Key = new Dictionary<string, AttributeValue>
            {
                { configuration.Table.PartitionKeyName, new AttributeValue { S = sagaPartitionKey } },
                { configuration.Table.SortKeyName, new AttributeValue { S = sagaSortKey } }
            },
            ConditionExpression =
                "#lease = :current_lease AND attribute_not_exists(#metadata)", // only if the lock is still the same that we acquired.
            ExpressionAttributeNames =
                new Dictionary<string, string>
                {
                    { "#metadata", SagaMetadataAttributeName },
                    { "#lease", SagaLeaseAttributeName },
                },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                { ":current_lease", new AttributeValue { N = currentLease } }
            },
            ReturnValues = ReturnValue.NONE,
            TableName = configuration.Table.TableName
        }, cancellationToken);

    readonly SagaPersistenceConfiguration configuration;
    readonly string sagaPartitionKey;
    readonly string sagaSortKey;
    readonly string currentLease;
}