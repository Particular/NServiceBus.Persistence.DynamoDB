namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
    using Extensibility;
    using Sagas;
    using static SagaMetadataAttributeNames;

    class SagaPersister : ISagaPersister
    {
        readonly SagaPersistenceConfiguration configuration;
        readonly string endpointIdentifier;
        readonly IAmazonDynamoDB dynamoDbClient;

#if NET
        readonly Random random = Random.Shared;
#else
        readonly Random random = new Random();
#endif

        public SagaPersister(IAmazonDynamoDB dynamoDbClient, SagaPersistenceConfiguration configuration, string endpointIdentifier)
        {
            this.configuration = configuration;
            this.endpointIdentifier = endpointIdentifier.ToUpperInvariant();
            this.dynamoDbClient = dynamoDbClient;
        }

        public async Task<TSagaData?> Get<TSagaData>(Guid sagaId, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default) where TSagaData : class, IContainSagaData
        {
            if (configuration.UsePessimisticLocking)
            {
                return await ReadWithLock<TSagaData>(sagaId, context, (IDynamoDBStorageSessionInternal)session, cancellationToken).ConfigureAwait(false);
            }

            // Using optimistic concurrency control
            var getItemRequest = new GetItemRequest
            {
                ConsistentRead = true,
                Key = new Dictionary<string, AttributeValue>
                {
                    { configuration.Table.PartitionKeyName, new AttributeValue { S = SagaPartitionKey(sagaId) } },
                    { configuration.Table.SortKeyName, new AttributeValue { S = SagaSortKey(sagaId) } }
                },
                TableName = configuration.Table.TableName
            };

            var response = await dynamoDbClient.GetItemAsync(getItemRequest, cancellationToken).ConfigureAwait(false);
            return !response.IsItemSet ? default : Deserialize<TSagaData>(response.Item, context);
        }

        async Task<TSagaData?> ReadWithLock<TSagaData>(Guid sagaId, ContextBag context,
            IDynamoDBStorageSessionInternal dynamoSession, CancellationToken cancellationToken)
            where TSagaData : class, IContainSagaData
        {
            using var timedTokenSource = new CancellationTokenSource(configuration.LeaseAcquisitionTimeout);
            using var sharedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timedTokenSource.Token);
            cancellationToken = sharedTokenSource.Token;

            var sagaPartitionKey = SagaPartitionKey(sagaId);
            var sagaSortKey = SagaSortKey(sagaId);

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    DateTimeOffset now = DateTimeOffset.UtcNow.Add(dynamoDbClient.Config.ClockOffset);
                    //update creates a new item if it doesn't exist
                    var updateItemRequest = new UpdateItemRequest
                    {
                        Key = new Dictionary<string, AttributeValue>
                        {
                            { configuration.Table.PartitionKeyName, new AttributeValue { S = sagaPartitionKey } },
                            { configuration.Table.SortKeyName, new AttributeValue { S = sagaSortKey } }
                        },
                        UpdateExpression = "SET #lease = :lease_timeout",
                        ConditionExpression = "attribute_not_exists(#lease) OR #lease < :now",
                        ExpressionAttributeNames = new Dictionary<string, string>
                        {
                            { "#lease", SagaLeaseAttributeName }
                        },
                        ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                        {
                            { ":now", new AttributeValue { N = now.ToFileTime().ToString() } },
                            { ":lease_timeout", new AttributeValue { N = now.Add(configuration.LeaseDuration).ToFileTime().ToString() } }
                        },
                        ReturnValues = ReturnValue.ALL_NEW,
                        TableName = configuration.Table.TableName
                    };

                    try
                    {
                        var response = await dynamoDbClient.UpdateItemAsync(updateItemRequest, cancellationToken)
                            .ConfigureAwait(false);
                        // we need to find out if the saga already exists or not and we do that by checking the whether the saga metadata attribute map exists
                        if (response.Attributes.ContainsKey(SagaMetadataAttributeName) &&
                            response.Attributes[SagaMetadataAttributeName].M.TryGetValue(SagaDataVersionAttributeName, out AttributeValue? versionAttributeValue))
                        {
                            // the saga exists
                            var sagaData = Deserialize<TSagaData>(response.Attributes, context);

                            // ensure we cleanup the lock even if no update/save operation is being committed
                            // note that a transactional batch can only contain a single operation per item in DynamoDB
                            dynamoSession.AddToBeExecutedWhenSessionDisposes(new UpdateSagaLock(sagaId, configuration,
                                sagaPartitionKey, sagaSortKey,
                                response.Attributes[SagaLeaseAttributeName].N,
                                versionAttributeValue.N));
                            return sagaData;
                        }

                        // it's a new saga (but we own the lock now)
                        // we need to delete the entry containing the lock
                        dynamoSession.AddToBeExecutedWhenSessionDisposes(new DeleteSagaLock(sagaId, configuration, sagaPartitionKey, sagaSortKey,
                            response.Attributes[SagaLeaseAttributeName].N));
                        return null;
                    }
                    catch (AmazonDynamoDBException e) when (e is ConditionalCheckFailedException or TransactionConflictException)
                    {
                        // Condition failed, saga data is already locked but we don't know for how long
                        await Task.Delay(random.Next(100, 300), cancellationToken)
                            .ConfigureAwait(false);
                    }
                }
                throw new OperationCanceledException(cancellationToken);
            }
#pragma warning disable PS0020 // When catching OperationCanceledException, cancellation needs to be properly accounted for
            catch (OperationCanceledException e) when (timedTokenSource.IsCancellationRequested)
#pragma warning restore PS0020 // When catching OperationCanceledException, cancellation needs to be properly accounted for
            {
                // cancelled due to lease acquisition timeout
                // we want to rethrow other OperationCanceledExceptions
                throw new TimeoutException($"Failed to acquire the lock for saga ID {sagaId}.", e);
            }
        }

        TSagaData? Deserialize<TSagaData>(Dictionary<string, AttributeValue> attributeValues, ContextBag context) where TSagaData : class, IContainSagaData
        {
            var sagaData = Mapper.ToObject<TSagaData>(attributeValues, configuration.MapperOptions);
            if (sagaData is null)
            {
                return default;
            }
            var currentVersion = int.Parse(attributeValues[SagaMetadataAttributeName].M[SagaDataVersionAttributeName].N);
            context.Set($"dynamo_version:{sagaData.Id}", currentVersion);
            return sagaData;
        }

        public Task Save(IContainSagaData sagaData, SagaCorrelationProperty correlationProperty, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            var dynamoSession = (IDynamoDBStorageSessionInternal)session;
            dynamoSession.Add(new TransactWriteItem
            {
                Put = new Put
                {
                    Item = Serialize(sagaData, 0),
                    // fail if a saga (not just the lock) already exists
                    // SaveSaga could overwrite an existing lock if the caller didn't acquire a lock before calling Save but Core is guaranteed to acquire the lock first. In such a case, optimistic concurrency would fail the commit from the lock-holder which is ok because Save is generally not guaranteed to be fully pessimistic locking and other persisters only apply optimistic concurrency guarantees on this operation.
                    ConditionExpression = "attribute_not_exists(#metadata.#version)",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#metadata", SagaMetadataAttributeName },
                        { "#version", SagaDataVersionAttributeName }
                    },
                    TableName = configuration.Table.TableName,
                }
            });

            if (configuration.UsePessimisticLocking)
            {
                // we can't remove the action directly because the transaction was not completed yet
                dynamoSession.MarkAsNoLongerNecessaryWhenSessionCommitted(lockCleanupId: sagaData.Id);
            }

            return Task.CompletedTask;
        }

        public Task Update(IContainSagaData sagaData, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            var currentVersion = context.Get<int>($"dynamo_version:{sagaData.Id}");
            var nextVersion = currentVersion + 1;

            var dynamoSession = (IDynamoDBStorageSessionInternal)session;
            dynamoSession.Add(new TransactWriteItem
            {
                Put = new Put
                {
                    Item = Serialize(sagaData, nextVersion),
                    ConditionExpression = "#metadata.#version = :current_version", // fail if modified in the meantime
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#metadata", SagaMetadataAttributeName },
                        { "#version", SagaDataVersionAttributeName }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":current_version", new AttributeValue { N = currentVersion.ToString() } }
                    },
                    TableName = configuration.Table.TableName
                }
            });

            if (configuration.UsePessimisticLocking)
            {
                // we can't remove the action directly because the transaction was not completed yet
                dynamoSession.MarkAsNoLongerNecessaryWhenSessionCommitted(lockCleanupId: sagaData.Id);
            }

            return Task.CompletedTask;
        }

        Dictionary<string, AttributeValue> Serialize(IContainSagaData sagaData, int version)
        {
            var sagaDataMap = Mapper.ToMap(sagaData, sagaData.GetType(), configuration.MapperOptions);
            sagaDataMap.Add(configuration.Table.PartitionKeyName, new AttributeValue { S = SagaPartitionKey(sagaData.Id) });
            sagaDataMap.Add(configuration.Table.SortKeyName, new AttributeValue { S = SagaSortKey(sagaData.Id) });
            sagaDataMap.Add(SagaMetadataAttributeName, new AttributeValue
            {
                M = new Dictionary<string, AttributeValue>
                {
                    { SagaDataVersionAttributeName, new AttributeValue { N = version.ToString() } },
                    { "TYPE", new AttributeValue { S = sagaData.GetType().FullName } },
                    { "SCHEMA_VERSION", new AttributeValue { S = "1.0.0" } }
                }
            });
            // released lease on save
            sagaDataMap.Add(SagaLeaseAttributeName, new AttributeValue { N = "-1" });
            return sagaDataMap;
        }

        public Task Complete(IContainSagaData sagaData, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            var currentVersion = context.Get<int>($"dynamo_version:{sagaData.Id}");

            var dynamoSession = (IDynamoDBStorageSessionInternal)session;
            dynamoSession.Add(new TransactWriteItem
            {
                Delete = new Delete
                {
                    Key = new Dictionary<string, AttributeValue>
                    {
                        {configuration.Table.PartitionKeyName, new AttributeValue {S = SagaPartitionKey(sagaData.Id)}},
                        {configuration.Table.SortKeyName, new AttributeValue {S = SagaSortKey(sagaData.Id)}}
                    },
                    ConditionExpression = "#metadata.#version = :current_version", // fail if modified in the meantime
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#metadata", SagaMetadataAttributeName },
                        { "#version", SagaDataVersionAttributeName }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":current_version", new AttributeValue { N = currentVersion.ToString() } }
                    },
                    TableName = configuration.Table.TableName,
                }
            });
            return Task.CompletedTask;
        }

        public Task<TSagaData?> Get<TSagaData>(string propertyName, object propertyValue, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
            where TSagaData : class, IContainSagaData
        {
            // Saga ID needs to be calculated the same way as in SagaIdGenerator does
            var sagaId = DynamoDBSagaIdGenerator.Generate(typeof(TSagaData), propertyName, propertyValue);
            return Get<TSagaData>(sagaId, session, context, cancellationToken);
        }

        string SagaPartitionKey(Guid sagaId) => $"SAGA#{endpointIdentifier}#{sagaId}";

        string SagaSortKey(Guid sagaId) => $"SAGA#{sagaId}";
    }
}