namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Text.Json;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.DocumentModel;
    using Amazon.DynamoDBv2.Model;
    using Extensibility;
    using Sagas;

    class SagaPersister : ISagaPersister
    {
        const string SagaDataVersionAttributeName = "___VERSION___";
        const string SagaLeaseAttributeName = "___LEASE_TIMEOUT___";

        readonly SagaPersistenceConfiguration configuration;
        readonly IAmazonDynamoDB dynamoDbClient;

#if NET
        readonly Random random = Random.Shared;
#else
        readonly Random random = new Random();
#endif

        public SagaPersister(SagaPersistenceConfiguration configuration, IAmazonDynamoDB dynamoDbClient)
        {
            this.configuration = configuration;
            this.dynamoDbClient = dynamoDbClient;
        }

        public async Task<TSagaData?> Get<TSagaData>(Guid sagaId, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default) where TSagaData : class, IContainSagaData
        {
            if (configuration.UsePessimisticLocking)
            {
                return await ReadWithLock<TSagaData>(sagaId, context, session, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                // Using optimistic concurrency control
                var getItemRequest = new GetItemRequest
                {
                    ConsistentRead = true,
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { configuration.Table.PartitionKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } },
                        { configuration.Table.SortKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } }, //Sort key
                    },
                    TableName = configuration.Table.TableName
                };

                var response = await dynamoDbClient.GetItemAsync(getItemRequest, cancellationToken).ConfigureAwait(false);
                return !response.IsItemSet ? default : Deserialize<TSagaData>(response.Item, context);
            }
        }

        async Task<TSagaData?> ReadWithLock<TSagaData>(Guid sagaId, ContextBag context,
            ISynchronizedStorageSession synchronizedStorageSession, CancellationToken cancellationToken)
            where TSagaData : class, IContainSagaData
        {
            using var timedTokenSource = new CancellationTokenSource(configuration.LeaseAcquisitionTimeout);
            using var sharedTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timedTokenSource.Token);
            cancellationToken = sharedTokenSource.Token;

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
                            { configuration.Table.PartitionKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } },
                            { configuration.Table.SortKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } }
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
                        // we need to find out if the saga already exists or not
                        if (response.Attributes.ContainsKey(SagaDataVersionAttributeName))
                        {
                            // the saga exists
                            var sagaData = Deserialize<TSagaData>(response.Attributes, context);

                            // ensure we cleanup the lock even if no update/save operation is being committed
                            // note that a transactional batch can only contain a single operation per item in DynamoDB
                            var dynamoSession = (DynamoDBSynchronizedStorageSession)synchronizedStorageSession;
                            dynamoSession.storageSession.CleanupActions[sagaId] = client => client.UpdateItemAsync(new UpdateItemRequest
                            {
                                Key = new Dictionary<string, AttributeValue>
                                {
                                    { configuration.Table.PartitionKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } },
                                    { configuration.Table.SortKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } }
                                },
                                UpdateExpression = "SET #lease = :released_lease",
                                ConditionExpression = "#lease = :current_lease AND #version = :current_version", // only if the lock is still the same that we acquired.
                                ExpressionAttributeNames =
                                    new Dictionary<string, string>
                                    {
                                    { "#lease", SagaLeaseAttributeName },
                                    { "#version", SagaDataVersionAttributeName }
                                    },
                                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                                {
                                    { ":current_lease", new AttributeValue { N = response.Attributes[SagaLeaseAttributeName].N } },
                                    { ":released_lease", new AttributeValue { N = "-1" } },
                                    { ":current_version", response.Attributes[SagaDataVersionAttributeName] }
                                },
                                ReturnValues = ReturnValue.NONE,
                                TableName = configuration.Table.TableName
                            });

                            return sagaData;
                        }
                        else
                        {
                            // it's a new saga (but we own the lock now)

                            // we need to delete the entry containing the lock
                            var dynamoSession = (DynamoDBSynchronizedStorageSession)synchronizedStorageSession;
                            dynamoSession.storageSession.CleanupActions[sagaId] = client => client.DeleteItemAsync(new DeleteItemRequest
                            {
                                Key = new Dictionary<string, AttributeValue>
                                {
                                    { configuration.Table.PartitionKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } },
                                    { configuration.Table.SortKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } }
                                },
                                ConditionExpression = "#lease = :current_lease AND attribute_not_exists(#version)", // only if the lock is still the same that we acquired.
                                ExpressionAttributeNames = new Dictionary<string, string>
                                {
                                    { "#lease", SagaLeaseAttributeName },
                                    { "#version", SagaDataVersionAttributeName }
                                },
                                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                                {
                                    { ":current_lease", new AttributeValue { N = response.Attributes[SagaLeaseAttributeName].N } },
                                },
                                ReturnValues = ReturnValue.NONE,
                                TableName = configuration.Table.TableName
                            });

                            return null;
                        }
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

        TSagaData Deserialize<TSagaData>(Dictionary<string, AttributeValue> attributeValues, ContextBag context) where TSagaData : class, IContainSagaData
        {
            var document = Document.FromAttributeMap(attributeValues);
            var sagaDataAsJson = document.ToJson();
            // All this is super allocation heavy. But for a first version that is OK
            var sagaData = JsonSerializer.Deserialize<TSagaData>(sagaDataAsJson);
            var currentVersion = int.Parse(attributeValues[SagaDataVersionAttributeName].N);
            context.Set($"dynamo_version:{sagaData!.Id}", currentVersion);
            return sagaData;
        }

        public Task Save(IContainSagaData sagaData, SagaCorrelationProperty correlationProperty, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            session.DynamoDBPersistenceSession().Add(new TransactWriteItem
            {
                Put = new Put
                {
                    Item = Serialize(sagaData, 0),
                    // fail if a saga (not just the lock) already exists
                    // SaveSaga could overwrite an existing lock if the caller didn't acquire a lock before calling Save but Core is guaranteed to acquire the lock first. In such a case, optimistic concurrency would fail the commit from the lock-holder which is ok because Save is generally not guaranteed to be fully pessimistic locking and other persisters only apply optimistic concurrency guarantees on this operation.
                    ConditionExpression = "attribute_not_exists(#version)",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        {"#version", SagaDataVersionAttributeName}
                    },
                    TableName = configuration.Table.TableName,
                }
            });

            // we can't remove the action directly because the transaction was not completed yet
            var dynamoSession = (DynamoDBSynchronizedStorageSession)session;
            dynamoSession.storageSession.SagaLocksReleased.Add(sagaData.Id);

            return Task.CompletedTask;
        }

        public Task Update(IContainSagaData sagaData, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            var currentVersion = context.Get<int>($"dynamo_version:{sagaData.Id}");
            var nextVersion = currentVersion + 1;

            session.DynamoDBPersistenceSession().Add(new TransactWriteItem
            {
                Put = new Put
                {
                    Item = Serialize(sagaData, nextVersion),
                    ConditionExpression = "#v = :cv", // fail if modified in the meantime
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#v", SagaDataVersionAttributeName }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":cv", new AttributeValue { N = currentVersion.ToString() } }
                    },
                    TableName = configuration.Table.TableName
                }
            });

            // we can't remove the action directly because the transaction was not completed yet
            var dynamoSession = (DynamoDBSynchronizedStorageSession)session;
            dynamoSession.storageSession.SagaLocksReleased.Add(sagaData.Id);

            return Task.CompletedTask;
        }

        Dictionary<string, AttributeValue> Serialize(IContainSagaData sagaData, int version)
        {
            // All this is super allocation heavy. But for a first version that is OK
            var sagaDataJson = JsonSerializer.Serialize(sagaData, sagaData.GetType());
            var doc = Document.FromJson(sagaDataJson);
            var map = doc.ToAttributeMap();
            map.Add(configuration.Table.PartitionKeyName, new AttributeValue { S = $"SAGA#{sagaData.Id}" });
            map.Add(configuration.Table.SortKeyName, new AttributeValue { S = $"SAGA#{sagaData.Id}" });  //Sort key
            // Version should probably be properly moved into metadata to not clash with existing things
            map.Add(SagaDataVersionAttributeName, new AttributeValue { N = version.ToString() });
            // release lease on save
            map.Add(SagaLeaseAttributeName, new AttributeValue { N = "-1" });
            // According to the best practices we should also add Type information probably here
            return map;
        }

        public Task Complete(IContainSagaData sagaData, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            var currentVersion = context.Get<int>($"dynamo_version:{sagaData.Id}");

            session.DynamoDBPersistenceSession().Add(new TransactWriteItem
            {
                Delete = new Delete()
                {
                    Key = new Dictionary<string, AttributeValue>
                    {
                        {configuration.Table.PartitionKeyName, new AttributeValue {S = $"SAGA#{sagaData.Id}"}},
                        {configuration.Table.SortKeyName, new AttributeValue {S = $"SAGA#{sagaData.Id}"}}, //Sort key
                    },
                    ConditionExpression = "#v = :cv", // fail if modified in the meantime
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#v", SagaDataVersionAttributeName }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":cv", new AttributeValue { N = currentVersion.ToString() } }
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
    }
}