﻿namespace NServiceBus.Persistence.DynamoDB
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
        const string SagaLockAttributeName = "NSB_lease_timeout";

        readonly SagaPersistenceConfiguration configuration;
        readonly IAmazonDynamoDB dynamoDbClient;

        public SagaPersister(SagaPersistenceConfiguration configuration, IAmazonDynamoDB dynamoDbClient)
        {
            this.configuration = configuration;
            this.dynamoDbClient = dynamoDbClient;
        }

        public async Task<TSagaData> Get<TSagaData>(Guid sagaId, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default) where TSagaData : class, IContainSagaData
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
                        { configuration.PartitionKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } },
                        { configuration.SortKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } }, //Sort key
                    },
                    TableName = configuration.TableName
                };

                var response = await dynamoDbClient.GetItemAsync(getItemRequest, cancellationToken).ConfigureAwait(false);
                return !response.IsItemSet ? default : Deserialize<TSagaData>(response.Item, context);
            }
        }

        async Task<TSagaData> ReadWithLock<TSagaData>(Guid sagaId, ContextBag context,
            ISynchronizedStorageSession synchronizedStorageSession, CancellationToken cancellationToken)
            where TSagaData : class, IContainSagaData
        {
            using var timedTokenSource = new CancellationTokenSource(configuration.LeaseAcquistionTimeout);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timedTokenSource.Token);
            cancellationToken = cts.Token;
            while (true)
            {
                //TODO should we throw a TimeoutException instead like CosmosDB?
                cancellationToken.ThrowIfCancellationRequested();

                //TODO: reset lock on successful commit. what about failed commits?
                DateTimeOffset now = DateTimeOffset.UtcNow;
                //update items creates a new item if it doesn't exist
                var updateItemRequest = new UpdateItemRequest
                {
                    Key = new Dictionary<string, AttributeValue>
                    {
                        { configuration.PartitionKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } },
                        { configuration.SortKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } }
                    },
                    UpdateExpression = "SET #lock = :lock_timeout", //TODO should we use lock or lease as the terminology?
                    ConditionExpression = "attribute_not_exists(#lock) OR #lock < :now",
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#lock", SagaLockAttributeName }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":now", new AttributeValue { N = now.ToFileTime().ToString() } },
                        { ":lock_timeout", new AttributeValue { N = now.Add(configuration.LeaseDuration).ToFileTime().ToString() } }
                    },
                    ReturnValues = ReturnValue.ALL_NEW,
                    TableName = configuration.TableName
                };

                try
                {
                    var response = await dynamoDbClient.UpdateItemAsync(updateItemRequest, cancellationToken)
                        .ConfigureAwait(false);
                    // we need to find out if the saga already exists or not
                    // TODO can we use a condition expression and figure out which condition failed? (new saga vs. lock failed)
                    if (response.Attributes.ContainsKey(SagaDataVersionAttributeName))
                    {
                        // the saga exists
                        var sagaData = Deserialize<TSagaData>(response.Attributes, context);

                        // ensure we cleanup the lock even if no update/save operation is being committed
                        // note that a transactional batch can only contain a single operation per item in DynamoDB
                        var dynamoSession = (DynamoDBSynchronizedStorageSession)synchronizedStorageSession;
                        dynamoSession.storageSession.CleanupAction = client => client.UpdateItemAsync(new UpdateItemRequest
                        {
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { configuration.PartitionKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } },
                                { configuration.SortKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } }
                            },
                            UpdateExpression = "SET #lock = :released_lock",
                            ConditionExpression = "#lock = :current_lock", // only if the lock is still the same that we acquired.
                            ExpressionAttributeNames =
                                new Dictionary<string, string> { { "#lock", SagaLockAttributeName } },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                { ":current_lock", new AttributeValue { N = response.Attributes[SagaLockAttributeName].N } },
                                { ":released_lock", new AttributeValue { N = "-1" } }
                            },
                            ReturnValues = ReturnValue.NONE,
                            TableName = configuration.TableName
                        });

                        return sagaData;
                    }
                    else
                    {
                        // it's a new saga (but we own the lock now)

                        // we need to delete the entry containing the lock
                        var dynamoSession = (DynamoDBSynchronizedStorageSession)synchronizedStorageSession;
                        dynamoSession.storageSession.CleanupAction = client => client.DeleteItemAsync(new DeleteItemRequest
                        {
                            Key = new Dictionary<string, AttributeValue>
                            {
                                { configuration.PartitionKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } },
                                { configuration.SortKeyName, new AttributeValue { S = $"SAGA#{sagaId}" } }
                            },
                            ConditionExpression = "#lock = :current_lock", // only if the lock is still the same that we acquired.
                            ExpressionAttributeNames =
                                new Dictionary<string, string> { { "#lock", SagaLockAttributeName } },
                            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                            {
                                { ":current_lock", new AttributeValue { N = response.Attributes[SagaLockAttributeName].N } },
                            },
                            ReturnValues = ReturnValue.NONE,
                            TableName = configuration.TableName
                        });

                        return null;
                    }
                }
                //TODO create spec test to verify error code
                catch (AmazonDynamoDBException e) when (e.ErrorCode == "ConditionalCheckFailedException")
                {
                    // Condition failed, saga data is already locked but we don't know for how long
                    //TODO if we decide to throw a different exception, we should catch for OperationCancelledException here
                    await Task.Delay(100, cancellationToken)
                        .ConfigureAwait(false); //TODO select better value and introduce jittering.
                }
            }
        }

        TSagaData Deserialize<TSagaData>(Dictionary<string, AttributeValue> attributeValues, ContextBag context) where TSagaData : class, IContainSagaData
        {
            var document = Document.FromAttributeMap(attributeValues);
            var sagaDataAsJson = document.ToJson();
            // All this is super allocation heavy. But for a first version that is OK
            var sagaData = JsonSerializer.Deserialize<TSagaData>(sagaDataAsJson);
            var currentVersion = int.Parse(attributeValues[SagaDataVersionAttributeName].N);
            context.Set($"dynamo_version:{sagaData.Id}", currentVersion);
            return sagaData;
        }

        public Task Save(IContainSagaData sagaData, SagaCorrelationProperty correlationProperty, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            session.DynamoDBPersistenceSession().Add(new TransactWriteItem
            {
                Put = new Put
                {
                    Item = Serialize(sagaData, 0),
                    ConditionExpression = "attribute_not_exists(#version)", // fail if a saga (not just the lock) already exists
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        {"#version", SagaDataVersionAttributeName}
                    },
                    TableName = configuration.TableName,
                }
            });

            var dynamoSession = (DynamoDBSynchronizedStorageSession)session;
            dynamoSession.storageSession.SagaLockReleased = true;

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
                    TableName = configuration.TableName
                }
            });

            var dynamoSession = (DynamoDBSynchronizedStorageSession)session;
            dynamoSession.storageSession.SagaLockReleased = true;

            return Task.CompletedTask;
        }

        Dictionary<string, AttributeValue> Serialize(IContainSagaData sagaData, int version)
        {
            // All this is super allocation heavy. But for a first version that is OK
            var sagaDataJson = JsonSerializer.Serialize(sagaData, sagaData.GetType());
            var doc = Document.FromJson(sagaDataJson);
            var map = doc.ToAttributeMap();
            map.Add(configuration.PartitionKeyName, new AttributeValue { S = $"SAGA#{sagaData.Id}" });
            map.Add(configuration.SortKeyName, new AttributeValue { S = $"SAGA#{sagaData.Id}" });  //Sort key
            // Version should probably be properly moved into metadata to not clash with existing things
            map.Add(SagaDataVersionAttributeName, new AttributeValue { N = version.ToString() });
            // release lock on save
            map.Add(SagaLockAttributeName, new AttributeValue { N = "-1" });
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
                        {configuration.PartitionKeyName, new AttributeValue {S = $"SAGA#{sagaData.Id}"}},
                        {configuration.SortKeyName, new AttributeValue {S = $"SAGA#{sagaData.Id}"}}, //Sort key
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
                    TableName = configuration.TableName,
                }
            });
            return Task.CompletedTask;
        }

        public Task<TSagaData> Get<TSagaData>(string propertyName, object propertyValue, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
            where TSagaData : class, IContainSagaData
        {
            // Saga ID needs to be calculated the same way as in SagaIdGenerator does
            var sagaId = DynamoDBSagaIdGenerator.Generate(typeof(TSagaData), propertyName, propertyValue);
            return Get<TSagaData>(sagaId, session, context, cancellationToken);
        }
    }
}