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
        readonly SagaPersistenceConfiguration options;
        readonly IAmazonDynamoDB dynamoDbClient;

        public SagaPersister(SagaPersistenceConfiguration options, IAmazonDynamoDB dynamoDbClient)
        {
            this.options = options;
            this.dynamoDbClient = dynamoDbClient;
        }

        public async Task<TSagaData> Get<TSagaData>(Guid sagaId, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default) where TSagaData : class, IContainSagaData
        {
            var getItemRequest = new GetItemRequest
            {
                ConsistentRead =
                    false, //TODO: Do we need to check the integrity of the read by counting the operations?
                // TODO make this configurable
                Key = new Dictionary<string, AttributeValue>
                    {
                        { "PK", new AttributeValue { S = $"SAGA#{sagaId}" } },
                        { "SK", new AttributeValue { S = $"SAGA#{sagaId}" } }, //Sort key
                    },
                TableName = options.TableName
            };

            var response = await dynamoDbClient.GetItemAsync(getItemRequest, cancellationToken).ConfigureAwait(false);
            return !response.IsItemSet ? default : Deserialize<TSagaData>(response.Item, context);
        }

        TSagaData Deserialize<TSagaData>(Dictionary<string, AttributeValue> attributeValues, ContextBag context) where TSagaData : class, IContainSagaData
        {
            var document = Document.FromAttributeMap(attributeValues);
            var sagaDataAsJson = document.ToJson();
            // All this is super allocation heavy. But for a first version that is OK
            var sagaData = JsonSerializer.Deserialize<TSagaData>(sagaDataAsJson);
            var currentVersion = int.Parse(attributeValues["___VERSION___"].N);
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
                    ConditionExpression = "attribute_not_exists(SK)", //Fail if already exists.
                    TableName = options.TableName,
                }
            });
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
                        { "#v", "___VERSION___" }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":cv", new AttributeValue { N = currentVersion.ToString() } }
                    },
                    TableName = options.TableName,
                }
            });
            return Task.CompletedTask;
        }

        Dictionary<string, AttributeValue> Serialize(IContainSagaData sagaData, int version)
        {
            // All this is super allocation heavy. But for a first version that is OK
            var sagaDataJson = JsonSerializer.Serialize(sagaData, sagaData.GetType());
            var doc = Document.FromJson(sagaDataJson);
            var map = doc.ToAttributeMap();
            map.Add("PK", new AttributeValue { S = $"SAGA#{sagaData.Id}" });
            map.Add("SK", new AttributeValue { S = $"SAGA#{sagaData.Id}" });  //Sort key
            // Version should probably be properly moved into metadata to not clash with existing things
            map.Add("___VERSION___", new AttributeValue { N = version.ToString() });
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
                        {"PK", new AttributeValue {S = $"SAGA#{sagaData.Id}"}},
                        {"SK", new AttributeValue {S = $"SAGA#{sagaData.Id}"}}, //Sort key
                    },
                    ConditionExpression = "#v = :cv", // fail if modified in the meantime
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        { "#v", "___VERSION___" }
                    },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":cv", new AttributeValue { N = currentVersion.ToString() } }
                    },
                    TableName = options.TableName,
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