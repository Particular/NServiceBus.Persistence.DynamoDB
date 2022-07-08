namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.DocumentModel;
    using Amazon.DynamoDBv2.Model;
    using Extensibility;
    using Newtonsoft.Json;
    using Sagas;

    class SagaPersister : ISagaPersister
    {
        const string SagaVersionContextPropertyPrefix = "NServiceBus.Persistence.DynamoDB.SagaVersion_";

        readonly SagaPersistenceConfiguration options;
        readonly IAmazonDynamoDB dynamoDbClient;
        readonly JsonSerializerSettings serializerSettings;

        public SagaPersister(SagaPersistenceConfiguration options, IAmazonDynamoDB dynamoDbClient)
        {
            this.options = options;
            this.dynamoDbClient = dynamoDbClient;
            // TODO we might want to make this configurable
            serializerSettings = new JsonSerializerSettings
            {
                TypeNameHandling = TypeNameHandling.None
            };
        }

        public async Task<TSagaData> Get<TSagaData>(Guid sagaId, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default) where TSagaData : class, IContainSagaData
        {
            var queryRequest = new QueryRequest
            {
                ConsistentRead =
                    false, //TODO: Do we need to check the integrity of the read by counting the operations?
                KeyConditionExpression = "PK = :sagaId and SK = :sagaId", //TODO: Allow users to override PK
                ExpressionAttributeValues =
                    new Dictionary<string, AttributeValue>
                    {
                            {":sagaId", new AttributeValue {S = $"SAGA#{sagaId}"}}
                    },
                TableName = options.TableNameCallback(typeof(TSagaData))
            };
            var response = await dynamoDbClient.QueryAsync(queryRequest, cancellationToken).ConfigureAwait(false);
            if (response.Count == 0)
            {
                return default;
            }

            var (sagaData, version) = Deserialize<TSagaData>(response.Items[0]); //TODO: Should we check if we get more than one item?
            context.Set(SagaVersionContextPropertyPrefix + typeof(TSagaData).FullName, version); //To allow for multiple sagas handling the same message
            return sagaData;
        }

        (TSagaData, int) Deserialize<TSagaData>(Dictionary<string, AttributeValue> attributeValues) where TSagaData : class, IContainSagaData
        {
            var document = Document.FromAttributeMap(attributeValues);
            var sagaDataAsJson = document.ToJson();
            // All this is super allocation heavy. But for a first version that is OK
            var sagaData = JsonConvert.DeserializeObject<TSagaData>(sagaDataAsJson, serializerSettings);
            var version = int.Parse(attributeValues["Version"].N);
            return (sagaData, version);
        }

        public Task Save(IContainSagaData sagaData, SagaCorrelationProperty correlationProperty, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            session.DynamoDBPersistenceSession().Add(new TransactWriteItem
            {
                Put = new Put
                {
                    Item = Serialize(sagaData, 0),
                    ConditionExpression = "attribute_not_exists(SK)", //Fail if already exists.
                    TableName = options.TableNameCallback(sagaData.GetType()),
                }
            });
            return Task.CompletedTask;
        }

        public Task Update(IContainSagaData sagaData, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            var version = context.Get<int>(SagaVersionContextPropertyPrefix + sagaData.GetType().FullName);

            session.DynamoDBPersistenceSession().Add(new TransactWriteItem
            {
                Put = new Put
                {
                    Item = Serialize(sagaData, version),
                    ConditionExpression = "TODO: Add optimistic concurrency check", //Fail if already exists.
                    TableName = options.TableNameCallback(sagaData.GetType()),
                }
            });
            return Task.CompletedTask;
        }

        Dictionary<string, AttributeValue> Serialize(IContainSagaData sagaData, int version)
        {
            // All this is super allocation heavy. But for a first version that is OK
            var sagaDataJson = JsonConvert.SerializeObject(sagaData, serializerSettings);
            var doc = Document.FromJson(sagaDataJson);
            var map = doc.ToAttributeMap();
            map.Add("PK", new AttributeValue { S = $"SAGA#{sagaData.Id}" });
            map.Add("SK", new AttributeValue { S = $"SAGA#{sagaData.Id}" });  //Sort key
            // Version should probably be properly moved into metadata to not clash with existing things
            map.Add("Version", new AttributeValue { N = version.ToString() });
            // According to the best practices we should also add Type information probably here
            return map;
        }

        public Task Complete(IContainSagaData sagaData, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default)
        {
            var version = context.Get<int>(SagaVersionContextPropertyPrefix + sagaData.GetType().FullName);

            session.DynamoDBPersistenceSession().Add(new TransactWriteItem
            {
                Delete = new Delete()
                {
                    Key = new Dictionary<string, AttributeValue>
                    {
                        {"PK", new AttributeValue {S = $"SAGA#{sagaData.Id}"}},
                        {"SK", new AttributeValue {S = $"SAGA#{sagaData.Id}"}}, //Sort key
                    },
                    ConditionExpression = "TODO: Add optimistic concurrency check", //Fail if already exists.
                    TableName = options.TableNameCallback(sagaData.GetType()),
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