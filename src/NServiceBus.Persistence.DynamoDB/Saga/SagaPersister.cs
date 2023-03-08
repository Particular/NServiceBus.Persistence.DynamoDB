namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.DataModel;
    using Amazon.DynamoDBv2.DocumentModel;
    using Amazon.DynamoDBv2.Model;
    using Extensibility;
    using Sagas;

    class SagaPersister : ISagaPersister
    {
        readonly SagaPersistenceConfiguration configuration;
        readonly IAmazonDynamoDB dynamoDbClient;
        readonly DynamoDBContext dynamoDbContext;

        public SagaPersister(SagaPersistenceConfiguration configuration, IAmazonDynamoDB dynamoDbClient)
        {
            this.configuration = configuration;
            this.dynamoDbClient = dynamoDbClient;
            dynamoDbContext = new DynamoDBContext(this.dynamoDbClient);
        }

        public async Task<TSagaData> Get<TSagaData>(Guid sagaId, ISynchronizedStorageSession session, ContextBag context, CancellationToken cancellationToken = default) where TSagaData : class, IContainSagaData
        {
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

        TSagaData Deserialize<TSagaData>(Dictionary<string, AttributeValue> attributeValues, ContextBag context) where TSagaData : class, IContainSagaData
        {
            var document = Document.FromAttributeMap(attributeValues);
            var sagaData = (TSagaData)dynamoDbContext.ConvertFromDocument(document, typeof(TSagaData));
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
                    ConditionExpression = "attribute_not_exists(#SK)", //Fail if already exists.
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        {"#SK", configuration.SortKeyName}
                    },
                    TableName = configuration.TableName,
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
                    TableName = configuration.TableName,
                }
            });
            return Task.CompletedTask;
        }

        Dictionary<string, AttributeValue> Serialize(IContainSagaData sagaData, int version)
        {
            var doc = dynamoDbContext.ConvertToDocument(sagaData, sagaData.GetType());
            var map = doc.ToAttributeMap();
            map.Add(configuration.PartitionKeyName, new AttributeValue { S = $"SAGA#{sagaData.Id}" });
            map.Add(configuration.SortKeyName, new AttributeValue { S = $"SAGA#{sagaData.Id}" });  //Sort key
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
                        {configuration.PartitionKeyName, new AttributeValue {S = $"SAGA#{sagaData.Id}"}},
                        {configuration.SortKeyName, new AttributeValue {S = $"SAGA#{sagaData.Id}"}}, //Sort key
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