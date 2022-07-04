namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
    using Amazon.Util;
    using Extensibility;
    using Outbox;
    using Transport;
    using TransportOperation = Outbox.TransportOperation;

    class OutboxPersister : IOutboxStorage
    {
        const string OperationsCountContextProperty = "NServiceBus.Persistence.DynamoDB.OutboxOperationsCount";

        public OutboxPersister(AmazonDynamoDBClient dynamoDbClient, string tableName, TimeSpan expirationPeriod)
        {
            this.dynamoDbClient = dynamoDbClient;
            this.tableName = tableName;
            this.expirationPeriod = expirationPeriod;
        }

        public Task<IOutboxTransaction> BeginTransaction(ContextBag context, CancellationToken cancellationToken = default)
        {
            var transaction = new DynamoDBOutboxTransaction(context);

            return Task.FromResult((IOutboxTransaction)transaction);
        }

        public async Task<OutboxMessage> Get(string messageId, ContextBag context, CancellationToken cancellationToken = default)
        {
            var queryRequest = new QueryRequest
            {
                ConsistentRead =
                    false, //TODO: Do we need to check the integrity of the read by counting the operations?
                KeyConditionExpression = "Id = :incomingId",
                ExpressionAttributeValues =
                    new Dictionary<string, AttributeValue> { { ":incomingId", new AttributeValue { S = messageId } } },
                TableName = tableName
            };
            var response = await dynamoDbClient.QueryAsync(queryRequest, cancellationToken).ConfigureAwait(false);

            if (response.Count == 0)
            {
                //TODO: Should we check the response code to throw if there is an error (other than 404)
                return null;
            }

            context.Set(OperationsCountContextProperty, response.Count);

            return DeserializeOutboxMessage(response.Items);
        }

        static OutboxMessage DeserializeOutboxMessage(List<Dictionary<string, AttributeValue>> responseItems)
        {
            var headerItem = responseItems.First(x => x["Index"].N == "0");
            var incomingId = headerItem["Id"].S;

            var operations = responseItems.Where(x => x["Index"].N != "0")
                .Select(DeserializeOperation).ToArray();

            return new OutboxMessage(incomingId, operations);
        }

        static TransportOperation DeserializeOperation(Dictionary<string, AttributeValue> attributeValues)
        {
            var messageId = attributeValues["MessageId"].S;
            var properties = new DispatchProperties(DeserializeStringDictionary(attributeValues["Properties"]));
            var headers = DeserializeStringDictionary(attributeValues["Headers"]);
            if (!attributeValues["Body"].B.TryGetBuffer(out var segment))
            {
                throw new Exception("Cannot get buffer from the body stream.");
            }

            return new TransportOperation(messageId, properties, segment, headers);
        }

        static Dictionary<string, string> DeserializeStringDictionary(AttributeValue attributeValue) =>
            attributeValue.M.ToDictionary(x => x.Key, x => x.Value.S);

        IEnumerable<TransactWriteItem> Serialize(OutboxMessage outboxMessage)
        {
            yield return new TransactWriteItem()
            {
                Put = new Put() //TODO: Do we need to additionally ensure it did not exist?
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        {"Id", new AttributeValue{ S = outboxMessage.MessageId}},
                        {"Index", new AttributeValue{ N = "0"}}, //Sort key
                        {"Dispatched", new AttributeValue{ BOOL = false}},
                        {"DispatchedAt", new AttributeValue { NULL = true}},
                        {"ExpireAt", new AttributeValue { NULL = true}} //TTL
                    },
                    TableName = tableName,
                }
            };
            var n = 1;
            foreach (var operation in outboxMessage.TransportOperations)
            {
                var bodyStream = new MemoryStream(operation.Body.ToArray());
                yield return new TransactWriteItem
                {
                    Put = new Put
                    {
                        Item = new Dictionary<string, AttributeValue>
                        {
                            {"Id", new AttributeValue{ S = outboxMessage.MessageId}},
                            {"Index", new AttributeValue{ N = n.ToString()}}, //Sort key
                            {"Dispatched", new AttributeValue{ BOOL = false}},
                            {"DispatchedAt", new AttributeValue { NULL = true}},
                            {"MessageId", new AttributeValue{ S = operation.MessageId}},
                            {"Properties", new AttributeValue{ M = SerializeStringDictionary(operation.Options)}},
                            {"Headers", new AttributeValue{ M = SerializeStringDictionary(operation.Headers)}},
                            {"Body", new AttributeValue{ B = bodyStream}},
                            {"ExpireAt", new AttributeValue { NULL = true}} //TTL
                        },
                        TableName = tableName
                    }
                };
                n++;
            }
        }

        static Dictionary<string, AttributeValue> SerializeStringDictionary(Dictionary<string, string> value)
        {
            return value.ToDictionary(x => x.Key, x => new AttributeValue { S = x.Value });
        }

        public Task Store(OutboxMessage message, IOutboxTransaction transaction, ContextBag context, CancellationToken cancellationToken = default)
        {
            var outboxTransaction = (DynamoDBOutboxTransaction)transaction;

            outboxTransaction.StorageSession.AddRange(Serialize(message));

            return outboxTransaction.Commit(cancellationToken);
        }

        public Task SetAsDispatched(string messageId, ContextBag context, CancellationToken cancellationToken = default)
        {
            var opsCount = context.Get<int>(OperationsCountContextProperty);

            var now = DateTime.UtcNow;
            var expirationTime = now.Add(expirationPeriod);
            int epochSeconds = AWSSDKUtils.ConvertToUnixEpochSeconds(expirationTime);

            return dynamoDbClient.BatchWriteItemAsync(new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    {tableName, Enumerable.Range(0, opsCount).Select(x => new WriteRequest
                    {
                        PutRequest = new PutRequest
                        {
                            Item = new Dictionary<string, AttributeValue>
                            {
                                {"Id", new AttributeValue{ S = messageId}},
                                {"Index", new AttributeValue{ N = x.ToString()}}, //Sort key
                                {"Dispatched", new AttributeValue{ BOOL = true}},
                                {"DispatchedAt", new AttributeValue { S = now.ToString("s")}},
                                {"ExpireAt", new AttributeValue { N = epochSeconds.ToString()}}
                            },
                        }
                    }).ToList()}
                },
            }, cancellationToken);
        }

#pragma warning disable IDE0052
        readonly AmazonDynamoDBClient dynamoDbClient;
        readonly string tableName;
        readonly TimeSpan expirationPeriod;
#pragma warning restore IDE0052

        internal static readonly string SchemaVersion = "1.0.0";
    }
}