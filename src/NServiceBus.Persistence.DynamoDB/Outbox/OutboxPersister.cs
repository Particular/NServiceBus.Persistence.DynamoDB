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

        public OutboxPersister(IAmazonDynamoDB dynamoDbClient, OutboxPersistenceConfiguration configuration)
        {
            this.dynamoDbClient = dynamoDbClient;
            this.configuration = configuration;
        }

        public Task<IOutboxTransaction> BeginTransaction(ContextBag context,
            CancellationToken cancellationToken = default)
        {
            var transaction = new DynamoDBOutboxTransaction(dynamoDbClient, context);

            return Task.FromResult((IOutboxTransaction)transaction);
        }

        public async Task<OutboxMessage> Get(string messageId, ContextBag context,
            CancellationToken cancellationToken = default)
        {
            var allItems = new List<Dictionary<string, AttributeValue>>();
            QueryResponse response = null;
            do
            {
                var queryRequest = new QueryRequest
                {
                    ConsistentRead =
                        false, //TODO: Do we need to check the integrity of the read by counting the operations?
                    KeyConditionExpression = "PK = :incomingId",
                    ExclusiveStartKey = response?.LastEvaluatedKey,
                    ExpressionAttributeValues =
                        new Dictionary<string, AttributeValue>
                        {
                            {":incomingId", new AttributeValue {S = $"OUTBOX#{messageId}"}}
                        },
                    TableName = configuration.TableName
                };
                response = await dynamoDbClient.QueryAsync(queryRequest, cancellationToken).ConfigureAwait(false);
                allItems.AddRange(response.Items);
            } while (response.LastEvaluatedKey.Count > 0);

            if (allItems.Count == 0)
            {
                //TODO: Should we check the response code to throw if there is an error (other than 404)
                return null;
            }

            return DeserializeOutboxMessage(allItems, context);
        }

        static OutboxMessage DeserializeOutboxMessage(List<Dictionary<string, AttributeValue>> responseItems,
            ContextBag contextBag)
        {
            var headerItem = responseItems.First();
            var incomingId = headerItem["PK"].S;
            // TODO: In case we delete stuff we can probably even remove this property
            int numberOfTransportOperations = Convert.ToInt32(headerItem["TransportOperations"].N);
            contextBag.Set(OperationsCountContextProperty, numberOfTransportOperations);

            var operations = Array.Empty<TransportOperation>();
            if (numberOfTransportOperations > 0)
            {
                operations = responseItems.Skip(1).Select(DeserializeOperation).ToArray();
            }

            return new OutboxMessage(incomingId, operations);
        }

        static TransportOperation DeserializeOperation(Dictionary<string, AttributeValue> attributeValues)
        {
            var messageId = attributeValues["MessageId"].S;
            var properties = new DispatchProperties(DeserializeStringDictionary(attributeValues["Properties"]));
            var headers = DeserializeStringDictionary(attributeValues["Headers"]);
            // this is all very wasteful but good enough for the prototype
            byte[] body = attributeValues["Body"].B.ToArray();
            return new TransportOperation(messageId, properties, body, headers);
        }

        static Dictionary<string, string> DeserializeStringDictionary(AttributeValue attributeValue) =>
            attributeValue.M.ToDictionary(x => x.Key, x => x.Value.S);

        IEnumerable<TransactWriteItem> Serialize(OutboxMessage outboxMessage, ContextBag contextBag)
        {
            contextBag.Set(OperationsCountContextProperty, outboxMessage.TransportOperations.Length);

            // DynamoDB has a limit of 400 KB per item. Transport Operations are likely to be larger
            // and could easily hit the 400 KB limit of an item when all operations would be serialized into
            // the same item. This is why multiple items are written for a single outbox record. With the transact
            // write items this can be done atomically.
            yield return new TransactWriteItem()
            {
                Put = new Put
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        {"PK", new AttributeValue {S = $"OUTBOX#{outboxMessage.MessageId}"}},
                        {"SK", new AttributeValue {S = $"OUTBOX#{outboxMessage.MessageId}#0"}}, //Sort key
                        {
                            "TransportOperations",
                            new AttributeValue {N = outboxMessage.TransportOperations.Length.ToString()}
                        },
                        {"Dispatched", new AttributeValue {BOOL = false}},
                        {"DispatchedAt", new AttributeValue {NULL = true}},
                        {"ExpireAt", new AttributeValue {NULL = true}} //TTL
                    },
                    ConditionExpression = "attribute_not_exists(SK)", //Fail if already exists
                    TableName = configuration.TableName,
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
                            {"PK", new AttributeValue {S = $"OUTBOX#{outboxMessage.MessageId}"}},
                            {"SK", new AttributeValue {S = $"OUTBOX#{outboxMessage.MessageId}#{n}"}}, //Sort key
                            {"Dispatched", new AttributeValue {BOOL = false}},
                            {"DispatchedAt", new AttributeValue {NULL = true}},
                            {"MessageId", new AttributeValue {S = operation.MessageId}},
                            // TODO: Make this better in terms of allocations?
                            {
                                "Properties",
                                new AttributeValue
                                {
                                    M = SerializeStringDictionary(operation.Options ?? new DispatchProperties()),
                                    IsMSet = true
                                }
                            },
                            {
                                "Headers",
                                new AttributeValue
                                {
                                    M = SerializeStringDictionary(operation.Headers ??
                                                                  new Dictionary<string, string>()),
                                    IsMSet = true
                                }
                            },
                            {"Body", new AttributeValue {B = bodyStream}},
                            {"ExpireAt", new AttributeValue {NULL = true}} //TTL
                        },
                        ConditionExpression = "attribute_not_exists(SK)", // Fail if already exists
                        TableName = configuration.TableName
                    }
                };
                n++;
            }
        }

        static Dictionary<string, AttributeValue> SerializeStringDictionary(Dictionary<string, string> value)
        {
            return value.ToDictionary(x => x.Key, x => new AttributeValue { S = x.Value });
        }

        public Task Store(OutboxMessage message, IOutboxTransaction transaction, ContextBag context,
            CancellationToken cancellationToken = default)
        {
            var outboxTransaction = (DynamoDBOutboxTransaction)transaction;

            outboxTransaction.StorageSession.AddRange(Serialize(message, context));

            return Task.CompletedTask;
        }

        public async Task SetAsDispatched(string messageId, ContextBag context,
            CancellationToken cancellationToken = default)
        {
            var opsCount = context.Get<int>(OperationsCountContextProperty);

            var now = DateTime.UtcNow;
            var expirationTime = now.Add(configuration.TimeToLive);
            int epochSeconds = AWSSDKUtils.ConvertToUnixEpochSeconds(expirationTime);

            var writeRequests = new List<WriteRequest>(opsCount + 1)
            {
                new()
                {
                    PutRequest = new PutRequest
                    {
                        Item = new Dictionary<string, AttributeValue>
                        {
                            {"PK", new AttributeValue {S = $"OUTBOX#{messageId}"}},
                            {"SK", new AttributeValue {S = $"OUTBOX#{messageId}#0"}}, //Sort key
                            {"TransportOperations", new AttributeValue {N = "0"}},
                            {"Dispatched", new AttributeValue {BOOL = true}},
                            {"DispatchedAt", new AttributeValue {S = now.ToString("s")}},
                            {"ExpireAt", new AttributeValue {N = epochSeconds.ToString()}}
                        },
                    }
                }
            };

            for (int i = 1; i <= opsCount; i++)
            {
                writeRequests.Add(new WriteRequest
                {
                    DeleteRequest = new DeleteRequest
                    {
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {"PK", new AttributeValue {S = $"OUTBOX#{messageId}"}},
                            {"SK", new AttributeValue {S = $"OUTBOX#{messageId}#{i}"}}, //Sort key
                        }
                    }
                });
            }

            // The idea here is to use batch write requests instead of transact write item requests because
            // transactions come with a cost. They cost double the amount of write units compared to batch writes.
            // Setting the outbox record as dispatch is an idempotent operation that doesn't require transactionality
            // so using the cheaper API in terms of write operations is a sane choice.
            var batchWriteItemRequest = new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    { configuration.TableName, writeRequests }
                },
            };
            await dynamoDbClient.BatchWriteItemAsync(batchWriteItemRequest, cancellationToken).ConfigureAwait(false);
        }

        readonly IAmazonDynamoDB dynamoDbClient;
        readonly OutboxPersistenceConfiguration configuration;
    }
}