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

        public OutboxPersister(IAmazonDynamoDB dynamoDbClient, string tableName, TimeSpan expirationPeriod)
        {
            this.dynamoDbClient = dynamoDbClient;
            this.tableName = tableName;
            this.expirationPeriod = expirationPeriod;
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
                    TableName = tableName
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
            int numberOfTransportOperations = Convert.ToInt32(headerItem["TranportOperations"].N);
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

            if (!attributeValues["Body"].B.TryGetBuffer(out var segment))
            {
                //throw new Exception("Cannot get buffer from the body stream.");
            }

            return new TransportOperation(messageId, properties, segment, headers);
        }

        static Dictionary<string, string> DeserializeStringDictionary(AttributeValue attributeValue) =>
            attributeValue.M.ToDictionary(x => x.Key, x => x.Value.S);

        IEnumerable<TransactWriteItem> Serialize(OutboxMessage outboxMessage, ContextBag contextBag)
        {
            contextBag.Set(OperationsCountContextProperty, outboxMessage.TransportOperations.Length);

            yield return new TransactWriteItem()
            {
                Put = new Put
                {
                    Item = new Dictionary<string, AttributeValue>
                    {
                        {"PK", new AttributeValue {S = $"OUTBOX#{outboxMessage.MessageId}"}},
                        {"SK", new AttributeValue {S = $"OUTBOX#{outboxMessage.MessageId}#0"}}, //Sort key
                        {
                            "TranportOperations",
                            new AttributeValue {N = outboxMessage.TransportOperations.Length.ToString()}
                        },
                        {"Dispatched", new AttributeValue {BOOL = false}},
                        {"DispatchedAt", new AttributeValue {NULL = true}},
                        {"ExpireAt", new AttributeValue {NULL = true}} //TTL
                    },
                    ConditionExpression = "attribute_not_exists(SK)",
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
                        ConditionExpression = "attribute_not_exists(SK)",
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
            var expirationTime = now.Add(expirationPeriod);
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
                            {"TranportOperations", new AttributeValue {N = "0"}},
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

            var batchWriteItemRequest = new BatchWriteItemRequest
            {
                RequestItems = new Dictionary<string, List<WriteRequest>>
                {
                    { tableName, writeRequests }
                },
            };
            await dynamoDbClient.BatchWriteItemAsync(batchWriteItemRequest, cancellationToken).ConfigureAwait(false);
        }

        readonly IAmazonDynamoDB dynamoDbClient;
        readonly string tableName;
        readonly TimeSpan expirationPeriod;

        internal static readonly string SchemaVersion = "1.0.0";
    }
}