namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Runtime.CompilerServices;
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

        public OutboxPersister(IAmazonDynamoDB dynamoDbClient, OutboxPersistenceConfiguration configuration, string endpointIdentifier)
        {
            this.dynamoDbClient = dynamoDbClient;
            this.configuration = configuration;
            this.endpointIdentifier = endpointIdentifier.ToUpperInvariant();
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
                    ConsistentRead = true,
                    KeyConditionExpression = $"#PK = :outboxId",
                    ExclusiveStartKey = response?.LastEvaluatedKey,
                    ExpressionAttributeNames =
                        new Dictionary<string, string> { { "#PK", configuration.PartitionKeyName } },
                    ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                    {
                        { ":outboxId", new AttributeValue { S = $"OUTBOX#{endpointIdentifier}#{messageId}" } }
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

        OutboxMessage DeserializeOutboxMessage(List<Dictionary<string, AttributeValue>> responseItems,
            ContextBag contextBag)
        {
            var headerItem = responseItems.First();
            var incomingId = headerItem[configuration.PartitionKeyName].S;
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

        TransportOperation DeserializeOperation(Dictionary<string, AttributeValue> attributeValues)
        {
            var messageId = attributeValues["MessageId"].S;
            var properties = new DispatchProperties(DeserializeStringDictionary(attributeValues["Properties"]));
            var headers = DeserializeStringDictionary(attributeValues["Headers"]);
            var bodyMemory = GetAndTrackBodyMemory(attributeValues["Body"], properties);
            return new TransportOperation(messageId, properties, bodyMemory, headers);
        }

        ReadOnlyMemory<byte> GetAndTrackBodyMemory(AttributeValue bodyValue, DispatchProperties properties)
        {
            MemoryStream bodyStream = bodyValue.B;
            int bodyStreamLength = (int)bodyStream.Length;
            var buffer = ArrayPool<byte>.Shared.Rent(bodyStreamLength);
            var bytesRead = bodyStream.Read(buffer, 0, bodyStreamLength);
            bufferTracking.Add(properties, new ReturnBuffer(buffer));
            ReadOnlyMemory<byte> bodyMemory = buffer.AsMemory(0, bytesRead);
            return bodyMemory;
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
                        {configuration.PartitionKeyName, new AttributeValue {S = $"OUTBOX#{endpointIdentifier}#{outboxMessage.MessageId}"}},
                        {configuration.SortKeyName, new AttributeValue {S = $"OUTBOX#{outboxMessage.MessageId}#0"}}, //Sort key
                        {
                            "TransportOperations",
                            new AttributeValue {N = outboxMessage.TransportOperations.Length.ToString()}
                        },
                        {"Dispatched", new AttributeValue {BOOL = false}},
                        {"DispatchedAt", new AttributeValue {NULL = true}},
                        {configuration.TimeToLiveAttributeName, new AttributeValue {NULL = true}} //TTL
                    },
                    ConditionExpression = "attribute_not_exists(#SK)", //Fail if already exists
                    ExpressionAttributeNames = new Dictionary<string, string>
                    {
                        {"#SK", configuration.SortKeyName}
                    },
                    TableName = configuration.TableName,
                }
            };
            var n = 1;
            foreach (var operation in outboxMessage.TransportOperations)
            {
                var bodyStream = new ReadOnlyMemoryStream(operation.Body);
                yield return new TransactWriteItem
                {
                    Put = new Put
                    {
                        Item = new Dictionary<string, AttributeValue>
                        {
                            {configuration.PartitionKeyName, new AttributeValue {S = $"OUTBOX#{endpointIdentifier}#{outboxMessage.MessageId}"}},
                            {configuration.SortKeyName, new AttributeValue {S = $"OUTBOX#{outboxMessage.MessageId}#{n}"}}, //Sort key
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
                            {configuration.TimeToLiveAttributeName, new AttributeValue {NULL = true}} //TTL
                        },
                        ConditionExpression = "attribute_not_exists(#SK)", //Fail if already exists
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            {"#SK", configuration.SortKeyName}
                        },
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
                            {configuration.PartitionKeyName, new AttributeValue {S = $"OUTBOX#{endpointIdentifier}#{messageId}"}},
                            {configuration.SortKeyName, new AttributeValue {S = $"OUTBOX#{messageId}#0"}}, //Sort key
                            {"TransportOperations", new AttributeValue {N = "0"}},
                            {"Dispatched", new AttributeValue {BOOL = true}},
                            {"DispatchedAt", new AttributeValue {S = now.ToString("s")}},
                            {configuration.TimeToLiveAttributeName, new AttributeValue {N = epochSeconds.ToString()}}
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
                            {configuration.PartitionKeyName, new AttributeValue {S = $"OUTBOX#{endpointIdentifier}#{messageId}"}},
                            {configuration.SortKeyName, new AttributeValue {S = $"OUTBOX#{messageId}#{i}"}}, //Sort key
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
        readonly string endpointIdentifier;
        readonly ConditionalWeakTable<DispatchProperties, ReturnBuffer> bufferTracking = new();

        sealed class ReturnBuffer
        {
            public ReturnBuffer(byte[] buffer) => this.buffer = buffer;

            ~ReturnBuffer()
            {
                if (buffer != null)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = null;
                }
            }

            byte[] buffer;
        }
    }
}