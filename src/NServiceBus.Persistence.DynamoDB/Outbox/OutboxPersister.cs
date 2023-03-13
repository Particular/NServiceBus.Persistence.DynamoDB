﻿namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Buffers;
    using System.Collections.Generic;
    using System.IO;
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
            var queryRequest = new QueryRequest
            {
                ConsistentRead = true,
                KeyConditionExpression = $"#PK = :outboxId",
                ExpressionAttributeNames =
                    new Dictionary<string, string> { { "#PK", configuration.PartitionKeyName } },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":outboxId", new AttributeValue { S = $"OUTBOX#{endpointIdentifier}#{messageId}" } }
                },
                TableName = configuration.TableName
            };
            QueryResponse response = null;
            int numberOfTransportOperations = 0;
            bool? foundHeaderEntry = null;
            List<Dictionary<string, AttributeValue>> transportOperationsAttributes = null;
            do
            {
                queryRequest.ExclusiveStartKey = response?.LastEvaluatedKey;
                response = await dynamoDbClient.QueryAsync(queryRequest, cancellationToken).ConfigureAwait(false);
                bool hasOutboxHeaderEntry = false;
                if (foundHeaderEntry == null && response.Items.Count >= 1)
                {
                    foundHeaderEntry = true;
                    var headerItem = response.Items[0];
                    numberOfTransportOperations = Convert.ToInt32(headerItem["NumberTransportOperations"].N);
                    hasOutboxHeaderEntry = true;
                }

                for (int i = hasOutboxHeaderEntry ? 1 : 0; i < response.Items.Count; i++)
                {
                    transportOperationsAttributes ??= new List<Dictionary<string, AttributeValue>>(numberOfTransportOperations);
                    transportOperationsAttributes.Add(response.Items[i]);
                }
            } while (response.LastEvaluatedKey.Count > 0);

            return foundHeaderEntry == null ?
                //TODO: Should we check the response code to throw if there is an error (other than 404)
                null : DeserializeOutboxMessage(messageId, numberOfTransportOperations, transportOperationsAttributes, context);
        }

        OutboxMessage DeserializeOutboxMessage(string messageId, int numberOfTransportOperations,
            List<Dictionary<string, AttributeValue>> transportOperationsAttributes, ContextBag contextBag)
        {
            // Using numberOfTransportOperations instead of transportOperationsAttributes.Count to account for
            // potential partial deletes
            contextBag.Set(OperationsCountContextProperty, numberOfTransportOperations);

            var operations = numberOfTransportOperations == 0
                ? Array.Empty<TransportOperation>()
                : new TransportOperation[numberOfTransportOperations];

            for (int i = 0; i < numberOfTransportOperations; i++)
            {
                operations[i] = DeserializeOperation(transportOperationsAttributes![i]);
            }

            return new OutboxMessage(messageId, operations);
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

        static Dictionary<string, string> DeserializeStringDictionary(AttributeValue attributeValue)
        {
            Dictionary<string, AttributeValue> attributeValues = attributeValue.M;
            var dictionary = new Dictionary<string, string>(attributeValues.Count);
            foreach (var pair in attributeValues)
            {
                dictionary.Add(pair.Key, pair.Value.S);
            }
            return dictionary;
        }

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
                            "NumberTransportOperations",
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
            var attributeValues = new Dictionary<string, AttributeValue>(value.Count);
            foreach (KeyValuePair<string, string> pair in value)
            {
                attributeValues.Add(pair.Key, new AttributeValue(pair.Value));
            }
            return attributeValues;
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
                            {"NumberTransportOperations", new AttributeValue {N = "0"}},
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
            // TODO: Cleanup this code
            if (writeRequests.Count < 25)
            {
                var batchWriteItemRequest = new BatchWriteItemRequest
                {
                    RequestItems = new Dictionary<string, List<WriteRequest>>
                    {
                        { configuration.TableName, writeRequests }
                    },
                };
                await dynamoDbClient.BatchWriteItemAsync(batchWriteItemRequest, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                var maxWriteRequests = new List<WriteRequest>(25);
                var batchWriteItemRequest = new BatchWriteItemRequest
                {
                    RequestItems = new Dictionary<string, List<WriteRequest>>
                    {
                        { configuration.TableName, writeRequests }
                    },
                };
                for (int i = 0; i < writeRequests.Count; i++)
                {
                    var request = writeRequests[i];
                    if (i != 0 && i % 25 == 0)
                    {
                        batchWriteItemRequest.RequestItems[configuration.TableName] = maxWriteRequests;
                        await dynamoDbClient.BatchWriteItemAsync(batchWriteItemRequest, cancellationToken).ConfigureAwait(false);
                        maxWriteRequests.Clear();
                    }
                    maxWriteRequests.Add(request);
                }

                if (maxWriteRequests.Count > 0)
                {
                    batchWriteItemRequest.RequestItems[configuration.TableName] = maxWriteRequests;
                    await dynamoDbClient.BatchWriteItemAsync(batchWriteItemRequest, cancellationToken).ConfigureAwait(false);
                }
            }
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