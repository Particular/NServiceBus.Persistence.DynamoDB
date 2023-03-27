namespace NServiceBus.Persistence.DynamoDB
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
    using Logging;
    using Outbox;
    using Transport;
    using TransportOperation = Outbox.TransportOperation;

    class OutboxPersister : IOutboxStorage
    {
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

        public async Task<OutboxMessage?> Get(string messageId, ContextBag context,
            CancellationToken cancellationToken = default)
        {
            var queryRequest = new QueryRequest
            {
                ConsistentRead = true,
                KeyConditionExpression = $"#PK = :outboxId",
                ExpressionAttributeNames =
                    new Dictionary<string, string> { { "#PK", configuration.Table.PartitionKeyName } },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":outboxId", new AttributeValue { S = $"OUTBOX#{endpointIdentifier}#{messageId}" } }
                },
                TableName = configuration.Table.TableName
            };
            QueryResponse? response = null;
            int numberOfTransportOperations = 0;
            bool? foundOutboxMetadataEntry = null;
            List<Dictionary<string, AttributeValue>>? transportOperationsAttributes = null;
            do
            {
                queryRequest.ExclusiveStartKey = response?.LastEvaluatedKey;
                response = await dynamoDbClient.QueryAsync(queryRequest, cancellationToken).ConfigureAwait(false);
                bool responseItemsHasOutboxMetadataEntry = false;
                if (foundOutboxMetadataEntry == null && response.Items.Count >= 1)
                {
                    foundOutboxMetadataEntry = true;
                    var headerItem = response.Items[0];
                    // In case the metadata is not marked as dispatched we want to know the number of transport operations
                    // in order to pre-populate the lists etc accordingly
                    if (!headerItem["Dispatched"].BOOL)
                    {
                        numberOfTransportOperations = Convert.ToInt32(headerItem["TransportOperationsCount"].N);
                    }
                    responseItemsHasOutboxMetadataEntry = true;
                }

                for (int i = responseItemsHasOutboxMetadataEntry ? 1 : 0; i < response.Items.Count; i++)
                {
                    transportOperationsAttributes ??= new List<Dictionary<string, AttributeValue>>(numberOfTransportOperations);
                    transportOperationsAttributes.Add(response.Items[i]);
                }
            } while (response.LastEvaluatedKey.Count > 0);

            return foundOutboxMetadataEntry == null ?
                //TODO: Should we check the response code to throw if there is an error (other than 404)
                null : DeserializeOutboxMessage(messageId, numberOfTransportOperations, transportOperationsAttributes, context);
        }

        OutboxMessage? DeserializeOutboxMessage(string messageId,
            int numberOfTransportOperations,
            List<Dictionary<string, AttributeValue>>? transportOperationsAttributes, ContextBag contextBag)
        {
            // Using numberOfTransportOperations instead of transportOperationsAttributes.Count to account for
            // potential partial deletes
            contextBag.Set($"dynamo_operations_count:{messageId}", numberOfTransportOperations);

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

        IReadOnlyCollection<TransactWriteItem> Serialize(OutboxMessage outboxMessage, ContextBag contextBag)
        {
            contextBag.Set($"dynamo_operations_count:{outboxMessage.MessageId}", outboxMessage.TransportOperations.Length);

            // DynamoDB has a limit of 400 KB per item. Transport Operations are likely to be larger
            // and could easily hit the 400 KB limit of an item when all operations would be serialized into
            // the same item. This is why multiple items are written for a single outbox record. With the transact
            // write items this can be done atomically.
            var transactWriteItems = new List<TransactWriteItem>(outboxMessage.TransportOperations.Length + 1)
            {
                new TransactWriteItem()
                {
                    Put = new Put
                    {
                        Item = new Dictionary<string, AttributeValue>
                        {
                            {
                                configuration.Table.PartitionKeyName,
                                new AttributeValue { S = $"OUTBOX#{endpointIdentifier}#{outboxMessage.MessageId}" }
                            },
                            {
                                configuration.Table.SortKeyName,
                                new AttributeValue { S = $"OUTBOX#METADATA#{outboxMessage.MessageId}" }
                            },
                            {
                                "TransportOperationsCount",
                                new AttributeValue { N = outboxMessage.TransportOperations.Length.ToString() }
                            },
                            { "Dispatched", new AttributeValue { BOOL = false } },
                            { "DispatchedAt", new AttributeValue { NULL = true } },
                            { configuration.Table.TimeToLiveAttributeName!, new AttributeValue { NULL = true } },
                        },
                        ConditionExpression = "attribute_not_exists(#SK)", //Fail if already exists
                        ExpressionAttributeNames =
                            new Dictionary<string, string> { { "#SK", configuration.Table.SortKeyName } },
                        TableName = configuration.Table.TableName,
                    }
                }
            };

            var n = 1;
            foreach (var operation in outboxMessage.TransportOperations)
            {
                var bodyStream = new ReadOnlyMemoryStream(operation.Body);
                transactWriteItems.Add(new TransactWriteItem
                {
                    Put = new Put
                    {
                        Item = new Dictionary<string, AttributeValue>
                        {
                            {configuration.Table.PartitionKeyName, new AttributeValue {S = $"OUTBOX#{endpointIdentifier}#{outboxMessage.MessageId}"}},
                            {configuration.Table.SortKeyName, new AttributeValue {S = $"OUTBOX#OPERATION#{outboxMessage.MessageId}#{n:D4}"}}, //Sort key
                            {"MessageId", new AttributeValue {S = operation.MessageId}},
                            {
                                "Properties",
                                new AttributeValue
                                {
                                    M = SerializeStringDictionary(operation.Options),
                                    IsMSet = true
                                }
                            },
                            {
                                "Headers",
                                new AttributeValue
                                {
                                    M = SerializeStringDictionary(operation.Headers),
                                    IsMSet = true
                                }
                            },
                            {"Body", new AttributeValue {B = bodyStream}},
                            {configuration.Table.TimeToLiveAttributeName!, new AttributeValue {NULL = true}}
                        },
                        ConditionExpression = "attribute_not_exists(#SK)", //Fail if already exists
                        ExpressionAttributeNames = new Dictionary<string, string>()
                        {
                            {"#SK", configuration.Table.SortKeyName}
                        },
                        TableName = configuration.Table.TableName
                    }
                });
                n++;
            }

            return transactWriteItems;
        }

        static Dictionary<string, AttributeValue> SerializeStringDictionary(Dictionary<string, string>? value)
        {
            if (value == null)
            {
                return new Dictionary<string, AttributeValue>(0);
            }

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
            var opsCount = context.Get<int>($"dynamo_operations_count:{messageId}");

            var now = DateTime.UtcNow;
            var expirationTime = now.Add(configuration.TimeToLive);
            int epochSeconds = AWSSDKUtils.ConvertToUnixEpochSeconds(expirationTime);

            var updateItem = new UpdateItemRequest
            {
                Key = new Dictionary<string, AttributeValue>
                {
                    {configuration.Table.PartitionKeyName, new AttributeValue {S = $"OUTBOX#{endpointIdentifier}#{messageId}"}},
                    {configuration.Table.SortKeyName, new AttributeValue {S = $"OUTBOX#METADATA#{messageId}"}}, //Sort key
                },
                UpdateExpression = "SET #dispatched = :dispatched, #dispatched_at = :dispatched_at, #ttl = :ttl",
                ExpressionAttributeNames = new Dictionary<string, string>
                {
                    {"#dispatched", "Dispatched"},
                    {"#dispatched_at", "DispatchedAt"},
                    {"#ttl", configuration.Table.TimeToLiveAttributeName!},
                },
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
                {
                    { ":dispatched", new AttributeValue {BOOL = true} },
                    { ":dispatched_at", new AttributeValue {S = now.ToString("s")} },
                    { ":ttl", new AttributeValue {N = epochSeconds.ToString()} },
                },
                TableName = configuration.Table.TableName,
                ReturnValues = ReturnValue.NONE,
            };

            // We first try to update the metadata record, if this fails we want to roll back the outbox message
            // Passing in the cancellation token here even though it could lead to more phantom transport operation
            // records.
            await dynamoDbClient.UpdateItemAsync(updateItem, cancellationToken).ConfigureAwait(false);

            // Next we do a best effort batch delete for the transport operation entries
            var writeRequests = new List<WriteRequest>(opsCount);
            for (int i = 1; i <= opsCount; i++)
            {
                writeRequests.Add(new WriteRequest
                {
                    DeleteRequest = new DeleteRequest
                    {
                        Key = new Dictionary<string, AttributeValue>
                        {
                            {configuration.Table.PartitionKeyName, new AttributeValue {S = $"OUTBOX#{endpointIdentifier}#{messageId}"}},
                            {configuration.Table.SortKeyName, new AttributeValue {S = $"OUTBOX#OPERATION#{messageId}#{i:D4}"}}, //Sort key
                        }
                    }
                });
            }

            // The idea here is to use batch write requests instead of transact write item requests because
            // transactions come with a cost. They cost double the amount of write units compared to batch writes.
            // Setting the outbox record as dispatch is an idempotent operation that doesn't require transactionality
            // so using the cheaper API in terms of write operations is a sane choice.
            var writeRequestBatches = WriteRequestBatcher.Batch(writeRequests);

            await dynamoDbClient.BatchWriteItemWithRetries(writeRequestBatches, configuration, Logger,
                    cancellationToken: cancellationToken)
                .ConfigureAwait(false);
        }

        readonly IAmazonDynamoDB dynamoDbClient;
        readonly OutboxPersistenceConfiguration configuration;
        readonly string endpointIdentifier;
        readonly ConditionalWeakTable<DispatchProperties, ReturnBuffer> bufferTracking = new();
        static readonly ILog Logger = LogManager.GetLogger<OutboxPersister>();

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

            byte[]? buffer;
        }
    }
}