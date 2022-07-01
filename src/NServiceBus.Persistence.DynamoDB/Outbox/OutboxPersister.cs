namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
    using Extensibility;
    using Outbox;
    using Transport;
    using TransportOperation = Outbox.TransportOperation;

    class OutboxPersister : IOutboxStorage
    {
        public OutboxPersister(AmazonDynamoDBClient dynamoDbClient, string tableName, int ttlInSeconds)
        {
            this.dynamoDbClient = dynamoDbClient;
            this.tableName = tableName;
            this.ttlInSeconds = ttlInSeconds;
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

            return DeserializeOutboxMessage(response.Items);
        }

        static OutboxMessage DeserializeOutboxMessage(List<Dictionary<string, AttributeValue>> responseItems)
        {
            var headerItem = responseItems.First(x => x.ContainsKey("Dispatched"));
            var incomingId = headerItem["Id"].S;

            var operations = responseItems.Where(x => !x.ContainsKey("Dispatched"))
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
                yield return new TransactWriteItem
                {
                    Put = new Put
                    {
                        Item = new Dictionary<string, AttributeValue>
                        {
                            {"Id", new AttributeValue{ S = outboxMessage.MessageId}},
                            {"Index", new AttributeValue{ N = n.ToString()}}, //Sort key
                            {"MessageId", new AttributeValue{ S = operation.MessageId}},
                            {"Properties", new AttributeValue{ M = SerializeStringDictionary(operation.Options)}},
                            {"Headers", new AttributeValue{ M = SerializeStringDictionary(operation.Headers)}},
                            {"Body", new AttributeValue{ B = operation}}, //TODO: Need a memory stream over read-only memory
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


            outboxTransaction.StorageSession.Add(new TransactWriteItem
            {
                Put = 
            });

            return outboxTransaction.Commit(cancellationToken);
        }

        public Task SetAsDispatched(string messageId, ContextBag context, CancellationToken cancellationToken = default)
        {
            return Task.CompletedTask;
        }

#pragma warning disable IDE0052
        readonly AmazonDynamoDBClient dynamoDbClient;
        readonly string tableName;
        readonly int ttlInSeconds;
#pragma warning restore IDE0052

        internal static readonly string SchemaVersion = "1.0.0";
    }
}