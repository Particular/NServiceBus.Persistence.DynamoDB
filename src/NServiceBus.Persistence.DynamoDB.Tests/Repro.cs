namespace NServiceBus.Persistence.DynamoDB.Tests;

using System.IO;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Extensibility;
using NUnit.Framework;

[TestFixture]
public class Repro
{
    [Test]
    public async Task Should_load_paginated_outbox_operations()
    {
        string messageId = Guid.NewGuid().ToString();
        var tableConfig = new OutboxPersistenceConfiguration();
        var mockDynamoDbClient = new MockDynamoDBClient();
        var responses = new Queue<QueryResponse>();
        // First response contains metadata entry but no transport operation
        responses.Enqueue(new QueryResponse
        {
            Items = new List<Dictionary<string, AttributeValue>>()
            {
                {
                    new Dictionary<string, AttributeValue>()
                    {
                        {
                            tableConfig.Table.SortKeyName, new AttributeValue($"OUTBOX#METADATA#{messageId}")
                        },
                        { "Dispatched", new AttributeValue() { BOOL = false } },
                        { "OperationsCount", new AttributeValue() { N = "1" } }
                    }
                }
            },
            LastEvaluatedKey = new Dictionary<string, AttributeValue>() { { "idk", new AttributeValue("idk") } }
        });
        responses.Enqueue(new QueryResponse()
        {
            Items = new List<Dictionary<string, AttributeValue>>()
            {
                {
                    new Dictionary<string, AttributeValue>()
                    {
                        { "MessageId", new AttributeValue(Guid.NewGuid().ToString())},
                        { "Properties", new AttributeValue() { M = new Dictionary<string, AttributeValue>(0)} },
                        { "Headers", new AttributeValue() { M = new Dictionary<string, AttributeValue>(0)} },
                        { "Body", new AttributeValue() { B = new MemoryStream() } }
                    }
                }
            },
        });
        mockDynamoDbClient.QueryRequestResponse = request => responses.Dequeue();
        var storage = new OutboxPersister(mockDynamoDbClient, new OutboxPersistenceConfiguration(), "bla");
        var result = await storage.Get(messageId, new ContextBag());

        Assert.NotNull(result);
        Assert.AreEqual(1, result.TransportOperations.Length);
    }
}