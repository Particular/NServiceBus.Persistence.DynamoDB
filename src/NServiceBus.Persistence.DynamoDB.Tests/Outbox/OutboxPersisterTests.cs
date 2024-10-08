namespace NServiceBus.Persistence.DynamoDB.Tests;

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using NServiceBus.Extensibility;
using NUnit.Framework;

[TestFixture]
public class OutboxPersisterTests
{
    [SetUp]
    public void SetUp()
    {
        client = new MockDynamoDBClient();

        persister = new OutboxPersister(client, new OutboxPersistenceConfiguration(), "endpointIdentifier");
    }

    [TearDown]
    public void TearDown() => client.Dispose();

    [Test]
    public async Task Should_update_metadata_as_a_dedicated_non_batched_update()
    {
        var contextBag = new ContextBag();
        contextBag.Set("dynamo_operations_count:someMessageId", 10);

        await persister.SetAsDispatched("someMessageId", contextBag);

        Assert.That(client.UpdateItemRequestsSent, Has.Count.EqualTo(1));
    }

    [Test]
    public void Should_not_execute_batched_operations_when_metadata_cannot_be_updated()
    {
        var contextBag = new ContextBag();
        contextBag.Set("dynamo_operations_count:someMessageId", 10);

        client.UpdateItemRequestResponse = _ => throw new AmazonClientException("");

        Assert.ThrowsAsync<AmazonClientException>(async () => await persister.SetAsDispatched("someMessageId", contextBag));
        Assert.That(client.BatchWriteRequestsSent, Has.Count.EqualTo(0));
    }

    [Test]
    public async Task Should_execute_batched_operations()
    {
        var contextBag = new ContextBag();
        contextBag.Set("dynamo_operations_count:someMessageId", 50);

        await persister.SetAsDispatched("someMessageId", contextBag);

        Assert.That(client.BatchWriteRequestsSent, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task Should_return_null_when_no_data_present()
    {
        var contextBag = new ContextBag();

        var record = await persister.Get("someMessageId", contextBag);

        Assert.That(record, Is.Null);
    }

    [Test]
    public async Task Should_return_record_when_data_present()
    {
        client.QueryRequestResponse = r => new QueryResponse
        {
            Items = [
                new()
                {
                    { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId")},
                    { "SK", new AttributeValue("OUTBOX#METADATA#someMessageId")},
                    { "Dispatched", new AttributeValue { BOOL = false }},
                    { "OperationsCount", new AttributeValue { N = "1" }}
                },
                new()
                {
                    { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId")},
                    { "SK", new AttributeValue("OUTBOX#OPERATION#someMessageId#0000")},
                    { "MessageId", new AttributeValue("someTransportOperationId")},
                    { "Properties", new AttributeValue()},
                    { "Headers", new AttributeValue()},
                    { "Body", new AttributeValue { B = new MemoryStream(Encoding.UTF8.GetBytes("Hello World"))}},
                },
            ]
        };

        var contextBag = new ContextBag();

        var record = await persister.Get("someMessageId", contextBag);

        Assert.That(record, Is.Not.Null);
        Assert.That(record.TransportOperations, Has.Length.EqualTo(1));
    }

    [Test]
    public async Task Should_return_record_when_data_present_and_paging_needed()
    {
        var called = 0;
        client.QueryRequestResponse = r =>
        {
            called++;
            if (called == 1)
            {
                return new QueryResponse
                {
                    Items =
                    [
                        new()
                        {
                            { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId") },
                            { "SK", new AttributeValue("OUTBOX#METADATA#someMessageId") },
                            { "Dispatched", new AttributeValue { BOOL = false } },
                            { "OperationsCount", new AttributeValue { N = "2" } }
                        },
                        new()
                        {
                            { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId") },
                            { "SK", new AttributeValue("OUTBOX#OPERATION#someMessageId#0000") },
                            { "MessageId", new AttributeValue("someTransportOperationId1") },
                            { "Properties", new AttributeValue() },
                            { "Headers", new AttributeValue() },
                            { "Body", new AttributeValue { B = new MemoryStream(Encoding.UTF8.GetBytes("Hello World")) } },
                        },
                    ],
                    LastEvaluatedKey = new Dictionary<string, AttributeValue>
                    {
                        { "SomeFakeEntry", new AttributeValue() }
                    }
                };
            }

            return new QueryResponse
            {
                Items =
                [
                    new()
                    {
                        { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId") },
                        { "SK", new AttributeValue("OUTBOX#OPERATION#someMessageId#0001") },
                        { "MessageId", new AttributeValue("someTransportOperationId2") },
                        { "Properties", new AttributeValue() },
                        { "Headers", new AttributeValue() },
                        { "Body", new AttributeValue { B = new MemoryStream(Encoding.UTF8.GetBytes("Hello World")) } },
                    },
                ],
            };
        };

        var contextBag = new ContextBag();

        var record = await persister.Get("someMessageId", contextBag);

        Assert.That(record, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(record.TransportOperations, Has.Length.EqualTo(2));
            Assert.That(called, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Should_return_record_even_if_metadata_and_transport_operations_are_paged_apart()
    {
        string messageId = Guid.NewGuid().ToString();
        var responses = new Queue<QueryResponse>();
        // First response contains metadata entry but no transport operation
        responses.Enqueue(new QueryResponse
        {
            Items = new List<Dictionary<string, AttributeValue>>
            {
                {
                    new()
                    {
                        { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId")},
                        { "SK", new AttributeValue($"OUTBOX#METADATA#{messageId}") },
                        { "Dispatched", new AttributeValue { BOOL = false } },
                        { "OperationsCount", new AttributeValue { N = "1" } }
                    }
                }
            },
            LastEvaluatedKey = new Dictionary<string, AttributeValue> { { "idk", new AttributeValue("idk") } }
        });
        responses.Enqueue(new QueryResponse
        {
            Items = new List<Dictionary<string, AttributeValue>>
            {
                {
                    new()
                    {
                        { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId")},
                        { "SK", new AttributeValue("OUTBOX#OPERATION#someMessageId#0000")},
                        { "MessageId", new AttributeValue(Guid.NewGuid().ToString())},
                        { "Properties", new AttributeValue { M = []} },
                        { "Headers", new AttributeValue { M = []} },
                        { "Body", new AttributeValue { B = new MemoryStream() } }
                    }
                }
            },
        });
        client.QueryRequestResponse = _ => responses.Dequeue();

        var contextBag = new ContextBag();

        var result = await persister.Get(messageId, contextBag);

        Assert.That(result, Is.Not.Null);
        Assert.That(result.TransportOperations.Length, Is.EqualTo(1));
    }

    [Test]
    public void Should_throw_when_transport_operations_not_complete()
    {
        // There is a metadata record but no transport operations returned
        // this can happen due to read-committed isolation levels
        client.QueryRequestResponse = r => new QueryResponse
        {
            Items = [
                new()
                {
                    { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId")},
                    { "SK", new AttributeValue("OUTBOX#METADATA#someMessageId")},
                    { "Dispatched", new AttributeValue { BOOL = false }},
                    { "OperationsCount", new AttributeValue { N = "1" }}
                }
            ]
        };

        var contextBag = new ContextBag();

        var exception = Assert.ThrowsAsync<PartialOutboxResultException>(async () => await persister.Get("someMessageId", contextBag));
        Assert.That(exception!.Message, Does.Contain("Partial outbox results retrieved with 0 instead of 1 transport operations while attempting to load the outbox records for message ID 'someMessageId'."));
    }

    [Test]
    public void Should_throw_when_transport_operations_not_complete_even_when_paging()
    {
        // There is a metadata record but no transport operations returned
        // this can happen due to read-committed isolation levels
        var responses = new Queue<QueryResponse>();
        // First response contains metadata entry but no transport operation
        responses.Enqueue(new QueryResponse
        {
            Items = new List<Dictionary<string, AttributeValue>>
            {
                {
                    new()
                    {
                        { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId")},
                        { "SK", new AttributeValue($"OUTBOX#METADATA#someMessageId") },
                        { "Dispatched", new AttributeValue { BOOL = false } },
                        { "OperationsCount", new AttributeValue { N = "2" } }
                    }
                }
            },
            LastEvaluatedKey = new Dictionary<string, AttributeValue> { { "idk", new AttributeValue("idk") } }
        });
        responses.Enqueue(new QueryResponse
        {
            Items = new List<Dictionary<string, AttributeValue>>
            {
                {
                    new()
                    {
                        { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId")},
                        { "SK", new AttributeValue("OUTBOX#OPERATION#someMessageId#0000")},
                        { "MessageId", new AttributeValue(Guid.NewGuid().ToString())},
                        { "Properties", new AttributeValue { M = []} },
                        { "Headers", new AttributeValue { M = []} },
                        { "Body", new AttributeValue { B = new MemoryStream() } }
                    }
                }
            },
        });
        client.QueryRequestResponse = _ => responses.Dequeue();

        var contextBag = new ContextBag();

        var exception = Assert.ThrowsAsync<PartialOutboxResultException>(async () => await persister.Get("someMessageId", contextBag));
        Assert.That(exception!.Message, Does.Contain("Partial outbox results retrieved with 1 instead of 2 transport operations while attempting to load the outbox records for message ID 'someMessageId'."));
    }

    [Test]
    public async Task Should_ignore_phantom_records_without_metadata()
    {
        client.QueryRequestResponse = r => new QueryResponse
        {
            Items = [
                new()
                {
                    { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId")},
                    { "SK", new AttributeValue("OUTBOX#OPERATION#someMessageId#0000")},
                    { "MessageId", new AttributeValue("someTransportOperationId")},
                    { "Properties", new AttributeValue()},
                    { "Headers", new AttributeValue()},
                    { "Body", new AttributeValue { B = new MemoryStream(Encoding.UTF8.GetBytes("Hello World"))}},
                },
            ]
        };

        var contextBag = new ContextBag();

        var record = await persister.Get("someMessageId", contextBag);

        Assert.That(record, Is.Null);
    }

    [Test]
    public async Task Should_ignore_phantom_records_spread_over_pages()
    {
        var called = 0;
        client.QueryRequestResponse = r =>
        {
            called++;
            if (called == 1)
            {
                return new QueryResponse
                {
                    Items =
                    [
                        new()
                        {
                            { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId") },
                            { "SK", new AttributeValue("OUTBOX#OPERATION#someMessageId#0000") },
                            { "MessageId", new AttributeValue("someTransportOperationId1") },
                            { "Properties", new AttributeValue() },
                            { "Headers", new AttributeValue() },
                            { "Body", new AttributeValue { B = new MemoryStream(Encoding.UTF8.GetBytes("Hello World")) } },
                        },
                    ],
                    LastEvaluatedKey = new Dictionary<string, AttributeValue>
                    {
                        { "SomeFakeEntry", new AttributeValue() }
                    }
                };
            }

            return new QueryResponse
            {
                Items =
                [
                    new()
                    {
                        { "PK", new AttributeValue("OUTBOX#endpointIdentifier#someMessageId") },
                        { "SK", new AttributeValue("OUTBOX#OPERATION#someMessageId#0001") },
                        { "MessageId", new AttributeValue("someTransportOperationId2") },
                        { "Properties", new AttributeValue() },
                        { "Headers", new AttributeValue() },
                        { "Body", new AttributeValue { B = new MemoryStream(Encoding.UTF8.GetBytes("Hello World")) } },
                    },
                ],
            };
        };

        var contextBag = new ContextBag();

        var record = await persister.Get("someMessageId", contextBag);

        Assert.Multiple(() =>
        {
            Assert.That(record, Is.Null);
            Assert.That(called, Is.EqualTo(1), "Should not try to fetch more records");
        });
    }

    MockDynamoDBClient client;
    OutboxPersister persister;
}