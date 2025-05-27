namespace NServiceBus.TransactionalSession.AcceptanceTests;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using AcceptanceTesting;
using Amazon.DynamoDBv2.Model;
using NUnit.Framework;

public class When_using_transactional_session : NServiceBusAcceptanceTest
{
    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_send_messages_and_store_document_in_synchronized_session_on_transactional_session_commit(bool outboxEnabled)
    {
        var partitionKey = nameof(When_using_transactional_session) + Guid.NewGuid().ToString("N");

        var context = await Scenario.Define<Context>()
            .WithEndpoint<AnEndpoint>(s => s.When(async (_, ctx) =>
            {
                using var scope = ctx.ServiceProvider.CreateScope();
                using var transactionalSession = scope.ServiceProvider.GetRequiredService<ITransactionalSession>();
                await transactionalSession.Open(new DynamoOpenSessionOptions());

                await transactionalSession.SendLocal(new SampleMessage(), CancellationToken.None);

                var dynamoSession = transactionalSession.SynchronizedStorageSession.DynamoPersistenceSession();
                dynamoSession.Add(new TransactWriteItem()
                {
                    Put = new Put()
                    {
                        TableName = SetupFixture.TableConfiguration.TableName,
                        Item = new Dictionary<string, AttributeValue>()
                        {
                            { SetupFixture.TableConfiguration.PartitionKeyName, new AttributeValue(partitionKey) },
                            { SetupFixture.TableConfiguration.SortKeyName, new AttributeValue(Guid.NewGuid().ToString()) },
                            { "Test", new AttributeValue(nameof(Should_send_messages_and_store_document_in_synchronized_session_on_transactional_session_commit))}
                        }
                    }
                });

                await transactionalSession.Commit(CancellationToken.None).ConfigureAwait(false);
            }))
            .Done(c => c.MessageReceived)
            .Run();

        var documents = await SetupFixture.DynamoDBClient.QueryAsync(new QueryRequest()
        {
            TableName = SetupFixture.TableConfiguration.TableName,
            ConsistentRead = true,
            KeyConditionExpression = "#pk = :pk",
            ExpressionAttributeNames =
                new Dictionary<string, string>()
                {
                    { "#pk", SetupFixture.TableConfiguration.PartitionKeyName }
                },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
            {
                { ":pk", new AttributeValue(partitionKey) }
            }
        });
        Assert.That(documents.Count, Is.EqualTo(1));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_send_messages_and_store_document_in_dynamo_session_on_transactional_session_commit(bool outboxEnabled)
    {
        var partitionKey = nameof(When_using_transactional_session) + Guid.NewGuid().ToString("N");

        var context = await Scenario.Define<Context>()
            .WithEndpoint<AnEndpoint>(s => s.When(async (_, ctx) =>
            {
                using var scope = ctx.ServiceProvider.CreateScope();
                using var transactionalSession = scope.ServiceProvider.GetRequiredService<ITransactionalSession>();
                await transactionalSession.Open(new DynamoOpenSessionOptions());

                await transactionalSession.SendLocal(new SampleMessage(), CancellationToken.None);

                var dynamoSession = scope.ServiceProvider.GetRequiredService<IDynamoStorageSession>();
                dynamoSession.Add(new TransactWriteItem()
                {
                    Put = new Put()
                    {
                        TableName = SetupFixture.TableConfiguration.TableName,
                        Item = new Dictionary<string, AttributeValue>()
                        {
                            { SetupFixture.TableConfiguration.PartitionKeyName, new AttributeValue(partitionKey) },
                            { SetupFixture.TableConfiguration.SortKeyName, new AttributeValue(Guid.NewGuid().ToString()) },
                            { "Test", new AttributeValue(nameof(Should_send_messages_and_store_document_in_synchronized_session_on_transactional_session_commit))}
                        }
                    }
                });

                await transactionalSession.Commit(CancellationToken.None).ConfigureAwait(false);
            }))
            .Done(c => c.MessageReceived)
            .Run();

        var documents = await SetupFixture.DynamoDBClient.QueryAsync(new QueryRequest()
        {
            TableName = SetupFixture.TableConfiguration.TableName,
            ConsistentRead = true,
            KeyConditionExpression = "#pk = :pk",
            ExpressionAttributeNames =
                new Dictionary<string, string>()
                {
                    { "#pk", SetupFixture.TableConfiguration.PartitionKeyName }
                },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
            {
                { ":pk", new AttributeValue(partitionKey) }
            }
        });
        Assert.That(documents.Count, Is.EqualTo(1));
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_not_send_messages_if_session_is_not_committed(bool outboxEnabled)
    {
        var partitionKey = nameof(When_using_transactional_session) + Guid.NewGuid().ToString("N");

        var context = await Scenario.Define<Context>()
            .WithEndpoint<AnEndpoint>(s => s.When(async (statelessSession, ctx) =>
            {
                using (var scope = ctx.ServiceProvider.CreateScope())
                using (var transactionalSession = scope.ServiceProvider.GetRequiredService<ITransactionalSession>())
                {
                    await transactionalSession.Open(new DynamoOpenSessionOptions());

                    var dynamoSession = transactionalSession.SynchronizedStorageSession.DynamoPersistenceSession();
                    dynamoSession.Add(new TransactWriteItem()
                    {
                        Put = new Put()
                        {
                            TableName = SetupFixture.TableConfiguration.TableName,
                            Item = new Dictionary<string, AttributeValue>()
                            {
                                { SetupFixture.TableConfiguration.PartitionKeyName, new AttributeValue(partitionKey) },
                                { SetupFixture.TableConfiguration.SortKeyName, new AttributeValue(Guid.NewGuid().ToString()) },
                                { "Test", new AttributeValue(nameof(Should_not_send_messages_if_session_is_not_committed))}
                            }
                        }
                    });

                    await transactionalSession.SendLocal(new SampleMessage());
                }

                //Send immediately dispatched message to finish the test
                await statelessSession.SendLocal(new CompleteTestMessage());
            }))
            .Done(c => c.CompleteMessageReceived)
            .Run();

        Assert.Multiple(() =>
        {
            Assert.That(context.CompleteMessageReceived, Is.True);
            Assert.That(context.MessageReceived, Is.False);
        });

        var documents = await SetupFixture.DynamoDBClient.QueryAsync(new QueryRequest()
        {
            TableName = SetupFixture.TableConfiguration.TableName,
            ConsistentRead = true,
            KeyConditionExpression = "#pk = :pk",
            ExpressionAttributeNames =
                new Dictionary<string, string>()
                {
                    { "#pk", SetupFixture.TableConfiguration.PartitionKeyName }
                },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
            {
                { ":pk", new AttributeValue(partitionKey) }
            }
        });
        Assert.That(documents.Items, Is.Empty);
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_send_immediate_dispatch_messages_even_if_session_is_not_committed(bool outboxEnabled)
    {
        var result = await Scenario.Define<Context>()
                .WithEndpoint<AnEndpoint>(s => s.When(async (_, ctx) =>
                {
                    using var scope = ctx.ServiceProvider.CreateScope();
                    using var transactionalSession = scope.ServiceProvider.GetRequiredService<ITransactionalSession>();

                    await transactionalSession.Open(new DynamoOpenSessionOptions());

                    var sendOptions = new SendOptions();
                    sendOptions.RequireImmediateDispatch();
                    sendOptions.RouteToThisEndpoint();
                    await transactionalSession.Send(new SampleMessage(), sendOptions, CancellationToken.None);
                }))
                .Done(c => c.MessageReceived)
                .Run();

        Assert.That(result.MessageReceived, Is.True);
    }

    [TestCase(true)]
    [TestCase(false)]
    public async Task Should_allow_using_synchronized_storage_even_when_there_are_no_outgoing_operations(bool outboxEnabled)
    {
        var partitionKey = nameof(When_using_transactional_session) + Guid.NewGuid().ToString("N");

        var context = await Scenario.Define<Context>()
            .WithEndpoint<AnEndpoint>(s => s.When(async (statelessSession, ctx) =>
            {
                using (var scope = ctx.ServiceProvider.CreateScope())
                using (var transactionalSession = scope.ServiceProvider.GetRequiredService<ITransactionalSession>())
                {
                    await transactionalSession.Open(new DynamoOpenSessionOptions());

                    var dynamoSession = transactionalSession.SynchronizedStorageSession.DynamoPersistenceSession();
                    dynamoSession.Add(new TransactWriteItem()
                    {
                        Put = new Put()
                        {
                            TableName = SetupFixture.TableConfiguration.TableName,
                            Item = new Dictionary<string, AttributeValue>()
                            {
                                { SetupFixture.TableConfiguration.PartitionKeyName, new AttributeValue(partitionKey) },
                                { SetupFixture.TableConfiguration.SortKeyName, new AttributeValue(Guid.NewGuid().ToString()) },
                                { "Test", new AttributeValue(nameof(Should_allow_using_synchronized_storage_even_when_there_are_no_outgoing_operations))}
                            }
                        }
                    });

                    // Deliberately not sending any messages via the transactional session before committing
                    await transactionalSession.Commit();
                }

                //Send immediately dispatched message to finish the test
                await statelessSession.SendLocal(new CompleteTestMessage());
            }))
            .Done(c => c.CompleteMessageReceived)
            .Run();

        var documents = await SetupFixture.DynamoDBClient.QueryAsync(new QueryRequest()
        {
            TableName = SetupFixture.TableConfiguration.TableName,
            ConsistentRead = true,
            KeyConditionExpression = "#pk = :pk",
            ExpressionAttributeNames =
                new Dictionary<string, string>()
                {
                    { "#pk", SetupFixture.TableConfiguration.PartitionKeyName }
                },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
            {
                { ":pk", new AttributeValue(partitionKey) }
            }
        });
        Assert.That(documents.Count, Is.EqualTo(1));
    }

    class Context : TransactionalSessionTestContext
    {
        public bool MessageReceived { get; set; }
        public bool CompleteMessageReceived { get; set; }
    }

    class AnEndpoint : EndpointConfigurationBuilder
    {
        public AnEndpoint()
        {
            if ((bool)TestContext.CurrentContext.Test.Arguments[0]!)
            {
                EndpointSetup<TransactionSessionWithOutboxEndpoint>();
            }
            else
            {
                EndpointSetup<TransactionSessionDefaultServer>();
            }
        }

        class SampleHandler(Context testContext) : IHandleMessages<SampleMessage>
        {
            public Task Handle(SampleMessage message, IMessageHandlerContext context)
            {
                testContext.MessageReceived = true;

                return Task.CompletedTask;
            }
        }

        class CompleteTestMessageHandler(Context testContext) : IHandleMessages<CompleteTestMessage>
        {
            public Task Handle(CompleteTestMessage message, IMessageHandlerContext context)
            {
                testContext.CompleteMessageReceived = true;

                return Task.CompletedTask;
            }
        }
    }

    class SampleMessage : ICommand
    {
    }

    class CompleteTestMessage : ICommand
    {
    }
}