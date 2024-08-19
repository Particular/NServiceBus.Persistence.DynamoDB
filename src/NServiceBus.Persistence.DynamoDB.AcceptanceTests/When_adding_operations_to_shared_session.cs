namespace NServiceBus.AcceptanceTests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AcceptanceTesting;
using Amazon.DynamoDBv2.Model;
using EndpointTemplates;
using NUnit.Framework;

public class When_adding_operations_to_shared_session : NServiceBusAcceptanceTest
{
    [Test]
    public async Task Should_commit_changes_with_session()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<EndpointAttachingTransactionOperations>(e => e
                .When(c => c.SendLocal(new TriggerMessage())))
            .Done(c => c.MessageReceived)
            .Run();

        var items = await SetupFixture.DynamoDBClient.QueryAsync(new QueryRequest
        {
            TableName = SetupFixture.TableConfiguration.TableName,
            ConsistentRead = true,
            KeyConditionExpression = "#pk = :pk",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                { "#pk", SetupFixture.TableConfiguration.PartitionKeyName }
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
            {
                { ":pk", new AttributeValue(context.ItemPK) }
            }
        });

        Assert.That(items.Count, Is.EqualTo(3));
    }

    [Test]
    public async Task Should_rollback_changes_with_session()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<EndpointAttachingTransactionOperations>(e => e
                .DoNotFailOnErrorMessages()
                .When(c => c.SendLocal(new TriggerMessage { FailHandler = true })))
            .Done(c => c.MessageReceived)
            .Run();

        var items = await SetupFixture.DynamoDBClient.QueryAsync(new QueryRequest
        {
            TableName = SetupFixture.TableConfiguration.TableName,
            ConsistentRead = true,
            KeyConditionExpression = "#pk = :pk",
            ExpressionAttributeNames = new Dictionary<string, string>
            {
                { "#pk", SetupFixture.TableConfiguration.PartitionKeyName }
            },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
            {
                { ":pk", new AttributeValue(context.ItemPK) }
            }
        });

        Assert.Multiple(() =>
        {
            Assert.That(items.Count, Is.EqualTo(0), "should have rolled back all enlisted database operations");
            Assert.That(context.FailedMessages.Single().Value.Count, Is.EqualTo(1), "the message should have failed");
        });
    }

    class Context : ScenarioContext
    {
        public string ItemPK { get; } = Guid.NewGuid().ToString("N");
        public bool MessageReceived { get; set; }
    }

    class EndpointAttachingTransactionOperations : EndpointConfigurationBuilder
    {
        public EndpointAttachingTransactionOperations() => EndpointSetup<DefaultServer>();

        class TriggerMessageHandler : IHandleMessages<TriggerMessage>
        {
            Context testContext;

            public TriggerMessageHandler(Context testContext) => this.testContext = testContext;

            public Task Handle(TriggerMessage message, IMessageHandlerContext context)
            {
                var session = context.SynchronizedStorageSession.DynamoPersistenceSession();
                session.Add(new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = SetupFixture.TableConfiguration.TableName,
                        Item = new Dictionary<string, AttributeValue>
                        {
                            {
                                SetupFixture.TableConfiguration.PartitionKeyName,
                                new AttributeValue(testContext.ItemPK)
                            },
                            {
                                SetupFixture.TableConfiguration.SortKeyName,
                                new AttributeValue("Session.Add")
                            },
                        }
                    }
                });

                session.AddRange(
                [
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = SetupFixture.TableConfiguration.TableName,
                            Item = new Dictionary<string, AttributeValue>
                            {
                                {
                                    SetupFixture.TableConfiguration.PartitionKeyName,
                                    new AttributeValue(testContext.ItemPK)
                                },
                                {
                                    SetupFixture.TableConfiguration.SortKeyName,
                                    new AttributeValue("Session.AddRange#1")
                                },
                            }
                        }
                    },
                    new TransactWriteItem
                    {
                        Put = new Put
                        {
                            TableName = SetupFixture.TableConfiguration.TableName,
                            Item = new Dictionary<string, AttributeValue>
                            {
                                {
                                    SetupFixture.TableConfiguration.PartitionKeyName,
                                    new AttributeValue(testContext.ItemPK)
                                },
                                {
                                    SetupFixture.TableConfiguration.SortKeyName,
                                    new AttributeValue("Session.AddRange#2")
                                },
                            }
                        }
                    }
                ]);

                testContext.MessageReceived = true;

                if (message.FailHandler)
                {
                    throw new SimulatedException();
                }

                return Task.CompletedTask;
            }
        }
    }

    class TriggerMessage : IMessage
    {
        public bool FailHandler { get; set; }
    }
}