namespace NServiceBus.AcceptanceTests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AcceptanceTesting;
using AcceptanceTesting.Customization;
using Amazon.DynamoDBv2.Model;
using EndpointTemplates;
using NUnit.Framework;

public class When_using_saga_and_adding_operations_to_shared_session : NServiceBusAcceptanceTest
{
    [Test]
    public async Task Should_commit_changes_with_session()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<EndpointWithSagaAndTransactionOperations>(e => e
                .When(c => c.SendLocal(new TriggerMessage())))
            .Done(c => c.MessageReceived)
            .Run();

        var items = await SetupFixture.DynamoDBClient.QueryAsync(new QueryRequest
        {
            TableName = SetupFixture.TableConfiguration.TableName,
            ConsistentRead = true,
            KeyConditionExpression = "#pk = :pk",
            ExpressionAttributeNames =
                new Dictionary<string, string> { { "#pk", SetupFixture.TableConfiguration.PartitionKeyName } },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
            {
                { ":pk", new AttributeValue(context.ItemPK) }
            }
        });

        var saga = await SetupFixture.DynamoDBClient.GetItemAsync(new GetItemRequest
        {
            TableName = SetupFixture.TableConfiguration.TableName,
            ConsistentRead = true,
            Key = new Dictionary<string, AttributeValue>
            {
                {
                    SetupFixture.TableConfiguration.PartitionKeyName,
                    new AttributeValue(
                        $"SAGA#{Conventions.EndpointNamingConvention(typeof(EndpointWithSagaAndTransactionOperations))}#{context.SagaId}")
                },
                { SetupFixture.TableConfiguration.SortKeyName, new AttributeValue($"SAGA#{context.SagaId}") }
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(items.Count, Is.EqualTo(3));
            Assert.That(saga.Item, Is.Not.Empty);
        });
    }

    [Test]
    public async Task Should_rollback_changes_with_session()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<EndpointWithSagaAndTransactionOperations>(e => e
                .DoNotFailOnErrorMessages()
                .When(c => c.SendLocal(new TriggerMessage { FailHandler = true })))
            .Done(c => c.MessageReceived)
            .Run();

        var items = await SetupFixture.DynamoDBClient.QueryAsync(new QueryRequest
        {
            TableName = SetupFixture.TableConfiguration.TableName,
            ConsistentRead = true,
            KeyConditionExpression = "#pk = :pk",
            ExpressionAttributeNames =
                new Dictionary<string, string> { { "#pk", SetupFixture.TableConfiguration.PartitionKeyName } },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>()
            {
                { ":pk", new AttributeValue(context.ItemPK) }
            }
        });

        var saga = await SetupFixture.DynamoDBClient.GetItemAsync(new GetItemRequest
        {
            TableName = SetupFixture.TableConfiguration.TableName,
            ConsistentRead = true,
            Key = new Dictionary<string, AttributeValue>
            {
                {
                    SetupFixture.TableConfiguration.PartitionKeyName,
                    new AttributeValue(
                        $"SAGA#{Conventions.EndpointNamingConvention(typeof(EndpointWithSagaAndTransactionOperations))}#{context.SagaId}")
                },
                { SetupFixture.TableConfiguration.SortKeyName, new AttributeValue($"SAGA#{context.SagaId}") }
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(items, Is.Empty, "should have rolled back all enlisted database operations");
            Assert.That(saga.Item, Is.Empty, "should have rolled back all saga database operations");
            Assert.That(context.FailedMessages.Single().Value, Has.Count.EqualTo(1), "the message should have failed");
        });
    }

    class Context : ScenarioContext
    {
        public string ItemPK { get; } = Guid.NewGuid().ToString("N");
        public bool MessageReceived { get; set; }
        public Guid SagaId { get; set; }
    }

    class EndpointWithSagaAndTransactionOperations : EndpointConfigurationBuilder
    {
        public EndpointWithSagaAndTransactionOperations() => EndpointSetup<DefaultServer>();

        class TriggerMessageHandler(Context testContext) : IHandleMessages<TriggerMessage>
        {
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
                            { SetupFixture.TableConfiguration.SortKeyName, new AttributeValue("Session.Add") },
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

                return Task.CompletedTask;
            }

            class MySagaData : ContainSagaData
            {
                public string Data { get; set; }
            }

            class MySaga(Context testContext) : Saga<MySagaData>, IAmStartedByMessages<TriggerMessage>
            {
                public Task Handle(TriggerMessage message, IMessageHandlerContext context)
                {
                    testContext.SagaId = Data.Id;
                    testContext.MessageReceived = true;

                    if (message.FailHandler)
                    {
                        throw new SimulatedException();
                    }

                    return Task.CompletedTask;
                }

                protected override void ConfigureHowToFindSaga(SagaPropertyMapper<MySagaData> mapper) =>
                    mapper.MapSaga(m => m.Data).ToMessage<TriggerMessage>(m => m.Data);
            }
        }
    }

    class TriggerMessage : IMessage
    {
        public string Data { get; set; } = Guid.NewGuid().ToString();
        public bool FailHandler { get; set; }
    }
}