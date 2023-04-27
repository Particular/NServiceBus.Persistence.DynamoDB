namespace NServiceBus.AcceptanceTests;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AcceptanceTesting;
using Amazon.DynamoDBv2.Model;
using EndpointTemplates;
using NUnit.Framework;
using Persistence.DynamoDB;

public class When_injecting_shared_session
{
    [Test]
    public async Task Should_commit_changes_with_session()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<EndpointAttachingTransactionOperations>(e => e
                .When(c => c.SendLocal(new TriggerMessage())))
            .Done(c => c.MessageReceived)
            .Run();

        var getItemRequest = new GetItemRequest
        {
            ConsistentRead = true,
            Key = new Dictionary<string, AttributeValue>
            {
                { SetupFixture.TableConfiguration.PartitionKeyName, new AttributeValue { S = context.ItemPK } },
                { SetupFixture.TableConfiguration.SortKeyName, new AttributeValue { S = context.ItemSK } }
            },
            TableName = SetupFixture.TableConfiguration.TableName
        };

        var itemResponse = await SetupFixture.DynamoDBClient.GetItemAsync(getItemRequest);
        var mappedDto = Mapper.ToObject<SomeDto>(itemResponse.Item);

        Assert.AreEqual(typeof(EndpointAttachingTransactionOperations.TriggerMessageHandler).FullName, mappedDto.SomeData);
        CollectionAssert.AreEqual(new List<int>
        {
            1,
            2,
            3,
            4,
            5
        }, mappedDto.Ints);
    }

    class Context : ScenarioContext
    {
        public string ItemPK { get; } = Guid.NewGuid().ToString("N");
        public string ItemSK { get; } = Guid.NewGuid().ToString("N");
        public bool MessageReceived { get; set; }
    }

    class EndpointAttachingTransactionOperations : EndpointConfigurationBuilder
    {
        public EndpointAttachingTransactionOperations() => EndpointSetup<DefaultServer>();

        internal class TriggerMessageHandler : IHandleMessages<TriggerMessage>
        {
            public TriggerMessageHandler(Context testContext, IDynamoStorageSession storageSession)
            {
                this.testContext = testContext;
                this.storageSession = storageSession;
            }

            public Task Handle(TriggerMessage message, IMessageHandlerContext context)
            {
                var someDto = new SomeDto
                {
                    SomeData = typeof(TriggerMessageHandler).FullName,
                    Ints = new List<int>
                    {
                        1,
                        2,
                        3,
                        4,
                        5
                    }
                };
                var itemMap = Mapper.ToMap(someDto);
                itemMap[SetupFixture.TableConfiguration.PartitionKeyName] = new AttributeValue(testContext.ItemPK);
                itemMap[SetupFixture.TableConfiguration.SortKeyName] = new AttributeValue(testContext.ItemSK);

                storageSession.Add(new TransactWriteItem
                {
                    Put = new Put
                    {
                        TableName = SetupFixture.TableConfiguration.TableName,
                        Item = itemMap
                    }
                });

                testContext.MessageReceived = true;
                return Task.CompletedTask;
            }

            readonly Context testContext;
            readonly IDynamoStorageSession storageSession;
        }
    }

    class TriggerMessage : IMessage
    {
    }

    class SomeDto
    {
        public string SomeData { get; set; }
        public List<int> Ints { get; set; }
    }
}