namespace NServiceBus.AcceptanceTests;

using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using AcceptanceTesting;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using EndpointTemplates;
using NUnit.Framework;
using Persistence.DynamoDB;

public class When_combining_lowlevel_with_shared_session_without_mapping : NServiceBusAcceptanceTest
{
    [Test]
    public async Task Should_work()
    {
        var customerId = Guid.NewGuid().ToString();

        var customerAttributes = new Dictionary<string, AttributeValue>
        {
            {"PK", new AttributeValue(customerId) },
            {"SK", new AttributeValue(customerId) },
            {"CustomerId", new AttributeValue(customerId) },
            {"CustomerPreferred", new AttributeValue { BOOL = false }},
        };
        await SetupFixture.DynamoDBClient.PutItemAsync(new PutItemRequest
        {
            Item = customerAttributes,
            TableName = SetupFixture.TableConfiguration.TableName,
        });

        await Scenario.Define<Context>()
            .WithEndpoint<EndpointUsingDynamoContext>(e => e
                .When(c => c.SendLocal(new MakeCustomerPreferred
                {
                    CustomerId = customerId
                })))
            .Done(c => c.MessageReceived)
            .Run();

        var modifiedCustomerAttributes = (await SetupFixture.DynamoDBClient.GetItemAsync(SetupFixture.TableConfiguration.TableName, new Dictionary<string, AttributeValue>
        {
            {"PK", new AttributeValue(customerId) },
            {"SK", new AttributeValue(customerId) },
        }, consistentRead: true)).Item;
        Assert.That(modifiedCustomerAttributes["CustomerPreferred"].BOOL, Is.True);
    }

    class Context : ScenarioContext
    {
        public bool MessageReceived { get; set; }
    }

    class EndpointUsingDynamoContext : EndpointConfigurationBuilder
    {
        public EndpointUsingDynamoContext() => EndpointSetup<DefaultServer>();

        class TriggerMessageHandler : IHandleMessages<MakeCustomerPreferred>
        {
            public TriggerMessageHandler(Context testContext, IDynamoClientProvider clientProvider)
            {
                client = clientProvider.Client;
                this.testContext = testContext;
            }

            public async Task Handle(MakeCustomerPreferred message, IMessageHandlerContext context)
            {
                var session = context.SynchronizedStorageSession.DynamoPersistenceSession();

                var customerAttributes = (await client.GetItemAsync(SetupFixture.TableConfiguration.TableName,
                    new Dictionary<string, AttributeValue>
                    {
                        { "PK", new AttributeValue(message.CustomerId) },
                        { "SK", new AttributeValue(message.CustomerId) },
                    }, context.CancellationToken)).Item;

                var customer = Mapper.ToObject<Customer>(customerAttributes);

                customer.CustomerPreferred = true;

                var modifiedCustomerAttributes = Mapper.ToMap(customer);
                // when PK and SK are not defined on the custom type they need to be added again
                // because the mapper doesn't have knowledge about what the PK and SK is
                modifiedCustomerAttributes["PK"] = customerAttributes["PK"];
                modifiedCustomerAttributes["SK"] = customerAttributes["SK"];

                session.Add(new TransactWriteItem
                {
                    Put = new()
                    {
                        Item = modifiedCustomerAttributes,
                        TableName = SetupFixture.TableConfiguration.TableName
                    }
                });

                testContext.MessageReceived = true;
            }

            readonly IAmazonDynamoDB client;
            readonly Context testContext;
        }
    }

    class Customer
    {
        public string PartitionKey { get; set; }

        public string SortKey { get; set; }

        public string CustomerId
        {
            get => PartitionKey;
            set
            {
                PartitionKey = value;
                SortKey = value;
            }
        }

        public bool CustomerPreferred { get; set; }
    }

    class MakeCustomerPreferred : IMessage
    {
        public string CustomerId { get; set; }
    }
}