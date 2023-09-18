namespace NServiceBus.AcceptanceTests;

using System;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using AcceptanceTesting;
using AcceptanceTesting.EndpointTemplates;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;
using EndpointTemplates;
using NUnit.Framework;
using Persistence.DynamoDB;

public class When_combining_document_with_shared_session_with_mapping : NServiceBusAcceptanceTest
{
    [Test]
    public async Task Should_work()
    {
        var customerId = Guid.NewGuid().ToString();

        var table = Table.LoadTable(SetupFixture.DynamoDBClient, SetupFixture.TableConfiguration.TableName);
        var customerDocument = new Document
        {
            {"PK", customerId },
            {"SK", customerId },
            {"CustomerId", customerId },
            {"CustomerPreferred", new DynamoDBBool(false)},
        };
        await table.PutItemAsync(customerDocument);

        await Scenario.Define<Context>()
            .WithEndpoint<EndpointUsingDynamoContext>(e => e
                .When(c => c.SendLocal(new MakeCustomerPreferred
                {
                    CustomerId = customerId
                })))
            .Done(c => c.MessageReceived)
            .Run();

        var modifiedCustomerDocument = await table.GetItemAsync(customerId, customerId);
        Assert.That(modifiedCustomerDocument["CustomerPreferred"].AsBoolean(), Is.True);
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
                table = Table.LoadTable(clientProvider.Client, SetupFixture.TableConfiguration.TableName);
                this.testContext = testContext;
            }

            public async Task Handle(MakeCustomerPreferred message, IMessageHandlerContext context)
            {
                var session = context.SynchronizedStorageSession.DynamoPersistenceSession();

                var customerDocument = await table.GetItemAsync(message.CustomerId, message.CustomerId,
                        context.CancellationToken)
                    .ConfigureAwait(false);

                var customer = Mapper.ToObject<Customer>(customerDocument.ToAttributeMap());

                customer.CustomerPreferred = true;

                session.Add(new TransactWriteItem
                {
                    Put = new()
                    {
                        Item = Mapper.ToMap(customer),
                        TableName = SetupFixture.TableConfiguration.TableName
                    }
                });

                testContext.MessageReceived = true;
            }

            readonly Table table;
            readonly Context testContext;
        }
    }

    class Customer
    {
        [JsonPropertyName("PK")]
        public string PartitionKey { get; set; }

        [JsonPropertyName("SK")]
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