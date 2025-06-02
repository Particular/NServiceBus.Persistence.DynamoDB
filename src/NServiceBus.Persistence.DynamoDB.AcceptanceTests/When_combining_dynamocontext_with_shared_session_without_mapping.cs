namespace NServiceBus.AcceptanceTests;

using System;
using System.Threading.Tasks;
using AcceptanceTesting;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using EndpointTemplates;
using NUnit.Framework;
using Persistence.DynamoDB;

public class When_combining_dynamocontext_with_shared_session_without_mapping : NServiceBusAcceptanceTest
{
    [Test]
    public async Task Should_work()
    {
        var customerId = Guid.NewGuid().ToString();

        var customer = new Customer { CustomerId = customerId };
        var dynamoContext = new DynamoDBContext(SetupFixture.DynamoDBClient);
        await dynamoContext.SaveAsync(customer, new DynamoDBOperationConfig { OverrideTableName = SetupFixture.TableConfiguration.TableName });

        await Scenario.Define<Context>()
            .WithEndpoint<EndpointUsingDynamoContext>(e => e
                .When(c => c.SendLocal(new MakeCustomerPreferred
                {
                    CustomerId = customerId
                })))
            .Done(c => c.MessageReceived)
            .Run();

        var modifiedCustomer = await dynamoContext.LoadAsync<Customer>(customerId, customerId,
            new DynamoDBOperationConfig { OverrideTableName = SetupFixture.TableConfiguration.TableName });
        Assert.That(modifiedCustomer.CustomerPreferred, Is.True);
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
                dynamoContext = new DynamoDBContext(clientProvider.Client);
                this.testContext = testContext;
            }

            public async Task Handle(MakeCustomerPreferred message, IMessageHandlerContext context)
            {
                var session = context.SynchronizedStorageSession.DynamoPersistenceSession();

                var customer = await dynamoContext.LoadAsync<Customer>(message.CustomerId, message.CustomerId,
                        new DynamoDBOperationConfig { OverrideTableName = SetupFixture.TableConfiguration.TableName },
                        context.CancellationToken)
                    .ConfigureAwait(false);

                customer.CustomerPreferred = true;

                var customerMap = Mapper.ToMap(customer);
                // when PK and SK are not defined on the custom type they need to be added again
                // because the mapper doesn't have knowledge about what the PK and SK is
                customerMap["PK"] = new AttributeValue(customer.CustomerId);
                customerMap["SK"] = new AttributeValue(customer.CustomerId);

                session.Add(new TransactWriteItem
                {
                    Put = new()
                    {
                        Item = customerMap,
                        TableName = SetupFixture.TableConfiguration.TableName
                    }
                });

                testContext.MessageReceived = true;
            }

            readonly DynamoDBContext dynamoContext;
            readonly Context testContext;
        }
    }

    class Customer
    {
        [DynamoDBHashKey("PK")]
        public string PartitionKey { get; set; }

        [DynamoDBRangeKey("SK")]
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

        [DynamoDBProperty]
        public bool CustomerPreferred { get; set; }
    }

    class MakeCustomerPreferred : IMessage
    {
        public string CustomerId { get; set; }
    }
}