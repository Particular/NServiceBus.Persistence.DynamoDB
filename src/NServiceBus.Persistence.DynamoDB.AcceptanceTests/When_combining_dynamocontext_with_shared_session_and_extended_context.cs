namespace NServiceBus.AcceptanceTests;

using System;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;
using AcceptanceTesting;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using EndpointTemplates;
using NUnit.Framework;
using Persistence.DynamoDB;
using static Persistence.DynamoDB.Serialization.DynamoDBAttributeSupportJsonExtensions;

public class When_combining_dynamocontext_with_shared_session_and_extended_context : NServiceBusAcceptanceTest
{
    [Test]
    public async Task Should_work()
    {
        var customerId = Guid.NewGuid().ToString();

        var customer = new Customer { CustomerId = customerId };

        var dynamoContext = new DynamoDBContextBuilder().WithDynamoDBClient(() => SetupFixture.DynamoDBClient).Build();
        await dynamoContext.SaveAsync(customer, new SaveConfig { OverrideTableName = SetupFixture.TableConfiguration.TableName });

        await Scenario.Define<Context>()
            .WithEndpoint<EndpointUsingDynamoContext>(e => e
                .When(c => c.SendLocal(new MakeCustomerPreferred
                {
                    CustomerId = customerId
                })))
            .Done(c => c.MessageReceived)
            .Run();

        var modifiedCustomer = await dynamoContext.LoadAsync<Customer>(customerId, customerId,
            new LoadConfig { OverrideTableName = SetupFixture.TableConfiguration.TableName });

        Assert.Multiple(() =>
        {
            Assert.That(modifiedCustomer.CustomerPreferred, Is.True);
            Assert.That(modifiedCustomer.IgnoredProperty, Is.Null);
        });
    }

    class Context : ScenarioContext
    {
        public bool MessageReceived { get; set; }
    }

    class EndpointUsingDynamoContext : EndpointConfigurationBuilder
    {
        public EndpointUsingDynamoContext() => EndpointSetup<DefaultServer>();

        class TriggerMessageHandler(Context testContext, IDynamoClientProvider clientProvider)
            : IHandleMessages<MakeCustomerPreferred>
        {
            public async Task Handle(MakeCustomerPreferred message, IMessageHandlerContext context)
            {
                var session = context.SynchronizedStorageSession.DynamoPersistenceSession();

                var customer = await dynamoContext.LoadAsync<Customer>(message.CustomerId, message.CustomerId,
                        new LoadConfig { OverrideTableName = SetupFixture.TableConfiguration.TableName },
                        context.CancellationToken)
                    .ConfigureAwait(false);

                customer.CustomerPreferred = true;
                customer.IgnoredProperty = "IgnoredProperty";

                // Thanks to the customized serializer options the mapper understands the context attributes
                var customerMap = Mapper.ToMap(customer, serializerOptions);

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

            readonly DynamoDBContext dynamoContext = new DynamoDBContextBuilder().WithDynamoDBClient(() => clientProvider.Client).Build();

            // Normally this should never be done on a handler as a field but for this test this is OK.
            readonly JsonSerializerOptions serializerOptions = new(Mapper.Default)
            {
                TypeInfoResolver = new DefaultJsonTypeInfoResolver
                {
                    Modifiers = { SupportObjectModelAttributes }
                }
            };
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

        [DynamoDBIgnore]
        public string IgnoredProperty { get; set; }
    }

    class MakeCustomerPreferred : IMessage
    {
        public string CustomerId { get; set; }
    }
}