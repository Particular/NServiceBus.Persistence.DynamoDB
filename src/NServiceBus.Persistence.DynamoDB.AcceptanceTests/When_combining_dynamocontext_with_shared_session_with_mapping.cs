﻿namespace NServiceBus.AcceptanceTests;

using System;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using AcceptanceTesting;
using Amazon.DynamoDBv2.DataModel;
using Amazon.DynamoDBv2.Model;
using EndpointTemplates;
using NUnit.Framework;
using Persistence.DynamoDB;

public class When_combining_dynamocontext_with_shared_session_with_mapping : NServiceBusAcceptanceTest
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
        Assert.That(modifiedCustomer.CustomerPreferred, Is.True);
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

            readonly DynamoDBContext dynamoContext = new DynamoDBContextBuilder().WithDynamoDBClient(() => clientProvider.Client).Build();
        }
    }

    class Customer
    {
        [DynamoDBHashKey("PK")]
        [JsonPropertyName("PK")]
        public string PartitionKey { get; set; }

        [DynamoDBRangeKey("SK")]
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

        [DynamoDBProperty("customer_preferred")]
        [JsonPropertyName("customer_preferred")]
        public bool CustomerPreferred { get; set; }
    }

    class MakeCustomerPreferred : IMessage
    {
        public string CustomerId { get; set; }
    }
}