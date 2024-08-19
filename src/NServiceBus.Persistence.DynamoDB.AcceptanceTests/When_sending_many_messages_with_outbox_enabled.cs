namespace NServiceBus.AcceptanceTests;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AcceptanceTesting;
using AcceptanceTesting.Customization;
using Amazon.DynamoDBv2.Model;
using EndpointTemplates;
using NUnit.Framework;
using static Persistence.DynamoDB.OutboxAttributeNames;

public class When_sending_many_messages_with_outbox_enabled : NServiceBusAcceptanceTest
{
    [Test]
    public async Task Should_work_and_delete_operations()
    {
        var messageId = Guid.NewGuid().ToString();
        var context = await Scenario.Define<Context>()
            .WithEndpoint<EndpointSendingManyMessages>(e => e
                .When(s =>
                {
                    var options = new SendOptions();
                    options.RouteToThisEndpoint();
                    options.SetMessageId(messageId);
                    return s.Send(new KickOffMessage(), options);
                }))
            .Done(c => c.MessagesReceived == 99)
            .Run();

        Assert.That(context.MessagesReceived, Is.EqualTo(99));

        var endpointName = Conventions.EndpointNamingConvention(typeof(EndpointSendingManyMessages));
        var queryRequest = new QueryRequest
        {
            ConsistentRead = true,
            KeyConditionExpression = "#PK = :outboxId",
            ExpressionAttributeNames =
                new Dictionary<string, string> { { "#PK", SetupFixture.TableConfiguration.PartitionKeyName } },
            ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                { ":outboxId", new AttributeValue { S = $"OUTBOX#{endpointName}#{messageId}" } }
            },
            TableName = SetupFixture.TableConfiguration.TableName
        };

        var response = await SetupFixture.DynamoDBClient.QueryAsync(queryRequest);
        // Only the metadata entry is left
        Assert.That(response.Items, Has.Count.EqualTo(1));

        var metadataAttributeMap = response.Items.Single();
        Assert.Multiple(() =>
        {
            // Should be marked as dispatched
            Assert.That(metadataAttributeMap[Dispatched].BOOL, Is.True);
            Assert.That(metadataAttributeMap[DispatchedAt].S, Is.Not.Null);
        });
    }

    class Context : ScenarioContext
    {
        public int MessagesReceived => messageReceiveCounter;

        public void Received() => Interlocked.Increment(ref messageReceiveCounter);

        int messageReceiveCounter;
    }

    class EndpointSendingManyMessages : EndpointConfigurationBuilder
    {
        public EndpointSendingManyMessages() => EndpointSetup<OutboxServer>();

        class KickOffHandler : IHandleMessages<KickOffMessage>
        {
            public async Task Handle(KickOffMessage message, IMessageHandlerContext context)
            {
                for (int i = 0; i < 99; i++)
                {
                    await context.SendLocal(new OutgoingMessage());
                }
            }
        }

        class OutgoingMessageHandler : IHandleMessages<OutgoingMessage>
        {
            public OutgoingMessageHandler(Context testContext) => this.testContext = testContext;

            public Task Handle(OutgoingMessage message, IMessageHandlerContext context)
            {
                testContext.Received();
                return Task.CompletedTask;
            }

            readonly Context testContext;
        }
    }

    class KickOffMessage : IMessage
    {
    }

    class OutgoingMessage : IMessage
    {
    }
}