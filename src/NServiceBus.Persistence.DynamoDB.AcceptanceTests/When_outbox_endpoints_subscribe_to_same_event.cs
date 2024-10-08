﻿namespace NServiceBus.AcceptanceTests;

using System.Threading.Tasks;
using AcceptanceTesting;
using EndpointTemplates;
using NUnit.Framework;

public class When_outbox_endpoints_subscribe_to_same_event : NServiceBusAcceptanceTest
{
    [Test]
    public async Task Should_process_event_on_all_subscribers()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<SubscriberA>(e => e
                .When(s => s.Publish(new TestEvent())))
            .WithEndpoint<SubscriberB>()
            .Done(c => c.SubscriberASentMessage && c.SubscriberBSentMessage)
            .Run();

        Assert.Multiple(() =>
        {
            Assert.That(context.SubscriberAReceivedEvent, Is.True);
            Assert.That(context.SubscriberASentMessage, Is.True);
            Assert.That(context.SubscriberBReceivedEvent, Is.True);
            Assert.That(context.SubscriberBSentMessage, Is.True);
        });
    }

    class Context : ScenarioContext
    {
        public bool SubscriberAReceivedEvent { get; set; }
        public bool SubscriberBReceivedEvent { get; set; }
        public bool SubscriberASentMessage { get; set; }
        public bool SubscriberBSentMessage { get; set; }
    }

    class SubscriberA : EndpointConfigurationBuilder
    {
        public SubscriberA() => EndpointSetup<OutboxServer>();

        class EventHandler : IHandleMessages<TestEvent>
        {
            readonly Context testContext;

            public EventHandler(Context testContext) => this.testContext = testContext;

            public Task Handle(TestEvent message, IMessageHandlerContext context)
            {
                testContext.SubscriberAReceivedEvent = true;
                return context.SendLocal(new OutgoingMessage());
            }
        }

        class OutgoingMessageHandler : IHandleMessages<OutgoingMessage>
        {
            readonly Context testContext;

            public OutgoingMessageHandler(Context testContext) => this.testContext = testContext;

            public Task Handle(OutgoingMessage message, IMessageHandlerContext context)
            {
                testContext.SubscriberASentMessage = true;
                return Task.CompletedTask;
            }
        }
    }

    class SubscriberB : EndpointConfigurationBuilder
    {
        public SubscriberB() => EndpointSetup<OutboxServer>();

        class EventHandler : IHandleMessages<TestEvent>
        {
            readonly Context testContext;

            public EventHandler(Context testContext) => this.testContext = testContext;

            public Task Handle(TestEvent message, IMessageHandlerContext context)
            {
                testContext.SubscriberBReceivedEvent = true;
                return context.SendLocal(new OutgoingMessage());
            }
        }

        class OutgoingMessageHandler : IHandleMessages<OutgoingMessage>
        {
            readonly Context testContext;

            public OutgoingMessageHandler(Context testContext) => this.testContext = testContext;

            public Task Handle(OutgoingMessage message, IMessageHandlerContext context)
            {
                testContext.SubscriberBSentMessage = true;
                return Task.CompletedTask;
            }
        }
    }

    class TestEvent : IEvent
    {
    }

    class OutgoingMessage : IMessage
    {
    }
}