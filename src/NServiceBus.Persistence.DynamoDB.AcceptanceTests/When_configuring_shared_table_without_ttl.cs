namespace NServiceBus.AcceptanceTests;

using System;
using AcceptanceTesting;
using EndpointTemplates;
using NUnit.Framework;
using Persistence.DynamoDB;

public class When_configuring_shared_table_without_ttl : NServiceBusAcceptanceTest
{
    [Test]
    public void Should_fail_startup()
    {
        var exception = Assert.ThrowsAsync<InvalidOperationException>(() =>
            Scenario.Define<ScenarioContext>()
                .WithEndpoint<OutboxEndpoint>(e => e
                    .CustomConfig(c => c
                        .UsePersistence<DynamoPersistence>()
                        .UseSharedTable(new TableConfiguration
                        {
                            TimeToLiveAttributeName = null // using a table without TTL attribute defined
                        })))
                .Done(c => c.EndpointsStarted)
                .Run());

        StringAssert.Contains("The outbox persistence table requires a time-to-live attribute to be defined", exception.Message);
    }

    class OutboxEndpoint : EndpointConfigurationBuilder
    {
        public OutboxEndpoint() =>
            EndpointSetup<DefaultServer>(c =>
            {
                c.ConfigureTransport().TransportTransactionMode = TransportTransactionMode.ReceiveOnly;
                c.EnableOutbox();
            });
    }
}