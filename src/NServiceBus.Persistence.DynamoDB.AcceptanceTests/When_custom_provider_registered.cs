namespace NServiceBus.AcceptanceTests;

using System;
using System.Threading.Tasks;
using AcceptanceTesting;
using Amazon.DynamoDBv2;
using EndpointTemplates;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Persistence.DynamoDB;

public class When_custom_provider_registered : NServiceBusAcceptanceTest
{
    [Test]
    public async Task Should_be_used()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<EndpointWithCustomProvider>(b =>
                b.When(session => session.SendLocal(new StartSaga { DataId = Guid.NewGuid() })))
            .Done(c => c.SagaReceivedMessage)
            .Run();

        Assert.True(context.ProviderWasCalled);
    }

    public class Context : ScenarioContext
    {
        public bool SagaReceivedMessage { get; set; }
        public bool ProviderWasCalled { get; set; }
    }

    public class EndpointWithCustomProvider : EndpointConfigurationBuilder
    {
        public EndpointWithCustomProvider() =>
            EndpointSetup<DefaultServer>(config =>
            {
                config.RegisterComponents(c =>
                    c.AddSingleton<IDynamoClientProvider>(b => new CustomProvider(b.GetService<Context>())));
            });

        public class JustASaga : Saga<JustASagaData>, IAmStartedByMessages<StartSaga>
        {
            public JustASaga(Context testContext)
                => this.testContext = testContext;

            public Task Handle(StartSaga message, IMessageHandlerContext context)
            {
                Data.DataId = message.DataId;
                testContext.SagaReceivedMessage = true;
                MarkAsComplete();
                return Task.CompletedTask;
            }

            protected override void ConfigureHowToFindSaga(SagaPropertyMapper<JustASagaData> mapper)
                => mapper.ConfigureMapping<StartSaga>(m => m.DataId).ToSaga(s => s.DataId);

            readonly Context testContext;
        }

        public class CustomProvider : IDynamoClientProvider
        {
            public CustomProvider(Context testContext)
                => this.testContext = testContext;

            public IAmazonDynamoDB Client
            {
                get
                {
                    testContext.ProviderWasCalled = true;
                    return SetupFixture.DynamoDBClient;
                }
            }

            readonly Context testContext;
        }

        public class JustASagaData : ContainSagaData
        {
            public virtual Guid DataId { get; set; }
        }
    }

    public class StartSaga : ICommand
    {
        public Guid DataId { get; set; }
    }
}