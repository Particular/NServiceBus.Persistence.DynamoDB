namespace NServiceBus.AcceptanceTests
{
    using System.Threading;
    using System.Threading.Tasks;
    using AcceptanceTesting;
    using EndpointTemplates;
    using Microsoft.Extensions.DependencyInjection;
    using NUnit.Framework;
    using Persistence.DynamoDB;

    //TODO: should not create tables when saga+outbox enabled but configured to be disabled
    public class When_installers_enabled : NServiceBusAcceptanceTest
    {
        [Test]
        public async Task Should_not_create_tables_when_outbox_and_sagas_not_used()
        {
            var context = await Scenario.Define<Context>()
                .WithEndpoint<EndpointWithInstallers>(e => e
                    .CustomConfig(c => c.DisableFeature<Features.Sagas>())) // disable sagas to simulate no saga on the endpoint
                .Done(c => c.EndpointsStarted)
                .Run();

            Assert.IsFalse(context.Installer.CreatedOutboxTable);
            Assert.IsFalse(context.Installer.CreatedSagaTable);
        }

        [Test]
        public async Task Should_create_saga_table_when_using_sagas()
        {
            var context = await Scenario.Define<Context>()
                .WithEndpoint<EndpointWithInstallers>()
                .Done(c => c.EndpointsStarted)
                .Run();

            Assert.IsFalse(context.Installer.CreatedOutboxTable);
            Assert.IsTrue(context.Installer.CreatedSagaTable);
        }

        [Test]
        public async Task Should_create_outbox_table_when_using_outbox()
        {
            var context = await Scenario.Define<Context>()
                .WithEndpoint<EndpointWithInstallers>(e => e
                    .CustomConfig(c =>
                    {
                        c.DisableFeature<Features.Sagas>(); // disable sagas to simulate no saga on the endpoint
                        c.ConfigureTransport().TransportTransactionMode = TransportTransactionMode.ReceiveOnly;
                        c.EnableOutbox();
                    }))
                .Done(c => c.EndpointsStarted)
                .Run();

            Assert.IsTrue(context.Installer.CreatedOutboxTable);
            Assert.IsFalse(context.Installer.CreatedSagaTable);
        }

        [Test]
        public async Task Should_not_create_tables_when_table_creation_disabled()
        {
            var context = await Scenario.Define<Context>()
                .WithEndpoint<EndpointWithInstallers>(e => e
                    .CustomConfig(c =>
                    {
                        c.ConfigureTransport().TransportTransactionMode = TransportTransactionMode.ReceiveOnly;
                        c.EnableOutbox();

                        c.UsePersistence<DynamoDBPersistence>().DisableTablesCreation();
                    }))
                .Done(c => c.EndpointsStarted)
                .Run();

            Assert.IsFalse(context.Installer.CreatedSagaTable);
            Assert.IsFalse(context.Installer.CreatedOutboxTable);
        }

        class Context : ScenarioContext
        {
            public FakeInstaller Installer { get; } = new FakeInstaller();
        }

        class EndpointWithInstallers : EndpointConfigurationBuilder
        {
            public EndpointWithInstallers() =>
                EndpointSetup<DefaultServer>((c, r) =>
                {
                    c.EnableInstallers();
                    c.RegisterComponents(sc => sc.AddSingleton<Installer>(((Context)r.ScenarioContext).Installer));
                });

            public class SomeSaga : Saga<SomeSaga.SomeSagaData>, IAmStartedByMessages<SomeSaga.SagaStartMessage>
            {
                protected override void ConfigureHowToFindSaga(SagaPropertyMapper<SomeSagaData> mapper) => mapper
                    .ConfigureMapping<SagaStartMessage>(m => m.SomeProperty).ToSaga(d => d.SomeProperty);

                public Task Handle(SagaStartMessage message, IMessageHandlerContext context) => throw new System.NotImplementedException();

                public class SomeSagaData : ContainSagaData
                {
                    public string SomeProperty { get; set; }
                }

                public class SagaStartMessage
                {
                    public string SomeProperty { get; set; }
                }
            }
        }

        class FakeInstaller : Installer
        {
            public bool CreatedOutboxTable { get; set; }
            public bool CreatedSagaTable { get; set; }

            public FakeInstaller() : base(null)
            {
            }

            public override Task CreateOutboxTableIfNotExists(OutboxPersistenceConfiguration outboxConfiguration,
                CancellationToken cancellationToken = default)
            {
                CreatedOutboxTable = true;
                return Task.CompletedTask;
            }

            public override Task CreateSagaTableIfNotExists(SagaPersistenceConfiguration sagaConfiguration,
                CancellationToken cancellationToken = default)
            {
                CreatedSagaTable = true;
                return Task.CompletedTask;
            }
        }
    }
}