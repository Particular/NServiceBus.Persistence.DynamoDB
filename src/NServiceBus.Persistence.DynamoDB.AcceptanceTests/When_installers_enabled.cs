namespace NServiceBus.AcceptanceTests;

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using AcceptanceTesting;
using EndpointTemplates;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Persistence.DynamoDB;

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

        Assert.That(context.Installer.TablesCreated, Is.Empty);
    }

    [Test]
    public async Task Should_create_saga_table_when_using_sagas()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<EndpointWithInstallers>()
            .Done(c => c.EndpointsStarted)
            .Run();

        Assert.That(context.Installer.TablesCreated, Does.Contain(context.SagaTableName));
        Assert.That(context.Installer.TablesCreated, Has.Count.EqualTo(1), "should only create saga table");
    }

    [Test]
    public async Task Should_create_outbox_table_when_using_outbox()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<EndpointWithInstallers>(e => e
                .CustomConfig((c, ctx) =>
                {
                    c.DisableFeature<Features.Sagas>(); // disable sagas to simulate no saga on the endpoint
                    c.ConfigureTransport().TransportTransactionMode = TransportTransactionMode.ReceiveOnly;
                    c.EnableOutbox().UseTable(SetupFixture.TableConfiguration with { TableName = ctx.OutboxTableName });
                }))
            .Done(c => c.EndpointsStarted)
            .Run();

        Assert.That(context.Installer.TablesCreated, Does.Contain(context.OutboxTableName));
        Assert.That(context.Installer.TablesCreated, Has.Count.EqualTo(1), "should only create outbox table");
    }

    [Test]
    public async Task Should_not_create_tables_when_table_creation_disabled()
    {
        var context = await Scenario.Define<Context>()
            .WithEndpoint<EndpointWithInstallers>(e => e
                .CustomConfig((c, ctx) =>
                {
                    c.ConfigureTransport().TransportTransactionMode = TransportTransactionMode.ReceiveOnly;
                    c.EnableOutbox().UseTable(SetupFixture.TableConfiguration with { TableName = ctx.OutboxTableName });

                    c.UsePersistence<DynamoPersistence>().DisableTablesCreation();
                }))
            .Done(c => c.EndpointsStarted)
            .Run();

        Assert.That(context.Installer.TablesCreated, Is.Empty);
    }

    class Context : ScenarioContext
    {
        public string OutboxTableName { get; } = Guid.NewGuid().ToString();
        public string SagaTableName { get; } = Guid.NewGuid().ToString();
        public FakeInstaller Installer { get; } = new FakeInstaller();
    }

    class EndpointWithInstallers : EndpointConfigurationBuilder
    {
        public EndpointWithInstallers() =>
            EndpointSetup<DefaultServer>((c, r) =>
            {
                var testContext = r.ScenarioContext as Context;

                var persistence = c.UsePersistence<DynamoPersistence>();
                persistence.DynamoClient(SetupFixture.DynamoDBClient);
                persistence.Sagas().Table.TableName = testContext.SagaTableName;

                c.EnableInstallers();
                c.RegisterComponents(sc => sc.AddSingleton<Installer>(testContext.Installer));
            });

        public class SomeSaga : Saga<SomeSaga.SomeSagaData>, IAmStartedByMessages<SomeSaga.SagaStartMessage>
        {
            protected override void ConfigureHowToFindSaga(SagaPropertyMapper<SomeSagaData> mapper) => mapper
                .MapSaga(s=>s.SomeProperty).ToMessage<SagaStartMessage>(m => m.SomeProperty);

            public Task Handle(SagaStartMessage message, IMessageHandlerContext context) => throw new NotImplementedException();

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
        public List<string> TablesCreated { get; } = [];

        public FakeInstaller() : base(null)
        {
        }

        public override Task CreateTable(TableConfiguration tableConfiguration, CancellationToken cancellationToken = default)
        {
            TablesCreated.Add(tableConfiguration.TableName);
            return Task.CompletedTask;
        }
    }
}