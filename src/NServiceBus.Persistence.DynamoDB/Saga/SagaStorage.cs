namespace NServiceBus.Persistence.DynamoDB;

using Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Sagas;

sealed class SagaStorage : Feature
{
    public SagaStorage()
    {
        Defaults(s =>
        {
            s.SetDefault<ISagaIdGenerator>(new SagaIdGenerator());
            s.SetDefault(new SagaPersistenceConfiguration());
        });

        Enable<SynchronizedStorage>();

        DependsOn<Sagas>();
        DependsOn<SynchronizedStorage>();
    }

    protected override void Setup(FeatureConfigurationContext context)
    {
        // By default, use the endpoint name for the saga table name to store all sagas
        var sagaConfiguration = context.Settings.Get<SagaPersistenceConfiguration>();

        if (sagaConfiguration.CreateTable)
        {
            context.Services.TryAddSingleton<Installer>();
            context.AddInstaller<SagaInstaller>();
        }

        context.Services.AddSingleton<ISagaPersister>(provider => new SagaPersister(
            provider.GetRequiredService<IDynamoClientProvider>().Client,
            sagaConfiguration,
            context.Settings.EndpointName()));
    }
}