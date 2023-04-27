namespace NServiceBus.Persistence.DynamoDB;

using Features;
using Microsoft.Extensions.DependencyInjection;
using Sagas;

class SagaStorage : Feature
{
    internal SagaStorage()
    {
        Defaults(s =>
        {
            s.SetDefault<ISagaIdGenerator>(new SagaIdGenerator());
            s.SetDefault(new SagaPersistenceConfiguration());
            s.EnableFeatureByDefault<SynchronizedStorage>();
        });

        DependsOn<Sagas>();
        DependsOn<SynchronizedStorage>();
    }

    protected override void Setup(FeatureConfigurationContext context)
    {
        // By default, use the endpoint name for the saga table name to store all sagas
        var sagaConfiguration = context.Settings.Get<SagaPersistenceConfiguration>();

        context.Services.AddSingleton<ISagaPersister>(provider => new SagaPersister(
            provider.GetRequiredService<IDynamoClientProvider>().Client,
            sagaConfiguration,
            context.Settings.EndpointName()));
    }
}