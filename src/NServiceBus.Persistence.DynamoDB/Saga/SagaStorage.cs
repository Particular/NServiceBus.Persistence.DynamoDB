namespace NServiceBus.Persistence.DynamoDB
{
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
                // TODO: Let's make sure the endpoint name adheres to the naming rules
                s.SetDefault(SettingsKeys.SagasTableName, s.EndpointName());
                s.EnableFeatureByDefault<SynchronizedStorage>();
            });

            DependsOn<Sagas>();
            DependsOn<SynchronizedStorage>();
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            NonNativePubSubCheck.ThrowIfMessageDrivenPubSubInUse(context);

            var sagaConfiguration = context.Settings.GetOrDefault<SagaPersistenceConfiguration>() ?? new SagaPersistenceConfiguration();

            context.Services.AddSingleton<ISagaPersister>(builder => new SagaPersister(sagaConfiguration));
        }
    }
}