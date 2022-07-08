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
                s.SetDefault(new SagaPersistenceConfiguration
                {
                    TableName = s.EndpointName()
                });
                s.EnableFeatureByDefault<SynchronizedStorage>();
            });

            DependsOn<Sagas>();
            DependsOn<SynchronizedStorage>();
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            NonNativePubSubCheck.ThrowIfMessageDrivenPubSubInUse(context);

            //Use endpoint name the saga table name for all sagas by default
            var sagaConfiguration = context.Settings.Get<SagaPersistenceConfiguration>();

            //TODO: Table name callback can be null
            context.Services.AddSingleton<ISagaPersister>(provider => new SagaPersister(sagaConfiguration, provider.GetRequiredService<IProvideDynamoDBClient>().Client));
        }
    }
}