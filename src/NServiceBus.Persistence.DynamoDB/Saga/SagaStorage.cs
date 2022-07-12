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
            // By default, use the endpoint name for the saga table name to store all sagas
            var sagaConfiguration = context.Settings.Get<SagaPersistenceConfiguration>();

            context.Services.AddSingleton<ISagaPersister>(provider => new SagaPersister(sagaConfiguration, provider.GetRequiredService<IProvideDynamoDBClient>().Client));
        }
    }
}