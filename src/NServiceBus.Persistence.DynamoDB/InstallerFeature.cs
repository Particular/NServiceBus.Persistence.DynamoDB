namespace NServiceBus.Persistence.DynamoDB
{
    using Features;
    using Microsoft.Extensions.DependencyInjection;

    class InstallerFeature : Feature
    {
        public InstallerFeature()
        {
            Defaults(s => s.SetDefault(new InstallerSettings()));
            DependsOn<SynchronizedStorage>();
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            var installer = new Installer(context.Settings.Get<IProvideDynamoDBClient>(),
                context.Settings.Get<InstallerSettings>(), context.Settings.Get<OutboxPersistenceConfiguration>(), context.Settings.Get<SagaPersistenceConfiguration>());
            context.Services.AddSingleton(installer);
        }
    }
}