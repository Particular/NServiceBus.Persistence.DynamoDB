namespace NServiceBus.Persistence.DynamoDB
{
    using Features;
    using Microsoft.Extensions.DependencyInjection;

    class InstallerFeature : Feature
    {
        public InstallerFeature()
        {
            //TODO: only create when needed
            Defaults(s => s.SetDefault(new InstallerSettings()));
            DependsOn<SynchronizedStorage>();
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            var settings = context.Settings.Get<InstallerSettings>();

            var sagaPersistenceConfiguration = context.Settings.Get<SagaPersistenceConfiguration>();
            settings.SagaTableName = sagaPersistenceConfiguration.TableName;

            var installer = new Installer(context.Settings.Get<IProvideDynamoDBClient>(),
                context.Settings.Get<InstallerSettings>(), context.Settings.Get<OutboxPersistenceConfiguration>());
            context.Services.AddSingleton(installer);
        }
    }
}