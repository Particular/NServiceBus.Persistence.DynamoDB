namespace NServiceBus.Persistence.DynamoDB
{
    using Features;
    using Microsoft.Extensions.DependencyInjection;

    class InstallerFeature : Feature
    {
        public InstallerFeature()
        {
            //TODO: only create when needed
            Defaults(s => s.SetDefault(new InstallerSettings()
            {
                CreateOutboxTable = true,
                CreateSagaTable = true
            }));
            DependsOn<SynchronizedStorage>();
        }

        protected override void Setup(FeatureConfigurationContext context)
        {
            var settings = context.Settings.Get<InstallerSettings>();
            context.Services.AddSingleton(settings);
            if (settings.Disabled)
            {
                return;
            }

            var tableName = context.Settings.Get<string>(SettingsKeys.OutboxTableName);
            settings.OutboxTableName = tableName;

            var sagaPersistenceConfiguration = context.Settings.Get<SagaPersistenceConfiguration>();
            settings.SagaTableName = sagaPersistenceConfiguration.TableName;
        }
    }
}