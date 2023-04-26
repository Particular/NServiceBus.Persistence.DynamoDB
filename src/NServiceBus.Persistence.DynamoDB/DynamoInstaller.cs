namespace NServiceBus.Persistence.DynamoDB
{
    using System.Threading;
    using System.Threading.Tasks;
    using Features;
    using Installation;
    using Settings;

    /// <summary>
    /// This is a wrapper since NServiceBus always registers implementations of <see cref="INeedToInstallSomething"/> in DI which means that all ctor arguments need to be resolvable via DI too. Instead, the actual, preconfigured, installer will be injected to simplify DI configuration.
    /// </summary>
    class DynamoInstaller : INeedToInstallSomething
    {
        readonly Installer installer;
        readonly IReadOnlySettings settings;

        public DynamoInstaller(Installer installer, IReadOnlySettings settings)
        {
            this.installer = installer;
            this.settings = settings;
        }

        public async Task Install(string identity, CancellationToken cancellationToken = default)
        {
            TableConfiguration? outboxTableConfiguration = null;
            if (settings.IsFeatureActive(typeof(OutboxStorage))
               && settings.TryGet(out OutboxPersistenceConfiguration outboxConfig)
               && outboxConfig.CreateTable)
            {
                outboxTableConfiguration = outboxConfig.Table;
                await installer.CreateTable(outboxTableConfiguration, cancellationToken).ConfigureAwait(false);
            }

            if (settings.IsFeatureActive(typeof(SagaStorage))
                && settings.TryGet(out SagaPersistenceConfiguration sagaConfig)
                && sagaConfig.CreateTable
                && outboxTableConfiguration?.TableName != sagaConfig.Table.TableName)
            {
                await installer.CreateTable(sagaConfig.Table, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}