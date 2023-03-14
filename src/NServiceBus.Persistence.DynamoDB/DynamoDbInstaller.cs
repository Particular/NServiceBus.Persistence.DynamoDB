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
    class DynamoDbInstaller : INeedToInstallSomething
    {
        readonly Installer installer;
        readonly IReadOnlySettings settings;

        public DynamoDbInstaller(Installer installer, IReadOnlySettings settings)
        {
            this.installer = installer;
            this.settings = settings;
        }

        public async Task Install(string identity, CancellationToken cancellationToken = default)
        {
            DynamoTableConfiguration sagaTableConfiguration = null;
            if (settings.IsFeatureActive(typeof(OutboxStorage))
               && settings.TryGet(out OutboxPersistenceConfiguration outboxConfig)
               && outboxConfig.CreateTable)
            {
                sagaTableConfiguration = outboxConfig.TableConfiguration;
                await installer.CreateTable(sagaTableConfiguration, cancellationToken).ConfigureAwait(false);
            }

            if (settings.IsFeatureActive(typeof(SagaStorage))
                && settings.TryGet(out SagaPersistenceConfiguration sagaConfig)
                && sagaConfig.CreateTable
                && sagaTableConfiguration != sagaConfig.TableConfiguration)
            {
                await installer.CreateTable(sagaConfig.TableConfiguration, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}