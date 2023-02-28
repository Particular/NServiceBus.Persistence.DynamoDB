namespace NServiceBus.Persistence.DynamoDB
{
    using System.Threading;
    using System.Threading.Tasks;
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
            if (!settings.ShouldCreateTables())
            {
                return;
            }

            await installer.CreateOutboxTableIfNotExists(settings.Get<OutboxPersistenceConfiguration>(),
                cancellationToken).ConfigureAwait(false);
            await installer.CreateSagaTableIfNotExists(settings.Get<SagaPersistenceConfiguration>(), cancellationToken)
                .ConfigureAwait(false);
        }
    }
}