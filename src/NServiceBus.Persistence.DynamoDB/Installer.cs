namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Installation;
    using Logging;

    class Installer : INeedToInstallSomething
    {
        public Installer(IProvideDynamoDBClient clientProvider, InstallerSettings settings)
        {
            installerSettings = settings;
            this.clientProvider = clientProvider;
        }

        public async Task Install(string identity, CancellationToken cancellationToken = default)
        {
            if (installerSettings == null || installerSettings.Disabled)
            {
                return;
            }

            try
            {
                await CreateTableIfNotExists(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (!(e is OperationCanceledException && cancellationToken.IsCancellationRequested))
            {
                log.Error("Could not complete the installation. ", e);
                throw;
            }
        }

        Task CreateTableIfNotExists(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

#pragma warning disable IDE0052
        InstallerSettings installerSettings;
        static ILog log = LogManager.GetLogger<Installer>();

        readonly IProvideDynamoDBClient clientProvider;
#pragma warning restore IDE0052
    }
}
