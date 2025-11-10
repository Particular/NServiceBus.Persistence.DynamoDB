namespace NServiceBus.Persistence.DynamoDB;

using System.Threading;
using System.Threading.Tasks;
using Installation;
using Settings;

sealed class SagaInstaller(IReadOnlySettings settings, Installer installer) : INeedToInstallSomething
{
    public async Task Install(string identity, CancellationToken cancellationToken = default)
    {
        var sagaConfiguration = settings.Get<SagaPersistenceConfiguration>();
        await installer.CreateTable(sagaConfiguration.Table, cancellationToken).ConfigureAwait(false);
    }
}