namespace NServiceBus.Persistence.DynamoDB;

using System.Threading;
using System.Threading.Tasks;
using Installation;
using Settings;

sealed class OutboxInstaller(IReadOnlySettings settings, Installer installer) : INeedToInstallSomething
{
    public async Task Install(string identity, CancellationToken cancellationToken = default)
    {
        if (settings.TryGet(out OutboxPersistenceConfiguration outboxConfig)
            && outboxConfig.CreateTable)
        {
            await installer.CreateTable(outboxConfig.Table, cancellationToken).ConfigureAwait(false);
        }
    }
}