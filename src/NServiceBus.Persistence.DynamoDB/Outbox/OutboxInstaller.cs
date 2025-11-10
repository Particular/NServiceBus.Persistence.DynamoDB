namespace NServiceBus.Persistence.DynamoDB;

using System.Threading;
using System.Threading.Tasks;
using Installation;
using Settings;

sealed class OutboxInstaller(IReadOnlySettings settings, Installer installer) : INeedToInstallSomething
{
    public async Task Install(string identity, CancellationToken cancellationToken = default)
    {
        var outboxConfiguration = settings.Get<OutboxPersistenceConfiguration>();
        await installer.CreateTable(outboxConfiguration.Table, cancellationToken).ConfigureAwait(false);
    }
}