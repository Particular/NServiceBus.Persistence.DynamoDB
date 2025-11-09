namespace NServiceBus.Persistence.DynamoDB;

using System.Threading;
using System.Threading.Tasks;
using Installation;
using Settings;

sealed class SagaInstaller(IReadOnlySettings settings, Installer installer) : INeedToInstallSomething
{
    public async Task Install(string identity, CancellationToken cancellationToken = default)
    {
        settings.TryGet(out OutboxPersistenceConfiguration outboxConfig);
        var outboxTableConfiguration = outboxConfig?.Table;

        if (settings.TryGet(out SagaPersistenceConfiguration sagaConfig)
            && sagaConfig.CreateTable
            && outboxTableConfiguration?.TableName != sagaConfig.Table.TableName)
        {
            await installer.CreateTable(sagaConfig.Table, cancellationToken).ConfigureAwait(false);
        }
    }
}