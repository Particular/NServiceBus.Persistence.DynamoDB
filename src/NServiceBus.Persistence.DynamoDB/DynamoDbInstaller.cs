namespace NServiceBus.Persistence.DynamoDB
{
    using System.Threading;
    using System.Threading.Tasks;
    using Installation;

    /// <summary>
    /// This is a wrapper since NServiceBus always registers implementations of <see cref="INeedToInstallSomething"/> in DI which means that all ctor arguments need to be resolvable via DI too. Instead, the actual, preconfigured, installer will be injected to simplify DI configuration.
    /// </summary>
    class DynamoDbInstaller : INeedToInstallSomething
    {
        Installer installer;

        public DynamoDbInstaller(Installer installer)
        {
            this.installer = installer;
        }

        public Task Install(string identity, CancellationToken cancellationToken = new CancellationToken())
        {
            return installer.Install(cancellationToken);
        }
    }
}