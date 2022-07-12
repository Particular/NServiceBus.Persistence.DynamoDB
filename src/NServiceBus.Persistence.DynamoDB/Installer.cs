namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
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
                await CreateOutboxTableIfNotExists(clientProvider.Client, installerSettings.OutboxTableName,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (!(e is OperationCanceledException && cancellationToken.IsCancellationRequested))
            {
                log.Error($"Could not complete the installation. An error occurred while creating the outbox table: {installerSettings.OutboxTableName}", e);
                throw;
            }

            try
            {
                await CreateSagaTableIfNotExists(clientProvider.Client, installerSettings.SagaTableName,
                    cancellationToken).ConfigureAwait(false);
            }
            catch (Exception e) when (!(e is OperationCanceledException && cancellationToken.IsCancellationRequested))
            {
                log.Error($"Could not complete the installation. An error occurred while creating the sagas table: {installerSettings.SagaTableName}", e);
                throw;
            }
        }

        async Task CreateOutboxTableIfNotExists(IAmazonDynamoDB client, string outboxTableName,
            CancellationToken cancellationToken)
        {
            // TODO: Maybe these should be configurable?
            var createTableRequest = new CreateTableRequest
            {
                TableName = outboxTableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new() { AttributeName = "PK", AttributeType = "S" },
                    new() { AttributeName = "SK", AttributeType = "S" }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new() { AttributeName = "PK", KeyType = "HASH" },
                    new() { AttributeName = "SK", KeyType = "RANGE" },
                },
                // TODO: Fix this, allow this to be configurable
                BillingMode = BillingMode.PAY_PER_REQUEST
            };

            await CreateTable(client, createTableRequest, cancellationToken).ConfigureAwait(false);
            await WaitForTableToBeActive(client, outboxTableName, cancellationToken).ConfigureAwait(false);
        }

        async Task CreateSagaTableIfNotExists(IAmazonDynamoDB client, string sagaTableName, CancellationToken cancellationToken)
        {
            // TODO: Maybe these should be configurable?
            var createTableRequest = new CreateTableRequest
            {
                TableName = sagaTableName,
                AttributeDefinitions = new List<AttributeDefinition>()
                {
                    new() { AttributeName = "PK", AttributeType = "S" },
                    new() { AttributeName = "SK", AttributeType = "S" }
                },
                KeySchema = new List<KeySchemaElement>()
                {
                    new() { AttributeName = "PK", KeyType = "HASH" },
                    new() { AttributeName = "SK", KeyType = "RANGE" },
                },
                // TODO: Fix this, allow this to be configurable
                BillingMode = BillingMode.PAY_PER_REQUEST
            };

            await CreateTable(client, createTableRequest, cancellationToken).ConfigureAwait(false);
            await WaitForTableToBeActive(client, sagaTableName, cancellationToken).ConfigureAwait(false);
        }

        static async Task CreateTable(IAmazonDynamoDB client, CreateTableRequest createTableRequest, CancellationToken cancellationToken)
        {
            try
            {
                await client.CreateTableAsync(createTableRequest, cancellationToken).ConfigureAwait(false);
            }
            catch (ResourceInUseException)
            {
                // Intentionally ignored when there are races
            }
        }

        static async Task WaitForTableToBeActive(IAmazonDynamoDB client, string tableName,
            CancellationToken cancellationToken)
        {
            var request = new DescribeTableRequest { TableName = tableName };

            TableStatus status;
            do
            {
                await Task.Delay(2000, cancellationToken).ConfigureAwait(false);

                var describeTableResponse =
                    await client.DescribeTableAsync(request, cancellationToken).ConfigureAwait(false);
                status = describeTableResponse.Table.TableStatus;
            } while (status != TableStatus.ACTIVE);
        }

        InstallerSettings installerSettings;
        static ILog log = LogManager.GetLogger<Installer>();

        readonly IProvideDynamoDBClient clientProvider;
    }
}