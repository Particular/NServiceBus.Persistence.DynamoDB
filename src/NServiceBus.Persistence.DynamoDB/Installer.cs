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

    class Installer
    {
        public Installer(IProvideDynamoDBClient clientProvider, InstallerSettings settings,
            OutboxPersistenceConfiguration outboxConfiguration)
        {
            installerSettings = settings;
            this.outboxConfiguration = outboxConfiguration;
            this.clientProvider = clientProvider;
        }

        public async Task Install(CancellationToken cancellationToken = default)
        {
            if (installerSettings == null || installerSettings.Disabled)
            {
                return;
            }

            if (installerSettings.CreateOutboxTable)
            {
                try
                {
                    await CreateOutboxTableIfNotExists(clientProvider.Client, outboxConfiguration.TableName,
                        cancellationToken).ConfigureAwait(false);
                }
                catch (Exception e) when (!(e is OperationCanceledException &&
                                            cancellationToken.IsCancellationRequested))
                {
                    log.Error(
                        $"Could not complete the installation. An error occurred while creating the outbox table: {outboxConfiguration.TableName}",
                        e);
                    throw;
                }
            }

            if (installerSettings.CreateSagaTable)
            {
                try
                {
                    await CreateSagaTableIfNotExists(clientProvider.Client, installerSettings.SagaTableName,
                        cancellationToken).ConfigureAwait(false);
                }
                catch (Exception e) when (!(e is OperationCanceledException &&
                                            cancellationToken.IsCancellationRequested))
                {
                    log.Error(
                        $"Could not complete the installation. An error occurred while creating the sagas table: {installerSettings.SagaTableName}",
                        e);
                    throw;
                }
            }
        }

        async Task CreateOutboxTableIfNotExists(IAmazonDynamoDB client, string outboxTableName,
            CancellationToken cancellationToken)
        {
            var createTableRequest = new CreateTableRequest
            {
                TableName = outboxTableName,
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new() { AttributeName = outboxConfiguration.PartitionKeyName, AttributeType = ScalarAttributeType.S },
                    new() { AttributeName = outboxConfiguration.SortKeyName, AttributeType = ScalarAttributeType.S },
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new() { AttributeName = outboxConfiguration.PartitionKeyName, KeyType = KeyType.HASH },
                    new() { AttributeName = outboxConfiguration.SortKeyName, KeyType = KeyType.RANGE },
                },
                BillingMode = installerSettings.BillingMode,
                ProvisionedThroughput = installerSettings.ProvisionedThroughput,
            };

            await CreateTable(client, createTableRequest, cancellationToken).ConfigureAwait(false);
            await WaitForTableToBeActive(client, outboxTableName, cancellationToken).ConfigureAwait(false);
            await ConfigureTimeToLive(client, outboxTableName, cancellationToken);

        }

        async Task ConfigureTimeToLive(IAmazonDynamoDB client, string outboxTableName, CancellationToken cancellationToken)
        {
            var ttlDescription = await client.DescribeTimeToLiveAsync(outboxTableName, cancellationToken);

            if (ttlDescription.TimeToLiveDescription.AttributeName != null)
            {
                if (ttlDescription.TimeToLiveDescription.AttributeName == outboxConfiguration.TimeToLiveAttributeName)
                {
                    // already contains TTL configuration
                    return;
                }

                throw new Exception(
                    $"The table {outboxTableName} has attribute {ttlDescription.TimeToLiveDescription.AttributeName} configured for the time to live. The outbox configuration is configured to use {outboxConfiguration.TimeToLiveAttributeName} which does not match. Adjust the outbox configuration to match the existing time to live column name or remove the existing time to live configuration on the table.");
            }

            await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
            {
                TableName = outboxTableName,
                TimeToLiveSpecification = new TimeToLiveSpecification
                {
                    AttributeName = outboxConfiguration.TimeToLiveAttributeName,
                    Enabled = true
                }
            }, cancellationToken);
        }

        async Task CreateSagaTableIfNotExists(IAmazonDynamoDB client, string sagaTableName,
            CancellationToken cancellationToken)
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

        static async Task CreateTable(IAmazonDynamoDB client, CreateTableRequest createTableRequest,
            CancellationToken cancellationToken)
        {
            try
            {
                var r = await client.CreateTableAsync(createTableRequest, cancellationToken).ConfigureAwait(false);
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
        readonly OutboxPersistenceConfiguration outboxConfiguration;
        static ILog log = LogManager.GetLogger<Installer>();

        readonly IProvideDynamoDBClient clientProvider;
    }
}