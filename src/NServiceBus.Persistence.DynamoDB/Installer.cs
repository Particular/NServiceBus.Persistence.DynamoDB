namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;

    class Installer
    {
        public Installer(IAmazonDynamoDB client)
        {
            this.client = client;
        }

        public async Task CreateOutboxTableIfNotExists(OutboxPersistenceConfiguration outboxConfiguration, CancellationToken cancellationToken = default)
        {
            var createTableRequest = new CreateTableRequest
            {
                TableName = outboxConfiguration.TableName,
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
                BillingMode = outboxConfiguration.BillingMode,
                ProvisionedThroughput = outboxConfiguration.ProvisionedThroughput
            };

            await CreateTable(createTableRequest, cancellationToken).ConfigureAwait(false);
            await WaitForTableToBeActive(outboxConfiguration.TableName, cancellationToken).ConfigureAwait(false);
            await ConfigureTimeToLive(outboxConfiguration, cancellationToken).ConfigureAwait(false);

        }

        async Task ConfigureTimeToLive(OutboxPersistenceConfiguration outboxConfiguration, CancellationToken cancellationToken)
        {
            var ttlDescription = await client.DescribeTimeToLiveAsync(outboxConfiguration.TableName, cancellationToken).ConfigureAwait(false);

            if (ttlDescription.TimeToLiveDescription.AttributeName != null)
            {
                if (ttlDescription.TimeToLiveDescription.AttributeName == outboxConfiguration.TimeToLiveAttributeName)
                {
                    // already contains TTL configuration
                    return;
                }

                throw new Exception(
                    $"The table {outboxConfiguration.TableName} has attribute {ttlDescription.TimeToLiveDescription.AttributeName} configured for the time to live. The outbox configuration is configured to use {outboxConfiguration.TimeToLiveAttributeName} which does not match. Adjust the outbox configuration to match the existing time to live column name or remove the existing time to live configuration on the table.");
            }

            await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
            {
                TableName = outboxConfiguration.TableName,
                TimeToLiveSpecification = new TimeToLiveSpecification
                {
                    AttributeName = outboxConfiguration.TimeToLiveAttributeName,
                    Enabled = true
                }
            }, cancellationToken).ConfigureAwait(false);
        }

        public async Task CreateSagaTableIfNotExists(SagaPersistenceConfiguration sagaConfiguration, CancellationToken cancellationToken = default)
        {
            var createTableRequest = new CreateTableRequest
            {
                TableName = sagaConfiguration.TableName,
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new() { AttributeName = sagaConfiguration.PartitionKeyName, AttributeType = ScalarAttributeType.S },
                    new() { AttributeName = sagaConfiguration.SortKeyName, AttributeType = ScalarAttributeType.S }
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new() { AttributeName = sagaConfiguration.PartitionKeyName, KeyType = KeyType.HASH },
                    new() { AttributeName = sagaConfiguration.SortKeyName, KeyType = KeyType.RANGE },
                },
                BillingMode = sagaConfiguration.BillingMode,
                ProvisionedThroughput = sagaConfiguration.ProvisionedThroughput,
            };

            await CreateTable(createTableRequest, cancellationToken).ConfigureAwait(false);
            await WaitForTableToBeActive(sagaConfiguration.TableName, cancellationToken).ConfigureAwait(false);
        }

        async Task CreateTable(CreateTableRequest createTableRequest,
            CancellationToken cancellationToken)
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

        async Task WaitForTableToBeActive(string tableName,
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

        readonly IAmazonDynamoDB client;
    }
}