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
        public Installer(IAmazonDynamoDB client, List<Tag> tags = null)
        {
            this.client = client;
            this.tags = tags ?? new List<Tag>(0);
        }

        public virtual async Task CreateOutboxTableIfNotExists(OutboxPersistenceConfiguration outboxConfiguration, CancellationToken cancellationToken = default)
        {
            await CreateTable(
                outboxConfiguration.TableName,
                outboxConfiguration.PartitionKeyName,
                outboxConfiguration.SortKeyName,
                outboxConfiguration.BillingMode,
                outboxConfiguration.ProvisionedThroughput,
                cancellationToken).ConfigureAwait(false);
            await ConfigureTimeToLive(outboxConfiguration, cancellationToken).ConfigureAwait(false);
        }

        public virtual Task CreateSagaTableIfNotExists(SagaPersistenceConfiguration sagaConfiguration, CancellationToken cancellationToken = default) => CreateTable(
                sagaConfiguration.TableName,
                sagaConfiguration.PartitionKeyName,
                sagaConfiguration.SortKeyName,
                sagaConfiguration.BillingMode,
                sagaConfiguration.ProvisionedThroughput,
                cancellationToken);

        async Task CreateTable(string tableName, string partitionKeyName, string sortKeyName, BillingMode billingMode, ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken)
        {
            var createTableRequest = new CreateTableRequest
            {
                TableName = tableName,
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new() { AttributeName = partitionKeyName, AttributeType = ScalarAttributeType.S },
                    new() { AttributeName = sortKeyName, AttributeType = ScalarAttributeType.S }
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new() { AttributeName = partitionKeyName, KeyType = KeyType.HASH },
                    new() { AttributeName = sortKeyName, KeyType = KeyType.RANGE },
                },
                BillingMode = billingMode,
                ProvisionedThroughput = provisionedThroughput,
                Tags = tags
            };

            try
            {
                await client.CreateTableAsync(createTableRequest, cancellationToken).ConfigureAwait(false);
            }
            catch (ResourceInUseException)
            {
                // Intentionally ignored when there are races
            }

            await WaitForTableToBeActive(tableName, cancellationToken).ConfigureAwait(false);
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

        readonly IAmazonDynamoDB client;
        readonly List<Tag> tags;
    }
}