#nullable enable

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
        public Installer(IAmazonDynamoDB client) => this.client = client;

        public virtual async Task CreateTable(TableConfiguration tableConfiguration,
            CancellationToken cancellationToken = default)
        {
            if (tableConfiguration.BillingMode == BillingMode.PROVISIONED && tableConfiguration.ProvisionedThroughput == null)
            {
                throw new ArgumentException(
                    $"The table is configured with provisioned billing mode but no throughput provision setting has been specified. Change billing mode to '{BillingMode.PAY_PER_REQUEST}' or add a '{nameof(ProvisionedThroughput)}' configuration");
            }

            var createTableRequest = new CreateTableRequest
            {
                TableName = tableConfiguration.TableName,
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new() { AttributeName = tableConfiguration.PartitionKeyName, AttributeType = ScalarAttributeType.S },
                    new() { AttributeName = tableConfiguration.SortKeyName, AttributeType = ScalarAttributeType.S },
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new() { AttributeName = tableConfiguration.PartitionKeyName, KeyType = KeyType.HASH },
                    new() { AttributeName = tableConfiguration.SortKeyName, KeyType = KeyType.RANGE },
                },
                BillingMode = tableConfiguration.BillingMode,
                ProvisionedThroughput = tableConfiguration.ProvisionedThroughput
            };

            await CreateTable(createTableRequest, cancellationToken).ConfigureAwait(false);
            await WaitForTableToBeActive(tableConfiguration.TableName, cancellationToken).ConfigureAwait(false);

            if (tableConfiguration.TimeToLiveAttributeName != null)
            {
                await ConfigureTimeToLive(tableConfiguration.TableName, tableConfiguration.TimeToLiveAttributeName, cancellationToken).ConfigureAwait(false);
            }

        }

        async Task ConfigureTimeToLive(string tableName, string ttlAttributeName, CancellationToken cancellationToken)
        {
            var ttlDescription = await client.DescribeTimeToLiveAsync(tableName, cancellationToken).ConfigureAwait(false);

            if (ttlDescription.TimeToLiveDescription.AttributeName != null)
            {
                if (ttlDescription.TimeToLiveDescription.AttributeName == ttlAttributeName)
                {
                    // already contains TTL configuration
                    return;
                }

                throw new Exception(
                    $"The table '{tableName}' has attribute '{ttlDescription.TimeToLiveDescription.AttributeName}' configured for the time to live. The outbox configuration is configured to use '{ttlAttributeName}' which does not match. Adjust the outbox configuration to match the existing time to live column name or remove the existing time to live configuration on the table.");
            }

            await client.UpdateTimeToLiveAsync(new UpdateTimeToLiveRequest
            {
                TableName = tableName,
                TimeToLiveSpecification = new TimeToLiveSpecification
                {
                    AttributeName = ttlAttributeName,
                    Enabled = true
                }
            }, cancellationToken).ConfigureAwait(false);
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