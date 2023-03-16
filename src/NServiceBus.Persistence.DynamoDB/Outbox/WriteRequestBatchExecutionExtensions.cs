#nullable enable

namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
    using NServiceBus.Logging;

    // While this class looks like it has some generic helper flavour it was deliberately kept as close
    // as possible to the requirements of the outbox persister and can only be used in that context.
    // Otherwise some significant refactoring would be required.
    static class WriteRequestBatchExecutionExtensions
    {
        public static async Task BatchWriteItemWithRetries(this IAmazonDynamoDB dynamoDbClient,
            List<List<WriteRequest>> writeRequestBatches, OutboxPersistenceConfiguration configuration, ILog logger,
            Func<TimeSpan, CancellationToken, Task>? delayOnFailure = default,
            TimeSpan? retryDelay = default, CancellationToken cancellationToken = default)
        {
            delayOnFailure ??= DefaultDelay;

            var operationCount = writeRequestBatches.Count;
            var batchWriteTasks = new Task[operationCount];
            for (var i = 0; i < operationCount; i++)
            {
                batchWriteTasks[i] = dynamoDbClient.WriteBatchWithRetries(writeRequestBatches[i], i + 1, operationCount,
                    configuration, logger, delayOnFailure, retryDelay,
                    cancellationToken);
            }
            await Task.WhenAll(batchWriteTasks).ConfigureAwait(false);
        }

        static async Task WriteBatchWithRetries(this IAmazonDynamoDB dynamoDbClient, List<WriteRequest> batch,
            int batchNumber, int totalBatches,
            OutboxPersistenceConfiguration configuration,
            ILog logger,
            Func<TimeSpan, CancellationToken, Task> delayOnFailure,
            TimeSpan? retryDelay,
            CancellationToken cancellationToken)
        {
            bool succeeded = true;
            // 5 is just an arbitrary number for now
            const int maximumNumberOfRetries = 5;
            for (int i = 0; i <= maximumNumberOfRetries; i++)
            {
                int attemptNumber = i + 1;
                try
                {
                    var response = await dynamoDbClient.WriteBatch(batch, batchNumber, totalBatches, configuration, logger, cancellationToken)
                        .ConfigureAwait(false);

                    if (!response.UnprocessedItems.TryGetValue(configuration.TableName, out var unprocessedBatch) ||
                        unprocessedBatch is not { Count: > 0 })
                    {
                        return;
                    }

                    batch = unprocessedBatch;
                    succeeded = false;

                    if (i == maximumNumberOfRetries)
                    {
                        continue;
                    }

                    var delay = CalculateDelay(attemptNumber, retryDelay);
                    if (logger.IsDebugEnabled)
                    {
                        logger.Debug($"({attemptNumber} / {maximumNumberOfRetries}) Retrying entries '{CreateBatchLogMessage(batch, configuration)}' that failed in batch '{batchNumber}/{totalBatches}' to table '{configuration.TableName}' with a delay of '{delay}'.");
                    }
                    else
                    {
                        logger.Info($"({attemptNumber} / {maximumNumberOfRetries}) Retrying entries that failed in batch '{batchNumber}/{totalBatches}' to table '{configuration.TableName}' with a delay of '{delay}'.");
                    }

                    await delayOnFailure(delay, cancellationToken).ConfigureAwait(false);
                }
                // If none of the items can be processed due to insufficient provisioned throughput on all of the tables in the request,
                // then BatchWriteItem returns a ProvisionedThroughputExceededException.
                catch (ProvisionedThroughputExceededException provisionedThroughputExceededException)
                {
                    succeeded = false;

                    if (i == maximumNumberOfRetries)
                    {
                        continue;
                    }

                    var delay = CalculateDelay(attemptNumber, retryDelay);
                    if (logger.IsDebugEnabled)
                    {
                        logger.Debug($"({attemptNumber} / {maximumNumberOfRetries}) Retrying entries '{CreateBatchLogMessage(batch, configuration)}' that failed in batch '{batchNumber}/{totalBatches}' to table '{configuration.TableName}' due to throttling  with a delay of '{delay}'.", provisionedThroughputExceededException);
                    }
                    else
                    {
                        logger.Info($"({attemptNumber} / {maximumNumberOfRetries}) Retrying entries that failed in batch '{batchNumber}/{totalBatches}' to table '{configuration.TableName}' due to throttling with a delay of '{delay}'.", provisionedThroughputExceededException);
                    }

                    await delayOnFailure(delay, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex) when (!ex.IsCausedBy(cancellationToken))
                {
                    logger.Error($"Error while writing batch '{batchNumber}/{totalBatches}', with entries '{CreateBatchLogMessage(batch, configuration)}' to table '{configuration.TableName}'", ex);
                    throw;
                }
            }


            if (!succeeded)
            {
                logger.Warn($"(5 / {maximumNumberOfRetries}) All retry attempts for batch '{batchNumber}/{totalBatches}' with entries '{CreateBatchLogMessage(batch, configuration)}' to table '{configuration.TableName}' exhausted.");
            }
        }

        static async Task<BatchWriteItemResponse> WriteBatch(this IAmazonDynamoDB dynamoDbClient,
            List<WriteRequest> batch, int batchNumber, int totalBatches,
            OutboxPersistenceConfiguration configuration, ILog logger, CancellationToken cancellationToken)
        {
            string? logBatchEntries = null;
            if (logger.IsDebugEnabled)
            {
                logBatchEntries = CreateBatchLogMessage(batch, configuration);
                logger.Debug(
                    $"Writing batch '{batchNumber}/{totalBatches}' with entries '{logBatchEntries}' to table '{configuration.TableName}'");
            }

            var batchWriteItemRequest = new BatchWriteItemRequest
            {
                RequestItems =
                    new Dictionary<string, List<WriteRequest>> { { configuration.TableName, batch } },
            };

            var result = await dynamoDbClient.BatchWriteItemAsync(batchWriteItemRequest, cancellationToken)
                .ConfigureAwait(false);

            if (logger.IsDebugEnabled)
            {
                logger.Debug(
                    $"Wrote batch '{batchNumber}/{totalBatches}' with entries '{logBatchEntries}' to table '{configuration.TableName}'");
            }

            return result;
        }

        static string CreateBatchLogMessage(IReadOnlyCollection<WriteRequest> batch, OutboxPersistenceConfiguration configuration)
        {
            var stringBuilder = new StringBuilder();

            foreach (var writeRequest in batch)
            {
                if (writeRequest.DeleteRequest is { } deleteRequest)
                {
                    stringBuilder.Append($"DELETE #PK {deleteRequest.Key[configuration.PartitionKeyName].S} / #SK {deleteRequest.Key[configuration.SortKeyName].S}, ");
                }

                if (writeRequest.PutRequest is { } putRequest)
                {
                    stringBuilder.Append($"PUT #PK {putRequest.Item[configuration.PartitionKeyName].S} / #SK {putRequest.Item[configuration.SortKeyName].S}, ");
                }
            }

            return stringBuilder.Length > 2 ? stringBuilder.ToString(0, stringBuilder.Length - 2) : stringBuilder.ToString();
        }

        static TimeSpan CalculateDelay(int attempt, TimeSpan? retryDelay)
        {
            var delay = Convert.ToInt32(retryDelay.GetValueOrDefault(TimeSpan.FromMilliseconds(250)).TotalMilliseconds);
            return TimeSpan.FromMilliseconds(attempt * delay);
        }

        static Task DefaultDelay(TimeSpan retryDelay, CancellationToken cancellationToken)
            => Task.Delay(retryDelay, cancellationToken);
    }
}