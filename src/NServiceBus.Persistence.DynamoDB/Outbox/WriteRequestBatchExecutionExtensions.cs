#nullable enable

namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Amazon.DynamoDBv2;
    using Amazon.DynamoDBv2.Model;
    using Logging;

    // Currently placed into outbox folder but could be moved elsewhere once needed in more places.
    static class WriteRequestBatchExecutionExtensions
    {
        public static async Task BatchWriteItemWithRetries<TArgs>(this IAmazonDynamoDB dynamoDbClient,
            List<List<WriteRequest>> writeRequestBatches, string tableName, ILog logger,
            Func<IReadOnlyCollection<WriteRequest>, TArgs?, string>? debugBatchLogMessageFormatter = default, TArgs? formatterArgs = default,
            Func<TimeSpan, CancellationToken, Task>? delayOnFailure = default,
            TimeSpan? retryDelay = default, CancellationToken cancellationToken = default)
        {
            debugBatchLogMessageFormatter ??= (_, _) => string.Empty;
            delayOnFailure ??= DefaultDelay;

            var operationCount = writeRequestBatches.Count;
            var batchWriteTasks = new Task[operationCount];
            for (var i = 0; i < operationCount; i++)
            {
                batchWriteTasks[i] = dynamoDbClient.WriteBatchWithRetries(writeRequestBatches[i], i + 1, operationCount,
                    tableName, logger, debugBatchLogMessageFormatter, formatterArgs, delayOnFailure, retryDelay,
                    cancellationToken);
            }
            await Task.WhenAll(batchWriteTasks).ConfigureAwait(false);
        }

        static async Task WriteBatchWithRetries<TArgs>(this IAmazonDynamoDB dynamoDbClient, List<WriteRequest> batch,
            int batchNumber, int totalBatches,
            string tableName,
            ILog logger,
            Func<IReadOnlyCollection<WriteRequest>, TArgs?, string> debugBatchLogMessageFormatter,
            TArgs? formatterArgs,
            Func<TimeSpan, CancellationToken, Task> delayOnFailure,
            TimeSpan? retryDelay,
            CancellationToken cancellationToken)
        {
            bool succeeded = true;
            while (!cancellationToken.IsCancellationRequested)
            {
                // 5 is just an arbitrary number for now
                for (int i = 0; i < 5; i++)
                {
                    try
                    {
                        var response = await dynamoDbClient.WriteBatch(batch, batchNumber, totalBatches, tableName, logger, debugBatchLogMessageFormatter, formatterArgs, cancellationToken)
                            .ConfigureAwait(false);

                        if (!response.UnprocessedItems.TryGetValue(tableName, out var unprocessedBatch) ||
                            unprocessedBatch is not { Count: > 0 })
                        {
                            return;
                        }

                        batch = unprocessedBatch;
                        succeeded = false;

                        if (logger.IsDebugEnabled)
                        {
                            logger.Debug($"Retrying entries '{debugBatchLogMessageFormatter(batch, formatterArgs)}' that failed in batch '{batchNumber}/{totalBatches}' to table '{tableName}'.");
                        }
                        else
                        {
                            logger.Info($"Retrying entries that failed in batch '{batchNumber}/{totalBatches}' to table '{tableName}'.");
                        }

                        await BatchDelay(i, retryDelay, delayOnFailure, cancellationToken).ConfigureAwait(false);
                    }
                    // If none of the items can be processed due to insufficient provisioned throughput on all of the tables in the request,
                    // then BatchWriteItem returns a ProvisionedThroughputExceededException.
                    catch (ProvisionedThroughputExceededException provisionedThroughputExceededException)
                        when (provisionedThroughputExceededException.Retryable is { Throttling: true })
                    {
                        succeeded = false;
                        if (logger.IsDebugEnabled)
                        {
                            logger.Debug($"Retrying entries '{debugBatchLogMessageFormatter(batch, formatterArgs)}' that failed in batch '{batchNumber}/{totalBatches}' to table '{tableName}' due to throttling.");
                        }
                        else
                        {
                            logger.Info($"Retrying entries that failed in batch '{batchNumber}/{totalBatches}' to table '{tableName}' due to throttling.");
                        }

                        await BatchDelay(i, retryDelay, delayOnFailure, cancellationToken).ConfigureAwait(false);
                    }
                    catch (Exception ex) when (!ex.IsCausedBy(cancellationToken))
                    {
                        logger.Error($"Error while writing batch '{batchNumber}/{totalBatches}', with entries '{debugBatchLogMessageFormatter(batch, formatterArgs)}' to table '{tableName}'", ex);
                        throw;
                    }
                }
            }

            if (!succeeded)
            {
                logger.Warn($"Unable to delete transport operation entries '{debugBatchLogMessageFormatter(batch, formatterArgs)}' for batch '{batchNumber}/{totalBatches}' in table '{tableName}'.");
            }
        }

        static async Task<BatchWriteItemResponse> WriteBatch<TArgs>(this IAmazonDynamoDB dynamoDbClient,
            List<WriteRequest> batch, int batchNumber, int totalBatches,
            string tableName, ILog logger, Func<IReadOnlyCollection<WriteRequest>, TArgs?, string> debugBatchLogMessageFormatter,
            TArgs? formatterArgs, CancellationToken cancellationToken)
        {
            string? logBatchEntries = null;
            if (logger.IsDebugEnabled)
            {
                logBatchEntries = debugBatchLogMessageFormatter(batch, formatterArgs);
                logger.Debug(
                    $"Writing batch '{batchNumber}/{totalBatches}' with entries '{logBatchEntries}' to table '{tableName}'");
            }

            var batchWriteItemRequest = new BatchWriteItemRequest
            {
                RequestItems =
                    new Dictionary<string, List<WriteRequest>> { { tableName, batch } },
            };

            var result = await dynamoDbClient.BatchWriteItemAsync(batchWriteItemRequest, cancellationToken)
                .ConfigureAwait(false);

            if (logger.IsDebugEnabled)
            {
                logger.Debug(
                    $"Wrote batch '{batchNumber}/{totalBatches}' with entries '{logBatchEntries}' to table '{tableName}'");
            }

            return result;
        }

        static Task BatchDelay(int attempt, TimeSpan? retryDelay, Func<TimeSpan, CancellationToken, Task> delay, CancellationToken cancellationToken)
            => delay(CalculateDelay(attempt, retryDelay), cancellationToken);

        static TimeSpan CalculateDelay(int attempt, TimeSpan? retryDelay)
        {
            var delay = Convert.ToInt32(retryDelay.GetValueOrDefault(TimeSpan.FromMilliseconds(250)));
            return TimeSpan.FromMilliseconds(Math.Min(delay, attempt * delay));
        }

        static Task DefaultDelay(TimeSpan retryDelay, CancellationToken cancellationToken)
            => Task.Delay(retryDelay, cancellationToken);
    }
}