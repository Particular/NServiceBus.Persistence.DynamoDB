#nullable enable

namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using Amazon.DynamoDBv2.Model;

    // Currently placed into outbox folder but could be moved elsewhere once needed in more places.
    static class WriteRequestBatcher
    {
        // We are returning mutable state here because the SDK requires materialized lists anyway.
        public static List<List<WriteRequest>> Batch(IReadOnlyCollection<WriteRequest> writeRequests)
        {
            var allWriteRequests = new List<List<WriteRequest>>((int)Math.Ceiling(writeRequests.Count / (double)MaximumNumberOfWriteRequestsInABatch));
            List<WriteRequest>? currentBatch = null;
            int index = 0;
            foreach (var writeRequest in writeRequests)
            {
                currentBatch ??= new List<WriteRequest>(MaximumNumberOfWriteRequestsInABatch);
                if (index != 0 && index % MaximumNumberOfWriteRequestsInABatch == 0)
                {
                    allWriteRequests.Add(currentBatch);
                    currentBatch = new List<WriteRequest>(MaximumNumberOfWriteRequestsInABatch);
                }
                currentBatch.Add(writeRequest);
                index++;
            }

            if (currentBatch?.Count > 0)
            {
                allWriteRequests.Add(currentBatch);
            }
            return allWriteRequests;
        }

        const int MaximumNumberOfWriteRequestsInABatch = 25;
    }
}