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
            if (writeRequests.Count == 0)
            {
                return allWriteRequests;
            }
            var currentBatch = new List<WriteRequest>(MaximumNumberOfWriteRequestsInABatch);
            allWriteRequests.Add(currentBatch);
            int index = 0;
            foreach (var writeRequest in writeRequests)
            {
                if (index != 0 && index % MaximumNumberOfWriteRequestsInABatch == 0)
                {
                    currentBatch = new List<WriteRequest>(MaximumNumberOfWriteRequestsInABatch);
                    allWriteRequests.Add(currentBatch);
                }
                currentBatch.Add(writeRequest);
                index++;
            }
            return allWriteRequests;
        }

        const int MaximumNumberOfWriteRequestsInABatch = 25;
    }
}