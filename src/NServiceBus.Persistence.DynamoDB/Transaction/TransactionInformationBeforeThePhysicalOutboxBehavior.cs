namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Threading.Tasks;
    using Microsoft.Extensions.DependencyInjection;
    using Pipeline;

    class TransactionInformationBeforeThePhysicalOutboxBehavior : IBehavior<ITransportReceiveContext, ITransportReceiveContext>
    {
        readonly IPartitionKeyFromHeadersExtractor partitionKeyExtractor;

        public TransactionInformationBeforeThePhysicalOutboxBehavior(IPartitionKeyFromHeadersExtractor partitionKeyExtractor)
        {
            this.partitionKeyExtractor = partitionKeyExtractor;
        }

        public Task Invoke(ITransportReceiveContext context, Func<ITransportReceiveContext, Task> next)
        {
            if (partitionKeyExtractor.TryExtract(context.Message.Headers, out var partitionKey))
            {
                // once we move to nullable reference type we can annotate the partition key with NotNullWhenAttribute and get rid of this check
                if (partitionKey.HasValue)
                {
                    context.Extensions.Set(partitionKey.Value);
                }
            }
            return next(context);
        }

        public class RegisterStep : Pipeline.RegisterStep
        {
            public RegisterStep(
                PartitionKeyExtractor partitionKeyExtractor) :
                base(nameof(TransactionInformationBeforeThePhysicalOutboxBehavior),
                typeof(TransactionInformationBeforeThePhysicalOutboxBehavior),
                "Populates the transaction information before the physical outbox.",
                b =>
                {
                    var partitionKeyFromHeadersExtractors = b.GetServices<IPartitionKeyFromHeadersExtractor>();
                    foreach (var extractor in partitionKeyFromHeadersExtractors)
                    {
                        partitionKeyExtractor.ExtractPartitionKeyFromHeaders(extractor);
                    }

                    return new TransactionInformationBeforeThePhysicalOutboxBehavior(partitionKeyExtractor);
                })
            {
            }
        }
    }
}