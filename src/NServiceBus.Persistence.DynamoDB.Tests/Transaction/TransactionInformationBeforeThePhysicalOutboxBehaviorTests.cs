namespace NServiceBus.Persistence.DynamoDB.Tests.Transaction
{
    using System.Collections.Generic;
    using System.Threading.Tasks;
    using NUnit.Framework;
    using Testing;

    [TestFixture]
    public class TransactionInformationBeforeThePhysicalOutboxBehaviorTests
    {
        [Test]
        public async Task Should_not_set_partition_key_when_partition_key_extractor_returns_false()
        {
            var extractor = new PartitionKeyExtractor(
                (IReadOnlyDictionary<string, string> headers, out PartitionKey? key) =>
                {
                    key = null;
                    return false;
                });

            var behavior = new TransactionInformationBeforeThePhysicalOutboxBehavior(extractor);

            var context = new TestableTransportReceiveContext();

            await behavior.Invoke(context, _ => Task.CompletedTask);

            Assert.That(context.Extensions.TryGet<PartitionKey>(out _), Is.False);
        }

        [Test]
        public async Task Should_set_partition_key_when_partition_key_extractor_returns_true()
        {
            var partitionKeyExtractor = new PartitionKeyExtractor(
                (IReadOnlyDictionary<string, string> headers, out PartitionKey? key) =>
                {
                    key = new PartitionKey(bool.TrueString);
                    return true;
                });

            var behavior = new TransactionInformationBeforeThePhysicalOutboxBehavior(partitionKeyExtractor);

            var context = new TestableTransportReceiveContext();

            await behavior.Invoke(context, _ => Task.CompletedTask);

            Assert.That(context.Extensions.TryGet<PartitionKey>(out var partitionKey), Is.True);
            Assert.AreEqual(new PartitionKey(bool.TrueString), partitionKey);
        }

        [Test]
        public async Task Should_pass_headers_to_partition_key_extractor()
        {
            IReadOnlyDictionary<string, string> capturedHeaders = null;
            var partitionKeyExtractor = new PartitionKeyExtractor(
                (IReadOnlyDictionary<string, string> headers, out PartitionKey? key) =>
                {
                    key = null;
                    capturedHeaders = headers;
                    return false;
                });

            var behavior = new TransactionInformationBeforeThePhysicalOutboxBehavior(partitionKeyExtractor);

            var context = new TestableTransportReceiveContext();
            context.Message.Headers.Add("TheAnswer", "Is42");

            await behavior.Invoke(context, _ => Task.CompletedTask);

            Assert.That(capturedHeaders, Is.EqualTo(context.Message.Headers));
        }

        delegate bool TryExtractPartitionKey(IReadOnlyDictionary<string, string> headers, out PartitionKey? partitionKey);

        class PartitionKeyExtractor : IPartitionKeyFromHeadersExtractor
        {
            readonly TryExtractPartitionKey tryExtract;

            public PartitionKeyExtractor(TryExtractPartitionKey tryExtract = default)
            {
                if (tryExtract == null)
                {
                    this.tryExtract = (IReadOnlyDictionary<string, string> headers,
                        out PartitionKey? partitionKey) =>
                    {
                        partitionKey = null;
                        return false;
                    };
                    return;
                }

                this.tryExtract = tryExtract;
            }

            public bool TryExtract(IReadOnlyDictionary<string, string> headers, out PartitionKey? partitionKey) =>
                tryExtract(headers, out partitionKey);
        }
    }
}