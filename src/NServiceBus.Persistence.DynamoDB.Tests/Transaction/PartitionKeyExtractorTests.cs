namespace NServiceBus.Persistence.DynamoDB.Tests.Transaction
{
    using System;
    using System.Collections.Generic;
    using NUnit.Framework;

    [TestFixture]
    public class PartitionKeyExtractorTests
    {
        PartitionKeyExtractor extractor;

        [SetUp]
        public void SetUp()
        {
            extractor = new PartitionKeyExtractor();
        }

        [Test]
        public void Should_not_extract_from_header_with_no_matching_key()
        {
            extractor.ExtractPartitionKeyFromHeader("AnotherHeaderKey");

            var headers = new Dictionary<string, string> { { "HeaderKey", "HeaderValue" } };

            var wasExtracted = extractor.TryExtract(headers, out var partitionKey);

            Assert.That(wasExtracted, Is.False);
            Assert.That(partitionKey, Is.Null);
        }

        [Test]
        public void Should_extract_from_header_with_first_match_winning()
        {
            extractor.ExtractPartitionKeyFromHeader("HeaderKey");
            extractor.ExtractPartitionKeyFromHeader("AnotherHeaderKey");

            var headers = new Dictionary<string, string>
            {
                { "AnotherHeaderKey", "AnotherHeaderValue" },
                { "HeaderKey", "HeaderValue" }
            };

            var wasExtracted = extractor.TryExtract(headers, out var partitionKey);

            Assert.That(wasExtracted, Is.True);
            Assert.That(partitionKey, Is.Not.Null.And.EqualTo(new PartitionKey("HeaderValue")));
        }

        [Test]
        public void Should_extract_from_header_with_key()
        {
            extractor.ExtractPartitionKeyFromHeader("HeaderKey");

            var headers = new Dictionary<string, string> { { "HeaderKey", "HeaderValue" } };

            var wasExtracted = extractor.TryExtract(headers, out var partitionKey);

            Assert.That(wasExtracted, Is.True);
            Assert.That(partitionKey, Is.Not.Null.And.EqualTo(new PartitionKey("HeaderValue")));
        }

        [Test]
        public void Should_extract_from_header_with_key_and_extractor()
        {
            extractor.ExtractPartitionKeyFromHeader("HeaderKey", value => value.Replace("__TOBEREMOVED__", string.Empty));

            var headers = new Dictionary<string, string> { { "HeaderKey", "HeaderValue__TOBEREMOVED__" } };

            var wasExtracted = extractor.TryExtract(headers, out var partitionKey);

            Assert.That(wasExtracted, Is.True);
            Assert.That(partitionKey, Is.Not.Null.And.EqualTo(new PartitionKey("HeaderValue")));
        }

        [Test]
        public void Should_extract_from_header_with_key_and_extractor_with_partition_key()
        {
            extractor.ExtractPartitionKeyFromHeader("HeaderKey", value => new PartitionKey(value.Replace("__TOBEREMOVED__", string.Empty)));

            var headers = new Dictionary<string, string> { { "HeaderKey", "HeaderValue__TOBEREMOVED__" } };

            var wasExtracted = extractor.TryExtract(headers, out var partitionKey);

            Assert.That(wasExtracted, Is.True);
            Assert.That(partitionKey, Is.Not.Null.And.EqualTo(new PartitionKey("HeaderValue")));
        }

        [Test]
        public void Should_extract_from_header_with_key_extractor_and_argument()
        {
            extractor.ExtractPartitionKeyFromHeader("HeaderKey", (value, toBeRemoved) => value.Replace(toBeRemoved, string.Empty), "__TOBEREMOVED__");

            var headers = new Dictionary<string, string> { { "HeaderKey", "HeaderValue__TOBEREMOVED__" } };

            var wasExtracted = extractor.TryExtract(headers, out var partitionKey);

            Assert.That(wasExtracted, Is.True);
            Assert.That(partitionKey, Is.Not.Null.And.EqualTo(new PartitionKey("HeaderValue")));
        }

        [Test]
        public void Should_extract_from_headers_with_extractor()
        {
            extractor.ExtractPartitionKeyFromHeaders(hdrs => new PartitionKey(hdrs["HeaderKey"]));

            var headers = new Dictionary<string, string> { { "HeaderKey", "HeaderValue" } };

            var wasExtracted = extractor.TryExtract(headers, out var partitionKey);

            Assert.That(wasExtracted, Is.True);
            Assert.That(partitionKey, Is.Not.Null.And.EqualTo(new PartitionKey("HeaderValue")));
        }

        [Test]
        public void Should_extract_from_headers_with_extractor_and_allow_null()
        {
            extractor.ExtractPartitionKeyFromHeaders(hdrs => null);

            var headers = new Dictionary<string, string> { { "HeaderKey", "HeaderValue" } };

            var wasExtracted = extractor.TryExtract(headers, out var partitionKey);

            Assert.That(wasExtracted, Is.False);
            Assert.That(partitionKey, Is.Null);
        }

        [Test]
        public void Should_extract_from_headers_with_extractor_and_argument()
        {
            extractor.ExtractPartitionKeyFromHeaders((hdrs, toBeRemoved) => new PartitionKey(hdrs["HeaderKey"].Replace(toBeRemoved, string.Empty)), "__TOBEREMOVED__");

            var headers = new Dictionary<string, string> { { "HeaderKey", "HeaderValue" } };

            var wasExtracted = extractor.TryExtract(headers, out var partitionKey);

            Assert.That(wasExtracted, Is.True);
            Assert.That(partitionKey, Is.Not.Null.And.EqualTo(new PartitionKey("HeaderValue")));
        }

        [Test]
        public void Should_extract_from_headers_with_extractor_and_argument_and_allow_null()
        {
            extractor.ExtractPartitionKeyFromHeaders((hdrs, toBeRemoved) => null, "__TOBEREMOVED__");

            var headers = new Dictionary<string, string> { { "HeaderKey", "HeaderValue" } };

            var wasExtracted = extractor.TryExtract(headers, out var partitionKey);

            Assert.That(wasExtracted, Is.False);
            Assert.That(partitionKey, Is.Null);
        }

        [Test]
        public void Should_extract_from_header_with_key_converter_partition_key_and_argument()
        {
            extractor.ExtractPartitionKeyFromHeader("HeaderKey", (value, toBeRemoved) => new PartitionKey(value.Replace(toBeRemoved, string.Empty)), "__TOBEREMOVED__");

            var headers = new Dictionary<string, string> { { "HeaderKey", "HeaderValue__TOBEREMOVED__" } };

            var wasExtracted = extractor.TryExtract(headers, out var partitionKey);

            Assert.That(wasExtracted, Is.True);
            Assert.That(partitionKey, Is.Not.Null.And.EqualTo(new PartitionKey("HeaderValue")));
        }

        [Test]
        public void Should_throw_when_header_is_already_mapped()
        {
            extractor.ExtractPartitionKeyFromHeader("HeaderKey");

            var exception = Assert.Throws<ArgumentException>(() => extractor.ExtractPartitionKeyFromHeader("HeaderKey"));

            Assert.That(exception.Message, Contains.Substring("The header key 'HeaderKey' is already being handled by a header extractor and cannot be processed by another one."));
        }

        // Just a silly helper to show that state can be passed
        class ArgumentHelper
        {
            public string Upper(string input) => input.ToUpperInvariant();
        }
    }
}