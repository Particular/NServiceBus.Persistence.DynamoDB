namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Text.Json;
    using NUnit.Framework;

    [TestFixture]
    public class MemoryStreamConverterTests
    {
        [SetUp]
        public void SetUp()
        {
            options = new JsonSerializerOptions { Converters = { new MemoryStreamConverter() } };
        }

        [Test]
        public void Should_roundtrip_streams()
        {
            var classWithMemoryStream = new ClassWithMemoryStream
            {
                SomeStream = new MemoryStream(Encoding.UTF8.GetBytes("Hello World"))
            };

            var serializeToDocument = JsonSerializer.SerializeToDocument(classWithMemoryStream, options);

            var deserialized = serializeToDocument.Deserialize<ClassWithMemoryStream>(options);

            CollectionAssert.AreEquivalent(classWithMemoryStream.SomeStream.ToArray(), deserialized.SomeStream.ToArray());
        }

        class ClassWithMemoryStream
        {
            public MemoryStream SomeStream { get; set; }
        }

        [Test]
        public void Should_round_trip_list_of_streams()
        {
            var classWithListOfMemoryStream = new ClassWithListMemoryStreams
            {
                Streams = new List<MemoryStream>
                {
                    new(Encoding.UTF8.GetBytes("Hello World 1")),
                    new(Encoding.UTF8.GetBytes("Hello World 2")),
                }
            };

            var serializeToDocument = JsonSerializer.SerializeToDocument(classWithListOfMemoryStream, options);

            var deserialized = serializeToDocument.Deserialize<ClassWithListMemoryStreams>(options);

            CollectionAssert.AreEquivalent(classWithListOfMemoryStream.Streams[0].ToArray(), deserialized.Streams[0].ToArray());
            CollectionAssert.AreEquivalent(classWithListOfMemoryStream.Streams[1].ToArray(), deserialized.Streams[1].ToArray());
        }

        class ClassWithListMemoryStreams
        {
            public List<MemoryStream> Streams { get; set; }
        }

        JsonSerializerOptions options;
    }
}