namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using NUnit.Framework;

    [TestFixture]
    public class DataSerializerTests
    {
        [Test]
        public void Should_roundtrip_streams()
        {
            var classWithMemoryStream = new ClassWithMemoryStream
            {
                SomeStream = new MemoryStream(Encoding.UTF8.GetBytes("Hello World"))
            };

            var attributes = DataSerializer.Serialize(classWithMemoryStream);

            var deserialized = DataSerializer.Deserialize<ClassWithMemoryStream>(attributes);

            CollectionAssert.AreEquivalent(classWithMemoryStream.SomeStream.ToArray(), deserialized.SomeStream.ToArray());
        }

        class ClassWithMemoryStream
        {
            public MemoryStream SomeStream { get; set; }
        }

        [Test]
        public void Should_roundtrip_list_streams()
        {
            var classWithListOfMemoryStream = new ClassWithListOfMemoryStream
            {
                Streams = new List<MemoryStream>
                {
                    new(Encoding.UTF8.GetBytes("Hello World 1")),
                    new(Encoding.UTF8.GetBytes("Hello World 2")),
                }
            };

            var attributes = DataSerializer.Serialize(classWithListOfMemoryStream);

            var deserialized = DataSerializer.Deserialize<ClassWithListOfMemoryStream>(attributes);

            CollectionAssert.AreEquivalent(classWithListOfMemoryStream.Streams[0].ToArray(), deserialized.Streams[0].ToArray());
            CollectionAssert.AreEquivalent(classWithListOfMemoryStream.Streams[1].ToArray(), deserialized.Streams[1].ToArray());
        }

        class ClassWithListOfMemoryStream
        {
            public List<MemoryStream> Streams { get; set; }
        }

        [Test]
        public void Should_roundtrip_nested_streams()
        {
            var classWithMemoryStream = new ClassWithNestedMemoryStream
            {
                SomeStream = new MemoryStream(Encoding.UTF8.GetBytes("Hello World 1")),
                Nested = new ClassWithNestedMemoryStream.Subclass
                {
                    SomeStream = new MemoryStream(Encoding.UTF8.GetBytes("Hello World 2"))
                }
            };

            var attributes = DataSerializer.Serialize(classWithMemoryStream);

            var deserialized = DataSerializer.Deserialize<ClassWithNestedMemoryStream>(attributes);

            CollectionAssert.AreEquivalent(classWithMemoryStream.SomeStream.ToArray(), deserialized.SomeStream.ToArray());
            CollectionAssert.AreEquivalent(classWithMemoryStream.Nested.SomeStream.ToArray(), deserialized.Nested.SomeStream.ToArray());
        }

        class ClassWithNestedMemoryStream
        {
            public MemoryStream SomeStream { get; set; }

            public Subclass Nested { get; set; }

            public class Subclass
            {
                public MemoryStream SomeStream { get; set; }
            }
        }
    }
}