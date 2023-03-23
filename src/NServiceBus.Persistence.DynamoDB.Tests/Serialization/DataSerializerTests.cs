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
            Assert.That(attributes[nameof(ClassWithMemoryStream.SomeStream)].B, Is.Not.Null);
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
            Assert.That(attributes[nameof(ClassWithListOfMemoryStream.Streams)].BS, Has.Count.EqualTo(2));
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
            Assert.That(attributes[nameof(ClassWithNestedMemoryStream.SomeStream)].B, Is.Not.Null);
            Assert.That(attributes[nameof(ClassWithNestedMemoryStream.Nested)].M[nameof(ClassWithNestedMemoryStream.SomeStream)].B, Is.Not.Null);
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

        [Test]
        public void Should_roundtrip_list_of_strings()
        {
            var classWithListOStrings = new ClassWithListOStrings
            {
                Strings = new List<string>
                {
                    "Hello World 1",
                    "Hello World 2"
                }
            };

            var attributes = DataSerializer.Serialize(classWithListOStrings);

            var deserialized = DataSerializer.Deserialize<ClassWithListOStrings>(attributes);

            CollectionAssert.AreEquivalent(classWithListOStrings.Strings, deserialized.Strings);
            Assert.That(attributes[nameof(ClassWithListOStrings.Strings)].SS, Has.Count.EqualTo(2));
        }

        class ClassWithListOStrings
        {
            public List<string> Strings { get; set; }
        }

        [Test]
        public void Should_roundtrip_numbers()
        {
            var classNumbers = new ClassNumbers
            {
                Int = 1,
                NullableInt = 1,
                Double = 1.5,
                Float = 1.5f,
            };

            var attributes = DataSerializer.Serialize(classNumbers);

            var deserialized = DataSerializer.Deserialize<ClassNumbers>(attributes);

            Assert.AreEqual(classNumbers.Int, deserialized.Int);
            Assert.AreEqual(classNumbers.NullableInt, deserialized.NullableInt);
            Assert.AreEqual(classNumbers.Double, deserialized.Double);
            Assert.AreEqual(classNumbers.Float, deserialized.Float);
            Assert.That(attributes[nameof(ClassNumbers.Int)].N, Is.EqualTo("1"));
            Assert.That(attributes[nameof(ClassNumbers.NullableInt)].N, Is.EqualTo("1"));
            Assert.That(attributes[nameof(ClassNumbers.Double)].N, Is.EqualTo("1.5"));
            Assert.That(attributes[nameof(ClassNumbers.Float)].N, Is.EqualTo("1.5"));
        }

        class ClassNumbers
        {
            public int Int { get; set; }
            public int? NullableInt { get; set; }
            public double Double { get; set; }
            public float Float { get; set; }
        }

        [Test]
        public void Should_roundtrip_list_of_numbers()
        {
            var classWithListOfNumbers = new ClassWithListOfNumbers
            {
                Ints = new List<int>
                {
                    1, 2
                },
                Doubles = new List<double>
                {
                    1.5, 2.5
                },
                Floats = new List<float>
                {
                    1.5f, 2.5f
                }
            };

            var attributes = DataSerializer.Serialize(classWithListOfNumbers);

            var deserialized = DataSerializer.Deserialize<ClassWithListOfNumbers>(attributes);

            CollectionAssert.AreEquivalent(classWithListOfNumbers.Ints, deserialized.Ints);
            CollectionAssert.AreEquivalent(classWithListOfNumbers.Doubles, deserialized.Doubles);
            CollectionAssert.AreEquivalent(classWithListOfNumbers.Floats, deserialized.Floats);
            Assert.That(attributes[nameof(ClassWithListOfNumbers.Ints)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithListOfNumbers.Doubles)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithListOfNumbers.Floats)].NS, Has.Count.EqualTo(2));
        }

        class ClassWithListOfNumbers
        {
            public List<int> Ints { get; set; }
            public List<double> Doubles { get; set; }
            public List<float> Floats { get; set; }
        }
    }
}