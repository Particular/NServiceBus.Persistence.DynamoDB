namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text;
    using System.Text.Json;
    using NUnit.Framework;

    [TestFixture]
    public class DataSerializerTests
    {
        [Test]
        public void Should_roundtrip_basic_poco()
        {
            var basicPoco = new BasicPoco
            {
                Guid = Guid.NewGuid(),
                String = "Hello World 1",
                Boolean = true,
            };

            var attributes = DataSerializer.Serialize(basicPoco);

            var deserialized = DataSerializer.Deserialize<BasicPoco>(attributes);

            Assert.AreEqual(basicPoco.Guid, deserialized.Guid);
            Assert.AreEqual(basicPoco.String, deserialized.String);
            Assert.AreEqual(basicPoco.Boolean, deserialized.Boolean);

            Assert.That(attributes[nameof(BasicPoco.Guid)].S, Is.Not.Null);
            Assert.That(attributes[nameof(BasicPoco.String)].S, Is.Not.Null);
            Assert.That(attributes[nameof(BasicPoco.Boolean)].BOOL, Is.True);
        }

        [Test]
        public void Should_skip_null_values_on_basic_poco()
        {
            var basicPoco = new BasicPoco
            {
                Guid = Guid.NewGuid(),
            };

            var attributes = DataSerializer.Serialize(basicPoco);

            var deserialized = DataSerializer.Deserialize<BasicPoco>(attributes);

            Assert.AreEqual(basicPoco.Guid, deserialized.Guid);
            Assert.AreEqual(basicPoco.String, deserialized.String);
            Assert.That(attributes[nameof(BasicPoco.Guid)].S, Is.Not.Null);
            Assert.That(attributes, Does.Not.ContainKey(nameof(BasicPoco.String)));
        }

        class BasicPoco
        {
            public string String { get; set; }
            public Guid Guid { get; set; }
            public bool Boolean { get; set; }
        }

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
                SByte = sbyte.MaxValue,
                Byte = byte.MaxValue,
                Long = long.MaxValue,
                ULong = ulong.MaxValue,
                Int = int.MaxValue,
                UInt = uint.MaxValue,
                Short = short.MaxValue,
                Ushort = ushort.MaxValue,
                Double = double.MaxValue,
                Float = float.MaxValue,
            };

            var attributes = DataSerializer.Serialize(classNumbers);

            var deserialized = DataSerializer.Deserialize<ClassNumbers>(attributes);

            Assert.AreEqual(classNumbers.Int, deserialized.Int);
            Assert.AreEqual(classNumbers.Double, deserialized.Double);
            Assert.AreEqual(classNumbers.Float, deserialized.Float);
            Assert.AreEqual(classNumbers.Long, deserialized.Long);
            Assert.AreEqual(classNumbers.ULong, deserialized.ULong);
            Assert.AreEqual(classNumbers.Short, deserialized.Short);
            Assert.AreEqual(classNumbers.Ushort, deserialized.Ushort);
            Assert.AreEqual(classNumbers.UInt, deserialized.UInt);
            Assert.AreEqual(classNumbers.SByte, deserialized.SByte);
            Assert.AreEqual(classNumbers.Byte, deserialized.Byte);

            Assert.That(attributes[nameof(ClassNumbers.Int)].N, Is.EqualTo("2147483647"));
            Assert.That(attributes[nameof(ClassNumbers.Double)].N, Does.EndWith("E+308"));
            Assert.That(attributes[nameof(ClassNumbers.Float)].N, Does.EndWith("E+38"));
            Assert.That(attributes[nameof(ClassNumbers.Long)].N, Is.EqualTo("9223372036854775807"));
            Assert.That(attributes[nameof(ClassNumbers.ULong)].N, Is.EqualTo("18446744073709551615"));
            Assert.That(attributes[nameof(ClassNumbers.Short)].N, Is.EqualTo("32767"));
            Assert.That(attributes[nameof(ClassNumbers.Ushort)].N, Is.EqualTo("65535"));
            Assert.That(attributes[nameof(ClassNumbers.UInt)].N, Is.EqualTo("4294967295"));
            Assert.That(attributes[nameof(ClassNumbers.SByte)].N, Is.EqualTo("127"));
            Assert.That(attributes[nameof(ClassNumbers.Byte)].N, Is.EqualTo("255"));
        }

        // BigInt is not supported by System.Text.Json
        class ClassNumbers
        {
            public byte Byte { get; set; }
            public short Short { get; set; }
            public ushort Ushort { get; set; }
            public int Int { get; set; }
            public double Double { get; set; }
            public long Long { get; set; }
            public ulong ULong { get; set; }
            public float Float { get; set; }
            public uint UInt { get; set; }
            public sbyte SByte { get; set; }
        }

        [Test]
        public void Should_roundtrip_enumerable_of_numbers()
        {
            var classWithListOfNumbers = new ClassWithListOfNumbers
            {
                Ints = new List<int>
                {
                    int.MinValue, int.MaxValue
                },
                Doubles = new List<double>
                {
                    double.MinValue, double.MaxValue
                },
                Floats = new List<float>
                {
                    float.MinValue, float.MaxValue
                },
                Bytes = new List<byte>
                {
                    byte.MinValue, byte.MaxValue
                },
                Shorts = new List<short>
                {
                    short.MinValue, short.MaxValue
                },
                UShorts = new List<ushort>
                {
                    ushort.MinValue, ushort.MaxValue
                },
                Longs = new List<long>
                {
                    long.MinValue, long.MaxValue
                },
                ULongs = new List<ulong>
                {
                    ulong.MinValue, ulong.MaxValue
                },
                UInts = new List<uint>
                {
                    uint.MinValue, uint.MaxValue
                },
                SBytes = new List<sbyte>
                {
                    sbyte.MinValue, sbyte.MaxValue
                }
            };

            var attributes = DataSerializer.Serialize(classWithListOfNumbers);

            var deserialized = DataSerializer.Deserialize<ClassWithListOfNumbers>(attributes);

            CollectionAssert.AreEquivalent(classWithListOfNumbers.Ints, deserialized.Ints);
            CollectionAssert.AreEquivalent(classWithListOfNumbers.Doubles, deserialized.Doubles);
            CollectionAssert.AreEquivalent(classWithListOfNumbers.Floats, deserialized.Floats);
            CollectionAssert.AreEquivalent(classWithListOfNumbers.Bytes, deserialized.Bytes);
            CollectionAssert.AreEquivalent(classWithListOfNumbers.Shorts, deserialized.Shorts);
            CollectionAssert.AreEquivalent(classWithListOfNumbers.UShorts, deserialized.UShorts);
            CollectionAssert.AreEquivalent(classWithListOfNumbers.Longs, deserialized.Longs);
            CollectionAssert.AreEquivalent(classWithListOfNumbers.ULongs, deserialized.ULongs);
            CollectionAssert.AreEquivalent(classWithListOfNumbers.UInts, deserialized.UInts);
            CollectionAssert.AreEquivalent(classWithListOfNumbers.SBytes, deserialized.SBytes);

            Assert.That(attributes[nameof(ClassWithListOfNumbers.Ints)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithListOfNumbers.Doubles)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithListOfNumbers.Floats)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithListOfNumbers.Bytes)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithListOfNumbers.Shorts)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithListOfNumbers.UShorts)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithListOfNumbers.Longs)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithListOfNumbers.ULongs)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithListOfNumbers.UInts)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithListOfNumbers.SBytes)].NS, Has.Count.EqualTo(2));
        }

        class ClassWithListOfNumbers
        {
            public List<int> Ints { get; set; }
            public List<double> Doubles { get; set; }
            public List<float> Floats { get; set; }
            public List<byte> Bytes { get; set; }
            public List<short> Shorts { get; set; }
            public List<ushort> UShorts { get; set; }
            public List<long> Longs { get; set; }
            public List<ulong> ULongs { get; set; }
            public List<uint> UInts { get; set; }
            public List<sbyte> SBytes { get; set; }
        }

        [Test]
        public void Should_detect_cyclic_references()
        {
            var reference = new ClassWithCyclicReference();

            var classWithCyclicReference = new ClassWithCyclicReference();

            reference.References = new List<ClassWithCyclicReference> { classWithCyclicReference };

            classWithCyclicReference.References = new List<ClassWithCyclicReference> { reference };

            Assert.Throws<JsonException>(() => DataSerializer.Serialize(classWithCyclicReference));
        }

        class ClassWithCyclicReference
        {
            public List<ClassWithCyclicReference> References { get; set; }
        }
    }
}