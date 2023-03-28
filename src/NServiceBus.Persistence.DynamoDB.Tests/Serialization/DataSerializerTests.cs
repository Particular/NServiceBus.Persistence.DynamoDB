namespace NServiceBus.Persistence.DynamoDB.Tests
{
    using System;
    using System.Collections.Generic;
    using System.Collections.Immutable;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Text.Json;
    using NUnit.Framework;

    [TestFixture]
    public class DataSerializerTests
    {
        [Test]
        public void Should_roundtrip_basic_poco()
        {
            var basicPocoId = Guid.NewGuid();
            var basicPoco = new BasicPoco
            {
                Guid = basicPocoId,
                String = "Hello World 1",
                Boolean = true,
            };

            var attributes = DataSerializer.Serialize(basicPoco);

            var deserialized = DataSerializer.Deserialize<BasicPoco>(attributes);

            Assert.AreEqual(basicPoco.Guid, deserialized.Guid);
            Assert.AreEqual(basicPoco.String, deserialized.String);
            Assert.AreEqual(basicPoco.Boolean, deserialized.Boolean);

            Assert.That(attributes[nameof(BasicPoco.Guid)].S, Is.EqualTo(basicPocoId.ToString()));
            Assert.That(attributes[nameof(BasicPoco.String)].S, Is.EqualTo("Hello World 1"));
            Assert.That(attributes[nameof(BasicPoco.Boolean)].BOOL, Is.True);
        }

        [Test]
        public void Should_skip_null_values_on_basic_poco()
        {
            var basicPocoId = Guid.NewGuid();
            var basicPoco = new BasicPoco
            {
                Guid = basicPocoId,
            };

            var attributes = DataSerializer.Serialize(basicPoco);

            var deserialized = DataSerializer.Deserialize<BasicPoco>(attributes);

            Assert.AreEqual(basicPoco.Guid, deserialized.Guid);
            Assert.That(attributes[nameof(BasicPoco.Guid)].S, Is.EqualTo(basicPocoId.ToString()));
            Assert.That(attributes, Does.Not.ContainKey(nameof(BasicPoco.String)));
        }

        class BasicPoco
        {
            public string String { get; set; }
            public Guid Guid { get; set; }
            public bool Boolean { get; set; }
        }

        [Test]
        public void Should_roundtrip_nested_poco()
        {
            var nestedPocoId = Guid.NewGuid();
            var nestedPoco = new NestedPoco
            {
                Guid = nestedPocoId,
                String = "Hello World 1",
                Boolean = true,
                SubPoco = new SubPoco
                {
                    Guid = Guid.NewGuid(),
                    String = "Hello World 2",
                    Boolean = false,
                    SubSubPoco = new SubSubPoco
                    {
                        Guid = Guid.NewGuid(),
                        String = "Hello World 3",
                        Boolean = true,
                    }
                }
            };

            var attributes = DataSerializer.Serialize(nestedPoco);

            var deserialized = DataSerializer.Deserialize<NestedPoco>(attributes);

            Assert.AreEqual(nestedPoco.Guid, deserialized.Guid);
            Assert.AreEqual(nestedPoco.String, deserialized.String);
            Assert.AreEqual(nestedPoco.Boolean, deserialized.Boolean);

            Assert.That(nestedPoco.SubPoco, Is.Not.Null);
            Assert.AreEqual(nestedPoco.SubPoco.Guid, deserialized.SubPoco.Guid);
            Assert.AreEqual(nestedPoco.SubPoco.String, deserialized.SubPoco.String);
            Assert.AreEqual(nestedPoco.SubPoco.Boolean, deserialized.SubPoco.Boolean);

            Assert.That(nestedPoco.SubPoco.SubSubPoco, Is.Not.Null);
            Assert.AreEqual(nestedPoco.SubPoco.SubSubPoco.Guid, deserialized.SubPoco.SubSubPoco.Guid);
            Assert.AreEqual(nestedPoco.SubPoco.SubSubPoco.String, deserialized.SubPoco.SubSubPoco.String);
            Assert.AreEqual(nestedPoco.SubPoco.SubSubPoco.Boolean, deserialized.SubPoco.SubSubPoco.Boolean);

            Assert.That(attributes[nameof(NestedPoco.Guid)].S, Is.EqualTo(nestedPocoId.ToString()));
            Assert.That(attributes[nameof(NestedPoco.String)].S, Is.EqualTo("Hello World 1"));
            Assert.That(attributes[nameof(NestedPoco.Boolean)].BOOL, Is.True);
        }

        [Test]
        public void Should_skip_null_values_on_nested_poco()
        {
            var nestedPoco = new NestedPoco
            {
                Guid = Guid.NewGuid(),
            };

            var attributes = DataSerializer.Serialize(nestedPoco);

            var deserialized = DataSerializer.Deserialize<NestedPoco>(attributes);

            Assert.AreEqual(nestedPoco.Guid, deserialized.Guid);
            Assert.That(nestedPoco.SubPoco, Is.Null);
            Assert.That(attributes[nameof(NestedPoco.Guid)].S, Is.Not.Null);

            Assert.That(attributes, Does.Not.ContainKey(nameof(NestedPoco.String)));
            Assert.That(attributes, Does.Not.ContainKey(nameof(NestedPoco.SubPoco)));
        }

        class NestedPoco
        {
            public string String { get; set; }
            public Guid Guid { get; set; }
            public bool Boolean { get; set; }
            public SubPoco SubPoco { get; set; }
        }

        class SubPoco
        {
            public string String { get; set; }
            public Guid Guid { get; set; }
            public bool Boolean { get; set; }
            public SubSubPoco SubSubPoco { get; set; }
        }

        class SubSubPoco
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
        public void Should_roundtrip_sets_streams()
        {
            var classWithListOfMemoryStream = new ClassWithSetOfMemoryStream
            {
                HashSetOfMemoryStreams = new HashSet<MemoryStream>
                {
                    new(Encoding.UTF8.GetBytes("Hello World 1")),
                    new(Encoding.UTF8.GetBytes("Hello World 2")),
                },
                ImmutableHashSetOfStreams = new HashSet<MemoryStream>
                {
                    new(Encoding.UTF8.GetBytes("Hello World 1")),
                    new(Encoding.UTF8.GetBytes("Hello World 2")),
                }.ToImmutableHashSet()
            };

            var attributes = DataSerializer.Serialize(classWithListOfMemoryStream);

            var deserialized = DataSerializer.Deserialize<ClassWithSetOfMemoryStream>(attributes);

            CollectionAssert.AreEquivalent(classWithListOfMemoryStream.HashSetOfMemoryStreams, deserialized.HashSetOfMemoryStreams);
            CollectionAssert.AreEquivalent(classWithListOfMemoryStream.ImmutableHashSetOfStreams, deserialized.ImmutableHashSetOfStreams);

            Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.HashSetOfMemoryStreams)].BS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.ImmutableHashSetOfStreams)].BS, Has.Count.EqualTo(2));
        }

        // Sorted sets don't really make sense here
        class ClassWithSetOfMemoryStream
        {
            public HashSet<MemoryStream> HashSetOfMemoryStreams { get; set; }
            public ImmutableHashSet<MemoryStream> ImmutableHashSetOfStreams { get; set; }
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
        public void Should_roundtrip_set_of_strings()
        {
            var classWithSetOfString = new ClassWithSetOStrings
            {
                HashSetOfString = new HashSet<string>
                {
                    "Hello World 1",
                    "Hello World 2"
                },
                SortedSetOfString = new SortedSet<string>
                {
                    "Hello World 1",
                    "Hello World 2"
                },
                ImmutableHashSetOfString = new HashSet<string>
                {
                    "Hello World 1",
                    "Hello World 2"
                }.ToImmutableHashSet(),
                ImmutableSortedSetOfString = new SortedSet<string>
                {
                    "Hello World 1",
                    "Hello World 2"
                }.ToImmutableSortedSet(),
            };

            var attributes = DataSerializer.Serialize(classWithSetOfString);

            var deserialized = DataSerializer.Deserialize<ClassWithSetOStrings>(attributes);

            CollectionAssert.AreEquivalent(classWithSetOfString.HashSetOfString, deserialized.HashSetOfString);
            CollectionAssert.AreEquivalent(classWithSetOfString.SortedSetOfString, deserialized.SortedSetOfString);
            CollectionAssert.AreEquivalent(classWithSetOfString.ImmutableHashSetOfString, deserialized.ImmutableHashSetOfString);
            CollectionAssert.AreEquivalent(classWithSetOfString.ImmutableSortedSetOfString, deserialized.ImmutableSortedSetOfString);

            Assert.That(attributes[nameof(ClassWithSetOStrings.HashSetOfString)].SS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOStrings.SortedSetOfString)].SS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOStrings.ImmutableHashSetOfString)].SS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOStrings.ImmutableSortedSetOfString)].SS, Has.Count.EqualTo(2));

            Assert.That(attributes[nameof(ClassWithSetOStrings.HashSetOfString)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOStrings.SortedSetOfString)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOStrings.ImmutableHashSetOfString)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOStrings.ImmutableSortedSetOfString)].L, Has.Count.Zero);
        }

        class ClassWithSetOStrings
        {
            public HashSet<string> HashSetOfString { get; set; }
            public SortedSet<string> SortedSetOfString { get; set; }
            public ImmutableHashSet<string> ImmutableHashSetOfString { get; set; }
            public ImmutableSortedSet<string> ImmutableSortedSetOfString { get; set; }
        }

        [Test]
        public void Should_roundtrip_list_of_strings()
        {
            var classWithListOStrings = new ClasWithListOfString
            {
                ListStrings = new List<string>
                {
                    "Hello World 1",
                    "Hello World 2"
                },
                ArrayStrings = new[]
                {
                    "Hello World 1",
                    "Hello World 2"
                }
            };

            var attributes = DataSerializer.Serialize(classWithListOStrings);

            var deserialized = DataSerializer.Deserialize<ClasWithListOfString>(attributes);

            CollectionAssert.AreEquivalent(classWithListOStrings.ListStrings, deserialized.ListStrings);
            CollectionAssert.AreEquivalent(classWithListOStrings.ArrayStrings, deserialized.ArrayStrings);

            Assert.That(attributes[nameof(ClasWithListOfString.ListStrings)].L, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClasWithListOfString.ListStrings)].SS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClasWithListOfString.ArrayStrings)].L, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClasWithListOfString.ArrayStrings)].SS, Has.Count.Zero);
        }

        // Not testing all possible enumeration types since this path goes directly through the default
        // JSON serialization behavior
        class ClasWithListOfString
        {
            public List<string> ListStrings { get; set; }
            public string[] ArrayStrings { get; set; }
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
                Decimal = decimal.MaxValue,
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
            Assert.AreEqual(classNumbers.Decimal, deserialized.Decimal);

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
            Assert.That(attributes[nameof(ClassNumbers.Decimal)].N, Is.EqualTo("79228162514264337593543950335"));
        }

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
            public decimal Decimal { get; set; }
        }

        [Test]
        public void Should_roundtrip_set_of_numbers()
        {
            var classWithHashSetOfNumbers = new ClassWithSetOfNumbers
            {
                Ints = new HashSet<int>
                {
                    int.MinValue, int.MaxValue
                },
                Doubles = new SortedSet<double>
                {
                    double.MinValue, double.MaxValue
                },
                Floats = new HashSet<float>
                {
                    float.MinValue, float.MaxValue
                }.ToImmutableHashSet(),
                Bytes = new HashSet<byte>
                {
                    byte.MinValue, byte.MaxValue
                }.ToImmutableSortedSet(),
                Shorts = new HashSet<short>
                {
                    short.MinValue, short.MaxValue
                },
                UShorts = new SortedSet<ushort>
                {
                    ushort.MinValue, ushort.MaxValue
                },
                Longs = new HashSet<long>
                {
                    long.MinValue, long.MaxValue
                }.ToImmutableHashSet(),
                ULongs = new HashSet<ulong>
                {
                    ulong.MinValue, ulong.MaxValue
                }.ToImmutableSortedSet(),
                UInts = new HashSet<uint>
                {
                    uint.MinValue, uint.MaxValue
                },
                SBytes = new SortedSet<sbyte>
                {
                    sbyte.MinValue, sbyte.MaxValue
                },
                Decimals = new HashSet<decimal>
                {
                    decimal.MinValue, decimal.MaxValue
                }.ToImmutableHashSet()
            };

            var attributes = DataSerializer.Serialize(classWithHashSetOfNumbers);

            var deserialized = DataSerializer.Deserialize<ClassWithSetOfNumbers>(attributes);

            CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Ints, deserialized.Ints);
            CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Doubles, deserialized.Doubles);
            CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Floats, deserialized.Floats);
            CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Bytes, deserialized.Bytes);
            CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Shorts, deserialized.Shorts);
            CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.UShorts, deserialized.UShorts);
            CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Longs, deserialized.Longs);
            CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.ULongs, deserialized.ULongs);
            CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.UInts, deserialized.UInts);
            CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.SBytes, deserialized.SBytes);
            CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Decimals, deserialized.Decimals);

            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Ints)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Doubles)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Floats)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Bytes)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Shorts)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.UShorts)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Longs)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.ULongs)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.UInts)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.SBytes)].NS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Decimals)].NS, Has.Count.EqualTo(2));

            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Ints)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Doubles)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Floats)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Bytes)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Shorts)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.UShorts)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Longs)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.ULongs)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.UInts)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.SBytes)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Decimals)].L, Has.Count.Zero);
        }

        class ClassWithSetOfNumbers
        {
            public HashSet<int> Ints { get; set; }
            public SortedSet<double> Doubles { get; set; }
            public ImmutableHashSet<float> Floats { get; set; }
            public ImmutableSortedSet<byte> Bytes { get; set; }
            public HashSet<short> Shorts { get; set; }
            public SortedSet<ushort> UShorts { get; set; }
            public ImmutableHashSet<long> Longs { get; set; }
            public ImmutableSortedSet<ulong> ULongs { get; set; }
            public HashSet<uint> UInts { get; set; }
            public SortedSet<sbyte> SBytes { get; set; }
            public ImmutableHashSet<decimal> Decimals { get; set; }
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