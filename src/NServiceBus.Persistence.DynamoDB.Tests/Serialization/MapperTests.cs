#pragma warning disable CA1711
namespace NServiceBus.Persistence.DynamoDB.Tests;

using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.DynamoDBv2.Model;
using NUnit.Framework;

[TestFixture]
public class MapperTests
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

        var attributes = Mapper.ToMap(basicPoco);

        var deserialized = Mapper.ToObject<BasicPoco>(attributes);

        Assert.AreEqual(basicPoco.Guid, deserialized.Guid);
        Assert.AreEqual(basicPoco.String, deserialized.String);
        Assert.AreEqual(basicPoco.Boolean, deserialized.Boolean);

        Assert.That(attributes[nameof(BasicPoco.Guid)].S, Is.EqualTo(basicPocoId.ToString()), "Should have been mapped to DynamoDB string attribute");
        Assert.That(attributes[nameof(BasicPoco.String)].S, Is.EqualTo("Hello World 1"), "Should have been mapped to DynamoDB string attribute");
        Assert.That(attributes[nameof(BasicPoco.Boolean)].BOOL, Is.True, "Should have been mapped to DynamoDB Bool attribute");
    }

    [Test]
    public void Should_skip_null_values_on_basic_poco()
    {
        var basicPocoId = Guid.NewGuid();
        var basicPoco = new BasicPoco
        {
            Guid = basicPocoId,
            String = null
        };

        var attributes = Mapper.ToMap(basicPoco);

        var deserialized = Mapper.ToObject<BasicPoco>(attributes);

        Assert.AreEqual(basicPoco.Guid, deserialized.Guid);
        Assert.AreEqual(basicPoco.String, deserialized.String);
        Assert.AreEqual(basicPoco.Boolean, deserialized.Boolean);

        Assert.That(attributes[nameof(BasicPoco.Guid)].S, Is.EqualTo(basicPocoId.ToString()), "Should have been mapped to DynamoDB string attribute");
        Assert.That(attributes, Does.Not.ContainKey(nameof(BasicPoco.String)), "Null attributes should not be mapped");
        Assert.That(attributes[nameof(BasicPoco.Boolean)].BOOL, Is.False, "Should have been mapped to DynamoDB Bool attribute");
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
        var subPocoId = Guid.NewGuid();
        var subSubPocoId = Guid.NewGuid();
        var nestedPoco = new NestedPoco
        {
            Guid = nestedPocoId,
            String = "Hello World 1",
            Boolean = true,
            SubPoco = new SubPoco
            {
                Guid = subPocoId,
                String = "Hello World 2",
                Boolean = false,
                SubSubPoco = new SubSubPoco
                {
                    Guid = subSubPocoId,
                    String = "Hello World 3",
                    Boolean = true,
                }
            }
        };

        var attributes = Mapper.ToMap(nestedPoco);

        var deserialized = Mapper.ToObject<NestedPoco>(attributes);

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

        Assert.That(attributes[nameof(NestedPoco.Guid)].S, Is.EqualTo(nestedPocoId.ToString()), "Should have been mapped to DynamoDB string attribute");
        Assert.That(attributes[nameof(NestedPoco.String)].S, Is.EqualTo("Hello World 1"), "Should have been mapped to DynamoDB string attribute");
        Assert.That(attributes[nameof(NestedPoco.Boolean)].BOOL, Is.True, "Should have been mapped to DynamoDB Bool attribute");

        Assert.That(attributes[nameof(NestedPoco.SubPoco)].M[nameof(SubPoco.Guid)].S, Is.EqualTo(subPocoId.ToString()), "Should have been mapped to DynamoDB string attribute");
        Assert.That(attributes[nameof(NestedPoco.SubPoco)].M[nameof(SubPoco.String)].S, Is.EqualTo("Hello World 2"), "Should have been mapped to DynamoDB string attribute");
        Assert.That(attributes[nameof(NestedPoco.SubPoco)].M[nameof(SubPoco.Boolean)].BOOL, Is.False, "Should have been mapped to DynamoDB Bool attribute");

        Assert.That(attributes[nameof(NestedPoco.SubPoco)].M[nameof(SubPoco.SubSubPoco)].M[nameof(SubSubPoco.Guid)].S, Is.EqualTo(subSubPocoId.ToString()), "Should have been mapped to DynamoDB string attribute");
        Assert.That(attributes[nameof(NestedPoco.SubPoco)].M[nameof(SubPoco.SubSubPoco)].M[nameof(SubSubPoco.String)].S, Is.EqualTo("Hello World 3"), "Should have been mapped to DynamoDB string attribute");
        Assert.That(attributes[nameof(NestedPoco.SubPoco)].M[nameof(SubPoco.SubSubPoco)].M[nameof(SubSubPoco.Boolean)].BOOL, Is.True, "Should have been mapped to DynamoDB Bool attribute");
    }

    [Test]
    public void Should_skip_null_values_on_nested_poco()
    {
        var nestedPoco = new NestedPoco
        {
            Guid = Guid.NewGuid(),
        };

        var attributes = Mapper.ToMap(nestedPoco);

        var deserialized = Mapper.ToObject<NestedPoco>(attributes);

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

        var attributes = Mapper.ToMap(classWithMemoryStream);

        // state should never leak
        Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
        Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));

        var deserialized = Mapper.ToObject<ClassWithMemoryStream>(attributes);

        // state should never leak
        Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
        Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));

        CollectionAssert.AreEquivalent(classWithMemoryStream.SomeStream.ToArray(), deserialized.SomeStream.ToArray());
        Assert.That(attributes[nameof(ClassWithMemoryStream.SomeStream)].B, Is.EqualTo(classWithMemoryStream.SomeStream));
    }

    public class ClassWithMemoryStream
    {
        public MemoryStream SomeStream { get; set; }
    }

    [Test]
    public void Should_cleanup_map_stream_state_even_under_failure_conditions()
    {
        var classWithMemoryStream = new ClassWithMemoryStreamAndUnserializableValue
        {
            SomeStream = new MemoryStream(Encoding.UTF8.GetBytes("Hello World")),
            UnsupportedStream = Stream.Null,
        };

        Assert.Throws<InvalidOperationException>(() => Mapper.ToMap(classWithMemoryStream));

        // state should never leak
        Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
        Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
    }

    [Test]
    public void Should_cleanup_object_stream_state_even_under_failure_conditions()
    {
        var attributeMap = new Dictionary<string, AttributeValue>
        {
            { "SomeStream", new AttributeValue { B = new MemoryStream(Encoding.UTF8.GetBytes("Hello World")) } },
            { "UnsupportedStream", new AttributeValue { B = null } },
        };

        Assert.Throws<SerializationException>(() => Mapper.ToObject<ClassWithMemoryStreamAndUnserializableValue>(attributeMap));

        // state should never leak
        Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
        Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
    }

    class ClassWithMemoryStreamAndUnserializableValue
    {
        public MemoryStream SomeStream { get; set; }

        public Stream UnsupportedStream { get; set; }
    }

    [Test]
    public void Should_cleanup_map_set_stream_state_even_under_failure_conditions()
    {
        var classWithSetOfStreams = new ClassWithSetOfMemoryStreamAndUnserializableValue
        {
            SomeStreams =
            [
                new(Encoding.UTF8.GetBytes("Hello World 1")),
                new(Encoding.UTF8.GetBytes("Hello World 2")),
            ],
            UnsupportedStream = Stream.Null,
        };

        Assert.Throws<InvalidOperationException>(() => Mapper.ToMap(classWithSetOfStreams));

        // state should never leak
        Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
        Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
    }

    [Test]
    public void Should_cleanup_object_set_stream_state_even_under_failure_conditions()
    {
        var attributeMap = new Dictionary<string, AttributeValue>
        {
            { "SomeStreams", new AttributeValue { BS =
            [
                new(Encoding.UTF8.GetBytes("Hello World 1")),
                new(Encoding.UTF8.GetBytes("Hello World 2")),
            ] } },
            { "UnsupportedStream", new AttributeValue { B = null } },
        };

        Assert.Throws<SerializationException>(() => Mapper.ToObject<ClassWithSetOfMemoryStreamAndUnserializableValue>(attributeMap));

        // state should never leak
        Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
        Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
    }

    class ClassWithSetOfMemoryStreamAndUnserializableValue
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Code", "PS0025:Dictionary keys should implement IEquatable<T>",
            Justification = "It's a test")]
        public HashSet<MemoryStream> SomeStreams { get; set; }

        public Stream UnsupportedStream { get; set; }
    }

    [Test]
    public void Should_roundtrip_sets_streams()
    {
        var classWithListOfMemoryStream = new ClassWithSetOfMemoryStream
        {
            HashSetOfMemoryStreams =
            [
                new(Encoding.UTF8.GetBytes("Hello World 1")),
                new(Encoding.UTF8.GetBytes("Hello World 2")),
            ],
            ImmutableHashSetOfStreams = new HashSet<MemoryStream>
            {
                new(Encoding.UTF8.GetBytes("Hello World 1")),
                new(Encoding.UTF8.GetBytes("Hello World 2")),
            }.ToImmutableHashSet()
        };

        var attributes = Mapper.ToMap(classWithListOfMemoryStream);

        // state should never leak
        Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
        Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));

        var deserialized = Mapper.ToObject<ClassWithSetOfMemoryStream>(attributes);

        // state should never leak
        Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
        Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));

        CollectionAssert.AreEquivalent(classWithListOfMemoryStream.HashSetOfMemoryStreams, deserialized.HashSetOfMemoryStreams);
        CollectionAssert.AreEquivalent(classWithListOfMemoryStream.ImmutableHashSetOfStreams, deserialized.ImmutableHashSetOfStreams);

        Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.HashSetOfMemoryStreams)].BS, Is.EqualTo(classWithListOfMemoryStream.HashSetOfMemoryStreams));
        Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.ImmutableHashSetOfStreams)].BS, Is.EqualTo(classWithListOfMemoryStream.ImmutableHashSetOfStreams));
    }

    // Sorted sets don't really make sense here
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Code", "PS0025:Dictionary keys should implement IEquatable<T>",
        Justification = "It's a test")]
    public class ClassWithSetOfMemoryStream
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

        var attributes = Mapper.ToMap(classWithMemoryStream);

        // state should never leak
        Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
        Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));

        var deserialized = Mapper.ToObject<ClassWithNestedMemoryStream>(attributes);

        // state should never leak
        Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
        Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));

        CollectionAssert.AreEquivalent(classWithMemoryStream.SomeStream.ToArray(), deserialized.SomeStream.ToArray());
        CollectionAssert.AreEquivalent(classWithMemoryStream.Nested.SomeStream.ToArray(), deserialized.Nested.SomeStream.ToArray());
        Assert.That(attributes[nameof(ClassWithNestedMemoryStream.SomeStream)].B, Is.EqualTo(classWithMemoryStream.SomeStream));
        Assert.That(attributes[nameof(ClassWithNestedMemoryStream.Nested)].M[nameof(ClassWithNestedMemoryStream.SomeStream)].B, Is.EqualTo(classWithMemoryStream.Nested.SomeStream));
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
        var classWithSetOfString = new ClassWithSetOfString
        {
            HashSetOfString =
            [
                "Hello World 1",
                "Hello World 2"
            ],
            SortedSetOfString =
            [
                "Hello World 1",
                "Hello World 2"
            ],
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

        var attributes = Mapper.ToMap(classWithSetOfString);

        var deserialized = Mapper.ToObject<ClassWithSetOfString>(attributes);

        CollectionAssert.AreEquivalent(classWithSetOfString.HashSetOfString, deserialized.HashSetOfString);
        CollectionAssert.AreEquivalent(classWithSetOfString.SortedSetOfString, deserialized.SortedSetOfString);
        CollectionAssert.AreEquivalent(classWithSetOfString.ImmutableHashSetOfString, deserialized.ImmutableHashSetOfString);
        CollectionAssert.AreEquivalent(classWithSetOfString.ImmutableSortedSetOfString, deserialized.ImmutableSortedSetOfString);

        Assert.That(attributes[nameof(ClassWithSetOfString.HashSetOfString)].SS, Has.Count.EqualTo(2));
        Assert.That(attributes[nameof(ClassWithSetOfString.SortedSetOfString)].SS, Has.Count.EqualTo(2));
        Assert.That(attributes[nameof(ClassWithSetOfString.ImmutableHashSetOfString)].SS, Has.Count.EqualTo(2));
        Assert.That(attributes[nameof(ClassWithSetOfString.ImmutableSortedSetOfString)].SS, Has.Count.EqualTo(2));

        Assert.That(attributes[nameof(ClassWithSetOfString.HashSetOfString)].L, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfString.SortedSetOfString)].L, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfString.ImmutableHashSetOfString)].L, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfString.ImmutableSortedSetOfString)].L, Has.Count.Zero);
    }

    public class ClassWithSetOfString
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
            ListStrings =
            [
                "Hello World 1",
                "Hello World 2"
            ],
            ArrayStrings = new[]
            {
                "Hello World 1",
                "Hello World 2"
            }
        };

        var attributes = Mapper.ToMap(classWithListOStrings);

        var deserialized = Mapper.ToObject<ClasWithListOfString>(attributes);

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
        var classNumbers = new ClassWithNumbers
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

        var attributes = Mapper.ToMap(classNumbers);

        var deserialized = Mapper.ToObject<ClassWithNumbers>(attributes);

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

        Assert.That(attributes[nameof(ClassWithNumbers.Int)].N, Is.EqualTo("2147483647"));
        Assert.That(attributes[nameof(ClassWithNumbers.Double)].N, Does.EndWith("E+308"));
        Assert.That(attributes[nameof(ClassWithNumbers.Float)].N, Does.EndWith("E+38"));
        Assert.That(attributes[nameof(ClassWithNumbers.Long)].N, Is.EqualTo("9223372036854775807"));
        Assert.That(attributes[nameof(ClassWithNumbers.ULong)].N, Is.EqualTo("18446744073709551615"));
        Assert.That(attributes[nameof(ClassWithNumbers.Short)].N, Is.EqualTo("32767"));
        Assert.That(attributes[nameof(ClassWithNumbers.Ushort)].N, Is.EqualTo("65535"));
        Assert.That(attributes[nameof(ClassWithNumbers.UInt)].N, Is.EqualTo("4294967295"));
        Assert.That(attributes[nameof(ClassWithNumbers.SByte)].N, Is.EqualTo("127"));
        Assert.That(attributes[nameof(ClassWithNumbers.Byte)].N, Is.EqualTo("255"));
        Assert.That(attributes[nameof(ClassWithNumbers.Decimal)].N, Is.EqualTo("79228162514264337593543950335"));
    }

    class ClassWithNumbers
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
            Ints =
            [
                int.MinValue,
                int.MaxValue
            ],
            Doubles =
            [
                double.MinValue,
                double.MaxValue
            ],
            Floats = new HashSet<float>
            {
                float.MinValue, float.MaxValue
            }.ToImmutableHashSet(),
            Bytes = new HashSet<byte>
            {
                byte.MinValue, byte.MaxValue
            }.ToImmutableSortedSet(),
            Shorts =
            [
                short.MinValue,
                short.MaxValue
            ],
            UShorts =
            [
                ushort.MinValue,
                ushort.MaxValue
            ],
            Longs = new HashSet<long>
            {
                long.MinValue, long.MaxValue
            }.ToImmutableHashSet(),
            ULongs = new HashSet<ulong>
            {
                ulong.MinValue, ulong.MaxValue
            }.ToImmutableSortedSet(),
            UInts =
            [
                uint.MinValue,
                uint.MaxValue
            ],
            SBytes =
            [
                sbyte.MinValue,
                sbyte.MaxValue
            ],
            Decimals = new HashSet<decimal>
            {
                decimal.MinValue, decimal.MaxValue
            }.ToImmutableHashSet()
        };

        var attributes = Mapper.ToMap(classWithHashSetOfNumbers);

        var deserialized = Mapper.ToObject<ClassWithSetOfNumbers>(attributes);

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

    public class ClassWithSetOfNumbers
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
    public void Should_roundtrip_empty_sets()
    {
        var classWithHashSetOfNumbers = new ClassWithEmpytSetOfNumbers();

        var attributes = Mapper.ToMap(classWithHashSetOfNumbers);

        var deserialized = Mapper.ToObject<ClassWithEmpytSetOfNumbers>(attributes);

        CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Ints, deserialized.Ints);
        CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Doubles, deserialized.Doubles);
        CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Floats, deserialized.Floats);
        CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Bytes, deserialized.Bytes);
        CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Shorts, deserialized.Shorts);
        CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Longs, deserialized.Longs);
        CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.ULongs, deserialized.ULongs);
        CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.UInts, deserialized.UInts);
        CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.SBytes, deserialized.SBytes);
        CollectionAssert.AreEquivalent(classWithHashSetOfNumbers.Decimals, deserialized.Decimals);

        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Ints)].NS, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Ints)].IsNSSet, Is.True);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Doubles)].NS, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Doubles)].IsNSSet, Is.True);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Floats)].NS, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Floats)].IsNSSet, Is.True);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Bytes)].NS, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Bytes)].IsNSSet, Is.True);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Shorts)].NS, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Shorts)].IsNSSet, Is.True);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Longs)].NS, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Longs)].IsNSSet, Is.True);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.ULongs)].NS, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.ULongs)].IsNSSet, Is.True);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.UInts)].NS, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.UInts)].IsNSSet, Is.True);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.SBytes)].NS, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.SBytes)].IsNSSet, Is.True);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Decimals)].NS, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithSetOfNumbers.Decimals)].IsNSSet, Is.True);
    }

    public class ClassWithEmpytSetOfNumbers
    {
        public HashSet<int> Ints { get; set; } = [];
        public SortedSet<double> Doubles { get; set; } = [];
        public ImmutableHashSet<float> Floats { get; set; } = [];
        public ImmutableSortedSet<byte> Bytes { get; set; } = [];
        public HashSet<short> Shorts { get; set; } = [];
        public SortedSet<ushort> UShorts { get; set; } = [];
        public ImmutableHashSet<long> Longs { get; set; } = [];
        public ImmutableSortedSet<ulong> ULongs { get; set; } = [];
        public HashSet<uint> UInts { get; set; } = [];
        public SortedSet<sbyte> SBytes { get; set; } = [];
        public ImmutableHashSet<decimal> Decimals { get; set; } = [];
    }

    [Test]
    public void Should_roundtrip_empty_lists()
    {
        var classWithEmptyCollections = new ClassWithEmptyCollections();

        var attributes = Mapper.ToMap(classWithEmptyCollections);

        var deserialized = Mapper.ToObject<ClassWithEmptyCollections>(attributes);

        CollectionAssert.AreEquivalent(classWithEmptyCollections.RecordArray, deserialized.RecordArray);
        CollectionAssert.AreEquivalent(classWithEmptyCollections.RecordList, deserialized.RecordList);
        CollectionAssert.AreEquivalent(classWithEmptyCollections.StringArray, deserialized.StringArray);
        CollectionAssert.AreEquivalent(classWithEmptyCollections.StringList, deserialized.StringList);

        Assert.That(attributes[nameof(ClassWithEmptyCollections.RecordArray)].L, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithEmptyCollections.RecordArray)].IsLSet, Is.True);
        Assert.That(attributes[nameof(ClassWithEmptyCollections.RecordList)].L, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithEmptyCollections.RecordList)].IsLSet, Is.True);
        Assert.That(attributes[nameof(ClassWithEmptyCollections.StringList)].L, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithEmptyCollections.StringList)].IsLSet, Is.True);
    }

    public class ClassWithEmptyCollections
    {
        public List<string> StringList { get; set; } = [];
        public string[] StringArray { get; set; } = [];
        public List<SimpleType> RecordList { get; set; } = [];
        public SimpleType[] RecordArray { get; set; } = [];
    }

    public record SimpleType(string Id, double Value);

    [Test]
    public void Should_roundtrip_empty_maps()
    {
        var classWithEmptyMaps = new ClassWithEmptyMaps();

        var attributes = Mapper.ToMap(classWithEmptyMaps);

        var deserialized = Mapper.ToObject<ClassWithEmptyMaps>(attributes);

        CollectionAssert.AreEquivalent(classWithEmptyMaps.SimpleDict, deserialized.SimpleDict);
        CollectionAssert.AreEquivalent(classWithEmptyMaps.SimpleTypeDict, deserialized.SimpleTypeDict);

        Assert.That(attributes[nameof(ClassWithEmptyMaps.SimpleDict)].M, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithEmptyMaps.SimpleDict)].IsMSet, Is.True);
        Assert.That(attributes[nameof(ClassWithEmptyMaps.SimpleTypeDict)].M, Has.Count.Zero);
        Assert.That(attributes[nameof(ClassWithEmptyMaps.SimpleTypeDict)].IsMSet, Is.True);
    }

    public class ClassWithEmptyMaps
    {
        public Dictionary<string, int> SimpleDict { get; set; } = [];
        public Dictionary<string, SimpleType> SimpleTypeDict { get; set; } = [];
    }

    [Test]
    public void Should_detect_cyclic_references()
    {
        var reference = new ClassWithCyclicReference();

        var classWithCyclicReference = new ClassWithCyclicReference();

        reference.References = [classWithCyclicReference];

        classWithCyclicReference.References = [reference];

        Assert.Throws<JsonException>(() => Mapper.ToMap(classWithCyclicReference));
    }

    class ClassWithCyclicReference
    {
        public List<ClassWithCyclicReference> References { get; set; }
    }

    [Test]
    public void Should_not_throw_for_non_cyclic_references()
    {
        var reference = new ClassWithCyclicReference();

        var classWithoutCyclicReference = new ClassWithCyclicReference();
        var anotherClass = new ClassWithCyclicReference();

        reference.References = [classWithoutCyclicReference];

        classWithoutCyclicReference.References = [anotherClass];

        Assert.DoesNotThrow(() => Mapper.ToMap(classWithoutCyclicReference));
    }

    [Test]
    public void Should_throw_for_non_object_root_types()
    {
        var exception = Assert.Throws<SerializationException>(() => Mapper.ToMap(new List<int>()));

        Assert.That(exception.Message, Does.StartWith("Unable to serialize the given type"));
    }

    [Test]
    public void Should_respect_options_argument_on_to_map()
    {
        // using the default options bypasses all custom converters
        var streamException = Assert.Throws<InvalidOperationException>(() => Mapper.ToMap(new ClassWithMemoryStream
        {
            SomeStream = new MemoryStream(Encoding.UTF8.GetBytes("Hello World 1"))
        }, JsonSerializerOptions.Default));
        Assert.That(streamException!.Message, Contains.Substring("not supported on this stream."));

        var hashSetException = Assert.Throws<InvalidOperationException>(() => Mapper.ToMap(new ClassWithSetOfMemoryStream
        {
            HashSetOfMemoryStreams =
            [
                new(Encoding.UTF8.GetBytes("Hello World 1")),
                new(Encoding.UTF8.GetBytes("Hello World 2")),
            ]
        }, JsonSerializerOptions.Default));
        Assert.That(hashSetException!.Message, Contains.Substring("not supported on this stream."));

        var stringSetAttributes =
            Mapper.ToMap(
                new ClassWithSetOfString
                {
                    HashSetOfString = ["Hello World 1", "Hello World 2",]
                }, JsonSerializerOptions.Default);

        Assert.That(stringSetAttributes, Has.Count.EqualTo(1));
        // Using the default serializer options, sets are converted to dynamo list types
        Assert.That(stringSetAttributes[nameof(ClassWithSetOfString.HashSetOfString)].IsLSet, Is.True);
        Assert.That(stringSetAttributes[nameof(ClassWithSetOfString.HashSetOfString)].NS, Is.Empty);

        var numberSetAttributes =
            Mapper.ToMap(
                new ClassWithSetOfNumbers
                {
                    Ints = [1, 2,]
                }, JsonSerializerOptions.Default);

        Assert.That(numberSetAttributes, Has.Count.EqualTo(1));
        // Using the default serializer options, sets are converted to dynamo list types
        Assert.That(numberSetAttributes[nameof(ClassWithSetOfNumbers.Ints)].IsLSet, Is.True);
        Assert.That(numberSetAttributes[nameof(ClassWithSetOfNumbers.Ints)].NS, Is.Empty);
    }

    [Test]
    public void Should_respect_default_context_on_to_map()
    {
        // using the default options bypasses all custom converters
        var streamException = Assert.Throws<InvalidOperationException>(() => Mapper.ToMap(new ClassWithMemoryStream
        {
            SomeStream = new MemoryStream(Encoding.UTF8.GetBytes("Hello World 1"))
        }, MapperTestsSourceContext.Default.ClassWithMemoryStream));
        Assert.That(streamException!.Message, Contains.Substring("not supported on this stream."));

        var hashSetException = Assert.Throws<InvalidOperationException>(() => Mapper.ToMap(new ClassWithSetOfMemoryStream
        {
            HashSetOfMemoryStreams =
            [
                new(Encoding.UTF8.GetBytes("Hello World 1")),
                new(Encoding.UTF8.GetBytes("Hello World 2")),
            ]
        }, MapperTestsSourceContext.Default.ClassWithSetOfMemoryStream));
        Assert.That(hashSetException!.Message, Contains.Substring("not supported on this stream."));

        var stringSetAttributes =
            Mapper.ToMap(
                new ClassWithSetOfString
                {
                    HashSetOfString = ["Hello World 1", "Hello World 2",]
                }, MapperTestsSourceContext.Default.ClassWithSetOfString);

        Assert.That(stringSetAttributes, Has.Count.EqualTo(1));
        Assert.That(stringSetAttributes[nameof(ClassWithSetOfString.HashSetOfString)].IsLSet, Is.True);
        Assert.That(stringSetAttributes[nameof(ClassWithSetOfString.HashSetOfString)].NS, Is.Empty);

        var numberSetAttributes =
            Mapper.ToMap(
                new ClassWithSetOfNumbers
                {
                    Ints = [1, 2,]
                }, MapperTestsSourceContext.Default.ClassWithSetOfNumbers);

        Assert.That(numberSetAttributes, Has.Count.EqualTo(1));
        Assert.That(numberSetAttributes[nameof(ClassWithSetOfNumbers.Ints)].IsLSet, Is.True);
        Assert.That(numberSetAttributes[nameof(ClassWithSetOfNumbers.Ints)].NS, Is.Empty);
    }

    [Test]
    public void Should_respect_default_options_on_to_object()
    {
        // using the default options bypasses all custom converters
        var streamException1 = Assert.Throws<SerializationException>(() => Mapper.ToObject(new Dictionary<string, AttributeValue>
        {
            { "SomeStream", new AttributeValue { B = new MemoryStream(Encoding.UTF8.GetBytes("Hello World")) } }
        }, typeof(ClassWithMemoryStream), JsonSerializerOptions.Default));
        Assert.That(streamException1!.Message, Contains.Substring("no converter to handle 'MemoryStream'"));

        var streamException2 = Assert.Throws<SerializationException>(() => Mapper.ToObject<ClassWithMemoryStream>(new Dictionary<string, AttributeValue>
        {
            { "SomeStream", new AttributeValue { B = new MemoryStream(Encoding.UTF8.GetBytes("Hello World")) } }
        }, JsonSerializerOptions.Default));
        Assert.That(streamException2!.Message, Contains.Substring("no converter to handle 'MemoryStream'"));

        var setStreamException1 = Assert.Throws<SerializationException>(() => Mapper.ToObject(new Dictionary<string, AttributeValue>
        {
            { "HashSetOfMemoryStreams", new AttributeValue { BS =
            [
                new(Encoding.UTF8.GetBytes("Hello World 1")),
                new(Encoding.UTF8.GetBytes("Hello World 2")),
            ] } }
        }, typeof(ClassWithSetOfMemoryStream), JsonSerializerOptions.Default));
        Assert.That(setStreamException1!.Message, Contains.Substring("no converter to handle 'Sets of MemoryStream'"));

        var setStreamException2 = Assert.Throws<SerializationException>(() => Mapper.ToObject<ClassWithSetOfMemoryStream>(new Dictionary<string, AttributeValue>
        {
            { "HashSetOfMemoryStreams", new AttributeValue { BS =
            [
                new(Encoding.UTF8.GetBytes("Hello World 1")),
                new(Encoding.UTF8.GetBytes("Hello World 2")),
            ] } }
        }, JsonSerializerOptions.Default));
        Assert.That(setStreamException2!.Message, Contains.Substring("no converter to handle 'Sets of MemoryStream'"));

        var setStringException1 = Assert.Throws<SerializationException>(() => Mapper.ToObject(new Dictionary<string, AttributeValue>
        {
            {
                "HashSetOfString", new AttributeValue { SS =
                [
                    "Hello World 1",
                    "Hello World 2",
                ] }
            }
        }, typeof(ClassWithSetOfString), JsonSerializerOptions.Default));
        Assert.That(setStringException1!.Message, Contains.Substring("no converter to handle 'Sets of String'"));

        var setStringException2 = Assert.Throws<SerializationException>(() => Mapper.ToObject<ClassWithSetOfString>(new Dictionary<string, AttributeValue>
        {
            {
                "HashSetOfString", new AttributeValue { SS =
                [
                    "Hello World 1",
                    "Hello World 2",
                ] }
            }
        }, JsonSerializerOptions.Default));
        Assert.That(setStringException2!.Message, Contains.Substring("no converter to handle 'Sets of String'"));

        var setNumberException1 = Assert.Throws<SerializationException>(() => Mapper.ToObject(new Dictionary<string, AttributeValue>
        {
            {
                "Ints", new AttributeValue { NS =
                [
                    "1",
                    "2",
                ] }
            }
        }, typeof(ClassWithSetOfNumbers), JsonSerializerOptions.Default));
        Assert.That(setNumberException1!.Message, Contains.Substring("no converter to handle 'Sets of Number'"));

        var setNumberException2 = Assert.Throws<SerializationException>(() => Mapper.ToObject<ClassWithSetOfNumbers>(new Dictionary<string, AttributeValue>
        {
            {
                "Ints", new AttributeValue { NS =
                [
                    "1",
                    "2",
                ] }
            }
        }, JsonSerializerOptions.Default));
        Assert.That(setNumberException2!.Message, Contains.Substring("no converter to handle 'Sets of Number'"));
    }

    [Test]
    public void Should_respect_default_context_on_to_object()
    {
        // using the default options bypasses all custom converters
        var streamException = Assert.Throws<SerializationException>(() => Mapper.ToObject(new Dictionary<string, AttributeValue>
        {
            { "SomeStream", new AttributeValue { B = new MemoryStream(Encoding.UTF8.GetBytes("Hello World")) } }
        }, MapperTestsSourceContext.Default.ClassWithMemoryStream));
        Assert.That(streamException!.Message, Contains.Substring("no converter to handle 'MemoryStream'"));

        var setStreamException = Assert.Throws<SerializationException>(() => Mapper.ToObject(new Dictionary<string, AttributeValue>
        {
            { "HashSetOfMemoryStreams", new AttributeValue { BS =
            [
                new(Encoding.UTF8.GetBytes("Hello World 1")),
                new(Encoding.UTF8.GetBytes("Hello World 2")),
            ] } }
        }, MapperTestsSourceContext.Default.ClassWithSetOfMemoryStream));
        Assert.That(setStreamException!.Message, Contains.Substring("no converter to handle 'Sets of MemoryStream'"));

        var setStringException = Assert.Throws<SerializationException>(() => Mapper.ToObject(new Dictionary<string, AttributeValue>
        {
            {
                "HashSetOfString", new AttributeValue { SS =
                [
                    "Hello World 1",
                    "Hello World 2",
                ] }
            }
        }, MapperTestsSourceContext.Default.ClassWithSetOfString));
        Assert.That(setStringException!.Message, Contains.Substring("no converter to handle 'Sets of String'"));

        var setNumberException = Assert.Throws<SerializationException>(() => Mapper.ToObject(new Dictionary<string, AttributeValue>
        {
            {
                "Ints", new AttributeValue { NS =
                [
                    "1",
                    "2",
                ] }
            }
        }, MapperTestsSourceContext.Default.ClassWithSetOfNumbers));
        Assert.That(setNumberException!.Message, Contains.Substring("no converter to handle 'Sets of Number'"));
    }
}

[JsonSourceGenerationOptions]
[JsonSerializable(typeof(MapperTests.ClassWithMemoryStream))]
[JsonSerializable(typeof(MapperTests.ClassWithSetOfMemoryStream))]
[JsonSerializable(typeof(MapperTests.ClassWithSetOfString))]
[JsonSerializable(typeof(MapperTests.ClassWithSetOfNumbers))]
partial class MapperTestsSourceContext : JsonSerializerContext
{
}
#pragma warning restore CA1711