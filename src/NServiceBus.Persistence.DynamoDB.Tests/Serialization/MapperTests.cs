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

        Assert.Multiple(() =>
        {
            Assert.That(deserialized.Guid, Is.EqualTo(basicPoco.Guid));
            Assert.That(deserialized.String, Is.EqualTo(basicPoco.String));
            Assert.That(deserialized.Boolean, Is.EqualTo(basicPoco.Boolean));

            Assert.That(attributes[nameof(BasicPoco.Guid)].S, Is.EqualTo(basicPocoId.ToString()), "Should have been mapped to DynamoDB string attribute");
            Assert.That(attributes[nameof(BasicPoco.String)].S, Is.EqualTo("Hello World 1"), "Should have been mapped to DynamoDB string attribute");
            Assert.That(attributes[nameof(BasicPoco.Boolean)].BOOL, Is.True, "Should have been mapped to DynamoDB Bool attribute");
        });
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

        Assert.Multiple(() =>
        {
            Assert.That(deserialized.Guid, Is.EqualTo(basicPoco.Guid));
            Assert.That(deserialized.String, Is.EqualTo(basicPoco.String));
            Assert.That(deserialized.Boolean, Is.EqualTo(basicPoco.Boolean));

            Assert.That(attributes[nameof(BasicPoco.Guid)].S, Is.EqualTo(basicPocoId.ToString()), "Should have been mapped to DynamoDB string attribute");
            Assert.That(attributes, Does.Not.ContainKey(nameof(BasicPoco.String)), "Null attributes should not be mapped");
        });
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

        Assert.Multiple(() =>
        {
            Assert.That(deserialized.Guid, Is.EqualTo(nestedPoco.Guid));
            Assert.That(deserialized.String, Is.EqualTo(nestedPoco.String));
            Assert.That(deserialized.Boolean, Is.EqualTo(nestedPoco.Boolean));

            Assert.That(nestedPoco.SubPoco, Is.Not.Null);
            Assert.That(deserialized.SubPoco.Guid, Is.EqualTo(nestedPoco.SubPoco.Guid));
            Assert.That(deserialized.SubPoco.String, Is.EqualTo(nestedPoco.SubPoco.String));
            Assert.That(deserialized.SubPoco.Boolean, Is.EqualTo(nestedPoco.SubPoco.Boolean));
        });

        Assert.That(nestedPoco.SubPoco.SubSubPoco, Is.Not.Null);
        Assert.That(deserialized.SubPoco.SubSubPoco.Guid, Is.EqualTo(nestedPoco.SubPoco.SubSubPoco.Guid));
        Assert.That(deserialized.SubPoco.SubSubPoco.String, Is.EqualTo(nestedPoco.SubPoco.SubSubPoco.String));
        Assert.That(deserialized.SubPoco.SubSubPoco.Boolean, Is.EqualTo(nestedPoco.SubPoco.SubSubPoco.Boolean));

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

        Assert.Multiple(() =>
        {
            Assert.That(deserialized.Guid, Is.EqualTo(nestedPoco.Guid));
            Assert.That(nestedPoco.SubPoco, Is.Null);
            Assert.That(attributes[nameof(NestedPoco.Guid)].S, Is.Not.Null);

            Assert.That(attributes, Does.Not.ContainKey(nameof(NestedPoco.String)));
        });
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

        Assert.Multiple(() =>
        {
            // state should never leak
            Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
            Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
        });

        var deserialized = Mapper.ToObject<ClassWithMemoryStream>(attributes);

        Assert.Multiple(() =>
        {
            // state should never leak
            Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
            Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
        });

        Assert.That(deserialized.SomeStream.ToArray(), Is.EquivalentTo(classWithMemoryStream.SomeStream.ToArray()));
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

        Assert.Multiple(() =>
        {
            // state should never leak
            Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
            Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
        });
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

        Assert.Multiple(() =>
        {
            // state should never leak
            Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
            Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
        });
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

        Assert.Multiple(() =>
        {
            // state should never leak
            Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
            Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
        });
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

        Assert.Multiple(() =>
        {
            // state should never leak
            Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
            Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
        });
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

        Assert.Multiple(() =>
        {
            // state should never leak
            Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
            Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
        });

        var deserialized = Mapper.ToObject<ClassWithSetOfMemoryStream>(attributes);

        Assert.Multiple(() =>
        {
            // state should never leak
            Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
            Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
        });

        Assert.That(deserialized.HashSetOfMemoryStreams, Is.EquivalentTo(classWithListOfMemoryStream.HashSetOfMemoryStreams));
        Assert.That(deserialized.ImmutableHashSetOfStreams, Is.EquivalentTo(classWithListOfMemoryStream.ImmutableHashSetOfStreams));

        Assert.Multiple(() =>
        {
            Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.HashSetOfMemoryStreams)].BS, Is.EqualTo(classWithListOfMemoryStream.HashSetOfMemoryStreams));
            Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.ImmutableHashSetOfStreams)].BS, Is.EqualTo(classWithListOfMemoryStream.ImmutableHashSetOfStreams));
        });
    }

    [Test]
    public void Should_remap_empty_sets_of_streams()
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

        var attributesBeforeEmpty = Mapper.ToMap(classWithListOfMemoryStream);

        var deserialized = Mapper.ToObject<ClassWithSetOfMemoryStream>(attributesBeforeEmpty);

        deserialized.HashSetOfMemoryStreams = [];
        deserialized.ImmutableHashSetOfStreams = [];

        var attributesAfterEmpty = Mapper.ToMap(deserialized);

        Assert.Multiple(() =>
        {
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfMemoryStream.HashSetOfMemoryStreams)].BS, Is.EqualTo(classWithListOfMemoryStream.HashSetOfMemoryStreams));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfMemoryStream.ImmutableHashSetOfStreams)].BS, Is.EqualTo(classWithListOfMemoryStream.ImmutableHashSetOfStreams));

            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfMemoryStream.HashSetOfMemoryStreams)].BS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfMemoryStream.ImmutableHashSetOfStreams)].BS, Has.Count.Zero);

            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfMemoryStream.HashSetOfMemoryStreams)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfMemoryStream.HashSetOfMemoryStreams)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfMemoryStream.ImmutableHashSetOfStreams)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfMemoryStream.ImmutableHashSetOfStreams)].IsLSet, Is.True);
        });
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
    public void Should_roundtrip_empty_sets_streams()
    {
        var classWithListOfMemoryStream = new ClassWithEmptySetOfMemoryStream();

        var attributes = Mapper.ToMap(classWithListOfMemoryStream);

        var deserialized = Mapper.ToObject<ClassWithEmptySetOfMemoryStream>(attributes);

        Assert.Multiple(() =>
        {

            Assert.That(deserialized.HashSetOfMemoryStreams, Is.EquivalentTo(classWithListOfMemoryStream.HashSetOfMemoryStreams));
            Assert.That(deserialized.ImmutableHashSetOfStreams, Is.EquivalentTo(classWithListOfMemoryStream.ImmutableHashSetOfStreams));

            // Due to DynamoDB not supporting empty binary sets we need to use the L attribute to represent empty sets
            Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.HashSetOfMemoryStreams)].BS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.HashSetOfMemoryStreams)].IsBSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.ImmutableHashSetOfStreams)].BS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.ImmutableHashSetOfStreams)].IsBSSet, Is.False);

            Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.HashSetOfMemoryStreams)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.HashSetOfMemoryStreams)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.ImmutableHashSetOfStreams)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfMemoryStream.ImmutableHashSetOfStreams)].IsLSet, Is.True);
        });
    }

    // Sorted sets don't really make sense here
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Code", "PS0025:Dictionary keys should implement IEquatable<T>",
        Justification = "It's a test")]
    public class ClassWithEmptySetOfMemoryStream
    {
        public HashSet<MemoryStream> HashSetOfMemoryStreams { get; set; } = [];
        public ImmutableHashSet<MemoryStream> ImmutableHashSetOfStreams { get; set; } = [];
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

        Assert.Multiple(() =>
        {
            // state should never leak
            Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
            Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
        });

        var deserialized = Mapper.ToObject<ClassWithNestedMemoryStream>(attributes);

        Assert.Multiple(() =>
        {
            // state should never leak
            Assert.That(MemoryStreamConverter.StreamMap.Value, Is.Empty);
            Assert.That(MemoryStreamConverter.StreamId.Value, Is.EqualTo(0));
        });

        Assert.That(deserialized.SomeStream.ToArray(), Is.EquivalentTo(classWithMemoryStream.SomeStream.ToArray()));
        Assert.That(deserialized.Nested.SomeStream.ToArray(), Is.EquivalentTo(classWithMemoryStream.Nested.SomeStream.ToArray()));
        Assert.Multiple(() =>
        {
            Assert.That(attributes[nameof(ClassWithNestedMemoryStream.SomeStream)].B, Is.EqualTo(classWithMemoryStream.SomeStream));
            Assert.That(attributes[nameof(ClassWithNestedMemoryStream.Nested)].M[nameof(ClassWithNestedMemoryStream.SomeStream)].B, Is.EqualTo(classWithMemoryStream.Nested.SomeStream));
        });
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

        Assert.That(deserialized.HashSetOfString, Is.EquivalentTo(classWithSetOfString.HashSetOfString));
        Assert.That(deserialized.SortedSetOfString, Is.EquivalentTo(classWithSetOfString.SortedSetOfString));
        Assert.That(deserialized.ImmutableHashSetOfString, Is.EquivalentTo(classWithSetOfString.ImmutableHashSetOfString));
        Assert.That(deserialized.ImmutableSortedSetOfString, Is.EquivalentTo(classWithSetOfString.ImmutableSortedSetOfString));

        Assert.Multiple(() =>
        {
            Assert.That(attributes[nameof(ClassWithSetOfString.HashSetOfString)].SS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfString.SortedSetOfString)].SS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfString.ImmutableHashSetOfString)].SS, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClassWithSetOfString.ImmutableSortedSetOfString)].SS, Has.Count.EqualTo(2));

            Assert.That(attributes[nameof(ClassWithSetOfString.HashSetOfString)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfString.SortedSetOfString)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfString.ImmutableHashSetOfString)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfString.ImmutableSortedSetOfString)].L, Has.Count.Zero);
        });
    }

    [Test]
    public void Should_remap_empty_sets_of_strings()
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

        var attributesBeforeEmpty = Mapper.ToMap(classWithSetOfString);

        var deserialized = Mapper.ToObject<ClassWithSetOfString>(attributesBeforeEmpty);

        deserialized.HashSetOfString = [];
        deserialized.SortedSetOfString = [];
        deserialized.ImmutableHashSetOfString = [];
        deserialized.ImmutableSortedSetOfString = [];

        var attributesAfterEmpty = Mapper.ToMap(deserialized);

        Assert.Multiple(() =>
        {
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfString.HashSetOfString)].SS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfString.SortedSetOfString)].SS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfString.ImmutableHashSetOfString)].SS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfString.ImmutableSortedSetOfString)].SS, Has.Count.EqualTo(2));

            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfString.HashSetOfString)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfString.HashSetOfString)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfString.SortedSetOfString)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfString.SortedSetOfString)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfString.ImmutableHashSetOfString)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfString.ImmutableHashSetOfString)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfString.ImmutableSortedSetOfString)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfString.ImmutableSortedSetOfString)].IsLSet, Is.False);

            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfString.HashSetOfString)].SS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfString.SortedSetOfString)].SS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfString.ImmutableHashSetOfString)].SS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfString.ImmutableSortedSetOfString)].SS, Has.Count.Zero);

            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfString.HashSetOfString)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfString.HashSetOfString)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfString.SortedSetOfString)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfString.SortedSetOfString)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfString.ImmutableHashSetOfString)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfString.ImmutableHashSetOfString)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfString.ImmutableSortedSetOfString)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfString.ImmutableSortedSetOfString)].IsLSet, Is.True);
        });
    }

    public class ClassWithSetOfString
    {
        public HashSet<string> HashSetOfString { get; set; }
        public SortedSet<string> SortedSetOfString { get; set; }
        public ImmutableHashSet<string> ImmutableHashSetOfString { get; set; }
        public ImmutableSortedSet<string> ImmutableSortedSetOfString { get; set; }
    }

    [Test]
    public void Should_roundtrip_empty_set_of_strings()
    {
        var classWithSetOfString = new ClassWithEmptySetOfString();

        var attributes = Mapper.ToMap(classWithSetOfString);

        var deserialized = Mapper.ToObject<ClassWithEmptySetOfString>(attributes);

        Assert.Multiple(() =>
        {
            Assert.That(deserialized.HashSetOfString, Is.EquivalentTo(classWithSetOfString.HashSetOfString));
            Assert.That(deserialized.SortedSetOfString, Is.EquivalentTo(classWithSetOfString.SortedSetOfString));
            Assert.That(deserialized.ImmutableHashSetOfString, Is.EquivalentTo(classWithSetOfString.ImmutableHashSetOfString));
            Assert.That(deserialized.ImmutableSortedSetOfString, Is.EquivalentTo(classWithSetOfString.ImmutableSortedSetOfString));

            // Due to DynamoDB not supporting empty string sets we need to use the L attribute to represent empty sets
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.HashSetOfString)].SS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.HashSetOfString)].IsSSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.SortedSetOfString)].SS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.HashSetOfString)].IsSSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.ImmutableHashSetOfString)].SS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.HashSetOfString)].IsSSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.ImmutableSortedSetOfString)].SS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.HashSetOfString)].IsSSSet, Is.False);

            Assert.That(attributes[nameof(ClassWithEmptySetOfString.HashSetOfString)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.HashSetOfString)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.SortedSetOfString)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.HashSetOfString)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.ImmutableHashSetOfString)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.HashSetOfString)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.ImmutableSortedSetOfString)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptySetOfString.HashSetOfString)].IsLSet, Is.True);
        });
    }

    public class ClassWithEmptySetOfString
    {
        public HashSet<string> HashSetOfString { get; set; } = [];
        public SortedSet<string> SortedSetOfString { get; set; } = [];
        public ImmutableHashSet<string> ImmutableHashSetOfString { get; set; } = [];
        public ImmutableSortedSet<string> ImmutableSortedSetOfString { get; set; } = [];
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

        Assert.That(deserialized.ListStrings, Is.EquivalentTo(classWithListOStrings.ListStrings));
        Assert.That(deserialized.ArrayStrings, Is.EquivalentTo(classWithListOStrings.ArrayStrings));

        Assert.Multiple(() =>
        {
            Assert.That(attributes[nameof(ClasWithListOfString.ListStrings)].L, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClasWithListOfString.ListStrings)].SS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClasWithListOfString.ArrayStrings)].L, Has.Count.EqualTo(2));
            Assert.That(attributes[nameof(ClasWithListOfString.ArrayStrings)].SS, Has.Count.Zero);
        });
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

        Assert.Multiple(() =>
        {
            Assert.That(deserialized.Int, Is.EqualTo(classNumbers.Int));
            Assert.That(deserialized.Double, Is.EqualTo(classNumbers.Double));
            Assert.That(deserialized.Float, Is.EqualTo(classNumbers.Float));
            Assert.That(deserialized.Long, Is.EqualTo(classNumbers.Long));
            Assert.That(deserialized.ULong, Is.EqualTo(classNumbers.ULong));
            Assert.That(deserialized.Short, Is.EqualTo(classNumbers.Short));
            Assert.That(deserialized.Ushort, Is.EqualTo(classNumbers.Ushort));
            Assert.That(deserialized.UInt, Is.EqualTo(classNumbers.UInt));
            Assert.That(deserialized.SByte, Is.EqualTo(classNumbers.SByte));
            Assert.That(deserialized.Byte, Is.EqualTo(classNumbers.Byte));
            Assert.That(deserialized.Decimal, Is.EqualTo(classNumbers.Decimal));

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
        });
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

        Assert.That(deserialized.Ints, Is.EquivalentTo(classWithHashSetOfNumbers.Ints));
        Assert.That(deserialized.Doubles, Is.EquivalentTo(classWithHashSetOfNumbers.Doubles));
        Assert.That(deserialized.Floats, Is.EquivalentTo(classWithHashSetOfNumbers.Floats));
        Assert.That(deserialized.Bytes, Is.EquivalentTo(classWithHashSetOfNumbers.Bytes));
        Assert.That(deserialized.Shorts, Is.EquivalentTo(classWithHashSetOfNumbers.Shorts));
        Assert.That(deserialized.UShorts, Is.EquivalentTo(classWithHashSetOfNumbers.UShorts));
        Assert.That(deserialized.Longs, Is.EquivalentTo(classWithHashSetOfNumbers.Longs));
        Assert.That(deserialized.ULongs, Is.EquivalentTo(classWithHashSetOfNumbers.ULongs));
        Assert.That(deserialized.UInts, Is.EquivalentTo(classWithHashSetOfNumbers.UInts));
        Assert.That(deserialized.SBytes, Is.EquivalentTo(classWithHashSetOfNumbers.SBytes));
        Assert.That(deserialized.Decimals, Is.EquivalentTo(classWithHashSetOfNumbers.Decimals));

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

    [Test]
    public void Should_remap_empty_sets_of_numbers()
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

        var attributesBeforeEmpty = Mapper.ToMap(classWithHashSetOfNumbers);

        var deserialized = Mapper.ToObject<ClassWithSetOfNumbers>(attributesBeforeEmpty);

        deserialized.Ints = [];
        deserialized.Doubles = [];
        deserialized.Floats = [];
        deserialized.Bytes = [];
        deserialized.Shorts = [];
        deserialized.UShorts = [];
        deserialized.Longs = [];
        deserialized.ULongs = [];
        deserialized.UInts = [];
        deserialized.SBytes = [];
        deserialized.Decimals = [];

        var attributesAfterEmpty = Mapper.ToMap(deserialized);

        Assert.Multiple(() =>
        {
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Ints)].NS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Doubles)].NS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Floats)].NS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Bytes)].NS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Shorts)].NS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.UShorts)].NS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Longs)].NS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.ULongs)].NS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.UInts)].NS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.SBytes)].NS, Has.Count.EqualTo(2));
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Decimals)].NS, Has.Count.EqualTo(2));

            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Ints)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Ints)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Doubles)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Doubles)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Floats)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Floats)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Bytes)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Bytes)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Shorts)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Shorts)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.UShorts)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.UShorts)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Longs)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Longs)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.ULongs)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.ULongs)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.UInts)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.UInts)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.SBytes)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.SBytes)].IsLSet, Is.False);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Decimals)].L, Has.Count.Zero);
            Assert.That(attributesBeforeEmpty[nameof(ClassWithSetOfNumbers.Decimals)].IsLSet, Is.False);

            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Ints)].NS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Doubles)].NS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Floats)].NS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Bytes)].NS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Shorts)].NS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.UShorts)].NS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Longs)].NS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.ULongs)].NS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.UInts)].NS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.SBytes)].NS, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Decimals)].NS, Has.Count.Zero);

            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Ints)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Ints)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Doubles)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Doubles)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Floats)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Floats)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Bytes)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Bytes)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Shorts)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Shorts)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.UShorts)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.UShorts)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Longs)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Longs)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.ULongs)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.ULongs)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.UInts)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.UInts)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.SBytes)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.SBytes)].IsLSet, Is.True);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Decimals)].L, Has.Count.Zero);
            Assert.That(attributesAfterEmpty[nameof(ClassWithSetOfNumbers.Decimals)].IsLSet, Is.True);
        });
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

        Assert.Multiple(() =>
        {
            Assert.That(deserialized.Ints, Is.EquivalentTo(classWithHashSetOfNumbers.Ints));
            Assert.That(deserialized.Doubles, Is.EquivalentTo(classWithHashSetOfNumbers.Doubles));
            Assert.That(deserialized.Floats, Is.EquivalentTo(classWithHashSetOfNumbers.Floats));
            Assert.That(deserialized.Bytes, Is.EquivalentTo(classWithHashSetOfNumbers.Bytes));
            Assert.That(deserialized.Shorts, Is.EquivalentTo(classWithHashSetOfNumbers.Shorts));
            Assert.That(deserialized.Longs, Is.EquivalentTo(classWithHashSetOfNumbers.Longs));
            Assert.That(deserialized.ULongs, Is.EquivalentTo(classWithHashSetOfNumbers.ULongs));
            Assert.That(deserialized.UInts, Is.EquivalentTo(classWithHashSetOfNumbers.UInts));
            Assert.That(deserialized.SBytes, Is.EquivalentTo(classWithHashSetOfNumbers.SBytes));
            Assert.That(deserialized.Decimals, Is.EquivalentTo(classWithHashSetOfNumbers.Decimals));

            // Due to DynamoDB not supporting empty number sets we need to use the L attribute to represent empty sets
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Ints)].NS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Ints)].IsNSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Doubles)].NS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Doubles)].IsNSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Floats)].NS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Floats)].IsNSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Bytes)].NS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Bytes)].IsNSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Shorts)].NS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Shorts)].IsNSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Longs)].NS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Longs)].IsNSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.ULongs)].NS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.ULongs)].IsNSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.UInts)].NS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.UInts)].IsNSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.SBytes)].NS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.SBytes)].IsNSSet, Is.False);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Decimals)].NS, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Decimals)].IsNSSet, Is.False);

            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Ints)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Ints)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Doubles)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Doubles)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Floats)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Floats)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Bytes)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Bytes)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Shorts)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Shorts)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Longs)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Longs)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.ULongs)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.ULongs)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.UInts)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.UInts)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.SBytes)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.SBytes)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Decimals)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithSetOfNumbers.Decimals)].IsLSet, Is.True);
        });
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

        Assert.Multiple(() =>
        {
            Assert.That(deserialized.RecordArray, Is.EquivalentTo(classWithEmptyCollections.RecordArray));
            Assert.That(deserialized.RecordList, Is.EquivalentTo(classWithEmptyCollections.RecordList));
            Assert.That(deserialized.StringArray, Is.EquivalentTo(classWithEmptyCollections.StringArray));
            Assert.That(deserialized.StringList, Is.EquivalentTo(classWithEmptyCollections.StringList));

            Assert.That(attributes[nameof(ClassWithEmptyCollections.RecordArray)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptyCollections.RecordArray)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithEmptyCollections.RecordList)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptyCollections.RecordList)].IsLSet, Is.True);
            Assert.That(attributes[nameof(ClassWithEmptyCollections.StringList)].L, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptyCollections.StringList)].IsLSet, Is.True);
        });
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

        Assert.Multiple(() =>
        {
            Assert.That(deserialized.SimpleDict, Is.EquivalentTo(classWithEmptyMaps.SimpleDict));
            Assert.That(deserialized.SimpleTypeDict, Is.EquivalentTo(classWithEmptyMaps.SimpleTypeDict));

            Assert.That(attributes[nameof(ClassWithEmptyMaps.SimpleDict)].M, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptyMaps.SimpleDict)].IsMSet, Is.True);
            Assert.That(attributes[nameof(ClassWithEmptyMaps.SimpleTypeDict)].M, Has.Count.Zero);
            Assert.That(attributes[nameof(ClassWithEmptyMaps.SimpleTypeDict)].IsMSet, Is.True);
        });
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
        Assert.Multiple(() =>
        {
            // Using the default serializer options, sets are converted to dynamo list types
            Assert.That(stringSetAttributes[nameof(ClassWithSetOfString.HashSetOfString)].IsLSet, Is.True);
            Assert.That(stringSetAttributes[nameof(ClassWithSetOfString.HashSetOfString)].NS, Is.Empty);
        });

        var numberSetAttributes =
            Mapper.ToMap(
                new ClassWithSetOfNumbers
                {
                    Ints = [1, 2,]
                }, JsonSerializerOptions.Default);

        Assert.That(numberSetAttributes, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            // Using the default serializer options, sets are converted to dynamo list types
            Assert.That(numberSetAttributes[nameof(ClassWithSetOfNumbers.Ints)].IsLSet, Is.True);
            Assert.That(numberSetAttributes[nameof(ClassWithSetOfNumbers.Ints)].NS, Is.Empty);
        });
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
        Assert.Multiple(() =>
        {
            Assert.That(stringSetAttributes[nameof(ClassWithSetOfString.HashSetOfString)].IsLSet, Is.True);
            Assert.That(stringSetAttributes[nameof(ClassWithSetOfString.HashSetOfString)].NS, Is.Empty);
        });

        var numberSetAttributes =
            Mapper.ToMap(
                new ClassWithSetOfNumbers
                {
                    Ints = [1, 2,]
                }, MapperTestsSourceContext.Default.ClassWithSetOfNumbers);

        Assert.That(numberSetAttributes, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(numberSetAttributes[nameof(ClassWithSetOfNumbers.Ints)].IsLSet, Is.True);
            Assert.That(numberSetAttributes[nameof(ClassWithSetOfNumbers.Ints)].NS, Is.Empty);
        });
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