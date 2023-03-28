#nullable enable
namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using Amazon.DynamoDBv2.Model;

    static class DataSerializer
    {
        static readonly JsonSerializerOptions serializerOptions =
            new JsonSerializerOptions { Converters = { new MemoryStreamConverter(), new HashSetMemoryStreamConverter(), new HashSetStringConverter(), new HashSetOfNumberConverter() } };

        public static Dictionary<string, AttributeValue> Serialize<TValue>(TValue value)
        {
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, serializerOptions);
            var attributeMapFromDocument = SerializeToAttributeMap(jsonDocument);
            return attributeMapFromDocument;
        }

        public static Dictionary<string, AttributeValue> Serialize(object value, Type type)
        {
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, type, serializerOptions);
            var attributeMapFromDocument = SerializeToAttributeMap(jsonDocument);
            return attributeMapFromDocument;
        }

        public static TValue? Deserialize<TValue>(Dictionary<string, AttributeValue> attributeValues)
        {
            var jsonObject = DeserializeElementFromAttributeMap(attributeValues);
            return jsonObject.Deserialize<TValue>(serializerOptions);
        }

        static Dictionary<string, AttributeValue> SerializeToAttributeMap(JsonDocument document)
        {
            if (document.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException("TBD");
            }

            return SerializeElementToAttributeMap(document.RootElement);
        }

        static Dictionary<string, AttributeValue> SerializeElementToAttributeMap(JsonElement element)
        {
            var dictionary = new Dictionary<string, AttributeValue>();

            foreach (var property in element.EnumerateObject())
            {
                AttributeValue serializeElement = SerializeElement(property.Value);
                if (serializeElement.NULL)
                {
                    continue;
                }
                dictionary.Add(property.Name, serializeElement);
            }

            return dictionary;
        }

        static AttributeValue SerializeElement(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                return SerializeElementToMap(element);
            }

            if (element.ValueKind == JsonValueKind.Array)
            {
                return SerializeElementToList(element);
            }

            if (element.ValueKind == JsonValueKind.False)
            {
                return FalseAttributeValue;
            }

            if (element.ValueKind == JsonValueKind.True)
            {
                return TrueAttributeValue;
            }

            if (element.ValueKind == JsonValueKind.Null)
            {
                return NullAttributeValue;
            }

            if (element.ValueKind == JsonValueKind.Number)
            {
                return new AttributeValue { N = element.ToString() };
            }

            return new AttributeValue(element.GetString());
        }

        static AttributeValue SerializeElementToList(JsonElement element)
        {
            var values = new List<AttributeValue>();
            foreach (var innerElement in element.EnumerateArray())
            {
                AttributeValue serializeElement = SerializeElement(innerElement);
                values.Add(serializeElement);
            }
            return new AttributeValue { L = values };
        }

        static AttributeValue SerializeElementToMap(JsonElement element)
        {
            foreach (var property in element.EnumerateObject())
            {
                if (property.NameEquals(HashSetMemoryStreamConverter.PropertyName))
                {
                    List<MemoryStream?>? streams = null;
                    foreach (var innerElement in property.Value.EnumerateArray())
                    {
                        streams ??= new List<MemoryStream?>(property.Value.GetArrayLength());
                        foreach (var streamElement in innerElement.EnumerateObject())
                        {
                            streams.Add(new MemoryStream(streamElement.Value.GetBytesFromBase64()));
                        }
                    }

                    return new AttributeValue { BS = streams };
                }

                if (property.NameEquals(HashSetOfNumberConverter.PropertyName))
                {
                    List<string?>? strings = null;
                    foreach (var innerElement in property.Value.EnumerateArray())
                    {
                        strings ??= new List<string?>(property.Value.GetArrayLength());
                        strings.Add(innerElement.ToString());
                    }

                    return new AttributeValue { NS = strings };
                }

                if (property.NameEquals(HashSetStringConverter.PropertyName))
                {
                    List<string?>? strings = null;
                    foreach (var innerElement in property.Value.EnumerateArray())
                    {
                        strings ??= new List<string?>(property.Value.GetArrayLength());
                        strings.Add(innerElement.GetString());
                    }

                    return new AttributeValue { SS = strings };
                }

                if (property.NameEquals(MemoryStreamConverter.PropertyName))
                {
                    return new AttributeValue { B = new MemoryStream(property.Value.GetBytesFromBase64()) };
                }
            }
            return new AttributeValue { M = SerializeElementToAttributeMap(element) };
        }

        static JsonObject DeserializeElementFromAttributeMap(Dictionary<string, AttributeValue> attributeValues)
        {
            var jsonObject = new JsonObject();
            foreach (var kvp in attributeValues)
            {
                AttributeValue attributeValue = kvp.Value;
                string attributeName = kvp.Key;

                jsonObject.Add(attributeName, DeserializeElement(attributeValue));
            }

            return jsonObject;
        }

        static JsonNode? DeserializeElement(AttributeValue attributeValue)
        {
            if (attributeValue.IsMSet)
            {
                return DeserializeElementFromAttributeMap(attributeValue.M);
            }

            if (attributeValue.IsLSet)
            {
                return DeserializeElementFromListSet(attributeValue.L);
            }

            if (attributeValue.B != null)
            {
                return new JsonObject
                {
                    [MemoryStreamConverter.PropertyName] = Convert.ToBase64String(attributeValue.B.ToArray())
                };
            }

            if (attributeValue.BS is { Count: > 0 })
            {
                var memoryStreamHashSet = new JsonObject();
                var streamHashSetContent = new JsonArray();
                foreach (var memoryStream in attributeValue.BS)
                {
                    streamHashSetContent.Add(new JsonObject
                    {
                        [MemoryStreamConverter.PropertyName] = Convert.ToBase64String(memoryStream.ToArray())
                    });
                }
                memoryStreamHashSet.Add(HashSetMemoryStreamConverter.PropertyName, streamHashSetContent);
                return memoryStreamHashSet;
            }

            if (attributeValue.SS is { Count: > 0 })
            {
                var stringHashSet = new JsonObject();
                var stringHashSetContent = new JsonArray();
                foreach (var stringValue in attributeValue.SS)
                {
                    stringHashSetContent.Add(stringValue);
                }
                stringHashSet.Add(HashSetStringConverter.PropertyName, stringHashSetContent);
                return stringHashSet;
            }

            if (attributeValue.NS is { Count: > 0 })
            {
                var numberHashSet = new JsonObject();
                var numberHashSetContent = new JsonArray();
                foreach (var numberValue in attributeValue.NS)
                {
                    numberHashSetContent.Add(JsonNode.Parse(numberValue));
                }
                numberHashSet.Add(HashSetOfNumberConverter.PropertyName, numberHashSetContent);
                return numberHashSet;
            }

            if (attributeValue.IsBOOLSet)
            {
                return attributeValue.BOOL;
            }

            if (attributeValue.NULL)
            {
                return default;
            }

            if (attributeValue.N != null)
            {
                return JsonNode.Parse(attributeValue.N);
            }

            return attributeValue.S;
        }

        static JsonArray DeserializeElementFromListSet(List<AttributeValue> attributeValues)
        {
            var array = new JsonArray();
            foreach (var attributeValue in attributeValues)
            {
                array.Add(DeserializeElement(attributeValue));
            }
            return array;
        }

        static readonly AttributeValue NullAttributeValue = new AttributeValue { NULL = true };
        static readonly AttributeValue TrueAttributeValue = new AttributeValue { BOOL = true };
        static readonly AttributeValue FalseAttributeValue = new AttributeValue { BOOL = false };
    }
}