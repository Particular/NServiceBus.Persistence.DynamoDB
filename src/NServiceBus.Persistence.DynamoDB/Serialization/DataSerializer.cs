namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using Amazon.DynamoDBv2.Model;

    static class DataSerializer
    {
        static readonly JsonSerializerOptions serializerOptions =
            new() { Converters = { new MemoryStreamConverter(), new HashSetMemoryStreamConverter(), new HashSetStringConverter(), new HashSetOfNumberConverter() } };

        public static Dictionary<string, AttributeValue> Serialize<TValue>(TValue value)
            where TValue : notnull
            => Serialize(value, typeof(TValue));

        public static Dictionary<string, AttributeValue> Serialize(object value, Type type)
        {
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, type, serializerOptions);
            if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new InvalidOperationException($"Unable to serialize the given type '{type}' because the json kind is not of type 'JsonValueKind.Object'.");
            }
            return ToAttributeMap(jsonDocument.RootElement);
        }

        public static TValue? Deserialize<TValue>(Dictionary<string, AttributeValue> attributeValues)
        {
            var jsonObject = ToNode(attributeValues);
            return jsonObject.Deserialize<TValue>(serializerOptions);
        }

        static AttributeValue ToAttribute(JsonElement element)
        {
            if (element.ValueKind == JsonValueKind.Object)
            {
                return ToMapAttribute(element);
            }

            if (element.ValueKind == JsonValueKind.Array)
            {
                return ToListAttribute(element);
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

        static Dictionary<string, AttributeValue> ToAttributeMap(JsonElement element)
        {
            var dictionary = new Dictionary<string, AttributeValue>();

            foreach (var property in element.EnumerateObject())
            {
                AttributeValue serializeElement = ToAttribute(property.Value);
                if (serializeElement.NULL)
                {
                    continue;
                }
                dictionary.Add(property.Name, serializeElement);
            }

            return dictionary;
        }

        static AttributeValue ToListAttribute(JsonElement element)
        {
            List<AttributeValue>? values = null;
            foreach (var innerElement in element.EnumerateArray())
            {
                values ??= new List<AttributeValue>(element.GetArrayLength());
                AttributeValue serializeElement = ToAttribute(innerElement);
                values.Add(serializeElement);
            }
            return new AttributeValue { L = values ?? new List<AttributeValue>(0) };
        }

        static AttributeValue ToMapAttribute(JsonElement element)
        {
            foreach (var property in element.EnumerateObject())
            {
                if (MemoryStreamConverter.TryExtract(property, out var stream))
                {
                    return new AttributeValue { B = stream };
                }

                if (HashSetMemoryStreamConverter.TryExtract(property, out var streamSet))
                {
                    return new AttributeValue { BS = streamSet };
                }

                if (HashSetOfNumberConverter.TryExtract(property, out var numberSEt))
                {
                    return new AttributeValue { NS = numberSEt };
                }

                if (HashSetStringConverter.TryExtract(property, out var stringSet))
                {
                    return new AttributeValue { SS = stringSet };
                }
            }
            return new AttributeValue { M = ToAttributeMap(element) };
        }

        static JsonNode? ToNode(AttributeValue attributeValue)
        {
            // check the simple cases first
            if (attributeValue.IsBOOLSet)
            {
                return attributeValue.BOOL;
            }

            if (attributeValue.NULL)
            {
                return default;
            }

            if (attributeValue.N is not null)
            {
                return JsonNode.Parse(attributeValue.N);
            }

            if (attributeValue.S is not null)
            {
                return attributeValue.S;
            }

            if (attributeValue.IsMSet)
            {
                return ToNode(attributeValue.M);
            }

            if (attributeValue.IsLSet)
            {
                return ToNode(attributeValue.L);
            }

            // check the more complex cases last
            if (MemoryStreamConverter.TryConvert(attributeValue.B, out var memoryStream))
            {
                return memoryStream;
            }

            if (HashSetMemoryStreamConverter.TryConvert(attributeValue.BS, out var memoryStreams))
            {
                return memoryStreams;
            }

            if (HashSetStringConverter.TryConvert(attributeValue.SS, out var stringHashSet))
            {
                return stringHashSet;
            }

            if (HashSetOfNumberConverter.TrConvert(attributeValue.NS, out var numberHashSet))
            {
                return numberHashSet;
            }

            throw new InvalidOperationException("Unable to convert the provided attribute value into a JsonElement");
        }

        static JsonNode ToNode(Dictionary<string, AttributeValue> attributeValues)
        {
            var jsonObject = new JsonObject();
            foreach (var kvp in attributeValues)
            {
                AttributeValue attributeValue = kvp.Value;
                string attributeName = kvp.Key;

                jsonObject.Add(attributeName, ToNode(attributeValue));
            }
            return jsonObject;
        }

        static JsonNode ToNode(List<AttributeValue> attributeValues)
        {
            var array = new JsonArray();
            foreach (var attributeValue in attributeValues)
            {
                array.Add(ToNode(attributeValue));
            }
            return array;
        }

        static readonly AttributeValue NullAttributeValue = new() { NULL = true };
        static readonly AttributeValue TrueAttributeValue = new() { BOOL = true };
        static readonly AttributeValue FalseAttributeValue = new() { BOOL = false };
    }
}