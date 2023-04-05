namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Text.Json.Serialization;
    using System.Text.Json.Serialization.Metadata;
    using Amazon.DynamoDBv2.Model;

    static class Mapper
    {
        static JsonSerializerOptions DefaultOptions { get; } = new(MapperOptions.Defaults);

        public static Dictionary<string, AttributeValue> ToMap<TValue>(TValue value, JsonSerializerOptions? options = null)
            where TValue : class
        {
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, options ?? DefaultOptions);
            if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
            {
                ThrowInvalidOperationExceptionForInvalidRoot(typeof(TValue));
            }
            return ToAttributeMap(jsonDocument.RootElement);
        }

        public static Dictionary<string, AttributeValue> ToMap<TValue>(TValue value, JsonTypeInfo<TValue> jsonTypeInfo)
            where TValue : class
        {
            Guard.AgainstNull(nameof(jsonTypeInfo), jsonTypeInfo);
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, jsonTypeInfo);
            if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
            {
                ThrowInvalidOperationExceptionForInvalidRoot(typeof(TValue));
            }
            return ToAttributeMap(jsonDocument.RootElement);
        }

        public static Dictionary<string, AttributeValue> ToMap(object value, Type type, JsonSerializerContext context)
        {
            Guard.AgainstNull(nameof(context), context);
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, type, context);
            if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
            {
                ThrowInvalidOperationExceptionForInvalidRoot(type);
            }
            return ToAttributeMap(jsonDocument.RootElement);
        }

        public static Dictionary<string, AttributeValue> ToMap(object value, Type type, JsonSerializerOptions? options = null)
        {
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, type, options ?? DefaultOptions);
            if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
            {
                ThrowInvalidOperationExceptionForInvalidRoot(type);
            }
            return ToAttributeMap(jsonDocument.RootElement);
        }

        [DoesNotReturn]
        static void ThrowInvalidOperationExceptionForInvalidRoot(Type type)
            => throw new InvalidOperationException($"Unable to serialize the given type '{type}' because the json kind is not of type 'JsonValueKind.Object'.");

        public static TValue? ToObject<TValue>(Dictionary<string, AttributeValue> attributeValues, JsonTypeInfo<TValue> jsonTypeInfo)
        {
            var jsonObject = ToNodeFromMap(attributeValues);
            return jsonObject.Deserialize(jsonTypeInfo);
        }

        public static TValue? ToObject<TValue>(Dictionary<string, AttributeValue> attributeValues, JsonSerializerOptions? options = null)
        {
            var jsonObject = ToNodeFromMap(attributeValues);
            return jsonObject.Deserialize<TValue>(options ?? DefaultOptions);
        }

        public static object? ToObject(Dictionary<string, AttributeValue> attributeValues, Type returnType, JsonSerializerOptions? options = null)
        {
            var jsonObject = ToNodeFromMap(attributeValues);
            return jsonObject.Deserialize(returnType, options ?? DefaultOptions);
        }

        public static object? ToObject(Dictionary<string, AttributeValue> attributeValues, Type returnType, JsonSerializerContext context)
        {
            var jsonObject = ToNodeFromMap(attributeValues);
            return jsonObject.Deserialize(returnType, context);
        }

        static AttributeValue ToAttributeFromElement(JsonElement element) =>
            element.ValueKind switch
            {
                JsonValueKind.Object => ToAttributeFromObject(element),
                JsonValueKind.Array => ToAttributeFromArray(element),
                JsonValueKind.False => FalseAttributeValue,
                JsonValueKind.True => TrueAttributeValue,
                JsonValueKind.Null => NullAttributeValue,
                JsonValueKind.Number => new AttributeValue { N = element.ToString() },
                JsonValueKind.Undefined => NullAttributeValue,
                JsonValueKind.String => new AttributeValue(element.GetString()),
                _ => ThrowInvalidOperationExceptionForInvalidValueKind(element.ValueKind),
            };

        [DoesNotReturn]
        static AttributeValue ThrowInvalidOperationExceptionForInvalidValueKind(JsonValueKind valueKind)
            => throw new InvalidOperationException($"ValueKind '{valueKind}' could not be mapped.");

        static Dictionary<string, AttributeValue> ToAttributeMap(JsonElement element)
        {
            var dictionary = new Dictionary<string, AttributeValue>();

            foreach (var property in element.EnumerateObject())
            {
                AttributeValue serializeElement = ToAttributeFromElement(property.Value);
                if (serializeElement.NULL)
                {
                    continue;
                }
                dictionary.Add(property.Name, serializeElement);
            }

            return dictionary;
        }

        static AttributeValue ToAttributeFromArray(JsonElement element)
        {
            var values = new List<AttributeValue>(element.GetArrayLength());
            foreach (var innerElement in element.EnumerateArray())
            {
                AttributeValue serializeElement = ToAttributeFromElement(innerElement);
                values.Add(serializeElement);
            }
            return new AttributeValue { L = values };
        }

        static AttributeValue ToAttributeFromObject(JsonElement element)
        {
            // JsonElements of type Object might contain custom converted objects that should be mapped to dedicated DynamoDB value types
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

                // if we reached this point we know there are no special cases to handle so let's stop trying to iterate
                break;
            }
            return new AttributeValue { M = ToAttributeMap(element) };
        }

        static JsonNode? ToNode(AttributeValue attributeValue) =>
            attributeValue switch
            {
                // check the simple cases first
                { IsBOOLSet: true } => attributeValue.BOOL,
                { NULL: true } => default,
                { N: not null } => JsonNode.Parse(attributeValue.N),
                { S: not null } => attributeValue.S,
                { IsMSet: true, } => ToNodeFromMap(attributeValue.M),
                { IsLSet: true } => ToNodeFromList(attributeValue.L),
                // check the more complex cases last
                { B: not null } => MemoryStreamConverter.ToNode(attributeValue.B),
                { BS.Count: > 0 } => HashSetMemoryStreamConverter.ToNode(attributeValue.BS),
                { SS.Count: > 0 } => HashSetStringConverter.ToNode(attributeValue.SS),
                { NS.Count: > 0 } => HashSetOfNumberConverter.ToNode(attributeValue.NS),
                _ => ThrowInvalidOperationExceptionForNonMappableAttribute()
            };

        [DoesNotReturn]
        static JsonNode ThrowInvalidOperationExceptionForNonMappableAttribute()
            => throw new InvalidOperationException("Unable to convert the provided attribute value into a JsonElement");

        static JsonNode ToNodeFromMap(Dictionary<string, AttributeValue> attributeValues)
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

        static JsonNode ToNodeFromList(List<AttributeValue> attributeValues)
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