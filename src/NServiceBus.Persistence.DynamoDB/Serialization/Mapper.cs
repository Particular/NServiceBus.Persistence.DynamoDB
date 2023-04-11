namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.Serialization;
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
            using var trackingState = new ClearTrackingState();
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, options ?? DefaultOptions);
            if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
            {
                ThrowForInvalidRoot(typeof(TValue));
            }
            return ToAttributeMap(jsonDocument.RootElement);
        }

        public static Dictionary<string, AttributeValue> ToMap<TValue>(TValue value, JsonTypeInfo<TValue> jsonTypeInfo)
            where TValue : class
        {
            Guard.AgainstNull(nameof(jsonTypeInfo), jsonTypeInfo);

            using var trackingState = new ClearTrackingState();
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, jsonTypeInfo);
            if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
            {
                ThrowForInvalidRoot(typeof(TValue));
            }
            return ToAttributeMap(jsonDocument.RootElement);
        }

        public static Dictionary<string, AttributeValue> ToMap(object value, Type type, JsonSerializerContext context)
        {
            Guard.AgainstNull(nameof(context), context);

            using var trackingState = new ClearTrackingState();
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, type, context);
            if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
            {
                ThrowForInvalidRoot(type);
            }
            return ToAttributeMap(jsonDocument.RootElement);
        }

        public static Dictionary<string, AttributeValue> ToMap(object value, Type type, JsonSerializerOptions? options = null)
        {
            using var trackingState = new ClearTrackingState();
            using var jsonDocument = JsonSerializer.SerializeToDocument(value, type, options ?? DefaultOptions);
            if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
            {
                ThrowForInvalidRoot(type);
            }
            return ToAttributeMap(jsonDocument.RootElement);
        }

        [DoesNotReturn]
        static void ThrowForInvalidRoot(Type type)
            => throw new SerializationException($"Unable to serialize the given type '{type}' because the json kind is not of type 'JsonValueKind.Object'.");

        public static TValue? ToObject<TValue>(Dictionary<string, AttributeValue> attributeValues, JsonTypeInfo<TValue> jsonTypeInfo)
        {
            using var trackingState = new ClearTrackingState();
            var jsonObject = ToNodeFromMap(attributeValues, jsonTypeInfo.Options);
            return jsonObject.Deserialize(jsonTypeInfo);
        }

        public static TValue? ToObject<TValue>(Dictionary<string, AttributeValue> attributeValues, JsonSerializerOptions? options = null)
        {
            options ??= DefaultOptions;
            using var trackingState = new ClearTrackingState();
            var jsonObject = ToNodeFromMap(attributeValues, options);
            return jsonObject.Deserialize<TValue>(options);
        }

        public static object? ToObject(Dictionary<string, AttributeValue> attributeValues, Type returnType, JsonSerializerOptions? options = null)
        {
            options ??= DefaultOptions;
            using var trackingState = new ClearTrackingState();
            var jsonObject = ToNodeFromMap(attributeValues, options);
            return jsonObject.Deserialize(returnType, options);
        }

        public static object? ToObject(Dictionary<string, AttributeValue> attributeValues, Type returnType, JsonSerializerContext context)
        {
            using var trackingState = new ClearTrackingState();
            var jsonObject = ToNodeFromMap(attributeValues, context.Options);
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
            if (MemoryStreamConverter.TryExtract(element, out var stream))
            {
                return new AttributeValue { B = stream };
            }

            if (SetOfMemoryStreamConverter.TryExtract(element, out var streamSet))
            {
                return new AttributeValue { BS = streamSet };
            }

            if (SetOfNumberConverter.TryExtract(element, out var numberSEt))
            {
                return new AttributeValue { NS = numberSEt };
            }

            if (SetOfStringConverter.TryExtract(element, out var stringSet))
            {
                return new AttributeValue { SS = stringSet };
            }
            return new AttributeValue { M = ToAttributeMap(element) };
        }

        static JsonNode? ToNode(AttributeValue attributeValue, JsonSerializerOptions jsonSerializerOptions) =>
            attributeValue switch
            {
                // check the simple cases first
                { IsBOOLSet: true } => attributeValue.BOOL,
                { NULL: true } => default,
                { N: not null } => JsonNode.Parse(attributeValue.N),
                { S: not null } => attributeValue.S,
                { IsMSet: true, } => ToNodeFromMap(attributeValue.M, jsonSerializerOptions),
                { IsLSet: true } => ToNodeFromList(attributeValue.L, jsonSerializerOptions),
                // check the more complex cases last
                { B: not null } => jsonSerializerOptions.Has<MemoryStreamConverter>() ? MemoryStreamConverter.ToNode(attributeValue.B) : ThrowForMissingConverter("MemoryStream"),
                { BS.Count: > 0 } => jsonSerializerOptions.Has<SetOfMemoryStreamConverter>() ? SetOfMemoryStreamConverter.ToNode(attributeValue.BS) : ThrowForMissingConverter("Sets of MemoryStream"),
                { SS.Count: > 0 } => jsonSerializerOptions.Has<SetOfStringConverter>() ? SetOfStringConverter.ToNode(attributeValue.SS) : ThrowForMissingConverter("Sets of String"),
                { NS.Count: > 0 } => jsonSerializerOptions.Has<SetOfNumberConverter>() ? SetOfNumberConverter.ToNode(attributeValue.NS) : ThrowForMissingConverter("Sets of Number"),
                _ => ThrowForNonMappableAttribute()
            };

        [DoesNotReturn]
        static JsonNode ThrowForMissingConverter(string converterInfo)
            => throw new SerializationException($"Unable to convert the provided attribute value into a JsonElement because there is no converter to handle '{converterInfo}'.");

        [DoesNotReturn]
        static JsonNode ThrowForNonMappableAttribute()
            => throw new SerializationException("Unable to convert the provided attribute value into a JsonElement");

        static JsonNode ToNodeFromMap(Dictionary<string, AttributeValue> attributeValues, JsonSerializerOptions jsonSerializerOptions)
        {
            var jsonObject = new JsonObject();
            foreach (var kvp in attributeValues)
            {
                AttributeValue attributeValue = kvp.Value;
                string attributeName = kvp.Key;

                jsonObject.Add(attributeName, ToNode(attributeValue, jsonSerializerOptions));
            }
            return jsonObject;
        }

        static JsonNode ToNodeFromList(List<AttributeValue> attributeValues, JsonSerializerOptions jsonSerializerOptions)
        {
            var array = new JsonArray();
            foreach (var attributeValue in attributeValues)
            {
                array.Add(ToNode(attributeValue, jsonSerializerOptions));
            }
            return array;
        }

        static readonly AttributeValue NullAttributeValue = new() { NULL = true };
        static readonly AttributeValue TrueAttributeValue = new() { BOOL = true };
        static readonly AttributeValue FalseAttributeValue = new() { BOOL = false };

        readonly struct ClearTrackingState : IDisposable
        {
            public void Dispose() => MemoryStreamConverter.ClearTrackingState();
        }
    }
}