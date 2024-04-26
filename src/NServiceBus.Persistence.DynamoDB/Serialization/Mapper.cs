namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using Amazon.DynamoDBv2.Model;

/// <summary>
/// Maps objects to and from dictionaries of <see cref="string"/> and <see cref="AttributeValue"/>.
/// </summary>
public static class Mapper
{
    static JsonSerializerOptions DefaultOptions { get; } = new(MapperOptions.Defaults);

    /// <summary>
    /// Maps a given <paramref name="value"/> to a dictionary of <see cref="AttributeValue"/> where the key
    /// represents the property name and the value the mapped property value represented as an attribute value
    /// </summary>
    /// <param name="value">The value to map.</param>
    /// <typeparam name="TValue">The value type.</typeparam>
    public static Dictionary<string, AttributeValue> ToMap<TValue>(TValue value)
        where TValue : class =>
        ToMap(value, default(JsonSerializerOptions));

    // This method can be made public to support custom serialization options which also enables source gen support.
    internal static Dictionary<string, AttributeValue> ToMap<TValue>(TValue value, JsonSerializerOptions? options)
        where TValue : class
    {
        options ??= DefaultOptions;
        using var trackingState = new ClearTrackingState();
        using var jsonDocument = JsonSerializer.SerializeToDocument(value, options);
        if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
        {
            ThrowForInvalidRoot(typeof(TValue));
        }
        return ToAttributeMap(jsonDocument.RootElement, options);
    }

    // This method can be made public to support custom serialization options which also enables source gen support.
    internal static Dictionary<string, AttributeValue> ToMap<TValue>(TValue value, JsonTypeInfo<TValue> jsonTypeInfo)
        where TValue : class
    {
        ArgumentNullException.ThrowIfNull(jsonTypeInfo);

        using var trackingState = new ClearTrackingState();
        using var jsonDocument = JsonSerializer.SerializeToDocument(value, jsonTypeInfo);
        if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
        {
            ThrowForInvalidRoot(typeof(TValue));
        }
        return ToAttributeMap(jsonDocument.RootElement, jsonTypeInfo.Options);
    }

    /// <summary>
    /// Maps a given <paramref name="value"/> to a dictionary of <see cref="AttributeValue"/> where the key
    /// represents the property name and the value the mapped property value represented as an attribute value
    /// </summary>
    /// <param name="value">The value to map.</param>
    /// <param name="type">The type of the value.</param>
    public static Dictionary<string, AttributeValue> ToMap(object value, Type type)
        => ToMap(value, type, default(JsonSerializerOptions));

    // This method can be made public to support custom serialization options which also enables source gen support.
    internal static Dictionary<string, AttributeValue> ToMap(object value, Type type, JsonSerializerContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        using var trackingState = new ClearTrackingState();
        using var jsonDocument = JsonSerializer.SerializeToDocument(value, type, context);
        if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
        {
            ThrowForInvalidRoot(type);
        }
        return ToAttributeMap(jsonDocument.RootElement, context.Options);
    }

    // This method can be made public to support custom serialization options which also enables source gen support.
    internal static Dictionary<string, AttributeValue> ToMap(object value, Type type, JsonSerializerOptions? options)
    {
        options ??= DefaultOptions;
        using var trackingState = new ClearTrackingState();
        using var jsonDocument = JsonSerializer.SerializeToDocument(value, type, options);
        if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
        {
            ThrowForInvalidRoot(type);
        }
        return ToAttributeMap(jsonDocument.RootElement, options);
    }

    [DoesNotReturn]
    static void ThrowForInvalidRoot(Type type)
        => throw new SerializationException($"Unable to serialize the given type '{type}' because the json kind is not of type 'JsonValueKind.Object'.");

    /// <summary>
    /// Maps a given dictionary of <see cref="AttributeValue"/> where the key
    /// represents the property name and the value the mapped property value represented as an attribute value
    /// to the specified <typeparamref name="TValue"/> type.
    /// </summary>
    /// <param name="attributeValues">The attribute values.</param>
    /// <typeparam name="TValue">The value type to map to.</typeparam>
    public static TValue? ToObject<TValue>(Dictionary<string, AttributeValue> attributeValues)
        => ToObject<TValue>(attributeValues, default(JsonSerializerOptions));

    // This method can be made public to support custom serialization options which also enables source gen support.
    internal static TValue? ToObject<TValue>(Dictionary<string, AttributeValue> attributeValues, JsonTypeInfo<TValue> jsonTypeInfo)
    {
        using var trackingState = new ClearTrackingState();
        var jsonObject = ToNodeFromMap(attributeValues, jsonTypeInfo.Options);
        return jsonObject.Deserialize(jsonTypeInfo);
    }

    // This method can be made public to support custom serialization options which also enables source gen support.
    internal static TValue? ToObject<TValue>(Dictionary<string, AttributeValue> attributeValues, JsonSerializerOptions? options)
    {
        options ??= DefaultOptions;
        using var trackingState = new ClearTrackingState();
        var jsonObject = ToNodeFromMap(attributeValues, options);
        return jsonObject.Deserialize<TValue>(options);
    }

    /// <summary>
    /// Maps a given dictionary of <see cref="AttributeValue"/> where the key
    /// represents the property name and the value the mapped property value represented as an attribute value
    /// to the specified <paramref name="returnType"/> type.
    /// </summary>
    /// <param name="attributeValues">The attribute values.</param>
    /// <param name="returnType">The return type to map to.</param>
    public static object? ToObject(Dictionary<string, AttributeValue> attributeValues, Type returnType)
        => ToObject(attributeValues, returnType, default(JsonSerializerOptions));

    // This method can be made public to support custom serialization options which also enables source gen support.
    internal static object? ToObject(Dictionary<string, AttributeValue> attributeValues, Type returnType, JsonSerializerOptions? options)
    {
        options ??= DefaultOptions;
        using var trackingState = new ClearTrackingState();
        var jsonObject = ToNodeFromMap(attributeValues, options);
        return jsonObject.Deserialize(returnType, options);
    }

    // This method can be made public to support custom serialization options which also enables source gen support.
    internal static object? ToObject(Dictionary<string, AttributeValue> attributeValues, Type returnType, JsonSerializerContext context)
    {
        using var trackingState = new ClearTrackingState();
        var jsonObject = ToNodeFromMap(attributeValues, context.Options);
        return jsonObject.Deserialize(returnType, context);
    }

    static AttributeValue ToAttributeFromElement(JsonElement element, JsonSerializerOptions options) =>
        element.ValueKind switch
        {
            JsonValueKind.Object => ToAttributeFromObject(element, options),
            JsonValueKind.Array => ToAttributeFromArray(element, options),
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

    static Dictionary<string, AttributeValue> ToAttributeMap(JsonElement element, JsonSerializerOptions options)
    {
        var dictionary = new Dictionary<string, AttributeValue>();

        foreach (var property in element.EnumerateObject())
        {
            AttributeValue serializeElement = ToAttributeFromElement(property.Value, options);
            if (serializeElement.NULL)
            {
                continue;
            }
            dictionary.Add(property.Name, serializeElement);
        }

        return dictionary;
    }

    static AttributeValue ToAttributeFromArray(JsonElement element, JsonSerializerOptions options)
    {
        var values = new List<AttributeValue>(element.GetArrayLength());
        foreach (var innerElement in element.EnumerateArray())
        {
            AttributeValue serializeElement = ToAttributeFromElement(innerElement, options);
            values.Add(serializeElement);
        }
        return new AttributeValue { L = values, IsLSet = true };
    }

    static AttributeValue ToAttributeFromObject(JsonElement element, JsonSerializerOptions options)
    {
        // JsonElements of type Object might contain custom converted objects that should be mapped to dedicated DynamoDB value types
        if (options.TryGet<MemoryStreamConverter>(out var converter) && converter.TryExtract(element, out var attributeValue))
        {
            return attributeValue;
        }

        if (options.TryGet<SetOfMemoryStreamConverter>(out converter) && converter.TryExtract(element, out attributeValue))
        {
            return attributeValue;
        }

        if (options.TryGet<SetOfNumberConverter>(out converter) && converter.TryExtract(element, out attributeValue))
        {
            return attributeValue;
        }

        if (options.TryGet<SetOfStringConverter>(out converter) && converter.TryExtract(element, out attributeValue))
        {
            return attributeValue;
        }

        return new AttributeValue { M = ToAttributeMap(element, options), IsMSet = true };
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
            { B: not null } => jsonSerializerOptions.TryGet<MemoryStreamConverter>(out var converter) ? converter.ToNode(attributeValue) : ThrowForMissingConverter("MemoryStream"),
            { IsBSSet: true } => jsonSerializerOptions.TryGet<SetOfMemoryStreamConverter>(out var converter) ? converter.ToNode(attributeValue) : ThrowForMissingConverter("Sets of MemoryStream"),
            { IsSSSet: true } => jsonSerializerOptions.TryGet<SetOfStringConverter>(out var converter) ? converter.ToNode(attributeValue) : ThrowForMissingConverter("Sets of String"),
            { IsNSSet: true } => jsonSerializerOptions.TryGet<SetOfNumberConverter>(out var converter) ? converter.ToNode(attributeValue) : ThrowForMissingConverter("Sets of Number"),
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