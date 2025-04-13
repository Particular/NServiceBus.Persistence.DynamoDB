namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.Serialization;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;
using System.Threading;
using Amazon.DynamoDBv2.Model;

/// <summary>
/// Maps objects to and from dictionaries of <see cref="string"/> and <see cref="AttributeValue"/>.
/// </summary>
public static class Mapper
{
    /// <summary>
    /// Provides the default <see cref="JsonSerializerOptions"/> used for serialization and deserialization augmented with
    /// specialized converters for DynamoDB types.
    /// </summary>
    public static JsonSerializerOptions Default
    {
        get
        {
            if (@default is not { } options)
            {
                options = GetOrCreateDefaultInstance(ref @default);
            }

            return options;
        }
    }

    /// <summary>
    /// The Default are never directly used to serialize and deserialize otherwise they become immutable
    /// </summary>
    static JsonSerializerOptions DefaultsOptions
    {
        get
        {
            if (defaultOptions is not { } options)
            {
                options = GetOrCreateDefaultInstance(ref defaultOptions);
            }

            return options;
        }
    }

    /// <summary>
    /// Maps a given <paramref name="value"/> to a dictionary of <see cref="AttributeValue"/> where the key
    /// represents the property name and the value the mapped property value represented as an attribute value
    /// </summary>
    /// <param name="value">The value to map.</param>
    /// <param name="options">The serializer options.</param>
    /// <typeparam name="TValue">The value type.</typeparam>
    [RequiresDynamicCode(DynamicCodeWarning)]
    [RequiresUnreferencedCode(UnreferencedCodeWarning)]
    public static Dictionary<string, AttributeValue> ToMap<TValue>(TValue value, JsonSerializerOptions? options = null)
        where TValue : class
    {
        options ??= DefaultsOptions;
        using var trackingState = new ClearTrackingState();
        using var jsonDocument = JsonSerializer.SerializeToDocument(value, options);
        if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
        {
            ThrowForInvalidRoot(typeof(TValue));
        }
        return ToAttributeMap(jsonDocument.RootElement, options);
    }

    /// <summary>
    /// Maps a given <paramref name="value"/> to a dictionary of <see cref="AttributeValue"/> where the key
    /// represents the property name and the value the mapped property value represented as an attribute value
    /// </summary>
    /// <param name="value">The value to map.</param>
    /// <param name="jsonTypeInfo">The type info.</param>
    /// <typeparam name="TValue">The value type.</typeparam>
    public static Dictionary<string, AttributeValue> ToMap<TValue>(TValue value, JsonTypeInfo<TValue> jsonTypeInfo)
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
    /// <param name="options">The serialization options</param>
    [RequiresDynamicCode(DynamicCodeWarning)]
    [RequiresUnreferencedCode(UnreferencedCodeWarning)]
    public static Dictionary<string, AttributeValue> ToMap(object value, Type type, JsonSerializerOptions? options = null)
    {
        options ??= DefaultsOptions;
        using var trackingState = new ClearTrackingState();
        using var jsonDocument = JsonSerializer.SerializeToDocument(value, type, options);
        if (jsonDocument.RootElement.ValueKind != JsonValueKind.Object)
        {
            ThrowForInvalidRoot(type);
        }
        return ToAttributeMap(jsonDocument.RootElement, options);
    }

    /// <summary>
    /// Maps a given <paramref name="value"/> to a dictionary of <see cref="AttributeValue"/> where the key
    /// represents the property name and the value the mapped property value represented as an attribute value
    /// </summary>
    /// <param name="value">The value to map.</param>
    /// <param name="type">The type of the value.</param>
    /// <param name="context">The serialization context.</param>
    public static Dictionary<string, AttributeValue> ToMap(object value, Type type, JsonSerializerContext context)
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

    [DoesNotReturn]
    static void ThrowForInvalidRoot(Type type)
        => throw new SerializationException($"Unable to serialize the given type '{type}' because the json kind is not of type 'JsonValueKind.Object'.");

    /// <summary>
    /// Maps a given dictionary of <see cref="AttributeValue"/> where the key
    /// represents the property name and the value the mapped property value represented as an attribute value
    /// to the specified <typeparamref name="TValue"/> type.
    /// </summary>
    /// <param name="attributeValues">The attribute values.</param>
    /// <param name="options">The serializer options.</param>
    /// <typeparam name="TValue">The value type to map to.</typeparam>
    [RequiresDynamicCode(DynamicCodeWarning)]
    [RequiresUnreferencedCode(UnreferencedCodeWarning)]
    public static TValue? ToObject<TValue>(Dictionary<string, AttributeValue> attributeValues, JsonSerializerOptions? options = null)
    {
        options ??= DefaultsOptions;
        using var trackingState = new ClearTrackingState();
        var jsonObject = ToNodeFromMap(attributeValues, options);
        return jsonObject.Deserialize<TValue>(options);
    }

    /// <summary>
    /// Maps a given dictionary of <see cref="AttributeValue"/> where the key
    /// represents the property name and the value the mapped property value represented as an attribute value
    /// to the specified <typeparamref name="TValue"/> type.
    /// </summary>
    /// <param name="attributeValues">The attribute values.</param>
    /// <param name="jsonTypeInfo">The type info.</param>
    /// <typeparam name="TValue">The value type to map to.</typeparam>
    public static TValue? ToObject<TValue>(Dictionary<string, AttributeValue> attributeValues, JsonTypeInfo<TValue> jsonTypeInfo)
    {
        using var trackingState = new ClearTrackingState();
        var jsonObject = ToNodeFromMap(attributeValues, jsonTypeInfo.Options);
        return jsonObject.Deserialize(jsonTypeInfo);
    }

    /// <summary>
    /// Maps a given dictionary of <see cref="AttributeValue"/> where the key
    /// represents the property name and the value the mapped property value represented as an attribute value
    /// to the specified <paramref name="returnType"/> type.
    /// </summary>
    /// <param name="attributeValues">The attribute values.</param>
    /// <param name="returnType">The return type to map to.</param>
    /// <param name="options">The serialization options.</param>
    [RequiresDynamicCode(DynamicCodeWarning)]
    [RequiresUnreferencedCode(UnreferencedCodeWarning)]
    public static object? ToObject(Dictionary<string, AttributeValue> attributeValues, Type returnType, JsonSerializerOptions? options = null)
    {
        options ??= DefaultsOptions;
        using var trackingState = new ClearTrackingState();
        var jsonObject = ToNodeFromMap(attributeValues, options);
        return jsonObject.Deserialize(returnType, options);
    }

    /// <summary>
    /// Maps a given dictionary of <see cref="AttributeValue"/> where the key
    /// represents the property name and the value the mapped property value represented as an attribute value
    /// to the specified <paramref name="returnType"/> type.
    /// </summary>
    /// <param name="attributeValues">The attribute values.</param>
    /// <param name="returnType">The return type to map to.</param>
    /// <param name="context">The serialization context.</param>
    public static object? ToObject(Dictionary<string, AttributeValue> attributeValues, Type returnType, JsonSerializerContext context)
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
            { NULL: true } => null,
            { N: not null } => JsonNode.Parse(attributeValue.N),
            { S: not null } => attributeValue.S,
            { IsMSet: true, } => ToNodeFromMap(attributeValue.M, jsonSerializerOptions),
            { IsLSet: true } => ToNodeFromList(attributeValue.L, jsonSerializerOptions),
            // check the more complex cases last
            { B: not null } => jsonSerializerOptions.TryGet<MemoryStreamConverter>(out var converter) ? converter.ToNode(attributeValue) : ThrowForMissingConverter("MemoryStream"),
            // Do not use IsBSSet, IsSSSet, or NSSet. It's unclear if these properties actually work correctly in all cases.
            // See https://github.com/aws/aws-sdk-net/issues/3297#issuecomment-2078955427
            { BS.Count: > 0 } => jsonSerializerOptions.TryGet<SetOfMemoryStreamConverter>(out var converter) ? converter.ToNode(attributeValue) : ThrowForMissingConverter("Sets of MemoryStream"),
            { SS.Count: > 0 } => jsonSerializerOptions.TryGet<SetOfStringConverter>(out var converter) ? converter.ToNode(attributeValue) : ThrowForMissingConverter("Sets of String"),
            { NS.Count: > 0 } => jsonSerializerOptions.TryGet<SetOfNumberConverter>(out var converter) ? converter.ToNode(attributeValue) : ThrowForMissingConverter("Sets of Number"),
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

    static JsonSerializerOptions GetOrCreateDefaultInstance(ref JsonSerializerOptions? location)
    {
        var options = new JsonSerializerOptions
        {
            Converters =
            {
                new MemoryStreamConverter(),
                new SetOfMemoryStreamConverter(),
                new SetOfStringConverter(),
                new SetOfNumberConverter()
            },
            TypeInfoResolver = JsonSerializer.IsReflectionEnabledByDefault
                ? new DefaultJsonTypeInfoResolver()
                : new EmptyJsonTypeInfoResolver(),
        };
        options.MakeReadOnly();

        return Interlocked.CompareExchange(ref location, options, null) ?? options;
    }

    sealed class EmptyJsonTypeInfoResolver : IJsonTypeInfoResolver
    {
        public JsonTypeInfo? GetTypeInfo(Type type, JsonSerializerOptions options) => null;
    }

    static readonly AttributeValue NullAttributeValue = new() { NULL = true };
    static readonly AttributeValue TrueAttributeValue = new() { BOOL = true };
    static readonly AttributeValue FalseAttributeValue = new() { BOOL = false };
    static JsonSerializerOptions? @default;
    static JsonSerializerOptions? defaultOptions;

    const string DynamicCodeWarning = "JSON serialization and deserialization might require types that cannot be statically analyzed and might need runtime code generation. Use System.Text.Json source generation for native AOT applications.";
    const string UnreferencedCodeWarning =
        "JSON serialization and deserialization might require types that cannot be statically analyzed. Use the overload that takes a JsonTypeInfo or JsonSerializerContext, or make sure all of the required types are preserved.";

    readonly struct ClearTrackingState : IDisposable
    {
        public void Dispose() => MemoryStreamConverter.ClearTrackingState();
    }
}