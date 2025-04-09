namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.DynamoDBv2.DataModel;

sealed class ObjectModelAttributeConverter : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert) =>
        !ObjectModelMetadataTrackerInspector.GetMetadata(typeToConvert).Empty;

    public override JsonConverter CreateConverter(
        Type typeToConvert, JsonSerializerOptions options)
    {
        var converter = (JsonConverter)Activator.CreateInstance(
            typeof(DynamoDBJsonConverterInner<>).MakeGenericType(typeToConvert),
            BindingFlags.Instance | BindingFlags.Public,
            null,
            [options],
            null)!;

        return converter;
    }

    sealed class DynamoDBJsonConverterInner<T>(JsonSerializerOptions options) : JsonConverter<T>
    {
        readonly JsonSerializerOptions _options = options.FromWithout<DynamoDBJsonConverterInner<T>>();

        // Create new options to avoid infinite recursion

        public override T? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            => JsonSerializer.Deserialize<T>(ref reader, _options);

        public override void Write(Utf8JsonWriter writer, T value, JsonSerializerOptions options)
        {
            var metadata = ObjectModelMetadataTrackerInspector.GetMetadata(typeof(T));
            writer.WriteStartObject();

            foreach (ObjectModelMetadataTrackerInspector.PropertyMetadata propertyMetadata in metadata.Properties)
            {
                // Get property value
                object? propValue = propertyMetadata.PropertyInfo.GetValue(value);
                if (propValue is null)
                {
                    continue;
                }

                string jsonPropertyName = propertyMetadata.PropertyName;

                writer.WritePropertyName(
                    options.PropertyNamingPolicy?.ConvertName(jsonPropertyName) ?? jsonPropertyName);

                // if (propertyAttr?.Converter != null)
                // {
                //     throw new JsonException("Custom converters are not supported.");
                // }

                JsonSerializer.Serialize(writer, propValue, propertyMetadata.PropertyInfo.PropertyType, _options);
            }

            writer.WriteEndObject();
        }
    }
}

static class ObjectModelMetadataTrackerInspector
{
    public record Metadata(
        string? TableName,
        IReadOnlyCollection<PropertyMetadata> Properties,
        PropertyMetadata? HashKey,
        PropertyMetadata? RangeKey)
    {
        public bool Empty => TableName is null && Properties is { Count: 0 } && HashKey is null && RangeKey == null;
    }

    public record PropertyMetadata(string PropertyName, PropertyInfo PropertyInfo);

    public static Metadata GetMetadata(Type type)
    {
        // Inheritance
        // Field should not be supported
        // There can only be one hash and sort key
        // Can range and sort key be null?
        // Caching
        var tableAttr = type.GetCustomAttribute<DynamoDBTableAttribute>();
        var properties = type.GetProperties();

        var propertyMetadata = new List<PropertyMetadata>(properties.Length);
        PropertyMetadata? hashKey = null;
        PropertyMetadata? rangeKey = null;
        foreach (PropertyInfo prop in properties)
        {
            if (!prop.CanRead)
            {
                continue;
            }

            DynamoDBHashKeyAttribute? hashKeyAttr = prop.GetCustomAttribute<DynamoDBHashKeyAttribute>();
            DynamoDBRangeKeyAttribute? rangeKeyAttr = prop.GetCustomAttribute<DynamoDBRangeKeyAttribute>();
            DynamoDBPropertyAttribute? propertyAttr = prop.GetCustomAttribute<DynamoDBPropertyAttribute>();
            DynamoDBIgnoreAttribute? ignoreAttribute = prop.GetCustomAttribute<DynamoDBIgnoreAttribute>();

            // Skip if no DynamoDB attributes are found
            if (hashKeyAttr == null && rangeKeyAttr == null && propertyAttr == null && ignoreAttribute == null)
            {
                continue;
            }

            if (hashKeyAttr != null)
            {
                hashKey = new PropertyMetadata(hashKeyAttr.AttributeName ?? prop.Name, prop);
                propertyMetadata.Add(hashKey);
                continue;
            }

            if (rangeKeyAttr != null)
            {
                rangeKey = new PropertyMetadata(rangeKeyAttr.AttributeName ?? prop.Name, prop);
                propertyMetadata.Add(rangeKey);
                continue;
            }

            if (propertyAttr != null)
            {
                propertyMetadata.Add(new PropertyMetadata(propertyAttr.AttributeName ?? prop.Name, prop));
                continue;
            }
        }

        return new Metadata(tableAttr?.TableName, propertyMetadata, hashKey, rangeKey);
    }
}