namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Serialization;
using Amazon.DynamoDBv2.DataModel;

sealed class ObjectModelAttributeConverter : JsonConverterFactory
{
    public override bool CanConvert(Type typeToConvert) =>
        typeToConvert.GetCustomAttribute<DynamoDBTableAttribute>() != null ||
        typeToConvert.GetProperties().Any(p =>
            p.GetCustomAttribute<DynamoDBHashKeyAttribute>() != null ||
            p.GetCustomAttribute<DynamoDBRangeKeyAttribute>() != null ||
            p.GetCustomAttribute<DynamoDBPropertyAttribute>() != null);

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
            writer.WriteStartObject();

            // TBD check the table attribute

            PropertyInfo[] properties = typeof(T).GetProperties();

            foreach (PropertyInfo prop in properties)
            {
                if (!prop.CanRead)
                {
                    continue;
                }

                DynamoDBHashKeyAttribute? hashKeyAttr = prop.GetCustomAttribute<DynamoDBHashKeyAttribute>();
                DynamoDBRangeKeyAttribute? rangeKeyAttr = prop.GetCustomAttribute<DynamoDBRangeKeyAttribute>();
                DynamoDBPropertyAttribute? propertyAttr = prop.GetCustomAttribute<DynamoDBPropertyAttribute>();

                // Skip if no DynamoDB attributes are found
                if (hashKeyAttr == null && rangeKeyAttr == null && propertyAttr == null)
                {
                    continue;
                }

                // Get property value
                object? propValue = prop.GetValue(value);
                if (propValue is null)
                {
                    continue;
                }

                string jsonPropertyName;

                if (hashKeyAttr != null && !string.IsNullOrEmpty(hashKeyAttr.AttributeName))
                {
                    jsonPropertyName = hashKeyAttr.AttributeName;
                }
                else if (rangeKeyAttr != null && !string.IsNullOrEmpty(rangeKeyAttr.AttributeName))
                {
                    jsonPropertyName = rangeKeyAttr.AttributeName;
                }
                else if (propertyAttr != null && !string.IsNullOrEmpty(propertyAttr.AttributeName))
                {
                    jsonPropertyName = propertyAttr.AttributeName;
                }
                else
                {
                    jsonPropertyName = prop.Name;
                }

                writer.WritePropertyName(
                    options.PropertyNamingPolicy?.ConvertName(jsonPropertyName) ?? jsonPropertyName);

                if (propertyAttr?.Converter != null)
                {
                    throw new JsonException("Custom converters are not supported.");
                }

                JsonSerializer.Serialize(writer, propValue, prop.PropertyType, _options);
            }

            writer.WriteEndObject();
        }
    }
}