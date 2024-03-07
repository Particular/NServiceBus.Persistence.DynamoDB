namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Amazon.DynamoDBv2.Model;

sealed class SetOfNumberConverter : JsonConverterFactory, IAttributeConverter
{
    // This is a cryptic property name to make sure we never clash with the user data
    const string PropertyName = "HashSetNumberContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

    public override bool CanConvert(Type typeToConvert)
    {
        if (!typeToConvert.IsGenericType)
        {
            return false;
        }

        var innerTypeToConvert = typeToConvert.GetGenericArguments()[0];
        var isNumberTypeSupported = innerTypeToConvert == typeof(byte) ||
                                    innerTypeToConvert == typeof(sbyte) ||
                                    innerTypeToConvert == typeof(ushort) ||
                                    innerTypeToConvert == typeof(uint) ||
                                    innerTypeToConvert == typeof(ulong) ||
                                    innerTypeToConvert == typeof(long) ||
                                    innerTypeToConvert == typeof(short) ||
                                    innerTypeToConvert == typeof(int) ||
                                    innerTypeToConvert == typeof(double) ||
                                    innerTypeToConvert == typeof(decimal) ||
                                    innerTypeToConvert == typeof(float);

        return isNumberTypeSupported &&
               // This check is fairly expensive so we do it only once we know it is a number type that is supported
               typeToConvert.IsAssignableToGenericType(typeof(ISet<>));
    }

    public override JsonConverter CreateConverter(Type type, JsonSerializerOptions options)
    {
        Type valueType = type.GetGenericArguments()[0];
        var converter = (JsonConverter)Activator.CreateInstance(
            typeof(SetConverter<,>)
                .MakeGenericType([type, valueType]),
            BindingFlags.Instance | BindingFlags.Public,
            binder: null,
            args: [options],
            culture: null)!;
        return converter;

    }

    public bool TryExtract(JsonElement element, [NotNullWhen(true)] out AttributeValue? attributeValue)
    {
        attributeValue = null;
        if (!element.TryGetProperty(PropertyName, out var property))
        {
            return false;
        }

        var numbersAsStrings = new List<string?>(property.GetArrayLength());
        foreach (var innerElement in property.EnumerateArray())
        {
            numbersAsStrings.Add(innerElement.ToString());
        }
        attributeValue = new AttributeValue { NS = numbersAsStrings };
        return true;
    }

    public JsonNode ToNode(AttributeValue attributeValue)
    {
        var jsonObject = new JsonObject();
        var array = new JsonArray();
        foreach (var numberAsString in attributeValue.NS)
        {
            array.Add(JsonNode.Parse(numberAsString));
        }
        jsonObject.Add(PropertyName, array);
        return jsonObject;
    }

    sealed class SetConverter<TSet, TValue> : JsonConverter<TSet>
        where TSet : ISet<TValue>
        where TValue : struct
    {
        public SetConverter(JsonSerializerOptions options)
            => optionsWithoutSetOfNumberConverter = options.FromWithout<SetOfNumberConverter>();

        public override TSet? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new JsonException();
            }

            reader.Read();
            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException();
            }

            string? propertyName = reader.GetString();
            if (propertyName != PropertyName)
            {
                throw new JsonException();
            }

            reader.Read();
            if (reader.TokenType != JsonTokenType.StartArray)
            {
                throw new JsonException();
            }

            var set = JsonSerializer.Deserialize<TSet>(ref reader, optionsWithoutSetOfNumberConverter);

            reader.Read();

            if (reader.TokenType != JsonTokenType.EndObject)
            {
                throw new JsonException();
            }
            return set;
        }

        public override void Write(Utf8JsonWriter writer, TSet value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(PropertyName);
            JsonSerializer.Serialize(writer, value, optionsWithoutSetOfNumberConverter);
            writer.WriteEndObject();
        }

        readonly JsonSerializerOptions optionsWithoutSetOfNumberConverter;
    }
}