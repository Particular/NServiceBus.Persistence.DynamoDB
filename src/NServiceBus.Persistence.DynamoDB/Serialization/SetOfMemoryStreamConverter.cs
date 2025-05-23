namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using Amazon.DynamoDBv2.Model;

sealed class SetOfMemoryStreamConverter : JsonConverterFactory, IAttributeConverter
{
    // This is a cryptic property name to make sure we never clash with the user data
    const string PropertyName = "HashSetMemoryStreamContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

    public override bool CanConvert(Type typeToConvert)
        => typeToConvert.IsGenericType && typeof(ISet<MemoryStream>).IsAssignableFrom(typeToConvert);

    public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
    {
        var converter = (JsonConverter)Activator.CreateInstance(
            typeof(SetConverter<>)
                .MakeGenericType(typeToConvert),
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

        var memoryStreams = new List<MemoryStream?>(property.GetArrayLength());
        foreach (var streamElement in property.EnumerateArray())
        {
            _ = MemoryStreamConverter.TryExtract(streamElement, out var stream);
            memoryStreams.Add(stream);
        }

        attributeValue = memoryStreams.Count == 0
            ? new AttributeValue { IsLSet = true }
            : new AttributeValue { BS = memoryStreams };
        return true;
    }

    public JsonNode ToNode(AttributeValue attributeValue)
    {
        var jsonObject = new JsonObject();
        var streamHashSetContent = new JsonArray();
        foreach (var memoryStream in attributeValue.BS)
        {
            streamHashSetContent.Add(MemoryStreamConverter.ToNode(memoryStream));
        }
        jsonObject.Add(PropertyName, streamHashSetContent);
        return jsonObject;
    }

    sealed class SetConverter<TSet>(JsonSerializerOptions options) : JsonConverter<TSet>
        where TSet : ISet<MemoryStream>
    {
        public override TSet? Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
        {
            // For empty sets stored in L
            if (reader.TokenType == JsonTokenType.StartArray)
            {
                var setStoredInL = JsonSerializer.Deserialize<TSet>(ref reader, optionsWithoutSetOfMemoryStreamConverter);
                return setStoredInL;
            }

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

            var set = JsonSerializer.Deserialize<TSet>(ref reader, optionsWithoutSetOfMemoryStreamConverter);

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
            JsonSerializer.Serialize(writer, value, optionsWithoutSetOfMemoryStreamConverter);
            writer.WriteEndObject();
        }

        readonly JsonSerializerOptions optionsWithoutSetOfMemoryStreamConverter = options.FromWithout<SetOfMemoryStreamConverter>();
    }
}