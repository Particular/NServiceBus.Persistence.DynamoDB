#nullable enable
namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Text.Json;
    using System.Text.Json.Serialization;

    sealed class HashSetStringConverter : JsonConverter<HashSet<string>>
    {
        public const string PropertyName = "HashSetStringContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

        public override HashSet<string> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
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

            reader.Read();

            var hashset = new HashSet<string>();
            while (reader.TokenType != JsonTokenType.EndArray)
            {
                hashset.Add(reader.GetString()!);

                reader.Read();
            }

            reader.Read();

            if (reader.TokenType != JsonTokenType.EndObject)
            {
                throw new JsonException();
            }
            return hashset;
        }

        public override void Write(Utf8JsonWriter writer, HashSet<string> value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(PropertyName);
            writer.WriteStartArray();
            foreach (string s in value)
            {
                writer.WriteStringValue(s);
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
        }
    }
}