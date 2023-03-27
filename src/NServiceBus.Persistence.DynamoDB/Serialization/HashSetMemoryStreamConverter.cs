#nullable enable
namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using System.Text.Json.Serialization;

    sealed class HashSetMemoryStreamConverter : JsonConverter<HashSet<MemoryStream>>
    {
        public const string PropertyName = "HashSetMemoryStreamContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

        public HashSetMemoryStreamConverter() => streamConverter = new MemoryStreamConverter();

        public override HashSet<MemoryStream> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
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

            var hashset = new HashSet<MemoryStream>();
            while (reader.TokenType != JsonTokenType.EndArray)
            {
                hashset.Add(streamConverter.Read(ref reader, typeof(MemoryStream), options));

                reader.Read();
            }

            reader.Read();

            if (reader.TokenType != JsonTokenType.EndObject)
            {
                throw new JsonException();
            }
            return hashset;
        }

        public override void Write(Utf8JsonWriter writer, HashSet<MemoryStream> value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();
            writer.WritePropertyName(PropertyName);
            writer.WriteStartArray();
            foreach (var stream in value)
            {
                streamConverter.Write(writer, stream, options);
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        readonly MemoryStreamConverter streamConverter;
    }
}