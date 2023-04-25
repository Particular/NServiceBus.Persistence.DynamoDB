namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Text.Json.Serialization;
    using System.Threading;
    using Amazon.DynamoDBv2.Model;

    sealed class MemoryStreamConverter : JsonConverter<MemoryStream>, IAttributeConverter
    {
        // This is a cryptic property name to make sure we never clash with the user data
        const string PropertyName = "MemoryStreamContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

        public override MemoryStream? Read(ref Utf8JsonReader reader, Type typeToConvert,
            JsonSerializerOptions options)
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
            if (reader.TokenType != JsonTokenType.String)
            {
                throw new JsonException();
            }

            GetStream(reader.GetGuid(), out var stream);

            reader.Read();

            if (reader.TokenType != JsonTokenType.EndObject)
            {
                throw new JsonException();
            }
            return stream;
        }

        public override void Write(Utf8JsonWriter writer, MemoryStream value, JsonSerializerOptions options)
        {
            Guid streamId = TrackStream(value);
            writer.WriteStartObject();
            writer.WriteString(PropertyName, streamId);
            writer.WriteEndObject();
        }

        public bool TryExtract(JsonElement element, [NotNullWhen(true)] out AttributeValue? attributeValue)
        {
            attributeValue = null;
            if (!TryExtract(element, out MemoryStream? memoryStream))
            {
                return false;
            }
            attributeValue = new AttributeValue { B = memoryStream };
            return true;
        }

        public JsonNode ToNode(AttributeValue attributeValue) => ToNode(attributeValue.B);

        public static bool TryExtract(JsonElement element, out MemoryStream? memoryStream)
        {
            memoryStream = null;
            if (!element.TryGetProperty(PropertyName, out var property))
            {
                return false;
            }

            GetStream(property.GetGuid(), out memoryStream);
            return true;
        }

        public static JsonNode ToNode(MemoryStream memoryStream)
            => new JsonObject
            {
                [PropertyName] = TrackStream(memoryStream)
            };

        public static void ClearTrackingState()
        {
            if (StreamMap.IsValueCreated)
            {
                StreamMap.Value!.Clear();
            }
        }

        static void GetStream(Guid streamId, out MemoryStream? memoryStream)
        {
            if (StreamMap.Value!.TryGetValue(streamId, out memoryStream))
            {
                StreamMap.Value.Remove(streamId);
            }
        }

        static Guid TrackStream(MemoryStream memoryStream)
        {
            var streamId = Guid.NewGuid();
            StreamMap.Value!.Add(streamId, memoryStream);
            return streamId;
        }

        // internal for tests
        internal static readonly ThreadLocal<Dictionary<Guid, MemoryStream>> StreamMap = new(() => new Dictionary<Guid, MemoryStream>());
    }
}