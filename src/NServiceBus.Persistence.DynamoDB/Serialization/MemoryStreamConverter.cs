namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Text.Json.Serialization;
    using System.Threading;

    sealed class MemoryStreamConverter : JsonConverter<MemoryStream>
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

        public static bool TryExtract(JsonProperty property, out MemoryStream? memoryStream)
        {
            memoryStream = null;
            if (!property.NameEquals(PropertyName))
            {
                return false;
            }

            GetStream(property.Value.GetGuid(), out memoryStream);
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