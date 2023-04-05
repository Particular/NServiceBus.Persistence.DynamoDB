namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.Json.Nodes;
    using System.Text.Json.Serialization;

    sealed class HashSetMemoryStreamConverter : JsonConverterFactory
    {
        // This is a cryptic property name to make sure we never clash with the user data
        const string PropertyName = "HashSetMemoryStreamContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

        public override bool CanConvert(Type typeToConvert)
            => typeToConvert.IsGenericType && typeof(ISet<MemoryStream>).IsAssignableFrom(typeToConvert);

        public override JsonConverter CreateConverter(Type typeToConvert, JsonSerializerOptions options)
        {
            var converter = (JsonConverter)Activator.CreateInstance(
                typeof(SetConverter<>)
                    .MakeGenericType(new Type[] { typeToConvert }),
                BindingFlags.Instance | BindingFlags.Public,
                binder: null,
                args: new[] { options },
                culture: null)!;
            return converter;
        }

        sealed class SetConverter<TSet> : JsonConverter<TSet> where TSet : ISet<MemoryStream>
        {
            public SetConverter(JsonSerializerOptions options)
                => optionsWithoutHashSetMemoryStreamConverter = options.FromWithout<HashSetMemoryStreamConverter>();

            public override TSet? Read(ref Utf8JsonReader reader, Type typeToConvert,
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
                if (reader.TokenType != JsonTokenType.StartArray)
                {
                    throw new JsonException();
                }

                var set = JsonSerializer.Deserialize<TSet>(ref reader, optionsWithoutHashSetMemoryStreamConverter);

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
                JsonSerializer.Serialize(writer, value, optionsWithoutHashSetMemoryStreamConverter);
                writer.WriteEndObject();
            }

            readonly JsonSerializerOptions optionsWithoutHashSetMemoryStreamConverter;
        }

        public static bool TryExtract(JsonProperty property, out List<MemoryStream?>? memoryStreams)
        {
            memoryStreams = null;
            if (!property.NameEquals(PropertyName))
            {
                return false;
            }

            memoryStreams = new List<MemoryStream?>(property.Value.GetArrayLength());
            foreach (var innerElement in property.Value.EnumerateArray())
            {
                foreach (var streamElement in innerElement.EnumerateObject())
                {
                    _ = MemoryStreamConverter.TryExtract(streamElement, out var stream);
                    memoryStreams.Add(stream);
                }
            }
            return true;
        }

        public static JsonNode ToNode(List<MemoryStream> memoryStreams)
        {
            var jsonObject = new JsonObject();
            var streamHashSetContent = new JsonArray();
            foreach (var memoryStream in memoryStreams)
            {
                streamHashSetContent.Add(MemoryStreamConverter.ToNode(memoryStream));
            }
            jsonObject.Add(PropertyName, streamHashSetContent);
            return jsonObject;
        }
    }
}