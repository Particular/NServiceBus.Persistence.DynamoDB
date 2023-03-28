#nullable enable
namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Reflection;
    using System.Text.Json;
    using System.Text.Json.Serialization;

    sealed class HashSetMemoryStreamConverter : JsonConverterFactory
    {
        public const string PropertyName = "HashSetMemoryStreamContent838D2F22-0D5B-4831-8C04-17C7A6329B31";

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
            {
                streamConverter = (JsonConverter<MemoryStream>)options.GetConverter(typeof(MemoryStream));
                memoryStreamOptions = new JsonSerializerOptions { Converters = { streamConverter } };
            }

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

                var set = JsonSerializer.Deserialize<TSet>(ref reader, memoryStreamOptions);

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
                writer.WriteStartArray();
                foreach (var stream in value)
                {
                    streamConverter.Write(writer, stream, options);
                }
                writer.WriteEndArray();
                writer.WriteEndObject();
            }

            readonly JsonConverter<MemoryStream> streamConverter;
            readonly JsonSerializerOptions memoryStreamOptions;
        }
    }
}