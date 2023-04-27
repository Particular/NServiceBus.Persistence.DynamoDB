namespace NServiceBus.Persistence.DynamoDB;

using System.Text.Json;

static class MapperOptions
{
    /// <summary>
    /// The defaults are never directly used to serialize and deserialize otherwise they become immutable
    /// </summary>
    public static JsonSerializerOptions Defaults { get; } =
        new()
        {
            Converters =
            {
                new MemoryStreamConverter(),
                new SetOfMemoryStreamConverter(),
                new SetOfStringConverter(),
                new SetOfNumberConverter()
            }
        };
}