namespace NServiceBus.Persistence.DynamoDB
{
    using System.Text.Json;
    using System.Text.Json.Serialization;

    static class JsonSerializerOptionsExtensions
    {
        public static JsonSerializerOptions FromWithout<TConverter>(this JsonSerializerOptions options)
            where TConverter : JsonConverter
        {
            var newOptions = new JsonSerializerOptions(options);
            JsonConverter? converterToRemove = null;
            foreach (var converter in newOptions.Converters)
            {
                if (converter is not TConverter)
                {
                    continue;
                }

                converterToRemove = converter;
                break;
            }

            if (converterToRemove != null)
            {
                newOptions.Converters.Remove(converterToRemove);
            }
            return newOptions;
        }
    }
}