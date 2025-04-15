namespace NServiceBus.Persistence.DynamoDB.Serialization;

using System.Linq;
using System.Text.Json.Serialization.Metadata;
using Amazon.DynamoDBv2.DataModel;

public static class DynamoDBAttributeSupportJsonExtensions
{
    public static void SupportObjectModelAttributes(JsonTypeInfo typeInfo)
    {
        if (typeInfo.Kind != JsonTypeInfoKind.Object)
        {
            return;
        }

        foreach (JsonPropertyInfo property in typeInfo.Properties)
        {
            var renamableAttributes = property.AttributeProvider?.GetCustomAttributes(typeof(DynamoDBRenamableAttribute), true) ?? [];
            if (renamableAttributes.SingleOrDefault() is DynamoDBRenamableAttribute renamable)
            {
                if (!string.IsNullOrEmpty(renamable.AttributeName))
                {
                    property.Name = renamable.AttributeName;
                }

                continue;
            }

            var ignoreAttributes = property.AttributeProvider?.GetCustomAttributes(typeof(DynamoDBRenamableAttribute), true) ?? [];
            if (ignoreAttributes.SingleOrDefault() is DynamoDBIgnoreAttribute)
            {
                property.ShouldSerialize = (_, __) => false;
                continue;
            }

            // all others ignore
            property.ShouldSerialize = (_, __) => false;
        }
    }
}