namespace NServiceBus.Persistence.DynamoDB;

using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using System.Text.Json.Nodes;
using Amazon.DynamoDBv2.Model;

interface IAttributeConverter
{
    bool TryExtract(JsonElement element, [NotNullWhen(true)] out AttributeValue? attributeValue);

    JsonNode ToNode(AttributeValue attributeValue);
}