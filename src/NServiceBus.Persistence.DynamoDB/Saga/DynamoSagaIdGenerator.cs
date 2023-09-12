namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.Buffers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

static class DynamoSagaIdGenerator
{
    public static Guid Generate(Type sagaEntityType, string correlationPropertyName, object correlationPropertyValue)
    {
        // assumes single correlated sagas since v6 doesn't allow more than one corr prop
        // will still have to use a GUID since moving to a string id will have to wait since its a breaking change
        var serializedPropertyValue = JsonSerializer.Serialize(correlationPropertyValue);
        return DeterministicGuid($"{sagaEntityType.FullName}_{correlationPropertyName}_{serializedPropertyValue}");
    }

    static Guid DeterministicGuid(string src)
    {
        var byteCount = Encoding.UTF8.GetByteCount(src);
        var stringBuffer = ArrayPool<byte>.Shared.Rent(byteCount);
        try
        {
            var numberOfBytesWritten = Encoding.UTF8.GetBytes(src.AsSpan(), stringBuffer);

            using var sha1CryptoServiceProvider = SHA1.Create();
            Span<byte> hashBuffer = stackalloc byte[20];
            if (!sha1CryptoServiceProvider.TryComputeHash(stringBuffer.AsSpan().Slice(0, numberOfBytesWritten), hashBuffer, out _))
            {
                var hashBufferLocal = sha1CryptoServiceProvider.ComputeHash(stringBuffer, 0, numberOfBytesWritten);
                hashBufferLocal.CopyTo(hashBuffer);
            }

            var guidBytes = hashBuffer.Slice(0, GuidSizeInBytes);
            return new Guid(guidBytes);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(stringBuffer, clearArray: true);
        }
    }

    const int GuidSizeInBytes = 16;
}
