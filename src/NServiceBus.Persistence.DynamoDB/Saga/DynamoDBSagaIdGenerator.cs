﻿namespace NServiceBus.Persistence.DynamoDB
{
    using System;
    using System.Buffers;
#if NETFRAMEWORK
    using System.Runtime.InteropServices;
    using System.Buffers.Binary;
#endif
    using System.Security.Cryptography;
    using System.Text;
    using System.Text.Json;

    static class DynamoDBSagaIdGenerator
    {
        public static Guid Generate(Type sagaEntityType, string correlationPropertyName, object correlationPropertyValue)
        {
            // assumes single correlated sagas since v6 doesn't allow more than one corr prop
            // will still have to use a GUID since moving to a string id will have to wait since its a breaking change
            var serializedPropertyValue = JsonSerializer.Serialize(correlationPropertyValue);
            return DeterministicGuid($"{sagaEntityType.FullName}_{correlationPropertyName}_{serializedPropertyValue}");
        }

#if NETFRAMEWORK
        static Guid DeterministicGuid(string src)
        {
            var byteCount = Encoding.UTF8.GetByteCount(src);
            var buffer = ArrayPool<byte>.Shared.Rent(byteCount);
            try
            {
                var numberOfBytesWritten = Encoding.UTF8.GetBytes(src.AsSpan(), buffer);

                using var sha1CryptoServiceProvider = SHA1.Create();
                var guidBytes = sha1CryptoServiceProvider.ComputeHash(buffer, 0, numberOfBytesWritten).AsSpan().Slice(0, GuidSizeInBytes);
                if (!TryParseGuidBytes(guidBytes, out var deterministicGuid))
                {
                    deterministicGuid = new Guid(guidBytes.ToArray());
                }
                return deterministicGuid;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer, clearArray: true);
            }
        }

        static bool TryParseGuidBytes(ReadOnlySpan<byte> bytes, out Guid guid)
        {
            if (bytes.Length != GuidSizeInBytes)
            {
                guid = default;
                return false;
            }

            if (BitConverter.IsLittleEndian)
            {
                guid = MemoryMarshal.Read<Guid>(bytes);
                return true;
            }

            // copied from https://github.com/dotnet/runtime/blob/9129083c2fc6ef32479168f0555875b54aee4dfb/src/libraries/System.Private.CoreLib/src/System/Guid.cs#L49
            // slower path for BigEndian:
            byte k = bytes[15];  // hoist bounds checks
            int a = BinaryPrimitives.ReadInt32LittleEndian(bytes);
            short b = BinaryPrimitives.ReadInt16LittleEndian(bytes.Slice(4));
            short c = BinaryPrimitives.ReadInt16LittleEndian(bytes.Slice(6));
            byte d = bytes[8];
            byte e = bytes[9];
            byte f = bytes[10];
            byte g = bytes[11];
            byte h = bytes[12];
            byte i = bytes[13];
            byte j = bytes[14];

            guid = new Guid(a, b, c, d, e, f, g, h, i, j, k);
            return true;
        }
#endif

#if NET
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
#endif
        const int GuidSizeInBytes = 16;
    }

#if NETFRAMEWORK
    static class SpanExtensions
    {
        public static unsafe int GetBytes(this Encoding encoding, ReadOnlySpan<char> src, Span<byte> dst)
        {
            if (src.IsEmpty)
            {
                return 0;
            }

            fixed (char* chars = &src.GetPinnableReference())
            fixed (byte* bytes = &dst.GetPinnableReference())
            {
                return encoding.GetBytes(chars, src.Length, bytes, dst.Length);
            }
        }
    }
#endif
}
