namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.IO;
using System.Runtime.InteropServices;

sealed class ReadOnlyMemoryStream : MemoryStream
{
    readonly ReadOnlyMemory<byte> memory;
    int position;

    public ReadOnlyMemoryStream(ReadOnlyMemory<byte> memory)
    {
        this.memory = memory;
        position = 0;
    }

    public override void Flush() => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override int Read(byte[] buffer, int offset, int count)
    {
        var bytesToCopy = Math.Min(count, memory.Length - position);

        var destination = buffer.AsSpan().Slice(offset, bytesToCopy);
        var source = memory.Span.Slice(position, bytesToCopy);

        source.CopyTo(destination);

        position += bytesToCopy;

        return bytesToCopy;
    }

    public override int Read(Span<byte> buffer)
    {
        var bytesToCopy = Math.Min(memory.Length - position, buffer.Length);
        if (bytesToCopy <= 0)
        {
            return 0;
        }

        var source = memory.Span.Slice(position, bytesToCopy);
        source.CopyTo(buffer);

        position += bytesToCopy;
        return bytesToCopy;
    }

    public override byte[] ToArray() => memory.ToArray();

    public override bool TryGetBuffer(out ArraySegment<byte> buffer) => MemoryMarshal.TryGetArray(memory, out buffer);

    public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => memory.Length;
    public override long Position { get => position; set => position = (int)value; }
}