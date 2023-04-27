namespace NServiceBus.Persistence.DynamoDB;

using System;
using System.Collections.Generic;

class StorageTransportOperation
{
    public StorageTransportOperation(Outbox.TransportOperation source)
    {
        MessageId = source.MessageId;
        Options = source.Options != null ? new Dictionary<string, string>(source.Options) : new Dictionary<string, string>();
        Body = source.Body;
        Headers = source.Headers;
    }

    public string MessageId { get; set; }
    public Dictionary<string, string> Options { get; set; }
    public ReadOnlyMemory<byte> Body { get; set; }
    public Dictionary<string, string> Headers { get; set; }
}