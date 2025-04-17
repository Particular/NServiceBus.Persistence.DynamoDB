namespace NServiceBus;

using System;
using System.Text.Json;
using Persistence.DynamoDB;

/// <summary>
/// The saga persistence configuration options.
/// </summary>
public class SagaPersistenceConfiguration
{
    /// <summary>
    /// The configuration of the table used by the outbox persistence.
    /// </summary>
    public TableConfiguration Table { get; set; } = new()
    {
        TimeToLiveAttributeName = null
    };

    /// <summary>
    /// Enables eventual consistent reads on table containing the saga data. This might reduce costs for reads since
    /// eventual consistent reads are cheaper than strongly consistent reads. However, it might lead to stale data being read which can lead
    /// too more retries in case of concurrent updates to the same saga.
    /// </summary>
    /// <remarks>This setting is mutually exclusive to <see cref="UsePessimisticLocking"/> meaning when opting into eventual
    /// consistent reads pessimistic locking is disabled when previously explicitly enabled and vice versa.</remarks>
    public bool UseEventuallyConsistentReads
    {
        get => useEventuallyConsistentReads.GetValueOrDefault(false);
        set
        {
            useEventuallyConsistentReads = value;
            if (usePessimisticLocking.HasValue)
            {
                usePessimisticLocking = !value;
            }
        }
    }

    bool? useEventuallyConsistentReads;

    /// <summary>
    /// Enables pessimistic locking mode to avoid concurrent modifications to the same saga. Enable this mode to reduce retries due to optimistic concurrency control violations.
    /// </summary>
    /// <remarks>This setting is mutually exclusive to <see cref="UseEventuallyConsistentReads"/> meaning when opting into pessimistic locking
    /// eventual consistent reads are disabled when previously explicitly enabled and vice versa.</remarks>
    public bool UsePessimisticLocking
    {
        get => usePessimisticLocking.GetValueOrDefault(false);
        set
        {
            usePessimisticLocking = value;
            if (useEventuallyConsistentReads.HasValue)
            {
                useEventuallyConsistentReads = !value;
            }
        }
    }

    bool? usePessimisticLocking;

    /// <summary>
    /// Determines whether the NServiceBus installer should create the Outbox table when enabled.
    /// </summary>
    internal bool CreateTable { get; set; } = true;

    /// <summary>
    /// Defines the lease duration when using pessimistic locking.
    /// </summary>
    public TimeSpan LeaseDuration { get; set; } = TimeSpan.FromSeconds(30); // based on SQS visibility timeout

    /// <summary>
    /// How long the client should attempt to acquire a lock when using pessimistic locking before giving up.
    /// </summary>
    public TimeSpan LeaseAcquisitionTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// The serializer options used to serialize and deserialize the saga data.
    /// </summary>
    public JsonSerializerOptions MapperOptions { get; set; } = new(Mapper.Default);
}