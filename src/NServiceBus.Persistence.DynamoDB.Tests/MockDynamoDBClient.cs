namespace NServiceBus.Persistence.DynamoDB.Tests;

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;
using Endpoint = Amazon.Runtime.Endpoints.Endpoint;

public sealed class MockDynamoDBClient : IAmazonDynamoDB
{
    readonly AmazonDynamoDBConfig config = new() { ServiceURL = "http://fakeServiceUrl" };

    public IClientConfig Config => config;

    public List<BatchWriteItemRequest> BatchWriteRequestsSent { get; } = [];

    public Func<BatchWriteItemRequest, BatchWriteItemResponse> BatchWriteRequestResponse = _ => new BatchWriteItemResponse();

    public Task<BatchWriteItemResponse> BatchWriteItemAsync(Dictionary<string, List<WriteRequest>> requestItems, CancellationToken cancellationToken = default)
        => BatchWriteItemAsync(new BatchWriteItemRequest(requestItems), cancellationToken);

    public Task<BatchWriteItemResponse> BatchWriteItemAsync(BatchWriteItemRequest request, CancellationToken cancellationToken = default)
    {
        BatchWriteRequestsSent.Add(request);
        return Task.FromResult(BatchWriteRequestResponse(request));
    }

    public List<UpdateItemRequest> UpdateItemRequestsSent { get; } = [];

    public Func<UpdateItemRequest, UpdateItemResponse> UpdateItemRequestResponse = _ => new UpdateItemResponse();
    public Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken = default)
    {
        UpdateItemRequestsSent.Add(request);
        return Task.FromResult(UpdateItemRequestResponse(request));
    }

    public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request,
        CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public List<TransactWriteItemsRequest> TransactWriteRequestsSent { get; } = [];

    public Func<TransactWriteItemsRequest, TransactWriteItemsResponse> TransactWriteRequestResponse = _ => new TransactWriteItemsResponse
    {
        HttpStatusCode = HttpStatusCode.OK
    };

    public Task<TransactWriteItemsResponse> TransactWriteItemsAsync(TransactWriteItemsRequest request,
        CancellationToken cancellationToken = default)
    {
        // we need to take a defensive copy otherwise when batches are cleared the data is lost
        var items = new List<TransactWriteItem>(request.TransactItems);
        request.TransactItems = items;
        TransactWriteRequestsSent.Add(request);
        return Task.FromResult(TransactWriteRequestResponse(request));
    }

    public List<QueryRequest> QueryRequestsSent { get; } = [];

    public Func<QueryRequest, QueryResponse> QueryRequestResponse = _ => new QueryResponse();

    public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request,
        CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = default)
    {
        QueryRequestsSent.Add(request);
        return Task.FromResult(QueryRequestResponse(request));
    }

    public void Dispose() { }

    #region NotImplemented

    public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, ReturnConsumedCapacity returnConsumedCapacity,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<CreateTableResponse> CreateTableAsync(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions,
        ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<CreateTableResponse> CreateTableAsync(CreateTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, ReturnValue returnValues,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<DeleteItemResponse> DeleteItemAsync(DeleteItemRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request,
        CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public Task<DeleteTableResponse> DeleteTableAsync(string tableName, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DeleteTableResponse> DeleteTableAsync(DeleteTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DescribeTableResponse> DescribeTableAsync(string tableName, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, bool? consistentRead,
        CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request,
        CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<ListTablesResponse> ListTablesAsync(CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, int? limit,
        CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public Task<ListTablesResponse> ListTablesAsync(int? limit, CancellationToken cancellationToken = default) => throw new NotImplementedException();

    public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, int limit,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<ListTablesResponse> ListTablesAsync(int limit, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<ListTablesResponse> ListTablesAsync(ListTablesRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, ReturnValue returnValues,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<ScanResponse> ScanAsync(string tableName, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, Dictionary<string, Condition> scanFilter,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, ReturnValue returnValues,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = default) => throw new System.NotImplementedException();

    public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request,
        CancellationToken cancellationToken = default) =>
        throw new System.NotImplementedException();

    public Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request) => throw new NotImplementedException();

    public IDynamoDBv2PaginatorFactory Paginators { get; }
    #endregion
}