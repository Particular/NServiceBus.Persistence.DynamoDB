namespace NServiceBus.Persistence.DynamoDB.Tests;

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

public class MockDynamoDBClient : IAmazonDynamoDB
{
    readonly AmazonDynamoDBConfig config = new() { ServiceURL = "http://fakeServiceUrl" };

    public IClientConfig Config => config;

    public List<BatchWriteItemRequest> BatchWriteRequestsSent { get; } = new();

    public Func<BatchWriteItemRequest, BatchWriteItemResponse> BatchWriteRequestResponse = _ => new BatchWriteItemResponse();

    public Task<BatchWriteItemResponse> BatchWriteItemAsync(Dictionary<string, List<WriteRequest>> requestItems, CancellationToken cancellationToken = default)
        => BatchWriteItemAsync(new BatchWriteItemRequest(requestItems), cancellationToken);

    public Task<BatchWriteItemResponse> BatchWriteItemAsync(BatchWriteItemRequest request, CancellationToken cancellationToken = default)
    {
        BatchWriteRequestsSent.Add(request);
        return Task.FromResult(BatchWriteRequestResponse(request));
    }

    public List<UpdateItemRequest> UpdateItemRequestsSent { get; } = new();

    public Func<UpdateItemRequest, UpdateItemResponse> UpdateItemRequestResponse = _ => new UpdateItemResponse();
    public Task<UpdateItemResponse> UpdateItemAsync(UpdateItemRequest request, CancellationToken cancellationToken = default)
    {
        UpdateItemRequestsSent.Add(request);
        return Task.FromResult(UpdateItemRequestResponse(request));
    }

    public List<TransactWriteItemsRequest> TransactWriteRequestsSent { get; } = new();

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

    public List<QueryRequest> QueryRequestsSent { get; } = new();

    public Func<QueryRequest, QueryResponse> QueryRequestResponse = _ => new QueryResponse();

    public Task<QueryResponse> QueryAsync(QueryRequest request, CancellationToken cancellationToken = default)
    {
        QueryRequestsSent.Add(request);
        return Task.FromResult(QueryRequestResponse(request));
    }

    #region NotImplemented

    public BatchWriteItemResponse BatchWriteItem(BatchWriteItemRequest request) => throw new NotImplementedException();

    public CreateBackupResponse CreateBackup(CreateBackupRequest request) => throw new NotImplementedException();

    public void Dispose() => throw new System.NotImplementedException();

    public BatchExecuteStatementResponse BatchExecuteStatement(BatchExecuteStatementRequest request) => throw new NotImplementedException();

    public Task<BatchExecuteStatementResponse> BatchExecuteStatementAsync(BatchExecuteStatementRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public BatchGetItemResponse BatchGetItem(Dictionary<string, KeysAndAttributes> requestItems, ReturnConsumedCapacity returnConsumedCapacity) => throw new NotImplementedException();

    public BatchGetItemResponse BatchGetItem(Dictionary<string, KeysAndAttributes> requestItems) => throw new NotImplementedException();

    public BatchGetItemResponse BatchGetItem(BatchGetItemRequest request) => throw new NotImplementedException();

    public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, ReturnConsumedCapacity returnConsumedCapacity,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public Task<BatchGetItemResponse> BatchGetItemAsync(Dictionary<string, KeysAndAttributes> requestItems, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public Task<BatchGetItemResponse> BatchGetItemAsync(BatchGetItemRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public BatchWriteItemResponse BatchWriteItem(Dictionary<string, List<WriteRequest>> requestItems) => throw new NotImplementedException();

    public Task<CreateBackupResponse> CreateBackupAsync(CreateBackupRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public CreateGlobalTableResponse CreateGlobalTable(CreateGlobalTableRequest request) => throw new NotImplementedException();

    public Task<CreateGlobalTableResponse> CreateGlobalTableAsync(CreateGlobalTableRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public CreateTableResponse CreateTable(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions,
        ProvisionedThroughput provisionedThroughput) =>
        throw new NotImplementedException();

    public CreateTableResponse CreateTable(CreateTableRequest request) => throw new NotImplementedException();

    public Task<CreateTableResponse> CreateTableAsync(string tableName, List<KeySchemaElement> keySchema, List<AttributeDefinition> attributeDefinitions,
        ProvisionedThroughput provisionedThroughput, CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public Task<CreateTableResponse> CreateTableAsync(CreateTableRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public DeleteBackupResponse DeleteBackup(DeleteBackupRequest request) => throw new NotImplementedException();

    public Task<DeleteBackupResponse> DeleteBackupAsync(DeleteBackupRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public DeleteItemResponse DeleteItem(string tableName, Dictionary<string, AttributeValue> key) => throw new NotImplementedException();

    public DeleteItemResponse DeleteItem(string tableName, Dictionary<string, AttributeValue> key, ReturnValue returnValues) => throw new NotImplementedException();

    public DeleteItemResponse DeleteItem(DeleteItemRequest request) => throw new NotImplementedException();

    public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public Task<DeleteItemResponse> DeleteItemAsync(string tableName, Dictionary<string, AttributeValue> key, ReturnValue returnValues,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public Task<DeleteItemResponse> DeleteItemAsync(DeleteItemRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public DeleteTableResponse DeleteTable(string tableName) => throw new NotImplementedException();

    public DeleteTableResponse DeleteTable(DeleteTableRequest request) => throw new NotImplementedException();

    public Task<DeleteTableResponse> DeleteTableAsync(string tableName, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public Task<DeleteTableResponse> DeleteTableAsync(DeleteTableRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public DescribeBackupResponse DescribeBackup(DescribeBackupRequest request) => throw new NotImplementedException();

    public Task<DescribeBackupResponse> DescribeBackupAsync(DescribeBackupRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public DescribeContinuousBackupsResponse DescribeContinuousBackups(DescribeContinuousBackupsRequest request) => throw new NotImplementedException();

    public Task<DescribeContinuousBackupsResponse> DescribeContinuousBackupsAsync(DescribeContinuousBackupsRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public DescribeContributorInsightsResponse DescribeContributorInsights(DescribeContributorInsightsRequest request) => throw new NotImplementedException();

    public Task<DescribeContributorInsightsResponse> DescribeContributorInsightsAsync(DescribeContributorInsightsRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public DescribeEndpointsResponse DescribeEndpoints(DescribeEndpointsRequest request) => throw new NotImplementedException();

    public Task<DescribeEndpointsResponse> DescribeEndpointsAsync(DescribeEndpointsRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public DescribeExportResponse DescribeExport(DescribeExportRequest request) => throw new NotImplementedException();

    public Task<DescribeExportResponse> DescribeExportAsync(DescribeExportRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public DescribeGlobalTableResponse DescribeGlobalTable(DescribeGlobalTableRequest request) => throw new NotImplementedException();

    public Task<DescribeGlobalTableResponse> DescribeGlobalTableAsync(DescribeGlobalTableRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public DescribeGlobalTableSettingsResponse DescribeGlobalTableSettings(DescribeGlobalTableSettingsRequest request) => throw new NotImplementedException();

    public Task<DescribeGlobalTableSettingsResponse> DescribeGlobalTableSettingsAsync(DescribeGlobalTableSettingsRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public DescribeImportResponse DescribeImport(DescribeImportRequest request) => throw new NotImplementedException();

    public Task<DescribeImportResponse> DescribeImportAsync(DescribeImportRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public DescribeKinesisStreamingDestinationResponse DescribeKinesisStreamingDestination(
        DescribeKinesisStreamingDestinationRequest request) =>
        throw new NotImplementedException();

    public Task<DescribeKinesisStreamingDestinationResponse> DescribeKinesisStreamingDestinationAsync(DescribeKinesisStreamingDestinationRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public DescribeLimitsResponse DescribeLimits(DescribeLimitsRequest request) => throw new NotImplementedException();

    public Task<DescribeLimitsResponse> DescribeLimitsAsync(DescribeLimitsRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public DescribeTableResponse DescribeTable(string tableName) => throw new NotImplementedException();

    public DescribeTableResponse DescribeTable(DescribeTableRequest request) => throw new NotImplementedException();

    public Task<DescribeTableResponse> DescribeTableAsync(string tableName, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public Task<DescribeTableResponse> DescribeTableAsync(DescribeTableRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public DescribeTableReplicaAutoScalingResponse DescribeTableReplicaAutoScaling(DescribeTableReplicaAutoScalingRequest request) => throw new NotImplementedException();

    public Task<DescribeTableReplicaAutoScalingResponse> DescribeTableReplicaAutoScalingAsync(DescribeTableReplicaAutoScalingRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public DescribeTimeToLiveResponse DescribeTimeToLive(string tableName) => throw new NotImplementedException();

    public DescribeTimeToLiveResponse DescribeTimeToLive(DescribeTimeToLiveRequest request) => throw new NotImplementedException();

    public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(string tableName, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public Task<DescribeTimeToLiveResponse> DescribeTimeToLiveAsync(DescribeTimeToLiveRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public DisableKinesisStreamingDestinationResponse DisableKinesisStreamingDestination(
        DisableKinesisStreamingDestinationRequest request) =>
        throw new NotImplementedException();

    public Task<DisableKinesisStreamingDestinationResponse> DisableKinesisStreamingDestinationAsync(DisableKinesisStreamingDestinationRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public EnableKinesisStreamingDestinationResponse EnableKinesisStreamingDestination(
        EnableKinesisStreamingDestinationRequest request) =>
        throw new NotImplementedException();

    public Task<EnableKinesisStreamingDestinationResponse> EnableKinesisStreamingDestinationAsync(EnableKinesisStreamingDestinationRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public ExecuteStatementResponse ExecuteStatement(ExecuteStatementRequest request) => throw new NotImplementedException();

    public Task<ExecuteStatementResponse> ExecuteStatementAsync(ExecuteStatementRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public ExecuteTransactionResponse ExecuteTransaction(ExecuteTransactionRequest request) => throw new NotImplementedException();

    public Task<ExecuteTransactionResponse> ExecuteTransactionAsync(ExecuteTransactionRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public ExportTableToPointInTimeResponse ExportTableToPointInTime(ExportTableToPointInTimeRequest request) => throw new NotImplementedException();

    public Task<ExportTableToPointInTimeResponse> ExportTableToPointInTimeAsync(ExportTableToPointInTimeRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public GetItemResponse GetItem(string tableName, Dictionary<string, AttributeValue> key) => throw new NotImplementedException();

    public GetItemResponse GetItem(string tableName, Dictionary<string, AttributeValue> key, bool consistentRead) => throw new NotImplementedException();

    public GetItemResponse GetItem(GetItemRequest request) => throw new NotImplementedException();

    public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public Task<GetItemResponse> GetItemAsync(string tableName, Dictionary<string, AttributeValue> key, bool consistentRead,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public Task<GetItemResponse> GetItemAsync(GetItemRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public ImportTableResponse ImportTable(ImportTableRequest request) => throw new NotImplementedException();

    public Task<ImportTableResponse> ImportTableAsync(ImportTableRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public ListBackupsResponse ListBackups(ListBackupsRequest request) => throw new NotImplementedException();

    public Task<ListBackupsResponse> ListBackupsAsync(ListBackupsRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public ListContributorInsightsResponse ListContributorInsights(ListContributorInsightsRequest request) => throw new NotImplementedException();

    public Task<ListContributorInsightsResponse> ListContributorInsightsAsync(ListContributorInsightsRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public ListExportsResponse ListExports(ListExportsRequest request) => throw new NotImplementedException();

    public Task<ListExportsResponse> ListExportsAsync(ListExportsRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public ListGlobalTablesResponse ListGlobalTables(ListGlobalTablesRequest request) => throw new NotImplementedException();

    public Task<ListGlobalTablesResponse> ListGlobalTablesAsync(ListGlobalTablesRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public ListImportsResponse ListImports(ListImportsRequest request) => throw new NotImplementedException();

    public Task<ListImportsResponse> ListImportsAsync(ListImportsRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public ListTablesResponse ListTables() => throw new NotImplementedException();

    public ListTablesResponse ListTables(string exclusiveStartTableName) => throw new NotImplementedException();

    public ListTablesResponse ListTables(string exclusiveStartTableName, int limit) => throw new NotImplementedException();

    public ListTablesResponse ListTables(int limit) => throw new NotImplementedException();

    public ListTablesResponse ListTables(ListTablesRequest request) => throw new NotImplementedException();

    public Task<ListTablesResponse> ListTablesAsync(CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public Task<ListTablesResponse> ListTablesAsync(string exclusiveStartTableName, int limit,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public Task<ListTablesResponse> ListTablesAsync(int limit, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public Task<ListTablesResponse> ListTablesAsync(ListTablesRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public ListTagsOfResourceResponse ListTagsOfResource(ListTagsOfResourceRequest request) => throw new NotImplementedException();

    public Task<ListTagsOfResourceResponse> ListTagsOfResourceAsync(ListTagsOfResourceRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public PutItemResponse PutItem(string tableName, Dictionary<string, AttributeValue> item) => throw new NotImplementedException();

    public PutItemResponse PutItem(string tableName, Dictionary<string, AttributeValue> item, ReturnValue returnValues) => throw new NotImplementedException();

    public PutItemResponse PutItem(PutItemRequest request) => throw new NotImplementedException();

    public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public Task<PutItemResponse> PutItemAsync(string tableName, Dictionary<string, AttributeValue> item, ReturnValue returnValues,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public Task<PutItemResponse> PutItemAsync(PutItemRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public QueryResponse Query(QueryRequest request) => throw new NotImplementedException();

    public RestoreTableFromBackupResponse RestoreTableFromBackup(RestoreTableFromBackupRequest request) => throw new NotImplementedException();

    public Task<RestoreTableFromBackupResponse> RestoreTableFromBackupAsync(RestoreTableFromBackupRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public RestoreTableToPointInTimeResponse RestoreTableToPointInTime(RestoreTableToPointInTimeRequest request) => throw new NotImplementedException();

    public Task<RestoreTableToPointInTimeResponse> RestoreTableToPointInTimeAsync(RestoreTableToPointInTimeRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public ScanResponse Scan(string tableName, List<string> attributesToGet) => throw new NotImplementedException();

    public ScanResponse Scan(string tableName, Dictionary<string, Condition> scanFilter) => throw new NotImplementedException();

    public ScanResponse Scan(string tableName, List<string> attributesToGet, Dictionary<string, Condition> scanFilter) => throw new NotImplementedException();

    public ScanResponse Scan(ScanRequest request) => throw new NotImplementedException();

    public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public Task<ScanResponse> ScanAsync(string tableName, Dictionary<string, Condition> scanFilter, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();

    public Task<ScanResponse> ScanAsync(string tableName, List<string> attributesToGet, Dictionary<string, Condition> scanFilter,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public Task<ScanResponse> ScanAsync(ScanRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public TagResourceResponse TagResource(TagResourceRequest request) => throw new NotImplementedException();

    public Task<TagResourceResponse> TagResourceAsync(TagResourceRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public TransactGetItemsResponse TransactGetItems(TransactGetItemsRequest request) => throw new NotImplementedException();

    public Task<TransactGetItemsResponse> TransactGetItemsAsync(TransactGetItemsRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public TransactWriteItemsResponse TransactWriteItems(TransactWriteItemsRequest request) => throw new NotImplementedException();

    public UntagResourceResponse UntagResource(UntagResourceRequest request) => throw new NotImplementedException();

    public Task<UntagResourceResponse> UntagResourceAsync(UntagResourceRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public UpdateContinuousBackupsResponse UpdateContinuousBackups(UpdateContinuousBackupsRequest request) => throw new NotImplementedException();

    public Task<UpdateContinuousBackupsResponse> UpdateContinuousBackupsAsync(UpdateContinuousBackupsRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public UpdateContributorInsightsResponse UpdateContributorInsights(UpdateContributorInsightsRequest request) => throw new NotImplementedException();

    public Task<UpdateContributorInsightsResponse> UpdateContributorInsightsAsync(UpdateContributorInsightsRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public UpdateGlobalTableResponse UpdateGlobalTable(UpdateGlobalTableRequest request) => throw new NotImplementedException();

    public Task<UpdateGlobalTableResponse> UpdateGlobalTableAsync(UpdateGlobalTableRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public UpdateGlobalTableSettingsResponse UpdateGlobalTableSettings(UpdateGlobalTableSettingsRequest request) => throw new NotImplementedException();

    public Task<UpdateGlobalTableSettingsResponse> UpdateGlobalTableSettingsAsync(UpdateGlobalTableSettingsRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public UpdateItemResponse UpdateItem(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates) => throw new NotImplementedException();

    public UpdateItemResponse UpdateItem(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, ReturnValue returnValues) => throw new NotImplementedException();

    public UpdateItemResponse UpdateItem(UpdateItemRequest request) => throw new NotImplementedException();

    public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public Task<UpdateItemResponse> UpdateItemAsync(string tableName, Dictionary<string, AttributeValue> key, Dictionary<string, AttributeValueUpdate> attributeUpdates, ReturnValue returnValues,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public UpdateTableResponse UpdateTable(string tableName, ProvisionedThroughput provisionedThroughput) => throw new NotImplementedException();

    public UpdateTableResponse UpdateTable(UpdateTableRequest request) => throw new NotImplementedException();

    public Task<UpdateTableResponse> UpdateTableAsync(string tableName, ProvisionedThroughput provisionedThroughput,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public Task<UpdateTableResponse> UpdateTableAsync(UpdateTableRequest request, CancellationToken cancellationToken = new CancellationToken()) => throw new System.NotImplementedException();
    public UpdateTableReplicaAutoScalingResponse UpdateTableReplicaAutoScaling(UpdateTableReplicaAutoScalingRequest request) => throw new NotImplementedException();

    public Task<UpdateTableReplicaAutoScalingResponse> UpdateTableReplicaAutoScalingAsync(UpdateTableReplicaAutoScalingRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();

    public UpdateTimeToLiveResponse UpdateTimeToLive(UpdateTimeToLiveRequest request) => throw new NotImplementedException();

    public Task<UpdateTimeToLiveResponse> UpdateTimeToLiveAsync(UpdateTimeToLiveRequest request,
        CancellationToken cancellationToken = new CancellationToken()) =>
        throw new System.NotImplementedException();
    public DeleteResourcePolicyResponse DeleteResourcePolicy(DeleteResourcePolicyRequest request) => throw new NotImplementedException();
    public Task<DeleteResourcePolicyResponse> DeleteResourcePolicyAsync(DeleteResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public GetResourcePolicyResponse GetResourcePolicy(GetResourcePolicyRequest request) => throw new NotImplementedException();
    public Task<GetResourcePolicyResponse> GetResourcePolicyAsync(GetResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public PutResourcePolicyResponse PutResourcePolicy(PutResourcePolicyRequest request) => throw new NotImplementedException();
    public Task<PutResourcePolicyResponse> PutResourcePolicyAsync(PutResourcePolicyRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public UpdateKinesisStreamingDestinationResponse UpdateKinesisStreamingDestination(UpdateKinesisStreamingDestinationRequest request) => throw new NotImplementedException();
    public Task<UpdateKinesisStreamingDestinationResponse> UpdateKinesisStreamingDestinationAsync(UpdateKinesisStreamingDestinationRequest request, CancellationToken cancellationToken = default) => throw new NotImplementedException();
    public Amazon.Runtime.Endpoints.Endpoint DetermineServiceOperationEndpoint(AmazonWebServiceRequest request) => throw new NotImplementedException();

    public IDynamoDBv2PaginatorFactory Paginators { get; }
    #endregion
}