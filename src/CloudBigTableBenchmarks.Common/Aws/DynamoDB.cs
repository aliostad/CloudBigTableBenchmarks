using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DataModel;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

namespace CloudBigTableBenchmarks.Common.Aws
{
    public class DynamoDb<T> : IBigTable
        where T : IPayload, new()
    {
        private DynamoDBContext _dynamoDbContext;
        private AmazonDynamoDBClient _client;
        private string _tableName;

        public DynamoDb(string tableName = "Payload")
        {
            _tableName = tableName;
            _dynamoDbContext = new DynamoDBContext(RegionEndpoint.EUWest1, new DynamoDBContextConfig()
            {
                SkipVersionCheck  = true
            });

            _client = new AmazonDynamoDBClient(new AmazonDynamoDBConfig()
            {
                RegionEndpoint = RegionEndpoint.EUWest1
            });
        }

        private void CreateTable(string tableName)
        {
            var request = new CreateTableRequest
            {
                TableName = tableName,
                ProvisionedThroughput = new ProvisionedThroughput(5,5),
                AttributeDefinitions = new List<AttributeDefinition>
                {
                    new AttributeDefinition("PartitionKey", ScalarAttributeType.S),
                    new AttributeDefinition("RowKey", ScalarAttributeType.S),
                },
                KeySchema = new List<KeySchemaElement>
                {
                    new KeySchemaElement("PartitionKey", KeyType.HASH),
                    new KeySchemaElement("RowKey", KeyType.RANGE)
                }
            };

            _client.CreateTable(request); 
        }


        public void BatchInsert(IEnumerable<IPayload> payloads)
        {
            var batchWrite = _dynamoDbContext.CreateBatchWrite<T>();
            batchWrite.AddPutItems(payloads.Cast<T>().ToArray());
           _dynamoDbContext.ExecuteBatchWrite(batchWrite);
        }

        public IPayload Get(string pk, string rk)
        {
            return _dynamoDbContext.Load<T>(pk, rk);
        }

        public IEnumerable<IPayload> GetRange(string pk, string startRange, string endRange)
        {
            QueryFilter filter = new QueryFilter("PartitionKey", QueryOperator.Equal, pk);
            filter.AddCondition("RowKey", QueryOperator.Between, startRange, endRange);
            return _dynamoDbContext.Query<T>(pk, QueryOperator.Between, new [] {startRange, endRange}).Cast<IPayload>();
        }

        public IEnumerable<IPayload> GetAll(string pk)
        {
            return _dynamoDbContext.Query<T>(pk).Cast<IPayload>();
        }
    }
}
