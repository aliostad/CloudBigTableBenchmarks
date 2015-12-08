using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudBigTableBenchmarks.Common.Azure
{
    public class TableStorage<T> : IBigTable
        where T : TableEntity, IPayload, new()
    {
        private CloudTable _table;

        public TableStorage(string connectionString, string tableName="Payload")
        {
            CloudStorageAccount account = null;
            try
            {
                account = CloudStorageAccount.Parse(connectionString);

            }
            catch (Exception e)
            {
                throw new ApplicationException("Connection string: " + connectionString, e);
            }

            var client = account.CreateCloudTableClient();
            _table = client.GetTableReference(tableName);
        }

        public void BatchInsert(IEnumerable<IPayload> payloads)
        {
            var batchOperation = new TableBatchOperation();
            foreach (var payload in payloads)
            {
                batchOperation.Add(TableOperation.InsertOrReplace((T) payload));    
            }

            _table.ExecuteBatch(batchOperation);
        }

        public IPayload Get(string pk, string rk)
        {
            return  (T) _table.Execute(TableOperation.Retrieve<T>(pk, rk)).Result;
        }

        public IEnumerable<IPayload> GetRange(string pk, string startRange, string endRange)
        {
            var filters = TableQuery.CombineFilters(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, startRange));

            filters = TableQuery.CombineFilters(filters,
                TableOperators.And,
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, endRange));

            return _table.ExecuteQuery(new TableQuery<T>().Where(filters));
        }

        public IEnumerable<IPayload> GetAll(string pk)
        {
            return _table.ExecuteQuery(new TableQuery<T>().Where(
                TableQuery.GenerateFilterCondition("PartitionKey", "eq", pk)));
        }
    }
}
