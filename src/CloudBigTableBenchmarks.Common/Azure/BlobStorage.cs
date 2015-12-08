using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace CloudBigTableBenchmarks.Common.Azure
{
    public class BlobStorage<T> : IBigTable // NOTE: IT IS NOT A BIG TABLE (it is Dynamo impl) - Just for illustration and comparison
        where T : IPayload, new()
    {
        private CloudBlobContainer _container;
        private string _rootPath;

        public BlobStorage(string connectionString, string containerName = "payload", string rootPath = "")
        {
            _rootPath = rootPath;

            CloudStorageAccount account = null;
            try
            {
                account = CloudStorageAccount.Parse(connectionString);
    
            }
            catch (Exception e)
            {
                throw new ApplicationException("Connection string: " + connectionString, e);
            }

            var client = account.CreateCloudBlobClient();
            _container = client.GetContainerReference(containerName);
            _container.CreateIfNotExists();

            if (!string.IsNullOrEmpty(_rootPath))
                _rootPath = _rootPath.Trim('/');
        }

        public string GetBlobName(string pk, string rk)
        {
            return String.Format("{0}/{1}/{2}", _rootPath, pk, rk);
        }

        public string GetBlobFolder(string pk)
        {
            return String.Format("{0}/{1}/", _rootPath, pk);
        }

        public void BatchInsert(IEnumerable<IPayload> payloads)
        {
            foreach (var payload in payloads)
            {
                var blob = _container.GetBlockBlobReference(GetBlobName(payload.PartitionKey, payload.RowKey));
                blob.UploadText(JsonConvert.SerializeObject(payload));
            }
        }

        public IPayload Get(string pk, string rk)
        {
            var blob = _container.GetBlockBlobReference(GetBlobName(pk, rk));
            if (!blob.Exists())
            {
                throw new InvalidOperationException("Blob does not exist: " + blob.Name);
            }

            var bytes = new byte[blob.Properties.Length];
            var text = blob.DownloadText();
            return JsonConvert.DeserializeObject<T>(text);
        }

        public IEnumerable<IPayload> GetRange(string pk, string startRange, string endRange)
        {
            var start = GetBlobName(pk, startRange);
            var end = GetBlobName(pk, endRange);
            foreach (var blob in _container.ListBlobs(GetBlobFolder(pk)))
            {
                var blockBlob = blob as CloudBlockBlob;
                if (blockBlob != null && blockBlob.Name.CompareTo(start)>=0 && blockBlob.Name.CompareTo(end) <= 0)
                {
                    var text = blockBlob.DownloadText();
                    yield return JsonConvert.DeserializeObject<T>(text);
                }
            }
        }

        public IEnumerable<IPayload> GetAll(string pk)
        {
            return GetRange(pk, "0", "zzzzzzzzzzzzzzzzzzz");
        }
    }
}
