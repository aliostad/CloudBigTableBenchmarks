using System;
using Amazon.DynamoDBv2.DataModel;

namespace CloudBigTableBenchmarks.Common.Aws
{
    public class Payload : IPayload
    {
        public string TheString { get; set; }
        public DateTime TheDate { get; set; }
        public double TheFloat { get; set; }
        public long TheInt { get; set; }
        public byte[] TheBinary { get; set; }

        [DynamoDBHashKey]
        public string PartitionKey { get; set; }

        [DynamoDBRangeKey]
        public string RowKey { get; set; }
        public string ETag { get; set; }

        [DynamoDBVersion]
        public int? Version { get; set; }

        public static Payload New(string pk, string rk, int bufferSize = 1024)
        {
            var payload = new Payload()
            {
                PartitionKey = pk,
                RowKey = rk
            };

            payload.Randomise(bufferSize);
            return payload;
        }
    }
}
