using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace CloudBigTableBenchmarks.Common.Azure
{
    public class PayloadEntity : TableEntity, IPayload
    {
        private static Random _random = new Random();

        public string TheString { get; set; }

        public DateTime TheDate { get; set; }

        public double TheFloat { get; set; }

        public long TheInt { get; set; }

        public byte[] TheBinary { get; set; }

        public static PayloadEntity New(int bufferSize = 4096)
        {
            var entity = new PayloadEntity();
            entity.Randomise(bufferSize);
            return entity;
        }
    }
}
