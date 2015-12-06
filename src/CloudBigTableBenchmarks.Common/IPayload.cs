using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudBigTableBenchmarks.Common
{
    public interface IPayload
    {
        string TheString { get; set; }
        DateTime TheDate { get; set; }
        double TheFloat { get; set; }
        long TheInt { get; set; }
        byte[] TheBinary { get; set; }
        string PartitionKey { get; set; }
        string RowKey { get; set; }
        string ETag { get; set; }
    }
}
