using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudBigTableBenchmarks.Common
{

    /// <summary>
    /// The makeup of the data:
    /// PartitionKey 
    ///     0-9999: each 100 RowKeys with 1KB binary. RK value = 100*PK+i where i is 
    ///     10,000-19,999: items have (PK-10,000)/10 rows, each with 25 byte binary. Row key values 0 to n-1 where n is number items.
    ///     20,000-29,999: items have (PK-10,000)/10 rows, each with 1KB binary. Row key values 0 to n-1 where n is number items, formatted to 000
    ///     30,000-39,999: items have (PK-20,000)/10 rows, each with 10KB binary. Row key values 0 to n-1 where n is number items, formatted to 000
    /// 
    /// </summary>
    public static class DataMakeup
    {
        public static string[] GetRowKeys(int pk)
        {
            if (pk < 10*1000)
            {
                return Enumerable.Range(100*pk, 100).Select(x => x.ToString()).ToArray();
            }
            else
            {
                return Enumerable.Range(0, (pk%10000) / 10).Select(x => x.ToString("{0:000}")).ToArray();
            }
        }

        public static int GetBufferSize(int pk)
        {
            if (pk < 10*1000)
                return 1024;
            if (pk < 20*1000)
                return 25;
            if (pk < 30 * 1000)
                return 1024;
            return 10*1024;
        }
    }
}
