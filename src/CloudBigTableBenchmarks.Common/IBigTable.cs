using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudBigTableBenchmarks.Common
{
    public interface IBigTable
    {
        void BatchInsert(IEnumerable<IPayload> payloads);

        IPayload Get(string pk, string rk);

        IEnumerable<IPayload> GetRange(string pk, string startRange, string endRange);

        IEnumerable<IPayload> GetAll(string pk);
    }
}
