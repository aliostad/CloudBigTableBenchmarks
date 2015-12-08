using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CloudBigTableBenchmarks.Common
{
    public class Batcher<T>
    {
        private List<T> _list = new List<T>();
        private int _batchSize;

        public Batcher(int batchSize)
        {
            _batchSize = batchSize;
        }

        public T[] Add(T t)
        {
            _list.Add(t);

            if (_list.Count >= _batchSize)
            {
                return Clear();
            }

            return null;
        }

        public T[] Clear()
        {
            var array = _list.ToArray();
            _list.Clear();
            return array;
        }
    }
}
