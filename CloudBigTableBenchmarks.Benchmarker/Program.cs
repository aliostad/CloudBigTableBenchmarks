using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CloudBigTableBenchmarks.Common;
using CloudBigTableBenchmarks.Common.Aws;
using CloudBigTableBenchmarks.Common.Azure;
using PerfIt;

namespace CloudBigTableBenchmarks.Benchmarker
{
    class Program
    {
        static void Main(string[] args)
        {

            try
            {
                if (args.Length < 4 || args.Length > 6)
                {
                    ConsoleWriteLine(ConsoleColor.White, "Usage: CloudBigTableBenchmarks.DataGen.exe <ats|abs|aws> <recordToRetrieve=1|range|all> <minPk> <maxPk> [pauseAfterEachReqInMilli=50] [rangeCount=10] ");
                    ConsoleWriteLine(ConsoleColor.Cyan, "Runs the benchmark and creates ETW events");
                    return;
                }

                var impl = args[0];
                var recordToRetrieve = args[1];
                var start = Convert.ToInt32(args[2]);
                var end = Convert.ToInt32(args[3]);
                var rangeCount = 10;
                if (args.Length > 5)
                    rangeCount = Convert.ToInt32(args[5]);
                var pauseAfterEachReqInMilli = 50;
                if(args.Length > 4)
                    pauseAfterEachReqInMilli = Convert.ToInt32(args[4]);
                
                var random = new Random();

                var impls = new Dictionary<string, Lazy<IBigTable>>()
                {
                    {"aws", new Lazy<IBigTable>(() => new DynamoDb<Payload>())},
                    {"ats", new Lazy<IBigTable>(() => new TableStorage<PayloadEntity>(Environment.GetEnvironmentVariable("ats_connection_string")))},
                    {"abs", new Lazy<IBigTable>(() => new BlobStorage<Payload>(Environment.GetEnvironmentVariable("ats_connection_string")))}
                };

                var qveries = new Dictionary<string, Func<int, IBigTable, string>>();

                qveries["1"] = (pk, bigTable) =>
                {
                    var rks = DataMakeup.GetRowKeys(pk);
                    var index = random.Next(0, rks.Length);
                    var payload = bigTable.Get(pk.ToString(), rks[index].ToString());
                    return String.Format("Got for policy [1]: {0}", payload.ToString());
                };

                qveries["all"] = (pk, bigTable) =>
                {
                    var payloads = bigTable.GetAll(pk.ToString());
                    return String.Format("Got for policy [all]: {0} items", payloads.Count());
                };

                qveries["range"] = (pk, bigTable) =>
                {
                    var rks = DataMakeup.GetRowKeys(pk);
                    IEnumerable<IPayload> payloads = null;
                    if(rks.Length < rangeCount)
                        payloads = bigTable.GetAll(pk.ToString());
                    else
                        payloads = bigTable.GetRange(pk.ToString(), "000", rangeCount.ToString("{0:000}"));
                    return String.Format("Got for policy [range]: {0} items", payloads.Count());
                };

                if (!impls.ContainsKey(impl))
                {
                    ConsoleWriteLine(ConsoleColor.Yellow, "Invalid impl: {0}", impl);
                    return;
                }

                if (!qveries.ContainsKey(recordToRetrieve))
                {
                    ConsoleWriteLine(ConsoleColor.Yellow, "Invalid recordToRetrieve: {0}", recordToRetrieve);
                    return;
                }

                var bigtablestore = impls[impl];
                var qvery = qveries[recordToRetrieve];

                var instrumentor = new SimpleInstrumentor(new InstrumentationInfo()
                {
                    CategoryName = "BigTable.Benchmarks",
                    Description = "Benchmarks on cloud impls of BigTable",
                    InstanceName = "BigTable.Benchmarks"
                }, publishCounters: false, publishEvent: true, raisePublishErrors: true);

                
                foreach (var pk in Enumerable.Range(start, end-start))
                {
                    var localPk = pk;
                    instrumentor.Instrument(() =>
                    {
                        qvery(localPk, bigtablestore.Value);
                    });

                    Thread.Sleep(pauseAfterEachReqInMilli);
                }
            }
            catch (Exception e)
            {
                ConsoleWriteLine(ConsoleColor.Red, "Exception: {0}", e);
            }


        }

        private static void ConsoleWrite(ConsoleColor color, string value, params object[] args)
        {
            var foregroundColor = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.Write(value, args);
            Console.ForegroundColor = foregroundColor;
        }

        private static void ConsoleWriteLine(ConsoleColor color, string value, params object[] args)
        {
            var foregroundColor = Console.ForegroundColor;
            Console.ForegroundColor = color;
            Console.WriteLine(value, args);
            Console.ForegroundColor = foregroundColor;
        }

    }
}
