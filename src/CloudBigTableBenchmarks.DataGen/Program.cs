using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DataModel;
using CloudBigTableBenchmarks.Common;
using CloudBigTableBenchmarks.Common.Aws;
using CloudBigTableBenchmarks.Common.Azure;

namespace CloudBigTableBenchmarks.DataGen
{
    class Program
    {
        static void Main(string[] args)
        {

            try
            {
                if (args.Length < 1 || args.Length > 3)
                {
                    ConsoleWriteLine(ConsoleColor.White, "Usage: CloudBigTableBenchmarks.DataGen.exe <ats|abs|aws> [start = 0] [end = 10,000]");
                    ConsoleWriteLine(ConsoleColor.Cyan, "Creates the data for the range specified to Azure Table Storage, Azure Blob Storage or AWS DynamoDB");
                    return;
                }

                var impls = new Dictionary<string, Lazy<IBigTable>>()
                {
                    {"aws", new Lazy<IBigTable>(() => new DynamoDb<Payload>())},
                    {"ats", new Lazy<IBigTable>(() => new TableStorage<PayloadEntity>(Environment.GetEnvironmentVariable("ats_connection_string")))},
                    {"abs", new Lazy<IBigTable>(() => new BlobStorage<Payload>(Environment.GetEnvironmentVariable("ats_connection_string")))}
                };

                var factories = new Dictionary<string, Func<string, string, int, IPayload>>()
                {
                    {"aws", Payload.New},
                    {"ats", PayloadEntity.New},
                    {"abs", Payload.New},
                };


                var impl = args[0];

                if (!impls.ContainsKey(impl))
                {
                    ConsoleWriteLine(ConsoleColor.Red, "Parameter not supported: {0}", impl);
                    return;
                }

                var factory = factories[impl];

                IBigTable bigtable = impls[impl].Value;
                int start = 0;
                int end = 10 * 1000;

                if (args.Length > 1)
                    start = Convert.ToInt32(args[1]);

                if (args.Length > 2)
                    end = Convert.ToInt32(args[2]);

                var batcher = new Batcher<IPayload>(40);
                int count = 0;
                for (int pk = start; pk < end; pk++)
                {
                    var rowKeys = DataMakeup.GetRowKeys(pk);
                    foreach (var rowKey in rowKeys)
                    {
                        if (pk % 10 == 0)
                        {
                            ConsoleWrite(ConsoleColor.Green, "\rWritten so far => ");
                            ConsoleWrite(ConsoleColor.Magenta, pk.ToString());
                        }

                        var payload = factory(pk.ToString(), rowKey, DataMakeup.GetBufferSize(pk));
                        Trace.WriteLine(payload.ToShortString());

                        var payloads = batcher.Add(payload);
                        if (payloads != null)
                            bigtable.BatchInsert(payloads);

                        count++;
                    }                    
                }

                var pls = batcher.Clear();
                if (pls != null && pls.Length > 0)
                    bigtable.BatchInsert(pls);

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
