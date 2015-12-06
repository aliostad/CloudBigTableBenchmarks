using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.DataModel;
using CloudBigTableBenchmarks.Common;
using CloudBigTableBenchmarks.Common.Aws;

namespace CloudBigTableBenchmarks.DataGen
{
    class Program
    {
        static void Main(string[] args)
        {

            if (args.Length == 0)
            {
                ConsoleWriteLine(ConsoleColor.White, "Usage: CloudBigTableBenchmarks.DataGen.exe <ats|abs|aws> [start = 0] [end = 1000,000]" );
                ConsoleWriteLine(ConsoleColor.Cyan, "Creates the data for the range specified to Azure Table Storage, Azure Blob Storage or AWS DynamoDB");
                return;
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
