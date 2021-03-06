﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using RandomGen;

namespace CloudBigTableBenchmarks.Common
{
    public static class IPayloadExtensions
    {
        private static Func<long> _rl = Gen.Random.Numbers.Longs(); 
        private static Func<double> _rf = Gen.Random.Numbers.Doubles(1, 1000 * 1000);
        private static Func<DateTime> _rd = Gen.Random.Time.Dates(DateTime.Now.AddYears(-20), DateTime.Now); 
        private static Func<string> _rs = Gen.Random.Text.Long(); 
        private static Random _random = new Random();

        public static void Randomise(this IPayload payload, int bufferSize = 1024)
        {
            payload.TheDate = _rd();
            payload.TheFloat = Math.Round(_rf(), 2);
            payload.TheInt = _rl();
            payload.TheString = _rs();
            payload.TheBinary = new byte[bufferSize];
            _random.NextBytes(payload.TheBinary);
        }

        public static string ToShortString(this IPayload payload)
        {
            return String.Format("{0}/{1}", payload.PartitionKey, payload.RowKey);
        }
    }
}
