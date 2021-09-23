// This app (.NET5) processes enqueued messages in parallel with alloeed limited parallel tasks.
// Tasks limit is INIT_GRANTED_NUM.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace ParallelApp1
{
    class Program
    {
        const int INIT_GRANTED_NUM = 2;

        static void Main(string[] args)
        {
            Processor<Message> processor = new(
                msg =>
                {
                    //...
                    Thread.Sleep(100);
                    Console.WriteLine($"    MessageId = {msg.Id}, CurrentThread: {Thread.CurrentThread.ManagedThreadId}");
                },
                n => Console.WriteLine($"{n} messages have been processed"),
                INIT_GRANTED_NUM);

            processor.EnqueueAndProcessMessages(CreateMessageArray(0, 10));
            processor.EnqueueAndProcessMessages(CreateMessageArray(10, 20));

            Thread.Sleep(3000);

            processor.EnqueueAndProcessMessages(CreateMessageArray(20, 30));
            processor.EnqueueAndProcessMessages(new Message { Id = 31 });

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();

            processor.Dispose();
        }

        static Message[] CreateMessageArray(int from, int to) =>
            CreateMessages(from, to)?.ToArray();

        static IEnumerable<Message> CreateMessages(int from, int to) 
        {
            for (var i = from; i < to; i++)
                yield return new() { Id = i + 1 };
        }
    }
}
