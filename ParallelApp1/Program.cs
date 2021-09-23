// This app (.NET5) processes enqueued messages in parallel with alloeed limited parallel tasks.
// Tasks limit is INIT_GRANTED_NUM.
// All messages to be processed are enqueued in advance for simplicity.

using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelApp1
{
    class Program
    {
        const int INIT_GRANTED_NUM = 2;

        static async Task Main(string[] args)
        {
            ConcurrentQueue<Message> cq = new();

            InitEnqueueMessages(cq);

            await cq.ProcessAsync(msg =>
            {
                //...
                Thread.Sleep(2000);
                Console.WriteLine($"    MessageId = {msg.Id}, CurrentThread: {Thread.CurrentThread.ManagedThreadId}");
            },
            INIT_GRANTED_NUM);

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();
        }

        static void InitEnqueueMessages(ConcurrentQueue<Message> cq)
        {
            const int n = 11;
            for (var i = 0; i < n; i++)
                cq.Enqueue(new() { Id = i + 1 });
        }
    }

    class Message
    {
        public int Id { init; get; }
    }

    static class ProcessEx 
    {
        public static async Task ProcessAsync(this ConcurrentQueue<Message> cq, Action<Message> processMessage, int grantedParallel)
        {
            using SemaphoreSlim sm = new(grantedParallel);
            ConcurrentBag<Task> tasks = new();
            while (cq.Any())
            {
                await sm.WaitAsync();
                tasks.Add(Task.Run(() =>
                {
                    Console.WriteLine($"Number of spare threads: {sm.CurrentCount}");

                    if (cq.TryDequeue(out Message msg))
                        processMessage(msg);

                    sm.Release(1);
                }));
            }

            Task.WaitAll(tasks.ToArray());
        }
    }
}
