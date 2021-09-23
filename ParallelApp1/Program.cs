// This app (.NET5) processes enqueued messages in parallel with alloeed limited parallel tasks.
// Tasks limit is INIT_GRANTED_NUM.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelApp1
{
    class Program
    {
        const int INIT_GRANTED_NUM = 2;

        static void Main(string[] args)
        {
            Processor processor = new(msg =>
            {
                //...
                Thread.Sleep(1000);
                Console.WriteLine($"    MessageId = {msg.Id}, CurrentThread: {Thread.CurrentThread.ManagedThreadId}");
            },
            INIT_GRANTED_NUM);

            processor.EnqueueMessages(CreateMessages(0, 10).ToArray());
            processor.EnqueueMessages(CreateMessages(10, 20).ToArray());
            processor.EnqueueMessages(new Message { Id = 21 });

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();
        }

        static IEnumerable<Message> CreateMessages(int from, int to) 
        {
            for (var i = from; i < to; i++)
                yield return new() { Id = i + 1 };
        }
    }

    class Message
    {
        public int Id { init; get; }
    }

    class Processor 
    {
        private ConcurrentQueue<Message> _cq = new();
        private Action<Message> _processMessage;
        private int _grantedParallelNum;
        private object _locker = new object();

        public Processor(Action<Message> processMessage, int grantedParallelNum)
        {
            _processMessage = processMessage;
            _grantedParallelNum = grantedParallelNum;
        }

        public void EnqueueMessages(params Message[] messages)
        {
            foreach (var msg in messages)
                _cq.Enqueue(msg);

            Process();
        }

        private void Process() =>
            Task.Run(async () =>
            {
                if (!Monitor.TryEnter(_locker))
                    return;

                using SemaphoreSlim sm = new(_grantedParallelNum);
                ConcurrentBag<Task> tasks = new();
                while (_cq.Any())
                {
                    await sm.WaitAsync();
                    tasks.Add(Task.Run(() =>
                    {
                        Console.WriteLine($"Number of spare threads: {sm.CurrentCount}");

                        if (_processMessage != null && _cq.TryDequeue(out Message msg))
                            _processMessage(msg);

                        sm.Release(1);
                    }));
                }

                Task.WaitAll(tasks.ToArray());

                Monitor.Exit(_locker);
            });
    }
}
