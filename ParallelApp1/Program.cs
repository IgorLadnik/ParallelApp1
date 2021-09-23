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
            Processor<Message> processor = new(msg =>
            {
                //...
                Thread.Sleep(100);
                Console.WriteLine($"    MessageId = {msg.Id}, CurrentThread: {Thread.CurrentThread.ManagedThreadId}");
            },
            INIT_GRANTED_NUM);

            processor.EnqueueAndProcessMessages(CreateMessages(0, 10).ToArray());
            processor.EnqueueAndProcessMessages(CreateMessages(10, 20).ToArray());

            Thread.Sleep(3000);

            processor.EnqueueAndProcessMessages(CreateMessages(20, 30).ToArray());
            processor.EnqueueAndProcessMessages(new Message { Id = 31 });

            Console.WriteLine("Press any key to quit...");
            Console.ReadKey();

            processor.Dispose();
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

    class Processor<T> : IDisposable
    {
        private ConcurrentQueue<T> _cq = new();
        private Action<T> _processMessage;
        private int _grantedParallelNum;
        private AutoResetEvent _ev = new AutoResetEvent(true);

        public Processor(Action<T> processMessage, int grantedParallelNum)
        {
            _processMessage = processMessage;
            _grantedParallelNum = grantedParallelNum;
        }

        public void EnqueueAndProcessMessages(params T[] messages)
        {
            foreach (var msg in messages)
                _cq.Enqueue(msg);

            Process();
        }

        private void Process()
        {
            if (!_ev.WaitOne(0))
                return;

            Task.Run(async () =>
            {
                Console.WriteLine("-> Processing ===================================");

                using SemaphoreSlim sm = new(_grantedParallelNum);
                ConcurrentBag<Task> tasks = new();
                while (_cq.Any())
                {
                    await sm.WaitAsync();
                    tasks.Add(Task.Run(() =>
                    {
                        Console.WriteLine($"Number of spare threads: {sm.CurrentCount}");

                        if (_processMessage != null && _cq.TryDequeue(out T msg))
                            _processMessage(msg);

                        sm.Release(1);
                    }));
                }

                Task.WaitAll(tasks.ToArray());

                _ev.Set();
            });
        }

        public void Dispose() =>
            _ev.Dispose();
    }
}
