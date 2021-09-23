using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace ParallelApp1
{
    public class Processor<T> : IDisposable
    {
        private ConcurrentQueue<T> _cq = new();
        private Action<T> _processMessage;
        private Action<int> _onEmptyQueue;
        private int _grantedParallelNum;
        private AutoResetEvent _ev = new AutoResetEvent(true);
        private int _numOfProcessed = 0;

        public Processor(Action<T> processMessage, Action<int> onEmptyQueue, int grantedParallelNum)
        {
            _processMessage = processMessage;
            _onEmptyQueue = onEmptyQueue;
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
                        {
                            try
                            {
                                _processMessage(msg);
                                Interlocked.Increment(ref _numOfProcessed);
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine($"***** ERROR: {e}");
                            }
                        }
                        sm.Release(1);
                    }));
                }

                Task.WaitAll(tasks.ToArray());

                try
                {
                    _onEmptyQueue?.Invoke(Interlocked.Exchange(ref _numOfProcessed, 0));
                }
                catch (Exception e)
                {
                    Console.WriteLine($"***** ERROR: {e}");
                }

                _ev.Set();
            });
        }

        public void Dispose() =>
            _ev.Dispose();
    }
}
