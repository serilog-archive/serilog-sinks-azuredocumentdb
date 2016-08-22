using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.AzureDocumentDb;

namespace Serilog.Core
{
    public abstract class ConcurrentLogEventSink : ILogEventSink, IDisposable
    {
        readonly CancellationTokenSource _cancelToken = new CancellationTokenSource();
        readonly BlockingCollection<LogEvent> _logEventsQueue;
        readonly Thread _workerThread;

        List<Task> _workerTasks = new List<Task>();

        protected ConcurrentLogEventSink()
        {
            _logEventsQueue = new BlockingCollection<LogEvent>(1000);
            _workerThread = new Thread(Pump) { IsBackground = true, Priority = ThreadPriority.AboveNormal };
            _workerThread.Start();
        }

        protected abstract void WriteLogEvent(LogEvent logEvent);

        void Pump()
        {
            try
            {
                var numProcessors = Environment.ProcessorCount;
                while (true)
                {
                    var next = _logEventsQueue.Take(_cancelToken.Token);
                    var workerTask = Task.Factory.StartNew((t) =>
                    {
                        WriteLogEvent(t as LogEvent);
                    }, next);

                    _workerTasks.Add(workerTask);
                    if (_workerTasks.Count >= numProcessors)
                    {
                        Task.WaitAll(_workerTasks.ToArray());
                        _workerTasks.Clear();
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Task.WaitAll(_workerTasks.ToArray());
                _logEventsQueue.AsParallel().ForAll(item => WriteLogEvent(item));
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine("{0} fatal error in worker thread: {1}", typeof(AzureDocumentDBSink), ex);
            }
        }

        #region IDisposable Support
        private bool disposedValue = false;

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    _cancelToken.Cancel();
                    _workerThread.Join();
                }

                disposedValue = true;
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion

        #region ILogEventSink Support
        public void Emit(LogEvent logEvent)
        {
            _logEventsQueue.Add(logEvent);
        }

        #endregion

    }
}