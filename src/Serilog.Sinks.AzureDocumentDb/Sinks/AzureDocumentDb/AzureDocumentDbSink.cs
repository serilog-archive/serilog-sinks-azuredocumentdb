// Copyright 2016 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;

namespace Serilog.Sinks.AzureDocumentDb
{
    public class AzureDocumentDBSink : ILogEventSink, IDisposable
    {
        readonly IFormatProvider _formatProvider;
        DocumentClient _client;
        Database _database;
        DocumentCollection _collection;
        bool _storeTimestampInUtc;


        public AzureDocumentDBSink(Uri endpointUri, string authorizationKey, string databaseName, string collectionName, IFormatProvider formatProvider, bool storeTimestampInUtc)
        {
            _formatProvider = formatProvider;
            _client = new DocumentClient(endpointUri, authorizationKey);
            _storeTimestampInUtc = storeTimestampInUtc;

            CreateDatabaseIfNotExistsAsync(databaseName).Wait();
            CreateCollectionIfNotExistsAsync(collectionName).Wait();

            InitializeParallelSink();
        }

        private async Task CreateDatabaseIfNotExistsAsync(string databaseName)
        {
            _database = _client.CreateDatabaseQuery().Where(x => x.Id == databaseName).AsEnumerable().FirstOrDefault();
            if (_database == null)
            {
                _database = await _client.CreateDatabaseAsync(new Database { Id = databaseName })
                    .ConfigureAwait(false);
            }
        }

        private async Task CreateCollectionIfNotExistsAsync(string collectionName)
        {
            _collection = _client.CreateDocumentCollectionQuery(_database.SelfLink).Where(x => x.Id == collectionName).AsEnumerable().FirstOrDefault();
            if (_collection == null)
            {
                var documentCollection = new DocumentCollection() { Id = collectionName };
                _collection = await _client.CreateDocumentCollectionAsync(_database.SelfLink, documentCollection)
                    .ConfigureAwait(false);
            }
        }

        #region Parallel Log Processing Support
        private CancellationTokenSource _cancelToken = new CancellationTokenSource();
        private BlockingCollection<LogEvent> _logEventsQueue;
        private Thread _workerThread;
        private List<Task> _workerTasks = new List<Task>();

        private void WriteLogEvent(LogEvent logEvent)
        {
            _client.CreateDocumentAsync(
                _collection.SelfLink,
                new Data.LogEvent(
                    logEvent,
                    logEvent.RenderMessage(_formatProvider),
                    _storeTimestampInUtc),
                new RequestOptions { }, false).Wait();
        }

        void InitializeParallelSink()
        {
            _logEventsQueue = new BlockingCollection<LogEvent>(1000);
            _workerThread = new Thread(Pump) { IsBackground = true, Priority = ThreadPriority.AboveNormal };
            _workerThread.Start();
        }

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

        #endregion

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
