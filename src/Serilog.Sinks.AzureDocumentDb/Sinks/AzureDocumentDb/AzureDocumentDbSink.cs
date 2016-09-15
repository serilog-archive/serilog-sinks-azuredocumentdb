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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;

namespace Serilog.Sinks.AzureDocumentDb
{
    public class AzureDocumentDBSink : ILogEventSink, IDisposable
    {
        private readonly IFormatProvider _formatProvider;

        private Uri _endpointUri;
        private string _authorizationKey;


        private string _bulkStoredProcedureLink;
        private const string BulkStoredProcedureId = "BulkImport";

        private Database _database;
        private DocumentCollection _collection;
        private readonly DocumentClient _client;
        private readonly bool _storeTimestampInUtc;

        public AzureDocumentDBSink(Uri endpointUri,
            string authorizationKey,
            string databaseName,
            string collectionName,
            IFormatProvider formatProvider,
            bool storeTimestampInUtc)
        {
            _formatProvider = formatProvider;
            _endpointUri = endpointUri;
            _authorizationKey = authorizationKey;

            _client = new DocumentClient(endpointUri,
                authorizationKey,
                new ConnectionPolicy
                {
                    ConnectionMode = ConnectionMode.Gateway,
                    ConnectionProtocol = Protocol.Https,
                    MaxConnectionLimit = Environment.ProcessorCount * 50 + 200
                });

            _storeTimestampInUtc = storeTimestampInUtc;

            CreateDatabaseIfNotExistsAsync(databaseName).Wait();
            CreateCollectionIfNotExistsAsync(collectionName).Wait();

            InitializeParallelSink();
        }

        private async Task CreateDatabaseIfNotExistsAsync(string databaseName)
        {
            await _client.OpenAsync();
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
            _collectionLink = _collection.SelfLink;
            await CreateBulkImportStoredProcedure(_client);
        }

        private async Task CreateBulkImportStoredProcedure(IDocumentClient client, bool dropExistingProc = false)
        {
            var currentAssembly = Assembly.GetExecutingAssembly();
            const string resourceName = "Serilog.Sinks.AzureDocumentDB.bulkImport.js";

            using (var resourceStream = currentAssembly.GetManifestResourceStream(resourceName))
            {
                if (resourceStream != null)
                {
                    var reader = new System.IO.StreamReader(resourceStream);
                    var bulkImportSrc = reader.ReadToEnd();
                    try
                    {
                        var sp = new StoredProcedure
                        {
                            Id = BulkStoredProcedureId,
                            Body = bulkImportSrc
                        };

                        var sproc = GetStoredProcedure(_collectionLink, sp.Id);

                        if (sproc != null && dropExistingProc)
                        {
                            await client.DeleteStoredProcedureAsync(sproc.SelfLink);
                            sproc = null;
                        }

                        if (sproc == null)
                        {
                            sproc = await client.CreateStoredProcedureAsync(_collectionLink, sp);
                        }

                        _bulkStoredProcedureLink = sproc.SelfLink;
                    }
                    catch (Exception ex)
                    {
                        SelfLog.WriteLine(ex.Message);
                    }
                }
            }
        }

        private StoredProcedure GetStoredProcedure(string collectionLink, string id)
        {
            return _client.CreateStoredProcedureQuery(collectionLink)
                .Where(s => s.Id == id).AsEnumerable().FirstOrDefault();

        }

        #region Parallel Log Processing Support

        private const int BatchSize = 300;
        private long _operationCount;
        private volatile bool _canStop;
        private string _collectionLink;

        private readonly Mutex _exceptionMut = new Mutex();
        private readonly List<Data.LogEvent> _logEventBatch = new List<Data.LogEvent>();
        private readonly CancellationTokenSource _cancelToken = new CancellationTokenSource();
        private readonly AutoResetEvent _timerResetEvent = new AutoResetEvent(false);
        private readonly ManualResetEventSlim _emitResetEvent = new ManualResetEventSlim(true);

        private BlockingCollection<ICollection<Data.LogEvent>> _logEventsQueue;
        private List<Thread> _workerThreads;
        private Task _timerTask;

        private Data.LogEvent ConvertLogEventToDataEvent(LogEvent logEvent)
        {
            return new Data.LogEvent(
                logEvent,
                logEvent.RenderMessage(_formatProvider),
                _storeTimestampInUtc);
        }

        private void WriteLogEventBulk(ICollection<Data.LogEvent> logEvents, string bulkStoredProcedureLink)
        {
            if (logEvents == null || logEvents.Count == 0)
            {
                return;
            }

            var argsJson = JsonConvert.SerializeObject(logEvents);
            var args = new[] { JsonConvert.DeserializeObject<dynamic>(argsJson) };

            try
            {
                _client.ExecuteStoredProcedureAsync<int>(bulkStoredProcedureLink, args).Wait(); 
            }
            catch (AggregateException e)
            {
                var exception = e.InnerException as DocumentClientException;
                if (exception != null)
                {
                    var ei = (DocumentClientException)e.InnerException;
                    try
                    {
                        _exceptionMut.WaitOne();

                        if (ei.StatusCode != null)
                            switch ((int)ei.StatusCode)
                            {
                                case 429:
                                    SelfLog.WriteLine("Waiting for {0} ms.", ei.RetryAfter.Milliseconds);
                                    Task.Delay(ei.RetryAfter);
                                    break;
                                default:
                                    if (bulkStoredProcedureLink == _bulkStoredProcedureLink)
                                    {
                                        CreateBulkImportStoredProcedure(_client, true).Wait();
                                    }
                                    break;
                            }
                        _client.ExecuteStoredProcedureAsync<int>(_bulkStoredProcedureLink, args).Wait();
                    }
                    finally
                    {
                        _exceptionMut.ReleaseMutex();
                    }
                }
            }

            Interlocked.Increment(ref _operationCount);
            SelfLog.WriteLine("OP# {0}, Thread# {1}, Messages {2}", _operationCount, Thread.CurrentThread.ManagedThreadId, logEvents.Count);
        }

        private void InitializeParallelSink()
        {
            _logEventsQueue = new BlockingCollection<ICollection<Data.LogEvent>>();
            _workerThreads = new List<Thread>();

            for (var i = 0; i < Environment.ProcessorCount; i++)
            {
                var thread = new Thread(Pump) { IsBackground = true, Priority = ThreadPriority.AboveNormal };
                thread.Start();
                _workerThreads.Add(thread);
            }

            _timerTask = Task.Factory.StartNew(() =>
            {
                while (!_canStop)
                {
                    _timerResetEvent.WaitOne(TimeSpan.FromSeconds(30));
                    _emitResetEvent.Reset();
                    try
                    {
                        Task.Delay(250);
                        if (_logEventBatch.Count <= 0)
                        {
                            continue;
                        }

                        WriteLogEventBulk(_logEventBatch.ToArray(), _bulkStoredProcedureLink);
                        _logEventBatch.Clear();
                    }
                    finally
                    {
                        _emitResetEvent.Set();
                    }
                }
            });
        }

        private void Pump()
        {
            try
            {
                while (true)
                {
                    var logEvents = _logEventsQueue.Take(_cancelToken.Token);
                    WriteLogEventBulk(logEvents, _bulkStoredProcedureLink);
                }
            }
            catch (OperationCanceledException)
            {
                _canStop = true;
                _timerResetEvent.Set();
                _emitResetEvent.Set();

                _timerTask.Wait();

                ICollection<Data.LogEvent> logEvents;

                while (_logEventsQueue.TryTake(out logEvents))
                {
                    WriteLogEventBulk(logEvents, _bulkStoredProcedureLink);
                }
            }
            catch (Exception ex)
            {
                SelfLog.WriteLine("{0} fatal error in worker thread: {1}", typeof(AzureDocumentDBSink), ex);
            }
        }

        #endregion

        #region IDisposable Support
        private bool _disposedValue;

        protected virtual void Dispose(bool disposing)
        {
            if (_disposedValue)
            {
                return;
            }

            if (disposing)
            {
                _cancelToken.Cancel();
                foreach (var thread in _workerThreads)
                {
                    thread.Join();
                }
            }

            _disposedValue = true;
        }

        public void Dispose()
        {
            Dispose(true);
        }

        #endregion

        #region ILogEventSink Support
        public void Emit(LogEvent logEvent)
        {
            if (_canStop)
            {
                return;
            }

            _emitResetEvent.Wait();
            _logEventBatch.Add(ConvertLogEventToDataEvent(logEvent));
            if (_logEventBatch.Count < BatchSize)
            {
                return;
            }
            _logEventsQueue.Add(_logEventBatch.ToArray());
            _logEventBatch.Clear();
        }

        #endregion
    }
}
