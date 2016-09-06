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
        private string BulkStoredProcedureId = "BulkImport";
        private string BulkStoredProcedureLink;
        private DocumentClient _client;
        private Database _database;
        private DocumentCollection _collection;
        private bool _storeTimestampInUtc;

        public AzureDocumentDBSink(Uri endpointUri, 
            string authorizationKey, 
            string databaseName, 
            string collectionName, 
            IFormatProvider formatProvider, 
            bool storeTimestampInUtc)
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

            await CreateBulkImportStoredProcedure(_client, _collection, false);
        }

        private async Task CreateBulkImportStoredProcedure( DocumentClient client, 
            DocumentCollection collection, 
            bool dropExistingProc=false)
        {
            var currentAssembly = Assembly.GetExecutingAssembly();
            var resourceName = "Serilog.Sinks.AzureDocumentDB.bulkImport.js";

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

                        var sproc = GetStoredProcedure(collection.SelfLink, sp.Id);
                        if(sproc != null && dropExistingProc == true)
                        {
                            await client.DeleteStoredProcedureAsync(sproc.SelfLink);
                            sproc = null;
                        }

                        if (sproc == null)
                        {
                            sproc = await client.CreateStoredProcedureAsync(collection.SelfLink, sp);
                        }

                        BulkStoredProcedureLink = sproc.SelfLink;
                    }
                    catch (Exception ex)
                    {
                        SelfLog.WriteLine(ex.Message);
                    }
                }
            }
        }

        private StoredProcedure GetStoredProcedure(string collectionLink, string Id)
        {
            return _client.CreateStoredProcedureQuery(collectionLink)
                .Where(s => s.Id == Id).AsEnumerable().FirstOrDefault();

        }

        #region Parallel Log Processing Support
        private CancellationTokenSource _cancelToken = new CancellationTokenSource();

        private BlockingCollection<LogEvent> _logEventsQueue;
        private List<LogEvent> _logEventBatch;

        private Thread _workerThread;
        private Thread _bulkThread;

        private AutoResetEvent _bulkResetEvent = new AutoResetEvent(false);
        private ManualResetEvent _pumpResetEvent = new ManualResetEvent(true);

        private volatile bool _canStop = false;
        private const int BATCH_SIZE = 100;

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

        private async Task<double> WriteLogEventBulk(IEnumerable<LogEvent> logEvents)
        {
            var logMsgs = logEvents.ToArray();
            if (logMsgs.Length > 0)
            {
                var argsJson = JsonConvert.SerializeObject(logMsgs, Newtonsoft.Json.Formatting.Indented);
                var args = new dynamic[] { JsonConvert.DeserializeObject<dynamic>(argsJson) };
                var numWrites = 0.0;

                try
                {
                    var resp = await _client.ExecuteStoredProcedureAsync<int>(BulkStoredProcedureLink, args);
                    numWrites = resp.RequestCharge;
                }
                catch (Microsoft.Azure.Documents.DocumentClientException)
                {
                    await CreateBulkImportStoredProcedure(_client, _collection, true);
                    var resp = await _client.ExecuteStoredProcedureAsync<int>(BulkStoredProcedureLink, args);
                    numWrites = resp.RequestCharge;
                }
                return numWrites;
            }
            return 0.0;
        }

        void InitializeParallelSink()
        {
            _logEventBatch = new List<LogEvent>();
            _logEventsQueue = new BlockingCollection<LogEvent>(1000);

            _bulkThread = new Thread(BulkPump) { IsBackground = true };
            _bulkThread.Start();

            _workerThread = new Thread(Pump) { IsBackground = true, Priority = ThreadPriority.AboveNormal };
            _workerThread.Start();
        }

        void BulkPump()
        {
            try
            {
                while (!_canStop)
                {
                    _bulkResetEvent.WaitOne(5000);
                    _pumpResetEvent.Reset();

                    var logEventsCopy = _logEventBatch.ToArray();
                    _logEventBatch.Clear();
                    _pumpResetEvent.Set();

                    WriteLogEventBulk(logEventsCopy).Wait();
                }
            }
            catch(Exception ex)
            {
                SelfLog.WriteLine(ex.Message);
                throw ex;
            }
        }

        void Pump()
        {
            try
            {
                while (true)
                {
                    var next = _logEventsQueue.Take(_cancelToken.Token);

                    _pumpResetEvent.WaitOne();
                    _logEventBatch.Add(next);

                    if(_logEventBatch.Count >= BATCH_SIZE)
                    {
                        _pumpResetEvent.Reset();
                        WaitHandle.SignalAndWait(_bulkResetEvent, _pumpResetEvent);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                _canStop = true;
                _bulkResetEvent.Set();

                _bulkThread.Join();

                WriteLogEventBulk(_logEventBatch).Wait();

                WriteLogEventBulk(_logEventsQueue).Wait();
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
            if (!_canStop)
            {
                _logEventsQueue.Add(logEvent);
            }
        }

        #endregion
    }
}
