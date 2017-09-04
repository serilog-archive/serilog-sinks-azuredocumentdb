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
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Newtonsoft.Json;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.Batch;
using Serilog.Sinks.Extensions;

namespace Serilog.Sinks.AzureDocumentDb
{
    internal class AzureDocumentDBSink : BatchProvider, ILogEventSink
    {
        private const string BulkStoredProcedureId = "BulkImport";
        private readonly DocumentClient _client;
        private readonly Mutex _exceptionMut = new Mutex();
        private readonly IFormatProvider _formatProvider;
        private readonly bool _storeTimestampInUtc;
        private readonly int? _timeToLive;

        private string _authorizationKey;
        private string _bulkStoredProcedureLink;
        private DocumentCollection _collection;
        private Database _database;
        private Uri _endpointUri;

        public AzureDocumentDBSink(
            Uri endpointUri,
            string authorizationKey,
            string databaseName,
            string collectionName,
            IFormatProvider formatProvider,
            bool storeTimestampInUtc,
            Protocol connectionProtocol,
            TimeSpan? timeToLive,
            int logBufferSize = 25_000,
            int batchSize = 100) : base(batchSize, logBufferSize)
        {
            _endpointUri = endpointUri;
            _authorizationKey = authorizationKey;
            _formatProvider = formatProvider;

            if ((timeToLive != null) && (timeToLive.Value != TimeSpan.MaxValue))
                _timeToLive = (int) timeToLive.Value.TotalSeconds;

            _client = new DocumentClient(
                endpointUri,
                authorizationKey,
                new ConnectionPolicy
                {
                    ConnectionMode =
                        connectionProtocol == Protocol.Https ? ConnectionMode.Gateway : ConnectionMode.Direct,
                    ConnectionProtocol = connectionProtocol,
                    MaxConnectionLimit = Environment.ProcessorCount * 50 + 200
                });

            _storeTimestampInUtc = storeTimestampInUtc;

            CreateDatabaseIfNotExistsAsync(databaseName)
                .Wait();
            CreateCollectionIfNotExistsAsync(collectionName)
                .Wait();

            JsonConvert.DefaultSettings = () =>
            {
                var settings = new JsonSerializerSettings() {ReferenceLoopHandling = ReferenceLoopHandling.Ignore};
                return settings;
            };
        }

        private async Task CreateDatabaseIfNotExistsAsync(string databaseName)
        {
            SelfLog.WriteLine($"Opening database {databaseName}");
            await _client.OpenAsync()
                .ConfigureAwait(false);
            _database = _client.CreateDatabaseQuery()
                    .Where(x => x.Id == databaseName)
                    .AsEnumerable()
                    .FirstOrDefault()
                ?? await _client.CreateDatabaseAsync(new Database {Id = databaseName})
                    .ConfigureAwait(false);
        }

        private async Task CreateCollectionIfNotExistsAsync(string collectionName)
        {
            SelfLog.WriteLine($"Creating collection: {collectionName}");
            _collection = _client.CreateDocumentCollectionQuery(_database.SelfLink)
                .Where(x => x.Id == collectionName)
                .AsEnumerable()
                .FirstOrDefault();
            if (_collection == null)
            {
                var documentCollection = new DocumentCollection {Id = collectionName, DefaultTimeToLive = -1};
                _collection = await _client.CreateDocumentCollectionAsync(_database.SelfLink, documentCollection)
                    .ConfigureAwait(false);
            }
            _collectionLink = _collection.SelfLink;
            await CreateBulkImportStoredProcedureAsync(_client)
                .ConfigureAwait(false);
        }

        private async Task CreateBulkImportStoredProcedureAsync(IDocumentClient client, bool dropExistingProc = false)
        {
            var currentAssembly = typeof(AzureDocumentDBSink).GetTypeInfo()
                .Assembly;

            SelfLog.WriteLine("Getting required resource.");
            var resourceName = currentAssembly.GetManifestResourceNames()
                .FirstOrDefault(w => w.EndsWith("bulkImport.js"));

            if (string.IsNullOrEmpty(resourceName))
            {
                SelfLog.WriteLine("Unable to find required resource.");
                return;
            }

            using (var resourceStream = currentAssembly.GetManifestResourceStream(resourceName))
            {
                if (resourceStream != null)
                {
                    var reader = new StreamReader(resourceStream);
                    var bulkImportSrc = await reader.ReadToEndAsync()
                        .ConfigureAwait(false);
                    try
                    {
                        var sp = new StoredProcedure {Id = BulkStoredProcedureId, Body = bulkImportSrc};

                        var sproc = GetStoredProcedure(_collectionLink, sp.Id);

                        if ((sproc != null) && dropExistingProc)
                        {
                            await client.DeleteStoredProcedureAsync(sproc.SelfLink)
                                .ConfigureAwait(false);
                            sproc = null;
                        }

                        if (sproc == null)
                            sproc = await client.CreateStoredProcedureAsync(_collectionLink, sp)
                                .ConfigureAwait(false);

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
                .Where(s => s.Id == id)
                .AsEnumerable()
                .FirstOrDefault();
        }

        #region Parallel Log Processing Support

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            if ((logEventsBatch == null) || (logEventsBatch.Count == 0))
                return true;

            var args = logEventsBatch.Select(x => x.Dictionary(_storeTimestampInUtc, _formatProvider));

            if ((_timeToLive != null) && (_timeToLive > 0))
                args = args.Select(
                    x =>
                    {
                        if (!x.Keys.Contains("ttl"))
                            x.Add("ttl", _timeToLive);
                        return x;
                    });

            try
            {
                SelfLog.WriteLine($"Sending batch of {logEventsBatch.Count} messages to DocumentDB");
                var storedProcedureResponse = await _client
                    .ExecuteStoredProcedureAsync<int>(_bulkStoredProcedureLink, args)
                    .ConfigureAwait(false);
                SelfLog.WriteLine(storedProcedureResponse.StatusCode.ToString());
                return storedProcedureResponse.StatusCode == HttpStatusCode.OK;
            }
            catch (AggregateException e)
            {
                SelfLog.WriteLine($"ERROR: {(e.InnerException ?? e).Message}");

                try
                {
                    _exceptionMut.WaitOne();

                    var exception = e.InnerException as DocumentClientException;
                    if (exception != null)
                    {
                        if (exception.StatusCode == null)
                        {
                            var ei = (DocumentClientException) e.InnerException;
                            if (ei?.StatusCode != null)
                            {
                                exception = ei;
                            }
                        }
                    }

                    if (exception?.StatusCode != null)
                    {
                        switch ((int) exception.StatusCode)
                        {
                            case 429:
                                var delayTask = Task.Delay(
                                    TimeSpan.FromMilliseconds(exception.RetryAfter.Milliseconds + 10));
                                delayTask.Wait();
                                break;
                            default:
                                await CreateBulkImportStoredProcedureAsync(_client, true)
                                    .ConfigureAwait(false);
                                break;
                        }
                    }
                }
                finally
                {
                    _exceptionMut.ReleaseMutex();
                }

                return false;
            }
        }

        #endregion

        #region ILogEventSink Support

        private string _collectionLink;

        public void Emit(LogEvent logEvent)
        {
            if (logEvent != null)
            {
                PushEvent(logEvent);
            }
        }

        #endregion
    }
}