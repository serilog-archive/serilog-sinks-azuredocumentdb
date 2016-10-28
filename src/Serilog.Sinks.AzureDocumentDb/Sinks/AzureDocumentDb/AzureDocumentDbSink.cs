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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
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


        public AzureDocumentDBSink(Uri endpointUri,
            string authorizationKey,
            string databaseName,
            string collectionName,
            IFormatProvider formatProvider,
            bool storeTimestampInUtc,
            Protocol connectionProtocol,
            TimeSpan? timeToLive) : base(batchSize:250, nThreads: Environment.ProcessorCount)
        {
            _endpointUri = endpointUri;
            _authorizationKey = authorizationKey;
            _formatProvider = formatProvider;

            if ((timeToLive != null) && (timeToLive.Value != TimeSpan.MaxValue))
                _timeToLive = (int) timeToLive.Value.TotalSeconds;

            _client = new DocumentClient(endpointUri,
                authorizationKey,
                new ConnectionPolicy
                {
                    ConnectionMode =
                        connectionProtocol == Protocol.Https ? ConnectionMode.Gateway : ConnectionMode.Direct,
                    ConnectionProtocol = connectionProtocol,
                    MaxConnectionLimit = Environment.ProcessorCount*50 + 200
                });

            _storeTimestampInUtc = storeTimestampInUtc;

            CreateDatabaseIfNotExistsAsync(databaseName).Wait();
            CreateCollectionIfNotExistsAsync(collectionName).Wait();
        }

        private async Task CreateDatabaseIfNotExistsAsync(string databaseName)
        {
            await _client.OpenAsync();
            _database = _client
                            .CreateDatabaseQuery()
                            .Where(x => x.Id == databaseName)
                            .AsEnumerable()
                            .FirstOrDefault() ?? await _client
                            .CreateDatabaseAsync(new Database {Id = databaseName})
                            .ConfigureAwait(false);
        }

        private async Task CreateCollectionIfNotExistsAsync(string collectionName)
        {
            _collection =
                _client.CreateDocumentCollectionQuery(_database.SelfLink)
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
            await CreateBulkImportStoredProcedure(_client);
        }

        private async Task CreateBulkImportStoredProcedure(IDocumentClient client, bool dropExistingProc = false)
        {
            var currentAssembly = Assembly.GetExecutingAssembly();

            var resourceName =
                currentAssembly.GetManifestResourceNames().FirstOrDefault(w => w.EndsWith("bulkImport.js"));

            using (var resourceStream = currentAssembly.GetManifestResourceStream(resourceName))
            {
                if (resourceStream != null)
                {
                    var reader = new StreamReader(resourceStream);
                    var bulkImportSrc = reader.ReadToEnd();
                    try
                    {
                        var sp = new StoredProcedure
                        {
                            Id = BulkStoredProcedureId,
                            Body = bulkImportSrc
                        };

                        var sproc = GetStoredProcedure(_collectionLink, sp.Id);

                        if ((sproc != null) && dropExistingProc)
                        {
                            await client.DeleteStoredProcedureAsync(sproc.SelfLink);
                            sproc = null;
                        }

                        if (sproc == null)
                            sproc = await client.CreateStoredProcedureAsync(_collectionLink, sp);

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

        protected override void WriteLogEvent(ICollection<LogEvent> logEventsBatch)
        {
            if ((logEventsBatch == null) || (logEventsBatch.Count == 0))
                return;

            var args = logEventsBatch.Select(x => x.Dictionary(_storeTimestampInUtc, _formatProvider));

            if ((_timeToLive != null) && (_timeToLive > 0))
                args = args.Select(x =>
                {
                    if (!x.Keys.Contains("ttl"))
                        x.Add("ttl", _timeToLive);
                    return x;
                });

            try
            {
                SelfLog.WriteLine($"Sending batch of {logEventsBatch.Count} messages to DocumentDB");
                _client.ExecuteStoredProcedureAsync<int>(_bulkStoredProcedureLink, args).Wait();
            }
            catch (AggregateException e)
            {
                var exception = e.InnerException as DocumentClientException;
                if (exception != null)
                {
                    var ei = (DocumentClientException) e.InnerException;
                    try
                    {
                        _exceptionMut.WaitOne();

                        if (ei.StatusCode != null)
                            switch ((int) ei.StatusCode)
                            {
                                case 429:
                                    SelfLog.WriteLine("Request rate is higher than subscription throughput. Waiting for {0} ms.", ei.RetryAfter.Milliseconds);
                                    Thread.Sleep(ei.RetryAfter.Milliseconds);
                                    break;
                                default:
                                    CreateBulkImportStoredProcedure(_client, true).Wait();
                                    break;
                            }
                    }
                    finally
                    {
                        _exceptionMut.ReleaseMutex();
                        _client.ExecuteStoredProcedureAsync<int>(_bulkStoredProcedureLink, args).Wait();
                    }
                }
            }
        }

        #endregion

        #region ILogEventSink Support

        private string _collectionLink;

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
        }

        #endregion
    }
}