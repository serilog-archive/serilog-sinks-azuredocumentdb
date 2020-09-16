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
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
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
        private readonly CosmosClient _client;
        private readonly IFormatProvider _formatProvider;
        private readonly bool _storeTimestampInUtc;
        private readonly int? _timeToLive;
        //private Colle _collection;
        private Database _database;
        private Container _container;
        private readonly SemaphoreSlim _semaphoreSlim;
        private string _partiotionKey = "id";

        public AzureDocumentDBSink(
            string endpointUri,
            string authorizationKey,
            string databaseName,
            string collectionName,
            string partitionKey,
            IFormatProvider formatProvider,
            bool storeTimestampInUtc,
            //Protocol connectionProtocol,
            TimeSpan? timeToLive,
            int logBufferSize = 25_000,
            int batchSize = 100) : base(batchSize, logBufferSize)
        {
            _formatProvider   = formatProvider;

            if ((timeToLive != null) && (timeToLive.Value != TimeSpan.MaxValue))
                _timeToLive = (int) timeToLive.Value.TotalSeconds;

            _client = new CosmosClientBuilder(endpointUri, authorizationKey)
                //.WithApplicationRegion(Regions.WestEurope)
                .WithConnectionModeGateway()
                .Build();

            _storeTimestampInUtc = storeTimestampInUtc;
            _semaphoreSlim       = new SemaphoreSlim(1, 1);

            CreateDatabaseAndContainerIfNotExistsAsync(databaseName, collectionName, partitionKey).Wait();

            JsonConvert.DefaultSettings = () =>
                                          {
                                              var settings = new JsonSerializerSettings()
                                              {
                                                  ReferenceLoopHandling = ReferenceLoopHandling.Ignore,
                                                  ContractResolver      = new DefaultContractResolver()
                                              };

                                              return settings;
                                          };
        }

        private async Task CreateDatabaseAndContainerIfNotExistsAsync(string databaseName, string containerName, string partitionKey)
        {
            SelfLog.WriteLine($"Opening database {databaseName}");
            _database = (await _client.CreateDatabaseIfNotExistsAsync(databaseName)).Database;
            _container = (await _database.DefineContainer(containerName, $"/{partitionKey}")
                // Define indexing policy with included and excluded paths
                // Define time to live (TTL) in seconds on container
                .WithDefaultTimeToLive(-1)
                .CreateAsync())
                .Container;
            await CreateBulkImportStoredProcedureAsync(_client).ConfigureAwait(false);
        }

        private async Task CreateBulkImportStoredProcedureAsync(CosmosClient client, bool dropExistingProc = false)
        {
            var currentAssembly = typeof(AzureDocumentDBSink).GetTypeInfo().Assembly;

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
                    var bulkImportSrc = await reader.ReadToEndAsync().ConfigureAwait(false);
                    try
                    {
                        var sproc = await _container.Scripts.CreateStoredProcedureAsync(new StoredProcedureProperties()
                        {
                            Id = BulkStoredProcedureId,
                            Body = bulkImportSrc,
                        });

                        //var sproc = GetStoredProcedure(_collectionLink, sp.Id);

                        //if ((sproc != null) && dropExistingProc)
                        //{
                        //    await client.DeleteStoredProcedureAsync(sproc.SelfLink).ConfigureAwait(false);
                        //    sproc = null;
                        //}

                        //if (sproc == null)
                        //    sproc = await client.CreateStoredProcedureAsync(_collectionLink, sp).ConfigureAwait(false);

                    }
                    catch (Exception ex)
                    {
                        SelfLog.WriteLine(ex.Message);
                    }
                }
            }
        }

        //private StoredProcedure GetStoredProcedure(string collectionLink, string id)
        //{
        //    return _client.CreateStoredProcedureQuery(collectionLink)
        //                  .Where(s => s.Id == id)
        //                  .AsEnumerable()
        //                  .FirstOrDefault();
        //}

        #region Parallel Log Processing Support

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            if (logEventsBatch == null || logEventsBatch.Count == 0)
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
            await _semaphoreSlim.WaitAsync().ConfigureAwait(false);
            try {
                SelfLog.WriteLine($"Sending batch of {logEventsBatch.Count} messages to DocumentDB");
                var storedProcedureResponse = await _container.Scripts.ExecuteStoredProcedureAsync<dynamic>(
                storedProcedureId: BulkStoredProcedureId, // your stored procedure name
                partitionKey: new PartitionKey(_partiotionKey),
                parameters: args.ToArray());

                SelfLog.WriteLine(storedProcedureResponse.StatusCode.ToString());

                return storedProcedureResponse.StatusCode == HttpStatusCode.OK;
            }
            catch (AggregateException e) {
                SelfLog.WriteLine($"ERROR: {(e.InnerException ?? e).Message}");

                var exception = e.InnerException as CosmosException;
                if (exception != null) {
                    if (exception.StatusCode == null) {
                        var ei = (CosmosException) e.InnerException;
                        if (ei?.StatusCode != null) {
                            exception = ei;
                        }
                    }
                }

                if (exception?.StatusCode == null)
                    return false;

                switch ((int) exception.StatusCode) {
                    case 429:
                        var delayTask = Task.Delay(TimeSpan.FromMilliseconds(exception.RetryAfter.Value.Milliseconds + 10));
                        delayTask.Wait();

                        break;
                    default:
                        await CreateBulkImportStoredProcedureAsync(_client, true).ConfigureAwait(false);

                        break;
                }

                return false;
            }
            finally {
                _semaphoreSlim.Release();
            }
        }

        #endregion

        #region ILogEventSink Support

        private string _collectionLink;

        public void Emit(LogEvent logEvent)
        {
            if (logEvent != null) {
                PushEvent(logEvent);
            }
        }

        #endregion
    }
}
