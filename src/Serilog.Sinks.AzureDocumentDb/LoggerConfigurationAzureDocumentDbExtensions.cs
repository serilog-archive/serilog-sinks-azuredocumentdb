// Copyright 2014 Serilog Contributors
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

namespace Serilog
{
    using System;
    using Microsoft.Azure.Documents.Client;
    using Serilog.Configuration;
    using Serilog.Events;
    using Serilog.Sinks.AzureDocumentDb;

    /// <summary>
    ///     Adds the WriteTo.AzureDocumentDb() extension method to <see cref="LoggerConfiguration" />.
    /// </summary>
    public static class LoggerConfigurationAzureDocumentDBExtensions
    {
        /// <summary>
        ///     Adds a sink that writes log events to a Azure DocumentDB table in the provided endpoint.
        /// </summary>
        /// <param name="loggerConfiguration">The logger configuration.</param>
        /// <param name="endpointUri">The endpoint URI of the document db.</param>
        /// <param name="authorizationKey">The authorization key of the db.</param>
        /// <param name="databaseName">The name of the database to use; will create if it doesn't exist.</param>
        /// <param name="collectionName">The name of the collection to use inside the database; will created if it doesn't exist.</param>
        /// <param name="restrictedToMinimumLevel">The minimum log event level required in order to write an event to the sink.</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <param name="storeTimestampInUtc">Store Timestamp in UTC</param>
        /// <param name="connectionProtocol">Specifies communication protocol used by driver to communicate with Azure DocumentDB services.</param>
        /// <param name="timeToLive">The lifespan of documents (roughly 24855 days maximum). Set null to disable document expiration. </param>
        /// <exception cref="ArgumentNullException">A required parameter is null.</exception>
        /// <exception cref="ArgumentOutOfRangeException">A required parameter value is out of acceptable range.</exception>

        public static LoggerConfiguration AzureDocumentDB(
            this LoggerSinkConfiguration loggerConfiguration,
            Uri endpointUri,
            string authorizationKey,
            string databaseName = "Diagnostics",
            string collectionName = "Logs",
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            IFormatProvider formatProvider = null,
            bool storeTimestampInUtc = false,
            Protocol connectionProtocol = Protocol.Https,
            TimeSpan? timeToLive = null)
        {
            if (loggerConfiguration == null) throw new ArgumentNullException(nameof(loggerConfiguration));
            if (endpointUri == null) throw new ArgumentNullException(nameof(endpointUri));
            if (authorizationKey == null) throw new ArgumentNullException(nameof(authorizationKey));
            if(timeToLive != null && timeToLive.Value > TimeSpan.FromDays(24855))
            {
                throw new ArgumentOutOfRangeException(nameof(timeToLive));
            }

            return loggerConfiguration.Sink(
                new AzureDocumentDBSink(
                    endpointUri,
                    authorizationKey,
                    databaseName,
                    collectionName,
                    formatProvider,
                    storeTimestampInUtc,
                    connectionProtocol,
                    timeToLive),
                restrictedToMinimumLevel);
        }
    }
}