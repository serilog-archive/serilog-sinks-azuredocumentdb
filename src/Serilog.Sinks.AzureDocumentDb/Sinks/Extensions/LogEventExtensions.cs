// Copyright 2016 Zethian Inc.
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
using System.Dynamic;
using System.Linq;
using Serilog.Events;

namespace Serilog.Sinks.Extensions
{
    internal static class LogEventExtensions
    {
        internal static IDictionary<string, object> Dictionary(this LogEvent logEvent, bool storeTimestampInUtc = false,
            IFormatProvider formatProvider = null)
        {
            return ConvertToDictionary(logEvent, storeTimestampInUtc, formatProvider);
        }

        internal static IDictionary<string, object> Dictionary(
            this IReadOnlyDictionary<string, LogEventPropertyValue> properties)
        {
            return ConvertToDictionary(properties);
        }

        #region Private implementation

        private static dynamic ConvertToDictionary(IReadOnlyDictionary<string, LogEventPropertyValue> properties)
        {
            var expObject = new ExpandoObject() as IDictionary<string, object>;
            foreach (var property in properties)
                expObject.Add(property.Key, Simplify(property.Value));
            return expObject;
        }

        private static dynamic ConvertToDictionary(LogEvent logEvent, bool storeTimestampInUtc,
            IFormatProvider formatProvider)
        {
            var eventObject = new ExpandoObject() as IDictionary<string, object>;

            eventObject.Add("EventIdHash", ComputeMessageTemplateHash(logEvent.MessageTemplate.Text));
            eventObject.Add("Timestamp", storeTimestampInUtc
                ? logEvent.Timestamp.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fffzzz")
                : logEvent.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.fffzzz"));

            eventObject.Add("Level", logEvent.Level.ToString());
            eventObject.Add("Message", logEvent.RenderMessage(formatProvider));
            eventObject.Add("MessageTemplate", logEvent.MessageTemplate.Text);
            eventObject.Add("Exception", logEvent.Exception);

            var eventProperties = logEvent.Properties.Dictionary();
            eventObject.Add("Properties", eventProperties);

            if (!eventProperties.Keys.Contains("_ttl"))
                return eventObject;

            int ttlValue;

            if (!int.TryParse(eventProperties["_ttl"].ToString(), out ttlValue))
            {
                TimeSpan ttlTimeSpan;
                if (TimeSpan.TryParse(eventProperties["_ttl"].ToString(), out ttlTimeSpan))
                {
                    ttlValue = (int) ttlTimeSpan.TotalSeconds;
                }
            }

            if (ttlValue <= 0)
                ttlValue = -1;
            eventObject.Add("ttl", ttlValue);

            return eventObject;
        }

        private static object Simplify(LogEventPropertyValue data)
        {
            var value = data as ScalarValue;
            if (value != null)
                return value.Value;

            var dictValue = data as DictionaryValue;
            if (dictValue != null)
            {
                var expObject = new ExpandoObject() as IDictionary<string, object>;
                foreach (var item in dictValue.Elements)
                {
                    var key = item.Key.Value as string;

                    if(key != null)
                        expObject.Add(key, Simplify(item.Value));
                }
                return expObject;
            }

            var seq = data as SequenceValue;
            if (seq != null)
                return seq.Elements.Select(Simplify).ToArray();

            var str = data as StructureValue;
            if (str == null) return null;
            {
                try
                {
                    if (str.TypeTag == null)
                        return str.Properties.ToDictionary(p => p.Name, p => Simplify(p.Value));

                    if (!str.TypeTag.StartsWith("DictionaryEntry") && !str.TypeTag.StartsWith("KeyValuePair"))
                        return str.Properties.ToDictionary(p => p.Name, p => Simplify(p.Value));

                    var key = Simplify(str.Properties[0].Value);
                    if (key == null)
                        return null;

                    var expObject = new ExpandoObject() as IDictionary<string, object>;
                    expObject.Add(key.ToString(), Simplify(str.Properties[1].Value));
                    return expObject;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }

            return null;
        }

        /// <summary>
        ///     ComputMessageTemplateHash a 32-bit hash of the provided <paramref name="messageTemplate" />. The
        ///     resulting hash value can be uses as an event id in lieu of transmitting the
        ///     full template string.
        /// </summary>
        /// <param name="messageTemplate">A message template.</param>
        /// <returns>A 32-bit hash of the template.</returns>
        private static uint ComputeMessageTemplateHash(string messageTemplate)
        {
            if (messageTemplate == null) throw new ArgumentNullException(nameof(messageTemplate));

            // Jenkins one-at-a-time https://en.wikipedia.org/wiki/Jenkins_hash_function
            unchecked
            {
                uint hash = 0;
                for (var i = 0; i < messageTemplate.Length; ++i)
                {
                    hash += messageTemplate[i];
                    hash += hash << 10;
                    hash ^= hash >> 6;
                }
                hash += hash << 3;
                hash ^= hash >> 11;
                hash += hash << 15;
                return hash;
            }
        }

        #endregion
    }
}