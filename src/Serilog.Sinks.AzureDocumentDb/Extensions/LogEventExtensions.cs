﻿using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using Serilog.Events;

namespace Serilog.Extensions
{
    public static class LogEventExtensions
    {
        public static IDictionary<string, object> Dictionary(this LogEvent logEvent, bool storeTimestampInUtc = false, IFormatProvider formatProvider = null)
        {
            return ConvertToDictionary(logEvent, storeTimestampInUtc, formatProvider);
        }

        public static IDictionary<string, object> Dictionary(
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

        private static dynamic ConvertToDictionary(LogEvent logEvent, bool storeTimestampInUtc, IFormatProvider formatProvider)
        {
            var eventObject = new ExpandoObject() as IDictionary<string, object>;
            eventObject.Add("Timestamp", storeTimestampInUtc
                ? logEvent.Timestamp.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss.fffzzz")
                : logEvent.Timestamp.ToString("yyyy-MM-dd HH:mm:ss.fffzzz"));

            eventObject.Add("Level", logEvent.Level.ToString());
            eventObject.Add("Message", logEvent.RenderMessage(formatProvider));
            eventObject.Add("Exception", logEvent.Exception);

            var eventProperties = logEvent.Properties.Dictionary();
            eventObject.Add("Properties", eventProperties);

            if (!eventProperties.Keys.Contains("_ttl"))
                return eventObject;

            int ttlValue;
            if (!int.TryParse(eventProperties["_ttl"].ToString(), out ttlValue))
                return eventObject;

            if (ttlValue < 0)
                ttlValue = -1;
            eventObject.Add("ttl", ttlValue);

            return eventObject;
        }

        private static object Simplify(LogEventPropertyValue data)
        {
            var value = data as ScalarValue;
            if (value != null)
                return value.Value;

            var dictValue = data as IReadOnlyDictionary<string, LogEventPropertyValue>;
            if (dictValue != null)
            {
                var expObject = new ExpandoObject() as IDictionary<string, object>;
                foreach (var item in dictValue.Keys)
                    expObject.Add(item, Simplify(dictValue[item]));
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

        #endregion
    }
}