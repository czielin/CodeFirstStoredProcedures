using Microsoft.SqlServer.Server;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CodeFirstStoredProcs
{
    /// <summary>
    /// Contains extension methods to Code First database objects for Stored Procedure processing
    /// Updated to include support for streaming and async
    /// </summary>
    internal static class CodeFirstStoredProcHelpers
    {

        /// <summary>
        /// Get the underlying class type for lists, etc. that implement IEnumerable<>.
        /// </summary>
        /// <param name="listtype"></param>
        /// <returns></returns>
        public static Type GetUnderlyingType(Type listtype)
        {
            Type basetype = null;
            foreach (Type i in listtype.GetInterfaces())
                if (i.IsGenericType && i.GetGenericTypeDefinition().Equals(typeof(IEnumerable<>)))
                    basetype = i.GetGenericArguments()[0];

            return basetype;
        }

        /// <summary>
        /// Get properties of a type that do not have the 'NotMapped' attribute
        /// </summary>
        /// <param name="t">Type to examine for properites</param>
        /// <returns>Array of properties that can be filled</returns>
        public static PropertyInfo[] GetMappedProperties(this Type t)
        {
            var props1 = t.GetProperties();
            var props2 = props1
                .Where(p => p.GetAttribute<NotMappedAttribute>() == null)
                .Select(p => p);
            return props2.ToArray();
        }

        /// <summary>
        /// Get an attribute for a type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="type"></param>
        /// <returns></returns>
        public static T GetAttribute<T>(this Type type)
            where T : Attribute
        {
            var attributes = type.GetCustomAttributes(typeof(T), false).FirstOrDefault();
            return (T)attributes;
        }

        /// <summary>
        /// Get an attribute for a property
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="propertyinfo"></param>
        /// <returns></returns>
        public static T GetAttribute<T>(this PropertyInfo propertyinfo)
            where T : Attribute
        {
            var attributes = propertyinfo.GetCustomAttributes(typeof(T), false).FirstOrDefault();
            return (T)attributes;
        }

        /// <summary>
        /// List all properties for an object by name, allow for attributes to override the name.
        /// </summary>
        /// <param name="props"></param>
        /// <returns></returns>
        public static Dictionary<String, PropertyInfo> GetPropertiesByName(PropertyInfo[] props)
        {
            Dictionary<String, PropertyInfo> propertymap = new Dictionary<string, PropertyInfo>(props.Length);
            for (int i = 0; i < props.Length; i++)
            {
                PropertyInfo p = props[i];
                // default name is property name, override of parameter name by attribute
                var attr = p.GetAttribute<StoredProcAttributes.Name>();
                var name = (null == attr) ? p.Name : attr.Value;

                propertymap.Add(name, p);
            }

            return propertymap;
        }

        /// <summary>
        /// List properties for an object for "Ordinal" attribute.
        /// </summary>
        /// <param name="props"></param>
        /// <returns></returns>
        public static Dictionary<Int32, PropertyInfo> GetPropertiesByOrdinal(PropertyInfo[] props)
        {
            var propertyMap = new Dictionary<Int32, PropertyInfo>();
            for (int i = 0; i < props.Length; i++)
            {
                PropertyInfo p = props[i];
                // default name is property name, override of parameter name by attribute
                var attr = p.GetAttribute<StoredProcAttributes.Ordinal>();
                if (attr != null)
                    propertyMap.Add(attr.Value, p);
            }

            return propertyMap;
        }

        /// <summary>
        /// Match DbDataReader "columns" with properties of the destination object by Name and Ordinal attributes. Name attributes
        /// are accepted before Ordinal attributes and both can be freely intermixed in a target object.
        /// </summary>
        /// <param name="reader">Returned data containing values to map to the object</param>
        /// <param name="t">Object containing target properties</param>
        /// <param name="currenttype">Iterator over defined return types, should be the Type of the object t param</param>
        /// <returns></returns>
        public static PropertyInfo[] MatchRecordProperties(this DbDataReader reader, object t, IEnumerator currenttype)
        {
            // get properties to save for the current destination type
            PropertyInfo[] props = ((Type)currenttype.Current).GetMappedProperties();
            Dictionary<String, PropertyInfo> propertymap = CodeFirstStoredProcHelpers.GetPropertiesByName(props);
            Dictionary<Int32, PropertyInfo> ordinalmap = CodeFirstStoredProcHelpers.GetPropertiesByOrdinal(props);

            // list the matching property for each returned field
            PropertyInfo[] propertylist = new PropertyInfo[reader.FieldCount];
            PropertyInfo p = null;

            // copy mapped properties
            for (int i = 0; i < reader.FieldCount; i++)
            {
                // reset the saved propertyinfo
                p = null;

                // get this column name
                String name = reader.GetName(i);

                // not found by name, try by ordinal
                try
                {
                    // if we don't have this property in our map, just skip it. Note: we're doing a currentculture w/ no case search for the key.
                    String key = propertymap.Keys.Where(k => k.Equals(name, StringComparison.CurrentCultureIgnoreCase)).FirstOrDefault();
                    if (!String.IsNullOrEmpty(key))
                    {
                        // get the relevant property for this column
                        p = propertymap[key];
                    }
                    else
                    {
                        Int32 ordinal = ordinalmap.Keys.Where(k => k.Equals(i)).FirstOrDefault();
                        if (0 < ordinal)
                        {
                            p = ordinalmap[ordinal];
                        }
                    }
                }
                catch (Exception ex)
                {
                    // tell the user *where* we had an exception
                    Exception outer = new Exception(String.Format("Exception identifying matching property for return column {0} in {1}",
                        name, t.GetType().Name), ex);

                    // something bad happened, pass on the exception
                    throw outer;
                }

                // track property infos  
                propertylist[i] = p;
            }

            return propertylist;
        }

        /// <summary>
        /// Read data for the current result row from a reader into a destination object, by the name
        /// of the properties on the destination object.
        /// </summary>
        /// <param name="reader">data reader holding return data</param>
        /// <param name="t">object to populate</param>
        /// <returns></returns>
        /// <param name="props">properties list to copy from result set row 'reader' to object 't'</param>
        /// <param name="token">Cancellation token for asyc process cancellation</param>
        public async static Task<object> ReadRecordAsync(this DbDataReader reader, object t,
            PropertyInfo[] props, CancellationToken token)
        {
            PropertyInfo p = null;
            // copy mapped properties
            for (int i = 0; i < reader.FieldCount; i++)
            {
                // Get the matching property for this column. If none, then skip it
                p = props[i];
                if (null == p)
                    continue;

                // get this column name
                String name = reader.GetName(i);

                // get the data from the reader into the target object
                try
                {
                    // see if we're being asked to write this property to a stream
                    var stream = p.GetAttribute<StoredProcAttributes.StreamOutput>();
                    if (null != stream)
                    {
                        // if yes, wait on the stream processing
                        await ReadFromStreamAsync(reader, t, name, p, stream, token);
                    }
                    else
                    {
                        // get the requested value from the returned dataset and handle null values
                        var data = await reader.GetFieldValueAsync<object>(i);
                        if (data.GetType() == typeof(System.DBNull))
                            p.SetValue(t, null, null);
                        else
                            p.SetValue(t, data, null);
                    }
                }
                catch (Exception ex)
                {
                    if (ex.GetType() == typeof(IndexOutOfRangeException))
                    {
                        // if the result set doesn't have this value, intercept the exception
                        // and set the property value to null / 0, if we can
                        if (p.CanWrite)
                        {
                            p.SetValue(t, null, null);
                        }
                    }
                    else
                    {
                        // tell the user *where* we had an exception
                        Exception outer = new Exception(String.Format("Exception processing return column {0} in {1}",
                            name, t.GetType().Name), ex);

                        // something bad happened, pass on the exception
                        throw outer;
                    }
                }
            }

            return t;
        }

        ///// <summary>
        ///// Read streamed data from SQL Server into a file or memory stream. If the target property for the data in object 't' is not
        ///// a stream, then copy the data to an array or String.
        ///// </summary>
        ///// <param name="reader"></param>
        ///// <param name="t"></param>
        ///// <param name="name"></param>
        ///// <param name="p"></param>
        ///// <param name="stream"></param>
        //[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2202:Do not dispose objects multiple times")]
        //private static void ReadFromStream(DbDataReader reader, object t, String name, PropertyInfo p, StoredProcAttributes.StreamOutput stream)
        //{
        //    // handle streamed values
        //    Stream tostream = CreateStream(stream, t);
        //    try
        //    {
        //        using (Stream fromstream = reader.GetStream(reader.GetOrdinal(name)))
        //        {
        //            fromstream.CopyTo(tostream);
        //        }

        //        // reset our stream position
        //        tostream.Seek(0, 0);

        //        // For array output, copy tostream to user's array and close stream since user will never see it
        //        if (p.PropertyType.Name.Contains("[]") || p.PropertyType.Name.Contains("Array"))
        //        {
        //            Byte[] item = new Byte[tostream.Length];
        //            tostream.Read(item, 0, (int)tostream.Length);
        //            p.SetValue(t, item, null);
        //            tostream.Close();
        //        }
        //        else if (p.PropertyType.Name.Contains("String"))
        //        {
        //            StreamReader r = new StreamReader(tostream, ((StoredProcAttributes.StreamToMemory)stream).GetEncoding());
        //            String text = r.ReadToEnd();
        //            p.SetValue(t, text, null);
        //            r.Close();
        //        }
        //        else if (p.PropertyType.Name.Contains("Stream"))
        //        {
        //            // NOTE: User will have to close the stream if they don't tell us to close file streams!
        //            if (typeof(StoredProcAttributes.StreamToFile) == stream.GetType() && !((StoredProcAttributes.StreamToFile)stream).LeaveStreamOpen)
        //            {
        //                tostream.Close();
        //            }

        //            // pass our created stream back to the user since they asked for a stream output
        //            p.SetValue(t, tostream, null);
        //        }
        //        else
        //        {
        //            throw new Exception(String.Format("Invalid property type for property {0}. Valid types are Stream, byte or character arrays and String",
        //                p.Name));
        //        }
        //    }
        //    catch (Exception)
        //    {
        //        // always close the stream on exception
        //        if (null != tostream)
        //            tostream.Close();

        //        // pass the exception on
        //        throw;
        //    }
        //}

        /// <summary>
        /// Read streamed data from SQL Server into a file or memory stream. If the target property for the data in object 't' is not
        /// a stream, then copy the data to an array or String.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="t"></param>
        /// <param name="name"></param>
        /// <param name="p"></param>
        /// <param name="stream"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        private static async Task ReadFromStreamAsync(DbDataReader reader, object t, String name, PropertyInfo p, StoredProcAttributes.StreamOutput stream,
            CancellationToken token)
        {
            // handle streamed values
            Stream tostream = CreateStream(stream, t);
            try
            {
                using (Stream fromstream = reader.GetStream(reader.GetOrdinal(name)))
                {
                    await fromstream.CopyToAsync(tostream, (int)fromstream.Length, token);
                }

                // reset our stream position
                tostream.Seek(0, 0);

                // For array output, copy tostream to user's array and close stream since user will never see it
                if (p.PropertyType.Name.Contains("[]") || p.PropertyType.Name.Contains("Array"))
                {
                    Byte[] item = new Byte[tostream.Length];
                    tostream.Read(item, 0, (int)tostream.Length);
                    p.SetValue(t, item, null);
                    tostream.Close();
                }
                else if (p.PropertyType.Name.Contains("String"))
                {
                    StreamReader r = new StreamReader(tostream, ((StoredProcAttributes.StreamToMemory)stream).GetEncoding());
                    String text = r.ReadToEnd();
                    p.SetValue(t, text, null);
                    r.Close();
                }
                else
                {
                    // NOTE: User will have to close the stream if they don't tell us to close file streams!
                    if (typeof(StoredProcAttributes.StreamToFile) == stream.GetType() && !((StoredProcAttributes.StreamToFile)stream).LeaveStreamOpen)
                    {
                        tostream.Close();
                    }

                    // pass our created stream back to the user since they asked for a stream output
                    p.SetValue(t, tostream, null);
                }
            }
            catch (Exception)
            {
                // always close the stream on an exception
                if (null != tostream)
                    tostream.Close();

                // pass on the error
                throw;
            }
        }

        /// <summary>
        /// Create a Stream for saving large object data from the server, use the
        /// stream attribute data
        /// </summary>
        /// <param name="format"></param>
        /// <param name="t"></param>
        /// <returns></returns>
        internal static Stream CreateStream(StoredProcAttributes.StreamOutput format, object t)
        {
            Stream output;

            if (typeof(StoredProcAttributes.StreamToFile) == format.GetType())
            {
                // File stream
                output = ((StoredProcAttributes.StreamToFile)format).CreateStream(t); ;

                // build name from location and name property
            }
            else
            {
                // Memory Stream
                output = ((StoredProcAttributes.StreamToMemory)format).CreateStream();
            }

            // if buffering was requested, overlay bufferedstream on our stream
            if (format.Buffered)
            {
                output = new BufferedStream(output);
            }

            return output;
        }

        /// <summary>
        /// Do the work of converting a source data object to SqlDataRecords 
        /// using the parameter attributes to create the table valued parameter definition
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        internal static IEnumerable<SqlDataRecord> TableValuedParameter(IList table)
        {
            // get the object type underlying our table
            Type t = CodeFirstStoredProcHelpers.GetUnderlyingType(table.GetType());

            // list of converted values to be returned to the caller
            List<SqlDataRecord> recordlist = new List<SqlDataRecord>();

            // get all mapped properties
            PropertyInfo[] props = CodeFirstStoredProcHelpers.GetMappedProperties(t);

            // get the column definitions, into an array; keep properties in a matching array. 
            // Can't use tuple since we need access to the array of sqlmetadata values.
            FlexArray<SqlMetaData> columnlist = new FlexArray<SqlMetaData>(props.Length);
            FlexArray<PropertyInfo> propertylist = new FlexArray<PropertyInfo>(props.Length);

            // get the propery column name to property name mapping
            // and generate the SqlMetaData for each property/column
            Dictionary<String, String> mapping = new Dictionary<string, string>();
            for (int i = 0; i < props.Length; i++)
            {
                PropertyInfo p = props[i];

                // default name is property name, override of parameter name by attribute
                var nameattr = p.GetAttribute<StoredProcAttributes.Name>();
                String name = (null == nameattr) ? p.Name : nameattr.Value;
                mapping.Add(name, p.Name);

                // get column type
                var ct = p.GetAttribute<StoredProcAttributes.ParameterType>();
                SqlDbType coltype = (null == ct) ? SqlDbType.Int : ct.Value;

                // create metadata column definition
                SqlMetaData column;
                switch (coltype)
                {
                    case SqlDbType.Binary:
                    case SqlDbType.Char:
                    case SqlDbType.NChar:
                    case SqlDbType.Image:
                    case SqlDbType.VarChar:
                    case SqlDbType.NVarChar:
                    case SqlDbType.Text:
                    case SqlDbType.NText:
                    case SqlDbType.VarBinary:
                        // get column size
                        var sa = p.GetAttribute<StoredProcAttributes.Size>();
                        int size = (null == sa) ? 50 : sa.Value;
                        column = new SqlMetaData(name, coltype, size);
                        break;

                    case SqlDbType.Decimal:
                        // get column precision and scale
                        var pa = p.GetAttribute<StoredProcAttributes.Precision>();
                        Byte precision = (null == pa) ? (byte)10 : pa.Value;
                        var sca = p.GetAttribute<StoredProcAttributes.Scale>();
                        Byte scale = (null == sca) ? (byte)2 : sca.Value;
                        column = new SqlMetaData(name, coltype, precision, scale);
                        break;

                    default:
                        column = new SqlMetaData(name, coltype);
                        break;
                }

                // See if this column has an ordinal. Add metadata and property to matching lists. 
                var ord = p.GetAttribute<StoredProcAttributes.Ordinal>();
                if (null != ord)
                {
                    columnlist.Insert(ord.Value, column);
                    propertylist.Insert(ord.Value, p);
                }
                else
                {
                    // Add metadata to column list, matching property to propertylist. Don't overlay items
                    // inserted at a specific index based on higher ordinal values, just iterate past them.
                    int j = i;
                    while (null != columnlist[j]) j++;
                    columnlist.Insert(j, column);
                    propertylist.Insert(j, p);
                }
            }

            // load each object in the input data table into sql data records
            foreach (object s in table)
            {
                // create the sql data record using the column definition. NOTE: Column meta data and
                // properties must be in the same order!! I'd use a tuple, but we need separate access to the 
                // columnlist array. Solution is matching arraylist objects, one for sqlmetadata and one for 
                // propertyinfos.
                SqlDataRecord record = new SqlDataRecord(columnlist.ToArray());
                for (int i = 0; i < columnlist.Length; i++)
                {
                    // get the value of the matching property
                    var value = propertylist[i].GetValue(s, null);

                    // set the value
                    record.SetValue(i, value);
                }

                // add the sql data record to our output list
                recordlist.Add(record);
            }

            // return our list of data records
            return recordlist;
        }
    }
}
