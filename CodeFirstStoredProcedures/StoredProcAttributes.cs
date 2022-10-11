using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Reflection;
using System.Text;

namespace CodeFirstStoredProcs
{
    /// <summary>
    /// Contains attributes for Stored Procedure processing
    /// </summary>
    public class StoredProcAttributes
    {
        /// <summary>
        /// Parameter name override. Default value for parameter name is the name of the 
        /// property. This overrides that default with a user defined name.
        /// </summary>
        public class Name : Attribute
        {
            public String Value { get; set; }

            public Name(String s)
                : base()
            {
                Value = s;
            }
        }

        /// <summary>
        /// Parameter ordinal specifier. Default value for parameter name is the name of the 
        /// property. This overrides that default with an explicit mapping to a result set ordinal.
        /// </summary>
        public class Ordinal : Attribute
        {
            public int Value { get; set; }

            public Ordinal(int value)
                : base()
            {
                Value = value;
            }
        }

        /// <summary>
        /// Size in bytes of returned data. Should be used on output and returncode parameters.
        /// </summary>
        public class Size : Attribute
        {
            public Int32 Value { get; set; }

            public Size(Int32 s)
                : base()
            {
                Value = s;
            }
        }

        /// <summary>
        /// Size in bytes of returned data. Should be used on output and returncode parameters.
        /// </summary>
        public class Precision : Attribute
        {
            public Byte Value { get; set; }

            public Precision(Byte s)
                : base()
            {
                Value = s;
            }
        }

        /// <summary>
        /// Size in bytes of returned data. Should be used on output and returncode parameters.
        /// </summary>
        public class Scale : Attribute
        {
            public Byte Value { get; set; }

            public Scale(Byte s)
                : base()
            {
                Value = s;
            }
        }

        /// <summary>
        /// Defines the direction of data flow for the property/parameter.
        /// </summary>
        public class Direction : Attribute
        {
            public ParameterDirection Value { get; set; }

            public Direction(ParameterDirection d)
            {
                Value = d;
            }
        }

        /// <summary>
        /// Define the SqlDbType for the parameter corresponding to this property.
        /// </summary>
        public class ParameterType : Attribute
        {
            public SqlDbType Value { get; set; }

            public ParameterType(SqlDbType t)
            {
                Value = t;
            }
        }

        /// <summary>
        /// Allows the setting of the parameter type name for user defined types in the database
        /// </summary>
        public class TypeName : Attribute
        {
            public String Value { get; set; }

            public TypeName(String t)
            {
                Value = t;
            }
        }

        /// <summary>
        /// Allows the setting of the user defined table type name for table valued parameters
        /// </summary>
        public class TableName : Attribute
        {
            public String Value { get; set; }

            public TableName(String t)
            {
                Value = t;
            }
        }

        /// <summary>
        /// Allows the setting of the user defined table type name for table valued parameters
        /// </summary>
        public class Schema : Attribute
        {
            public String Value { get; set; }

            public Schema(String t)
            {
                Value = t;
            }
        }

        /// <summary>
        /// Allows the setting of the user defined table type name for table valued parameters
        /// </summary>
        public class ReturnTypes : Attribute
        {
            public Type[] Returns { get; set; }

            public ReturnTypes(params Type[] values)
            {
                Returns = values;
            }
        }

        /// <summary>
        /// Allows the setting of the user defined table type name for table valued parameters
        /// </summary>
        public class StreamOutput : Attribute
        {
            public Boolean Buffered { get; set; }
            public Boolean LeaveStreamOpen { get; set; }

            public StreamOutput()
            {
            }
        }

        /// <summary>
        /// Stream to File output
        /// </summary>
        public class StreamToFile : StreamOutput
        {
            public String FileNameField { get; set; }
            public String Location { get; set; }

            /// <summary>
            /// Create the file stream using location attribute data and filename in returned data
            /// </summary>
            /// <param name="t"></param>
            /// <returns></returns>
            internal Stream CreateStream(object t)
            {
                String filename = Location;
                var tp = t.GetType();
                var p = tp.GetProperty(FileNameField);
                if (null != p)
                {
                    var name = p.GetValue(t, null);
                    if (null != name)
                        filename = Path.Combine(filename, name.ToString());
                }

                return new FileStream(filename, FileMode.OpenOrCreate, FileAccess.Write);

            }

            public StreamToFile()
            {
            }
        }

        /// <summary>
        /// Stream to MemoryStream, Array or String
        /// </summary>
        public class StreamToMemory : StreamOutput
        {
            public String Encoding { get; set; }

            /// <summary>
            /// Create Memory Stream 
            /// </summary>
            /// <returns></returns>
            internal Stream CreateStream()
            {
                return new MemoryStream();
            }

            /// <summary>
            /// Resolve Encoding for conversion of MemoryStream to String
            /// </summary>
            /// <returns></returns>
            internal System.Text.Encoding GetEncoding()
            {
                var method = typeof(System.Text.Encoding).GetMethod(Encoding);
                return (System.Text.Encoding)typeof(System.Text.Encoding).InvokeMember(Encoding,
                    BindingFlags.Public | BindingFlags.Static | BindingFlags.GetProperty | BindingFlags.IgnoreCase, null, null, null);
            }

            public StreamToMemory()
            {
                if (String.IsNullOrEmpty(Encoding))
                {
                    Encoding = "Default";
                }
            }
        }
    }
}
