using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CodeFirstStoredProcs
{
    /// <summary>
    /// Genericized version of StoredProc object, takes a .Net POCO object type for the parameters. 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class StoredProc<T> : StoredProc
    {
        //-----------------------------------------------------------------------------------------
        // New Style Interface; more in line with EF style 
        //-----------------------------------------------------------------------------------------

        /// <summary>
        /// New Interface
        /// Call the stored procedure; InitializeStoredProcs must be called prior to using this method
        /// </summary>
        /// <param name="data">POCO object containing data to be sent to the stored procedure</param>
        /// <param name="types">Types of POCO objects that will be used to house any returned data</param>
        /// <returns>List of lists containing result data from stored proc</returns>
        public ResultsList CallStoredProc(T data, params Type[] types)
        {
            return CallStoredProc(this.commandTimeout, data, types);
        }

        /// <summary>
        /// New Interface
        /// Call the stored procedure; InitializeStoredProcs must be called prior to using this method
        /// </summary>
        /// <param name="CommandTimeout">Timeout value for this command execution</param>
        /// <param name="data">POCO object containing data to be sent to the stored procedure</param>
        /// <param name="types">Types of POCO objects that will be used to house any returned data</param>
        /// <returns>List of lists containing result data from stored proc</returns>
        public ResultsList CallStoredProc(int? CommandTimeout, T data, params Type[] types)
        {
            // protect ourselves from the old style of calling this 
            if (null == _context)
            {
                throw new Exception("Not Properly Initialized. Call InitializeStoredProcs in the DbContext constructor.");
            }

            // set up default return types if none provided
            if (null == types)
                types = new Type[] { };

            // Set up the stored proc parameters
            if (String.IsNullOrEmpty(procname))
            {
                SetupStoredProc(types);
            }

            return CallStoredProc(CommandTimeout, Transaction, data, types);
        }

        /// <summary>
        /// New Interface
        /// Call the stored procedure; InitializeStoredProcs must be called prior to using this method
        /// </summary>
        /// <param name="CommandTimeout">Timeout value for this command execution</param>
        /// <param name="data">POCO object containing data to be sent to the stored procedure</param>
        /// <param name="types">Types of POCO objects that will be used to house any returned data</param>
        /// <returns>List of lists containing result data from stored proc</returns>
        public ResultsList CallStoredProc(int? CommandTimeout, DbTransaction transaction, T data, params Type[] types)
        {
            // protect ourselves from the old style of calling this 
            if (null == _context)
            {
                throw new Exception("Not Properly Initialized. Call InitializeStoredProcs in the DbContext constructor.");
            }

            // set up default return types if none provided
            if (null == types)
                types = new Type[] { };

            // Set up the stored proc parameters
            if (String.IsNullOrEmpty(procname))
            {
                SetupStoredProc(types);
            }

            commandTimeout = CommandTimeout;
            Transaction = transaction;

            return CodeFirstStoredProcs.CallStoredProc(_context, this, CommandTimeout, transaction, data);
        }

        /// <summary>
        /// New Interface, Asnyc
        /// Call the stored procedure; InitializeStoredProcs must be called prior to using this method
        /// </summary>
        /// <param name="data">POCO object containing data to be sent to the stored procedure</param>
        /// <param name="types">Types of POCO objects that will be used to house any returned data</param>
        /// <returns>List of lists containing result data from stored proc</returns>
        public async Task<ResultsList> CallStoredProcAsync(T data, params Type[] types)
        {
            return await CallStoredProcAsync(this.cancellationToken, this.commandTimeout, this.Transaction, data, types);
        }

        /// <summary>
        /// New Interface, Asnyc
        /// Call the stored procedure; InitializeStoredProcs must be called prior to using this method
        /// </summary>
        /// <param name="CommandTimeout">Timeout value for this execution</param>
        /// <param name="data">POCO object containing data to be sent to the stored procedure</param>
        /// <param name="types">Types of POCO objects that will be used to house any returned data</param>
        /// <returns>List of lists containing result data from stored proc</returns>
        public async Task<ResultsList> CallStoredProcAsync(int? CommandTimeout, T data, params Type[] types)
        {
            commandTimeout = CommandTimeout;
            return await CallStoredProcAsync(this.cancellationToken, CommandTimeout, this.Transaction, data, types);
        }

        /// <summary>
        /// New Interface, Asnyc
        /// Call the stored procedure; InitializeStoredProcs must be called prior to using this method
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <param name="data">POCO object containing data to be sent to the stored procedure</param>
        /// <param name="types">Types of POCO objects that will be used to house any returned data</param>
        /// <returns>List of lists containing result data from stored proc</returns>
        public async Task<ResultsList> CallStoredProcAsync(CancellationToken token, T data, params Type[] types)
        {
            cancellationToken = token;
            return await CallStoredProcAsync(token, this.commandTimeout, this.Transaction, data, types);
        }

        /// <summary>
        /// New Interface, Asnyc
        /// Call the stored procedure; InitializeStoredProcs must be called prior to using this method
        /// </summary>
        /// <param name="token">Cancellation token</param>
        /// <param name="data">POCO object containing data to be sent to the stored procedure</param>
        /// <param name="types">Types of POCO objects that will be used to house any returned data</param>
        /// <returns>List of lists containing result data from stored proc</returns>
        public async Task<ResultsList> CallStoredProcAsync(DbTransaction transaction, T data, params Type[] types)
        {
            Transaction = transaction;
            return await CallStoredProcAsync(this.cancellationToken, this.commandTimeout, transaction, data, types);
        }

        /// <summary>
        /// New Interface, Asnyc
        /// Call the stored procedure; InitializeStoredProcs must be called prior to using this method
        /// </summary>
        /// <param name="token">Cancellation Token (optional) </param>
        /// <param name="CommandTimeout">Timout value for this command execution</param>
        /// <param name="data">POCO object containing data to be sent to the stored procedure</param>
        /// <param name="transaction">sql transaction in which to enroll the stored procedure call</param>
        /// <param name="types">Types of POCO objects that will be used to house any returned data</param>
        /// <returns>List of lists containing result data from stored proc</returns>
        public async Task<ResultsList> CallStoredProcAsync(CancellationToken token, int? CommandTimeout, DbTransaction transaction, T data, params Type[] types)
        {
            // protect ourselves from the old style of calling this 
            if (null == _context)
            {
                throw new Exception("Not Properly Initialized. Call InitializeStoredProcs in the DbContext constructor.");
            }

            if (null == types)
                types = new Type[] { };

            if (String.IsNullOrEmpty(procname))
            {
                SetupStoredProc(types);
            }

            cancellationToken = token;
            commandTimeout = CommandTimeout;
            Transaction = transaction;

            return await CodeFirstStoredProcs.CallStoredProcAsync(_context, this, token, CommandTimeout, transaction, data);
        }

        /// <summary>
        /// Constructor for new style interface. This is called by InitializeStoredProcs
        /// </summary>
        /// <param name="context">DbContext that this procedure call will use for database connectivity</param>
        public StoredProc(DbContext context)
            : base(context)
        {
            // save database context for processing
            _context = context;

            // initialize properties
            cancellationToken = CancellationToken.None;
            commandTimeout = null;
            Transaction = null;
        }

        //-----------------------------------------------------------------------------------------
        // Old Style Interface; kept for backwards compatibility
        //-----------------------------------------------------------------------------------------

        /// <summary>
        /// Constructor. Note that the return type objects must have a default constructor!
        /// </summary>
        /// <param name="types">Types returned by the stored procedure. Order is important!</param>
        public StoredProc(params Type[] types)
            : base()
        {
            schema = "dbo";

            // analyse return types
            SetupStoredProc(types);

            // initialize properties
            cancellationToken = CancellationToken.None;
            commandTimeout = null;
            Transaction = null;
        }

        /// <summary>
        /// Set the schema and proc name paramters from attributes and provided input type, and
        /// store the indicated return types for handling output from the stored proc call
        /// </summary>
        /// <param name="types">List of types that can be returned by the stored procedure</param>
        private void SetupStoredProc(Type[] types)
        {
            // set default schema if not set via attributes on the property in DbContext
            if (String.IsNullOrEmpty(schema))
            {
                schema = "dbo";

                // allow override by attribute on the input type object
                var schema_attr = typeof(T).GetAttribute<StoredProcAttributes.Schema>();
                if (null != schema_attr)
                    schema = schema_attr.Value;
            }

            // set proc name if it was not set on the property in DbContext
            if (String.IsNullOrEmpty(procname))
            {
                // set default proc name
                procname = typeof(T).Name;

                // allow override by attribute
                var procname_attr = typeof(T).GetAttribute<StoredProcAttributes.Name>();
                if (null != procname_attr)
                    procname = procname_attr.Value;
            }

            outputtypes.AddRange(types);
        }

        /// <summary>
        /// Contains a mapping of property names to parameter names. We do this since this mapping is complex; 
        /// i.e. the default parameter name may be overridden by the Name attribute
        /// </summary>
        internal Dictionary<String, String> MappedParams = new Dictionary<string, string>();

        /// <summary>
        /// Store output parameter values back into the data object
        /// </summary>
        /// <param name="parms">List of parameters</param>
        /// <param name="data">Source data object</param>
        internal void ProcessOutputParms(IEnumerable<SqlParameter> parms, T data)
        {
            // get the list of properties for this type
            PropertyInfo[] props = typeof(T).GetMappedProperties();

            // we want to write data back to properties for every non-input only parameter
            foreach (SqlParameter parm in parms
                .Where(p => p.Direction != ParameterDirection.Input)
                .Select(p => p))
            {
                // get the property name mapped to this parameter
                String propname = MappedParams.Where(p => p.Key == parm.ParameterName).Select(p => p.Value).First();

                // extract the matchingproperty and set its value
                PropertyInfo prop = props.Where(p => p.Name == propname).FirstOrDefault();

                // Store output parm value, handle null returns
                if (parm.Value.GetType() == typeof(System.DBNull))
                    prop.SetValue(data, null, null);
                else
                    prop.SetValue(data, parm.Value, null);
            }
        }

        /// <summary>
        /// Convert parameters from type T properties to SqlParameters
        /// </summary>
        /// <param name="data">Source data object</param>
        /// <returns></returns>
        internal IEnumerable<SqlParameter> Parameters(T data)
        {
            // clear the parameter to property mapping since we'll be recreating this
            MappedParams.Clear();

            // list of parameters we'll be returning
            List<SqlParameter> parms = new List<SqlParameter>();

            // properties that we're converting to parameters are everything without
            // a NotMapped attribute
            foreach (PropertyInfo p in typeof(T).GetMappedProperties())
            {
                //---------------------------------------------------------------------------------
                // process attributes
                //---------------------------------------------------------------------------------

                // create parameter and store default name - property name
                SqlParameter holder = new SqlParameter()
                {
                    ParameterName = p.Name
                };

                // override of parameter name by attribute
                var name = p.GetAttribute<StoredProcAttributes.Name>();
                if (null != name)
                    holder.ParameterName = name.Value;

                // save direction (default is input)
                var dir = p.GetAttribute<StoredProcAttributes.Direction>();
                if (null != dir)
                    holder.Direction = dir.Value;

                // save size
                var size = p.GetAttribute<StoredProcAttributes.Size>();
                if (null != size)
                    holder.Size = size.Value;

                // save database type of parameter
                var parmtype = p.GetAttribute<StoredProcAttributes.ParameterType>();
                if (null != parmtype)
                    holder.SqlDbType = parmtype.Value;

                // save user-defined type name
                var typename = p.GetAttribute<StoredProcAttributes.TypeName>();
                if (null != typename)
                    holder.TypeName = typename.Value;

                // save precision
                var precision = p.GetAttribute<StoredProcAttributes.Precision>();
                if (null != precision)
                    holder.Precision = precision.Value;

                // save scale
                var scale = p.GetAttribute<StoredProcAttributes.Scale>();
                if (null != scale)
                    holder.Scale = scale.Value;

                // get streaming 
                var stream = p.GetAttribute<StoredProcAttributes.StreamOutput>();

                // save the mapping between the parameter name and property name, since the parameter
                // name can be overridden
                MappedParams.Add(holder.ParameterName, p.Name);

                //---------------------------------------------------------------------------------
                // Save parameter value
                //---------------------------------------------------------------------------------

                // store table values, scalar value or null
                if (null == data)
                {
                    holder.Value = DBNull.Value;
                }
                else
                {
                    var value = p.GetValue(data, null);
                    if (value == null)
                    {
                        // set database null marker for null value
                        holder.Value = DBNull.Value;
                    }
                    else if (SqlDbType.Structured == holder.SqlDbType)
                    {
                        // catcher - tvp must be ienumerable type
                        if (!(value is IEnumerable))
                            throw new InvalidCastException(String.Format("{0} must be an IEnumerable Type", p.Name));

                        // set a null sqlparameter object for empty tables
                        if (0 == ((IList)value).Count)
                        {
                            holder.Value = DBNull.Value;
                        }
                        else
                        {
                            // ge the type underlying the IEnumerable
                            Type basetype = CodeFirstStoredProcHelpers.GetUnderlyingType(value.GetType());

                            // get the table valued parameter table type name
                            var schema = p.GetAttribute<StoredProcAttributes.Schema>();
                            if (null == schema && null != basetype)
                                schema = basetype.GetAttribute<StoredProcAttributes.Schema>();

                            var tvpname = p.GetAttribute<StoredProcAttributes.TableName>();
                            if (null == tvpname && null != basetype)
                                tvpname = basetype.GetAttribute<StoredProcAttributes.TableName>();

                            holder.TypeName = (null != schema) ? schema.Value : "dbo";
                            holder.TypeName += ".";
                            holder.TypeName += (null != tvpname) ? tvpname.Value : p.Name;

                            // generate table valued parameter
                            holder.Value = CodeFirstStoredProcHelpers.TableValuedParameter((IList)value);
                        }
                    }
                    else
                    {
                        // process normal scalar value
                        holder.Value = value;
                    }
                }

                // add parameter to list
                parms.Add(holder);
            }

            return parms;
        }

        /// <summary>
        /// Fluent API - assign owner (schema)
        /// </summary>
        /// <param name="owner"></param>
        /// <returns></returns>
        public new StoredProc<T> HasOwner(String owner)
        {
            base.HasOwner(owner);
            return this;
        }

        /// <summary>
        /// Fluent API - assign procedure name
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public new StoredProc<T> HasName(String name)
        {
            base.HasName(name);
            return this;
        }

        /// <summary>
        /// Fluent API - set the data types of resultsets returned by the stored procedure. 
        /// Order is important! Note that the return type objects must have a default constructor!
        /// </summary>
        /// <param name="types"></param>
        /// <returns></returns>
        public new StoredProc<T> ReturnsTypes(params Type[] types)
        {
            base.ReturnsTypes(types);
            return this;
        }

        /// <summary>
        /// Command time out  - limit the time spent on a transaction
        /// </summary>
        public new StoredProc<T> UseTimeout(int timeout)
        {
            commandTimeout = timeout;
            return this;
        }

        /// <summary>
        /// Cancellation Token, used to signal cancellation by a user or other process
        /// </summary>
        public new StoredProc<T> UseCancellationToken(CancellationToken token)
        {
            cancellationToken = token;
            return this;
        }

        /// <summary>
        /// Tranasaction to enroll the sqlcommand in; should not be required w/ EF 6, as enrollment 
        /// should be automatic by then.
        /// </summary>
        public new StoredProc<T> UseTransaction(SqlTransaction tran)
        {
            Transaction = tran;
            return this;
        }

        /// <summary>
        /// Tranasaction to enroll the sqlcommand in; should not be required w/ EF 6, as enrollment 
        /// should be automatic by then.
        /// </summary>
        public new StoredProc<T> UseTransaction(DbTransaction tran)
        {
            Transaction = tran;
            return this;
        }

        public new StoredProc<T> UseTransaction(DbContextTransaction tran)
        {
            return UseTransaction((DbTransaction)tran.UnderlyingTransaction);
        }

    }
}
