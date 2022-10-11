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
    /// Contains extension methods to Code First database objects for Stored Procedure processing
    /// Updated to include support for streaming and for async
    /// </summary>
    public static class CodeFirstStoredProcs
    {
        /// <summary>
        /// New Interface 
        /// Locate and initialize all the stored proc properties in this DbContext. This should be 
        /// called in the DbContext constructor.
        /// </summary>
        /// <param name="context"></param>
        public static void InitializeStoredProcs(this DbContext context)
        {
            Type contexttype = context.GetType();
            foreach (PropertyInfo proc in contexttype.GetProperties()
                .Where(p => p.PropertyType.Name.Contains("StoredProc")))
            {
                // create StoredProc object and save in DbContext property
                object m = proc.PropertyType.GetConstructor(new Type[] { contexttype }).Invoke(new DbContext[] { context });
                proc.SetValue(context, m);

                // see if there is a Name attribute on this property
                var nameattr = proc.GetAttribute<StoredProcAttributes.Name>();
                if (null != nameattr)
                {
                    ((StoredProc)m).HasName(nameattr.Value);
                }

                // see if there is a Schema attribute on this property
                var schemaattr = proc.GetAttribute<StoredProcAttributes.Schema>();
                if (null != schemaattr)
                {
                    ((StoredProc)m).HasOwner(schemaattr.Value);
                }

                // see if there is a ReturnTypes attribute on this property
                var typesattr = proc.GetAttribute<StoredProcAttributes.ReturnTypes>();
                if (null != typesattr)
                {
                    ((StoredProc)m).ReturnsTypes(typesattr.Returns);
                }

            }
        }

        /// <summary>
        /// Set up the DBCommand object, adding in options and parameters
        /// </summary>
        /// <param name="procname">Name of the stored proc</param>
        /// <param name="parms">SQLParameters to pass. Override: no parm created for TVP that is DBNull</param>
        /// <param name="CommandTimeout">Set Command Timeout Override</param>
        /// <param name="transaction">Transaction in which to enroll this proc call</param>
        /// <param name="cmd">DBCommand object representing the command to the database</param>
        private static void SetupStoredProcCall(String procname, IEnumerable<SqlParameter> parms, int? CommandTimeout, DbTransaction transaction, DbCommand cmd)
        {
            // command to execute is our stored procedure
            cmd.Transaction = transaction;
            cmd.CommandText = procname;
            cmd.CommandType = System.Data.CommandType.StoredProcedure;

            // Assign command timeout value, if one was provided
            cmd.CommandTimeout = null == CommandTimeout ? cmd.CommandTimeout : (int)CommandTimeout;

            // move parameters to command object
            if (null != parms)
                foreach (SqlParameter p in parms)
                {
                    // Don't send any parm for null table-valued parameters
                    if (!(SqlDbType.Structured == p.SqlDbType && DBNull.Value == p.Value))
                    {
                        cmd.Parameters.Add(p);
                    }
                }
        }

        /// <summary>
        /// Generic Typed version of calling a stored procedure - original interface
        /// </summary>
        /// <typeparam name="T">Type of object containing the parameter data</typeparam>
        /// <param name="context">Database Context to use for the call</param>
        /// <param name="procedure">Generic Typed stored procedure object</param>
        /// <param name="data">The actual object containing the parameter data</param>
        /// <returns></returns>
        public static ResultsList CallStoredProc<T>(this DbContext context, StoredProc<T> procedure, T data)
        {
            IEnumerable<SqlParameter> parms = procedure.Parameters(data);
            CancellationToken token = new CancellationToken();
            ResultsList results = Task<ResultsList>.Run<ResultsList>(() =>
                context.ReadFromStoredProcAsync(procedure.fullname, token, parms, null, procedure.commandBehavior, procedure.Transaction, procedure.returntypes)
                ).Result;
            procedure.ProcessOutputParms(parms, data);
            return results ?? new ResultsList();
        }

        /// <summary>
        /// Call a stored procedure, passing in the stored procedure object and a list of parameters - original interface
        /// </summary>
        /// <param name="context">Database context used for the call</param>
        /// <param name="procedure">Stored Procedure</param>
        /// <param name="parms">List of parameters</param>
        /// <returns></returns>
        public static ResultsList CallStoredProc(this DbContext context, StoredProc procedure, IEnumerable<SqlParameter> parms = null)
        {
            CancellationToken token = new CancellationToken();
            ResultsList results = Task<ResultsList>.Run<ResultsList>(() =>
                context.ReadFromStoredProcAsync(procedure.fullname, token, parms, null, procedure.commandBehavior, procedure.Transaction, procedure.returntypes)
                ).Result;
            return results ?? new ResultsList();
        }

        /// <summary>
        /// Generic Typed version of calling a stored procedure; add timeout parameter
        /// </summary>
        /// <typeparam name="T">Type of object containing the parameter data</typeparam>
        /// <param name="context">Database Context to use for the call</param>
        /// <param name="procedure">Generic Typed stored procedure object</param>
        /// <param name="CommandTimeout">Timeout for stored procedure call</param>
        /// <param name="data">The actual object containing the parameter data</param>
        /// <returns></returns>
        public static ResultsList CallStoredProc<T>(this DbContext context, StoredProc<T> procedure, int? CommandTimeout, T data)
        {
            IEnumerable<SqlParameter> parms = procedure.Parameters(data);
            CancellationToken token = new CancellationToken();
            ResultsList results = Task<ResultsList>.Run<ResultsList>(() =>
                context.ReadFromStoredProcAsync(procedure.fullname, token, parms, CommandTimeout, procedure.commandBehavior, procedure.Transaction, procedure.returntypes)
                ).Result;
            procedure.ProcessOutputParms(parms, data);
            return results ?? new ResultsList();
        }

        /// <summary>
        /// Generic Typed version of calling a stored procedure; add timeout parameter
        /// </summary>
        /// <typeparam name="T">Type of object containing the parameter data</typeparam>
        /// <param name="context">Database Context to use for the call</param>
        /// <param name="procedure">Generic Typed stored procedure object</param>
        /// <param name="CommandTimeout">Timeout for stored procedure call</param>
        /// <param name="transaction">Sql transaction in which to enroll the stored procedure call</param>
        /// <param name="data">The actual object containing the parameter data</param>
        /// <returns></returns>
        public static ResultsList CallStoredProc<T>(this DbContext context, StoredProc<T> procedure, int? CommandTimeout, DbTransaction transaction, T data)
        {
            IEnumerable<SqlParameter> parms = procedure.Parameters(data);
            CancellationToken token = new CancellationToken();
            ResultsList results = Task<ResultsList>.Run<ResultsList>(() =>
                context.ReadFromStoredProcAsync(procedure.fullname, token, parms, CommandTimeout, procedure.commandBehavior, transaction, procedure.returntypes)
                ).Result;
            procedure.ProcessOutputParms(parms, data);
            return results ?? new ResultsList();
        }

        /// <summary>
        /// Call a stored procedure, passing in the stored procedure object and a list of parameters; add timeout parameter to call 
        /// </summary>
        /// <param name="context">Database context used for the call</param>
        /// <param name="CommandTimeout">Timeout for stored procedure call</param>
        /// <param name="procedure">Stored Procedure</param>
        /// <param name="parms">List of parameters</param>
        /// <returns></returns>
        public static ResultsList CallStoredProc(this DbContext context, StoredProc procedure, int? CommandTimeout, IEnumerable<SqlParameter> parms = null)
        {
            CancellationToken token = new CancellationToken();
            ResultsList results = Task<ResultsList>.Run<ResultsList>(() =>
                context.ReadFromStoredProcAsync(procedure.fullname, token, parms, CommandTimeout, procedure.commandBehavior, procedure.Transaction, procedure.returntypes)
                ).Result;
            return results ?? new ResultsList();
        }

        /// <summary>
        /// Call a stored procedure, passing in the stored procedure object and a list of parameters; add timeout parameter to call 
        /// </summary>
        /// <param name="context">Database context used for the call</param>
        /// <param name="procedure">Stored Procedure</param>
        /// <param name="CommandTimeout">Timeout for stored procedure call</param>
        /// <param name="transaction">Sql transaction in which to enroll the stored procedure call</param>
        /// <param name="parms">List of parameters</param>
        /// <returns></returns>
        public static ResultsList CallStoredProc(this DbContext context, StoredProc procedure, int? CommandTimeout, CommandBehavior commandbehavior, DbTransaction transaction, IEnumerable<SqlParameter> parms = null)
        {
            CancellationToken token = new CancellationToken();
            ResultsList results = Task<ResultsList>.Run<ResultsList>(() =>
                context.ReadFromStoredProcAsync(procedure.fullname, token, parms, CommandTimeout, commandbehavior, transaction, procedure.returntypes)
                ).Result;
            return results ?? new ResultsList();
        }

        /// <summary>
        /// Generic Typed version of calling a stored procedure - async interface
        /// </summary>
        /// <typeparam name="T">Type of object containing the parameter data</typeparam>
        /// <param name="context">Database Context to use for the call</param>
        /// <param name="procedure">Generic Typed stored procedure object</param>
        /// <param name="data">The actual object containing the parameter data</param>
        /// <returns></returns>
        public static async Task<ResultsList> CallStoredProcAsync<T>(this DbContext context, StoredProc<T> procedure, T data)
        {
            IEnumerable<SqlParameter> parms = procedure.Parameters(data);
            ResultsList results = await context.ReadFromStoredProcAsync(procedure.fullname, CancellationToken.None, parms, null, procedure.commandBehavior, procedure.Transaction, procedure.returntypes);
            procedure.ProcessOutputParms(parms, data);
            return results ?? new ResultsList();
        }

        /// <summary>
        /// Call a stored procedure, passing in the stored procedure object and a list of parameters - async interface
        /// </summary>
        /// <param name="context">Database context used for the call</param>
        /// <param name="procedure">Stored Procedure</param>
        /// <param name="parms">List of parameters</param>
        /// <returns></returns>
        public static async Task<ResultsList> CallStoredProcAsync(this DbContext context, StoredProc procedure, IEnumerable<SqlParameter> parms = null)
        {
            ResultsList results = await context.ReadFromStoredProcAsync(procedure.fullname, CancellationToken.None, parms, null, procedure.commandBehavior, procedure.Transaction, procedure.returntypes);
            return results ?? new ResultsList();
        }

        /// <summary>
        /// Generic Typed version of calling a stored procedure; add timeout parameter - async interface
        /// </summary>
        /// <typeparam name="T">Type of object containing the parameter data</typeparam>
        /// <param name="context">Database Context to use for the call</param>
        /// <param name="procedure">Generic Typed stored procedure object</param>
        /// <param name="token">Cancellation token for asyc process cancellation</param>
        /// <param name="CommandTimeout">Timeout for stored procedure call</param>
        /// <param name="transaction">Sql transaction in which to enroll the stored procedure call</param>
        /// <param name="data">The actual object containing the parameter data</param>
        /// <returns></returns>
        public static async Task<ResultsList> CallStoredProcAsync<T>(this DbContext context, StoredProc<T> procedure, CancellationToken token, int? CommandTimeout, DbTransaction transaction, T data)
        {
            IEnumerable<SqlParameter> parms = procedure.Parameters(data);
            ResultsList results = await context.ReadFromStoredProcAsync(procedure.fullname, token, parms, CommandTimeout, procedure.commandBehavior, transaction, procedure.returntypes);
            procedure.ProcessOutputParms(parms, data);
            return results ?? new ResultsList();
        }

        /// <summary>
        /// Call a stored procedure, passing in the stored procedure object and a list of parameters; add timeout parameter to call - async interface
        /// </summary>
        /// <param name="context">Database context used for the call</param>
        /// <param name="procedure">Stored Procedure</param>
        /// <param name="token">Cancellation token for asyc process cancellation</param>
        /// <param name="CommandTimeout">Timeout for stored procedure call</param>
        /// <param name="transaction">Sql transaction in which to enroll the stored procedure call</param>
        /// <param name="parms">List of parameters</param>
        /// <returns></returns>
        public static async Task<ResultsList> CallStoredProcAsync(this DbContext context, StoredProc procedure, CancellationToken token, int? CommandTimeout, DbTransaction transaction, IEnumerable<SqlParameter> parms = null)
        {
            ResultsList results = await context.ReadFromStoredProcAsync(procedure.fullname, token, parms, CommandTimeout, procedure.commandBehavior, transaction, procedure.returntypes);
            return results ?? new ResultsList();
        }

        /// <summary>
        /// internal
        /// 
        /// Call a stored procedure and get results back. - async version
        /// </summary>
        /// <param name="context">Code First database context object</param>
        /// <param name="procname">Qualified name of proc to call</param>
        /// <param name="token">Cancellation token for asyc process cancellation</param>
        /// <param name="parms">List of ParameterHolder objects - input and output parameters</param>
        /// <param name="CommandTimeout">Timeout for stored procedure call</param>
        /// <param name="transaction">Sql transaction in which to enroll the stored procedure call</param>
        /// <param name="outputtypes">List of types to expect in return. Each type *must* have a default constructor.</param>
        /// <returns></returns>
        internal static async Task<ResultsList> ReadFromStoredProcAsync(this DbContext context,
            String procname,
            CancellationToken token,
            IEnumerable<SqlParameter> parms = null,
            int? CommandTimeout = null,
            CommandBehavior commandbehavior = CommandBehavior.Default,
            DbTransaction transaction = null,
            params Type[] outputtypes)
        {
            // create our output set object
            ResultsList results = new ResultsList();

            // ensure that we have a type list, even if it's empty
            IEnumerator currenttype = (null == outputtypes) ?
                new Type[0].GetEnumerator() :
                outputtypes.GetEnumerator();

            // handle to the database connection object
            var connection = context.Database.Connection;
            Boolean closeconnection = false;

            try
            {
                // open the connect for use and create a command object
                if (connection.State != ConnectionState.Open)
                {
                    await connection.OpenAsync(token);
                    closeconnection = true;
                }

                // create command object and execute
                using (var cmd = connection.CreateCommand())
                {
                    // set up command object and add parms
                    SetupStoredProcCall(procname, parms, CommandTimeout, transaction, cmd);

                    // Do It! This actually makes the database call
                    var reader = await cmd.ExecuteReaderAsync(commandbehavior, token);

                    // get the type we're expecting for the first result. If no types specified,
                    // ignore all results
                    if (currenttype.MoveNext())
                    {
                        // process results - repeat this loop for each result set returned by the stored proc
                        // for which we have a result type specified
                        do
                        {
                            // list which property will be used for each field in the result set
                            PropertyInfo[] propertylist = null;

                            // create a destination for our results
                            List<object> current = new List<object>();

                            // process the result set
                            while (await reader.ReadAsync(token))
                            {
                                // create an object to hold this result
                                object item = ((Type)currenttype.Current).GetConstructor(System.Type.EmptyTypes).Invoke(new object[0]);

                                // build propertylist the first time through
                                propertylist = propertylist ?? reader.MatchRecordProperties(item, currenttype);

                                // copy data elements by parameter name from result to destination object
                                await reader.ReadRecordAsync(item, propertylist, token);

                                // add newly populated item to our output list
                                current.Add(item);
                            }

                            // add this result set to our return list
                            results.Add(current);
                        }
                        while (await reader.NextResultAsync(token) && currenttype.MoveNext());
                    }

                    // close up the reader, we're done saving results
                    reader.Close();
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Error reading from stored proc " + procname + ": " + ex.Message, ex);
            }
            finally
            {
                if (closeconnection)
                    connection.Close();
            }

            return results;
        }
    }
}
