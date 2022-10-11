using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CodeFirstStoredProcs
{
    /// <summary>
    /// Represents a Stored Procedure in the database. Note that the return type objects
    /// must have a default constructor!
    /// </summary>
    public class StoredProc
    {
        // store a db context
        internal DbContext _context { get; set; }

        /// <summary>
        /// Database owner of this object
        /// </summary>
        public String schema { get; set; }

        /// <summary>
        /// Name of the stored procedure
        /// </summary>
        public String procname { get; set; }

        /// <summary>
        /// Fluent API - assign owner (schema)
        /// </summary>
        /// <param name="owner"></param>
        /// <returns></returns>
        public StoredProc HasOwner(String owner)
        {
            schema = owner;
            return this;
        }

        /// <summary>
        /// Fluent API - assign procedure name
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public StoredProc HasName(String name)
        {
            procname = name;
            return this;
        }

        /// <summary>
        /// Fluent API - set the data types of resultsets returned by the stored procedure. 
        /// Order is important! Note that the return type objects must have a default constructor!
        /// </summary>
        /// <param name="types"></param>
        /// <returns></returns>
        public StoredProc ReturnsTypes(params Type[] types)
        {
            outputtypes.AddRange(types);
            return this;
        }

        /// <summary>
        /// Command Behavior for 
        /// </summary>
        public CommandBehavior commandBehavior { get; set; }

        /// <summary>
        /// Get the fully (schema plus owner) name of the stored procedure
        /// </summary>
        internal String fullname
        {
            get { return schema + "." + procname; }
        }

        /// <summary>
        /// Command time out  - limit the time spent on a transaction
        /// </summary>
        public int? commandTimeout { get; set; }
        public StoredProc UseTimeout(int timeout)
        {
            commandTimeout = timeout;
            return this;
        }

        /// <summary>
        /// Cancellation Token, used to signal cancellation by a user or other process
        /// </summary>
        public CancellationToken cancellationToken { get; set; }
        public StoredProc UseCancellationToken(CancellationToken token)
        {
            cancellationToken = token;
            return this;
        }

        /// <summary>
        /// Tranasaction to enroll the sqlcommand in; should not be required w/ EF 6, as enrollment 
        /// should be automatic by then.
        /// </summary>
        public StoredProc UseTransaction(SqlTransaction tran)
        {
            Transaction = tran;
            return this;
        }

        /// <summary>
        /// Tranasaction to enroll the sqlcommand in; required if using
        /// connection.BeginTransaction or database.BeginTransaction instead of TransactionScope.
        /// </summary>
        public DbTransaction Transaction { get; set; }

        /// <summary>
        /// Compatibility with previous versions - replace public variable
        /// with property that sets the Transaction value
        /// </summary>
        public SqlTransaction sqlTransaction
        {
            get
            {
                return (SqlTransaction)Transaction;
            }

            set
            {
                Transaction = (DbTransaction)value;
            }

        }

        public StoredProc UseTransaction(DbTransaction tran)
        {
            Transaction = tran;
            return this;
        }

        public StoredProc UseTransaction(DbContextTransaction tran)
        {
            return UseTransaction((DbTransaction)tran.UnderlyingTransaction);
        }

        //-----------------------------------------------------------------------------------------
        // New style interface
        //-----------------------------------------------------------------------------------------
        public StoredProc(DbContext context)
        {
            // save database context for processing
            _context = context;

            // default values 
            cancellationToken = CancellationToken.None;
            commandTimeout = null;
            Transaction = null;
        }

        /// <summary>
        /// New Interface
        /// Execute the stored proc. Note that the proc name must be set elsewhere, by HasName directly,
        /// either during setup using the fluent interface or in code prior to it's first use.
        /// </summary>
        /// <param name="types"></param>
        /// <returns></returns>
        public ResultsList CallStoredProc(params Type[] types)
        {
            return CallStoredProc(this.commandTimeout, types);
        }

        /// <summary>
        /// New Interface
        /// Execute the stored proc. Note that the proc name must be set elsewhere, by HasName directly,
        /// either during setup using the fluent interface or in code prior to it's first use.
        /// </summary>
        /// <param name="CommandTimeout">Timeout value for this command execution</param>
        /// <param name="types"></param>
        /// <returns></returns>
        public ResultsList CallStoredProc(int? CommandTimeout, params Type[] types)
        {
            return CallStoredProc(CommandTimeout, this.Transaction, types);
        }

        /// <summary>
        /// New Interface
        /// Execute the stored proc. Note that the proc name must be set elsewhere, by HasName directly,
        /// either during setup using the fluent interface or in code prior to it's first use.
        /// </summary>
        /// <param name="CommandTimeout">Timeout value for this command execution</param>
        /// <param name="types"></param>
        /// <returns></returns>
        public ResultsList CallStoredProc(int? CommandTimeout, DbTransaction transaction, params Type[] types)
        {
            // protect ourselves from the old style of calling this 
            if (null == _context)
            {
                throw new Exception("Not Properly Initialized. Call InitializeStoredProcs in the DbContext constructor.");
            }

            if (String.IsNullOrEmpty(procname))
            {
                throw new Exception("Not properly Initialized. Missing stored procedure name.");
            }

            if (null != types)
                outputtypes.AddRange(types);

            commandTimeout = CommandTimeout;
            Transaction = transaction;

            return CodeFirstStoredProcs.CallStoredProc(_context, this, CommandTimeout, this.commandBehavior, this.Transaction);
        }

        /// <summary>
        /// New Interface, Async
        /// Execute the stored proc. Note that the proc name must be set elsewhere, by HasName directly,
        /// either during setup using the fluent interface or in code prior to it's first use.
        /// </summary>
        /// <param name="types">List of output types from the stored proc</param>
        /// <returns></returns>
        public async Task<ResultsList> CallStoredProcAsync(params Type[] types)
        {
            return await CallStoredProcAsync(this.cancellationToken, this.commandTimeout, this.Transaction, types);
        }

        /// <summary>
        /// New Interface, Async
        /// Execute the stored proc. Note that the proc name must be set elsewhere, by HasName directly,
        /// either during setup using the fluent interface or in code prior to it's first use.
        /// </summary>
        /// <param name="token">Cancellation token (optional) </param>
        /// <param name="types">List of output types from the stored proc</param>
        /// <returns></returns>
        public async Task<ResultsList> CallStoredProcAsync(CancellationToken token, params Type[] types)
        {
            return await CallStoredProcAsync(token, this.commandTimeout, this.Transaction, types);
        }

        /// <summary>
        /// New Interface, Async
        /// Execute the stored proc. Note that the proc name must be set elsewhere, by HasName directly,
        /// either during setup using the fluent interface or in code prior to it's first use.
        /// </summary>
        /// <param name="CommandTimeout">Timeout value for this command execution</param>
        /// <param name="types">List of output types from the stored proc</param>
        /// <returns></returns>
        public async Task<ResultsList> CallStoredProcAsync(int? CommandTimeout, params Type[] types)
        {
            return await CallStoredProcAsync(this.cancellationToken, CommandTimeout, this.Transaction, types);
        }

        /// <summary>
        /// New Interface, Async
        /// Execute the stored proc. Note that the proc name must be set elsewhere, by HasName directly,
        /// either during setup using the fluent interface or in code prior to it's first use.
        /// </summary>
        /// <param name="CommandTimeout">Timeout value for this command execution</param>
        /// <param name="types">List of output types from the stored proc</param>
        /// <returns></returns>
        public async Task<ResultsList> CallStoredProcAsync(DbTransaction transaction, params Type[] types)
        {
            return await CallStoredProcAsync(this.cancellationToken, this.commandTimeout, transaction, types);
        }

        /// <summary>
        /// New Interface, Async
        /// Execute the stored proc. Note that the proc name must be set elsewhere, by HasName directly,
        /// either during setup using the fluent interface or in code prior to it's first use.
        /// </summary>
        /// <param name="token">Cancellation token (optional) </param>
        /// <param name="CommandTimeout">Timeout value for this execution</param>
        /// <param name="transaction">sql transaction in which to enroll the stored procedure call</param>
        /// <param name="types">List of output types from the stored proc</param>
        /// <returns></returns>
        public async Task<ResultsList> CallStoredProcAsync(CancellationToken token, int? CommandTimeout, DbTransaction transaction, params Type[] types)
        {
            // protect ourselves from the old style of calling this 
            if (null == _context)
            {
                throw new Exception("Not Properly Initialized. Call InitializeStoredProcs in the DbContext constructor.");
            }

            if (String.IsNullOrEmpty(procname))
            {
                throw new Exception("Not properly Initialized. Missing stored procedure name.");
            }

            if (null != types)
                outputtypes.AddRange(types);

            cancellationToken = token;
            commandTimeout = CommandTimeout;
            Transaction = transaction;

            return await CodeFirstStoredProcs.CallStoredProcAsync(_context, this, token, CommandTimeout, transaction);
        }

        //-----------------------------------------------------------------------------------------
        // Original constructors
        //-----------------------------------------------------------------------------------------

        public StoredProc()
        {
            schema = "dbo";
        }

        public StoredProc(String name)
        {
            schema = "dbo";
            procname = name;
        }

        public StoredProc(String name, params Type[] types)
        {
            schema = "dbo";
            procname = name;
            outputtypes.AddRange(types);
        }

        public StoredProc(String owner, String name, params Type[] types)
        {
            schema = owner;
            procname = name;
            outputtypes.AddRange(types);
        }

        /// <summary>
        /// List of data types that this stored procedure returns as result sets. 
        /// Order is important!
        /// </summary>
        internal List<Type> outputtypes = new List<Type>();

        /// <summary>
        /// Get an array of types returned
        /// </summary>
        internal Type[] returntypes
        {
            get { return outputtypes.ToArray(); }
        }
    }
}
