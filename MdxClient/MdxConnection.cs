using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.AnalysisServices.AdomdClient;
using System.Data;
using System.Data.Common;

namespace MdxClient
{
    /// <summary>
    /// Represents an open connection to a SQL Server Analysis Services database.
    /// </summary>
    public class MdxConnection : DbConnection
    {
        private AdomdConnection _connection;

        /// <summary>
        /// Initializes a new instance of the MdxConnection class.
        /// </summary>
        public MdxConnection()
        {
            _connection = new AdomdConnection();
        }

        /// <summary>
        /// Initializes a new instance of the MdxConnection class when given a string that contains the connection string.
        /// </summary>
        /// <param name="connectionString">The connection used to open the SQL Server Analysis Services database. 
        /// </param>
        public MdxConnection(string connectionString)
        {
            _connection = new AdomdConnection(connectionString);
        }

        internal MdxConnection(MdxConnection connection)
        {
            _connection = new AdomdConnection(connection.Connection);
        }

        internal AdomdConnection Connection
        { 
            get
            {
                return _connection;
            }
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            if (IsolationLevel.Unspecified == isolationLevel || IsolationLevel.ReadCommitted == isolationLevel)
            {
                return new MdxTransaction(this);
            }
            throw new NotSupportedException();
        }

        /// <summary>
        /// Changes the current database for an open MdxConnection.
        /// </summary>
        /// <param name="database">The name of the database to use instead of the current database.</param>
        public override void ChangeDatabase(string databaseName)
        {
            _connection.ChangeDatabase(databaseName);
        }

        /// <summary>
        /// Closes the connection to the database.
        /// </summary>
        public override void Close()
        {
            _connection.Close();
        }

        /// <summary>
        /// Gets or sets the string used to open a SQL Server Analysis Services database.
        /// </summary>
        public override string ConnectionString
        {
            get
            {
                return _connection.ConnectionString;
            }
            set
            {
                _connection.ConnectionString = value;
            }
        }

        /// <summary>
        /// Gets the time to wait while trying to establish a connection before terminating the attempt and generating an error.
        /// </summary>
        public override int ConnectionTimeout
        {
            get { return _connection.ConnectionTimeout; }
        }

        public new MdxCommand CreateCommand()
        {
            return (MdxCommand)base.CreateCommand();
        }

        protected override DbCommand CreateDbCommand()
        {
            return new MdxCommand { Connection = this };
        }

        public override string DataSource
        {
            get { return this.ConnectionString; }
        }

        /// <summary>
        /// Gets the name of the current database or the database to be used after a connection is opened.
        /// </summary>
        public override string Database
        {
            get { return _connection.Database; }
        }

        /// <summary>
        /// Opens a database connection with the property settings specified by the connection string.
        /// </summary>
        public override void Open()
        {
            _connection.Open();
        }

        /// <summary>
        /// Gets a string that represents the version of the server to which the object is connected.
        /// </summary>
        public override string ServerVersion
        {
            get { return _connection.ServerVersion; }
        }

        /// <summary>
        /// Gets the state of the MdxConnection.
        /// </summary>
        public override ConnectionState State
        {
            get { return _connection.State; }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                try
                {
                    _connection.Dispose();
                }
                catch (Exception e)
                {
                    throw new SystemException("An exception of type " + e.GetType() + " was encountered while closing the MdxTransaction.");
                }
            }

            base.Dispose(disposing);
        }
    }    
}
