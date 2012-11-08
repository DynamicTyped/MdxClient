using System;
using System.Data;
using System.Data.Common;
using System.ComponentModel;

namespace MdxClient
{
    /// <summary>
    /// Represents a set of data commands and a database connection that are used to fill the System.Data.DataSet and update a 
    /// SQL Server Analysis Services database.
    /// </summary>
    public class MdxDataAdapter : DbDataAdapter, IDbDataAdapter, IDataAdapter
    {
        /// <summary>
        /// Gets or sets an MDX statement used to select records in the data source.
        /// </summary>
        public new MdxCommand SelectCommand { get; set; }

        IDbCommand IDbDataAdapter.SelectCommand
        {
            get
            {
                return this.SelectCommand;
            }
            set
            {
                this.SelectCommand = (MdxCommand) value;
            }
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public new MdxCommand DeleteCommand
        {
            get
            {
                return null;
            }
            set
            {
                if (value != null)
                {
                    throw new NotSupportedException();
                }
            }
        }

        IDbCommand IDbDataAdapter.DeleteCommand
        {
            get
            {
                return null;
            }
            set
            {
                if (value != null)
                {
                    throw new NotSupportedException();
                }
            }
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public new MdxCommand InsertCommand
        {
            get
            {
                return null;
            }
            set
            {
                if (value != null)
                {
                    throw new NotSupportedException();
                }
            }
        }

        IDbCommand IDbDataAdapter.InsertCommand
        {
            get
            {
                return null;
            }
            set
            {
                if (value != null)
                {
                    throw new NotSupportedException();
                }
            }
        }

        [EditorBrowsable(EditorBrowsableState.Never)]
        public new MdxCommand UpdateCommand
        {
            get
            {
                return null;
            }
            set
            {
                if (value != null)
                {
                    throw new NotSupportedException();
                }
            }
        }

        IDbCommand IDbDataAdapter.UpdateCommand
        {
            get
            {
                return null;
            }
            set
            {
                if (value != null)
                {
                    throw new NotSupportedException();
                }
            }
        }

        /// <summary>
        /// Initializes a new instance of the MdxDataAdapter class.
        /// </summary>
        public MdxDataAdapter()
        { }

        /// <summary>
        /// Initializes a new instance of the MdxDataAdapter class with the specified MdxCommand.
        /// </summary>
        /// <param name="selectCommandText">An MdxCommand to be used by the MdxDataAdapter.SelectCommand property.</param>
        public MdxDataAdapter(MdxCommand selectCommand)
        {
            this.SelectCommand = selectCommand;
        }

        /// <summary>
        /// Initializes a new instance of the MdxDataAdapter class with the specified MdxCommand and MdxConnection.
        /// </summary>
        /// <param name="selectCommandText">The MDX statement to be used by the MdxDataAdapter.SelectCommand property.</param>
        /// <param name="selectConnection">An MdxConnection representing the connection.</param>
        public MdxDataAdapter(string selectCommandText, MdxConnection selectConnection)
        {
            MdxCommand command = new MdxCommand(selectCommandText);
            command.Connection = selectConnection;
            this.SelectCommand = command;
        }

        /// <summary>
        /// Initializes a new instance of the MdxDataAdapter class with the specified MdxCommand and MdxConnection.
        /// </summary>
        /// <param name="selectCommandText">The MDX statement to be used by the MdxDataAdapter.SelectCommand property.</param>        
        /// <param name="selectConnectionString">The connection string.</param>
        public MdxDataAdapter(string selectCommandText, string selectConnectionString) 
            : this(selectCommandText, new MdxConnection(selectConnectionString))
        { }

        protected override RowUpdatedEventArgs CreateRowUpdatedEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
        {
            throw new NotSupportedException();
        }

        protected override RowUpdatingEventArgs CreateRowUpdatingEvent(DataRow dataRow, IDbCommand command, StatementType statementType, DataTableMapping tableMapping)
        {
            throw new NotSupportedException();
        }

        protected override void OnRowUpdated(RowUpdatedEventArgs value)
        {
            throw new NotSupportedException();
        }

        protected override void OnRowUpdating(RowUpdatingEventArgs value)
        {
            throw new NotSupportedException();
        }

        public override int Update(DataSet dataSet)
        {
            throw new NotSupportedException();
        }

        protected override int Update(DataRow[] dataRows, DataTableMapping tableMapping)
        {
            throw new NotSupportedException();
        }
    }
}