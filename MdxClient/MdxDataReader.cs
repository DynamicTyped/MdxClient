using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.AnalysisServices.AdomdClient;
using System.Data;
using System.Globalization;
using System.Collections;
using System.Data.Common;

namespace MdxClient
{
    public class MdxDataReader : DbDataReader
    {
        private bool _open = true;
        private int _currentRow = -1;

        private ResultSet _resultSet;
        private MdxConnection _connection = null;
        private DataTable _schemaTable;
        private static string[] _schemaTableColumnNames = new string[]
		{
			"ColumnName", 
			"ColumnOrdinal", 
			"ColumnSize", 
			"NumericPrecision", 
			"NumericScale", 
			"DataType", 
			"ProviderType", 
			"IsLong", 
			"AllowDBNull", 
			"IsReadOnly", 
			"IsRowVersion", 
			"IsUnique", 
			"IsKeyColumn", 
			"IsAutoIncrement", 
			"BaseSchemaName", 
			"BaseCatalogName", 
			"BaseTableName", 
			"BaseColumnName"
		};
        private static Type[] _schemaTableColumnTypes = new Type[]
		{
			typeof(string), 
			typeof(int), 
			typeof(int), 
			typeof(int), 
			typeof(int), 
			typeof(Type), 
			typeof(object), 
			typeof(bool), 
			typeof(bool), 
			typeof(bool), 
			typeof(bool), 
			typeof(bool), 
			typeof(bool), 
			typeof(bool), 
			typeof(string), 
			typeof(string), 
			typeof(string), 
			typeof(string)
		};

        internal MdxDataReader(ResultSet resultSet)
        {
            _resultSet = resultSet;
            _schemaTable = CreateSchemaTable();
        }

        internal MdxDataReader(ResultSet resultSet, MdxConnection connection)
        {
            _resultSet = resultSet;
            _connection = connection;
            _schemaTable = CreateSchemaTable();
        }

        /// <summary>
        /// Closes the MdxDataReader object.
        /// </summary>
        public override void Close()
        {
            _open = false;
        }

        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.  This always returns 0.
        /// </summary>
        public override int Depth
        {
            get { return 0; }
        }

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        public override int FieldCount
        {
            get { return _resultSet.Columns.Count; }
        }

        /// <summary>
        /// Gets the value of the specified column as a Boolean.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override bool GetBoolean(int ordinal)
        {
            return GetType<bool>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a byte.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override byte GetByte(int ordinal)
        {
            return GetType<byte>(ordinal);
        }

        public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets the value of the specified column as a single character.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override char GetChar(int ordinal)
        {
            return GetType<char>(ordinal);
        }

        public override long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Gets name of the data type of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override string GetDataTypeName(int ordinal)
        {
            return _resultSet.Columns[ordinal].Type.ToString();
        }

        /// <summary>
        /// Gets the value of the specified column as a DateTime object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override DateTime GetDateTime(int ordinal)
        {
            return GetType<DateTime>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a Decimal object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override decimal GetDecimal(int ordinal)
        {
            return GetType<decimal>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a double-precision floating point number.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override double GetDouble(int ordinal)
        {
            return GetType<double>(ordinal);
        }

        /// <summary>
        /// Returns an IEnumerator that can be used to iterate through the rows in the data reader.
        /// </summary>
        public override IEnumerator GetEnumerator()
        {
            return new DbEnumerator(this, (_connection != null));
        }

        /// <summary>
        /// Gets the data type of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override Type GetFieldType(int ordinal)
        {
            return _resultSet.Columns[ordinal].Type;
        }

        /// <summary>
        /// Gets the value of the specified column as a single-precision floating point number.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override float GetFloat(int ordinal)
        {
            return GetType<float>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a globally-unique identifier (GUID).
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override Guid GetGuid(int ordinal)
        {
            return GetType<Guid>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a 16-bit signed integer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override short GetInt16(int ordinal)
        {
            return GetType<short>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a 32-bit signed integer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override int GetInt32(int ordinal)
        {
            return GetType<int>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as a 64-bit signed integer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override long GetInt64(int ordinal)
        {
            return GetType<long>(ordinal);
        }

        /// <summary>
        /// Gets the name of the column, given the zero-based column ordinal.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override string GetName(int ordinal)
        {
            return _resultSet.Columns[ordinal].Name;
        }

        /// <summary>
        /// Gets the column ordinal given the name of the column.
        /// </summary>
        /// <param name="name">The name of the column.</param>
        public override int GetOrdinal(string name)
        {
            var column = _resultSet.Columns.Select((a, i) => new { Name = a.Name, Index = i }).SingleOrDefault(a => a.Name == name);
            if (null != column)
            {
                return column.Index;
            }
            
            throw new ArgumentException("Could not find specified column in results");
        }

        /// <summary>
        /// Returns a DataTable that describes the column metadata of the MdxDataReader.
        /// </summary>
        public override DataTable GetSchemaTable()
        {
            return _schemaTable;
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of String.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override string GetString(int ordinal)
        {
            return GetType<string>(ordinal);
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of Object.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override object GetValue(int ordinal)
        {
            return GetType<object>(ordinal);
        }

        /// <summary>
        /// Populates an array of objects with the column values of the current row.
        /// </summary>
        /// <param name="values">An array of Object into which to copy the attribute columns.</param>
        public override int GetValues(object[] values)
        {
            int i = 0;
            if (null != values)
            {
                for (; i < values.Length && i < _resultSet.Columns.Count; i++)
                {
                    values[i] = _resultSet.Rows[_currentRow].Cells[i].Value;
                }
            }

            return i;
        }

        /// <summary>
        /// Gets a value that indicates whether this MdxDataReader contains one or more rows.
        /// </summary>
        public override bool HasRows
        {
            get { return (_resultSet.Rows.Count > 0); }
        }

        /// <summary>
        /// Gets a value indicating whether the MdxDataReader is closed.
        /// </summary>
        public override bool IsClosed
        {
            get { return !_open; }
        }

        /// <summary>
        /// Gets a value that indicates whether the column contains nonexistent or missing values.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        public override bool IsDBNull(int ordinal)
        {
            return _resultSet.Rows[_currentRow].Cells[ordinal] == null;
        }

        /// <summary>
        /// Advances the reader to the next result when reading the results of a batch of statements.  This always returns false.
        /// </summary>
        public override bool NextResult()
        {
            return false;
        }

        /// <summary>
        /// Advances the reader to the next record in a result set.
        /// </summary>
        public override bool Read()
        {
            if (++_currentRow >= _resultSet.Rows.Count)
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        public override int RecordsAffected
        {
            get { throw new NotSupportedException(); }
        }

        public override object this[string name]
        {
            get { return GetOrdinal(name); }
        }

        public override object this[int ordinal]
        {
            get { return _resultSet.Rows[_currentRow].Cells[ordinal].Value; }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                Close();               
            }

            base.Dispose(disposing);
        }

        private T GetType<T>(int ordinal)
        {
            return (T)_resultSet.Rows[_currentRow].Cells[ordinal].Value;
        }

        private DataTable CreateSchemaTable()
        {
            DataTable dataTable = new DataTable();
            dataTable.Locale = CultureInfo.InvariantCulture;
            for (int i = 0; i < 18; i++)
            {
                dataTable.Columns.Add(MdxDataReader._schemaTableColumnNames[i], MdxDataReader._schemaTableColumnTypes[i]);
            }

            foreach (Column column in _resultSet.Columns)
            {
                DataRow dataRow = dataTable.NewRow();
                dataRow["ColumnName"] = column.Name;
                dataRow["ColumnOrdinal"] = column.CellOrdinal;
                dataRow["ColumnSize"] = 0;
                if (column.Type == typeof(decimal))
                {
                    dataRow["NumericPrecision"] = 19;
                    dataRow["NumericScale"] = 4;
                }
                else
                {
                    dataRow["NumericPrecision"] = 0;
                    dataRow["NumericScale"] = 0;
                }
                dataRow["DataType"] = column.Type;
                dataRow["ProviderType"] = column.Type;
                dataRow["IsLong"] = false;
                dataRow["AllowDBNull"] = true;
                dataRow["IsReadOnly"] = true;
                dataRow["IsRowVersion"] = false;
                dataRow["IsUnique"] = false;
                dataRow["IsKeyColumn"] = false;
                dataRow["IsAutoIncrement"] = false;
                dataRow["BaseSchemaName"] = null;
                dataRow["BaseCatalogName"] = null;
                dataRow["BaseTableName"] = null;
                dataRow["BaseColumnName"] = null;
                dataTable.Rows.Add(dataRow);
            }

            return dataTable;
        }
    }
}
