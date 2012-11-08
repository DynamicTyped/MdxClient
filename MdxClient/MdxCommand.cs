using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Xml;
using System.Xml.Linq;
using Microsoft.AnalysisServices.AdomdClient;
using System.Data.Common;
using System.Globalization;

namespace MdxClient
{
    /// <summary>
    /// Represents an MDX statement to execute against a SQL Server Analysis Services database.
    /// </summary>
    public class MdxCommand : DbCommand
    {

        #region private variables
        
        private AdomdCommand _command;
        private MdxConnection _connection;
        private MdxTransaction _transaction;
        private MdxParameterCollection _parameters;
        private XNamespace _namespace = "urn:schemas-microsoft-com:xml-analysis:mddataset";
        private XNamespace _xsiNs = "http://www.w3.org/2001/XMLSchema-instance";
        private IEnumerable<ColumnMap> _columnMap;
        #endregion

        #region constructors
                
        /// <summary>
        /// Initializes a new instance of the MdxCommand class.
        /// </summary>
        public MdxCommand()
        {
            // Normally I would utilize the this on the constructor to reduce code, but since we are 
            // using an internal object the this's overwrite each other, so repeating code :(
            
            _command = new AdomdCommand();
            _connection = null;
            _parameters = null;
            _transaction = null;
            this.CommandTimeout = 0;
            this.CommandText = null;
        }

        /// <summary>
        /// Initializes a new instance of the MdxCommand class with the text of the query.
        /// </summary>
        /// <param name="commandText">The text of the query.</param>
        public MdxCommand(string commandText) 
        {
            _command = new AdomdCommand(commandText);            
            _connection = null;
            _parameters = null;
            _transaction = null;
            this.CommandTimeout = 0;
        }

        /// <summary>
        /// Initializes a new instance of the MdxCommand class with the text of the query and a MdxConnection.
        /// </summary>
        /// <param name="commandText">The text of the query.</param>
        /// <param name="connection">An MdxConnection representing the connection to SQL Server Analysis Services.</param>
        public MdxCommand(string commandText, MdxConnection connection) 
        {
            _command = new AdomdCommand(commandText, connection.Connection);
            _connection = connection;
            _parameters = null;
            _transaction = null;
            this.CommandTimeout = 0;
        }

        #endregion

        #region overrides
        
        /// <summary>
        /// Tries to cancel the execution of an MdxCommand.
        /// </summary>
        public override void Cancel()
        {
            _command.Cancel();
        }

        /// <summary>
        /// Gets or sets the MDX statement to execute at the data source.
        /// </summary>
        public override string CommandText
        {
            get
            {
                return _command.CommandText;
            }
            set
            {
                _command.CommandText = value;
            }
        }

        /// <summary>
        /// Gets or sets the wait time before terminating the attempt to execute a command and generating an error.
        /// </summary>
        public override int CommandTimeout
        {
            get
            {
                return _command.CommandTimeout;
            }
            set
            {
                _command.CommandTimeout = value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating how the MdxCommand.CommandText property is to be interpreted.
        /// </summary>
        public override CommandType CommandType
        {
            get
            {
                return _command.CommandType;
            }
            set
            {
                _command.CommandType = value;
            }
        }

        protected override DbParameter CreateDbParameter()
        {
            return new MdxParameter();
        }

        /// <summary>
        /// Gets or sets the MdxConnection used by this instance of the MdxCommand.
        /// </summary>
        protected override DbConnection DbConnection
        {
            get
            {
                return _connection;
            }
            set
            {
                _connection = (MdxConnection)value;
            }
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get
            {
                if (this._parameters == null)
                {
                    this._parameters = new MdxParameterCollection(this);
                }

                return this._parameters;
            }
        }

        protected override DbTransaction DbTransaction
        {
            get
            {
                return this._transaction;
            }
            set
            {
                if (null == value)
                {
                    this._transaction = null;
                    return;
                }
                if (this._transaction.Connection != this._connection)
                {
                    throw new InvalidOperationException("MdxCommand.Connection and MdxTransaction.Connection must be the same MdxConnection.");
                }
                this._transaction = (MdxTransaction)value;
            }
        }

        public override bool DesignTimeVisible
        {
            get
            {
                return false;
            }
            set
            {
                if (value == true)
                    throw new ArgumentException("", "value");
            }
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            _command.Connection = new AdomdConnection(Connection.ConnectionString);
            _command.Connection.Open();

            var allParameters = GetParams();
            _columnMap = allParameters.Where(a => a.Name.StartsWith(ColumnMap.ColumnRename));

            var parameters = allParameters.Except(_columnMap, new ColumnMapComparer());

            foreach (ColumnMap parameter in parameters)
            {
                // dapper 1.7+ rips off the @, this allows for either @ or no prefix to be found and replaced
                var name = parameter.Name.StartsWith(ColumnMap.Parameter) ? parameter.Name : ColumnMap.Parameter + parameter.Name;
                _command.CommandText = _command.CommandText.Replace(name, parameter.Value.ToString());
                
            }

            var trace = new System.Diagnostics.TraceSource("mdx");
            trace.TraceData(System.Diagnostics.TraceEventType.Information, 0, _command.CommandText);

            ResultSet results = PopulateFromXml(_command.ExecuteXmlReader());

            if (behavior == CommandBehavior.CloseConnection)
            {
                return new MdxDataReader(results, _connection);
            }
            else
            {
                return new MdxDataReader(results);
            }
        }

        public override int ExecuteNonQuery()
        {
            throw new NotSupportedException();
        }      

        /// <summary>
        /// Executes the query, and returns the first column of the first row in the 
        /// result set returned by the query. Additional columns or rows are ignored.
        /// </summary>
        public override object ExecuteScalar()
        {
            object result = null;
            using (DbDataReader ds = base.ExecuteReader())
            {
                if (ds.Read() && ds.FieldCount > 0)
                {
                    result = ds.GetValue(0);
                }             
            }
            return result;
        }

        /// <summary>
        /// Creates a prepared version of the command on an instance of SQL Server Analysis Services.
        /// </summary>
        public override void Prepare()
        {
            _command.Prepare();
        }

        public override UpdateRowSource UpdatedRowSource
        {
            get
            {
                throw new NotSupportedException();
            }
            set
            {
                throw new NotSupportedException();
            }
        }

        #endregion

        #region private methods
        
        private IEnumerable<ColumnMap> GetParams()
        {
            return Parameters.OfType<IDbDataParameter>()                             
                             .Select(a => new ColumnMap()
                             {
                                 Name = a.ParameterName,
                                 Type = a.DbType == DbType.AnsiString ? (DbType?)null : a.DbType,
                                 Value = a.Value
                             });
        }

        private ResultSet PopulateFromXml(XmlReader xmlReader)
        {
            ResultSet crs = new ResultSet();

            using (xmlReader)
            {
                XDocument doc = XDocument.Load(xmlReader);

                List<Tuple> axis = GetAxis(doc);
                var rows = axis.Where(a => a.Axis == "Axis1");
                var columns = axis.Where(a => a.Axis == "Axis0");
                var cells = GetCellData(doc);

                int rowColumnCount = AddColumnsFromRowAxis(rows, crs);
                AddColumnsFromColumnAxis(columns, cells, crs);

                processOrdinalColumns(crs);

                AddRows(rows, cells, rowColumnCount, crs);
            }

            return crs;
        }

        private void processOrdinalColumns(ResultSet crs)
        {
            foreach (var column in _columnMap)
            {
                int ordinal;
                if (Int32.TryParse(column.NameWithoutPrefixes, out ordinal))
                {
                    if (ordinal >= 0 && ordinal < crs.Columns.Count)
                    {
                        crs.Columns[ordinal].Name = column.Value.ToString();
                    }
                }
            }
        }

        private List<Tuple> GetAxis(XDocument doc)
        {
            var x = from axis in doc.Root.Elements(_namespace + "Axes").Elements(_namespace + "Axis")
                    from tuple in axis.Elements(_namespace + "Tuples").Elements(_namespace + "Tuple")
                   select new Tuple()
                   {
                       Axis = axis.Attribute("name").Value,
                       Members = (
                           from member in tuple.Elements(_namespace + "Member")
                           select new Member()
                           {
                               Caption = member.Element(_namespace + "Caption").Value,
                               UniqueName = member.Element(_namespace + "UName").Value,
                               LevelName = member.Element(_namespace + "LName").Value,
                               DimensionProperties = (from property in member.Elements()
                                                      where property.Name.LocalName.StartsWith("_x005B_")
                                                      select new DimensionProperty()
                                                      {
                                                          UniqueName = System.Xml.XmlConvert.DecodeName(property.Name.LocalName),
                                                          Caption = property.Value
                                                      }).ToList()
                           }
                       ).ToList()
                   };

            return x.ToList();

        }       

        private void SetColumnNameAndType(Column column, string defalutName)
        {
            SetColumnNameAndType(column, defalutName, null);
        }

        private void SetColumnNameAndType(Column column, string defaultName, Type defalutType)
        {            
            ColumnMap cm = GetColumnMap(defaultName);

            if (null != cm)
            {
                cm.MappedFrom = defaultName;
                column.Name = cm.Value.ToString();
                if (null != defalutType)
                {
                    column.Type = GetTypeFromDbType(cm.Type);
                }
            }
            else
            {
                column.Name = defaultName;
                column.Type = defalutType;
            }            
        }

        private Type GetTypeFromDbType(DbType? dbType)
        {
            if (null == dbType)
            {
                return typeof(string);
            }
            else
            {
                return TypeConverter.ToNetType(dbType.Value);
            }
        }

        private int AddColumnsFromRowAxis(IEnumerable<Tuple> rows, ResultSet crs)
        {
            int columnCount = 0;
            if (null != rows && rows.Any())
            {
                foreach (var member in rows.First().Members)
                {
                    Column column = new Column();
                    SetColumnNameAndType(column, member.LevelName, typeof(string));                    

                    column.Items.Add(column.Name);                   
                    crs.Columns.Add(column);
                    columnCount++;
                }

                foreach (var extraColumn in (from x in rows
                                             from y in x.Members
                                             from z in y.DimensionProperties
                                             select new
                                             {
                                                 ParentColumn = y.LevelName,
                                                 ChildColumn = z.UniqueName
                                             }).Distinct())
                {
                    Column column = new Column();
                    SetColumnNameAndType(column, extraColumn.ChildColumn, typeof(string));
                    
                    crs.Columns.Add(column);
                    column.Items.Add(column.Name);
                    columnCount++;
                }
            }

            return columnCount;
        }

        private List<Cell> GetCellData(XDocument doc)
        {
            var cellData = from cell in doc.Root.Elements(_namespace + "CellData").Elements(_namespace + "Cell")
                           select new Cell()
                           {
                               Ordinal = (int)cell.Attribute("CellOrdinal"),
                               FormattedValue = cell.Elements(_namespace + "FmtValue").Any() ? cell.Element(_namespace + "FmtValue").Value : cell.Element(_namespace + "Value").Value,
                               Value = cell.Element(_namespace + "Value").Value,
                               Type = (string)cell.Element(_namespace + "Value").Attribute(_xsiNs + "type"),
                               
                           };

            return cellData.ToList();
        }

        private void AddColumnsFromColumnAxis(IEnumerable<Tuple> columns, IEnumerable<Cell> cells, ResultSet crs)
        {            
            int columnCount = 0;
            foreach (var tuple in columns)
            {
                StringBuilder sb = new StringBuilder();
                Column column = new Column();

                foreach (var member in tuple.Members)
                {
                    sb.Append(member.UniqueName);
                    column.Items.Add(member.Caption);
                }

                SetColumnNameAndType(column, sb.ToString());                
                column.Ordinal = ++columnCount;
                crs.Columns.Add(column);                            
            }
            
            // this is done after all the columns are added because we need to know the total column count for any of the modus math to work correctly
            // at least any of the modulus math i could think of so far :)
            crs.Columns.Where(a => a.Type == null).ToList().ForEach(a => a.Type = GetTypeForColumn(cells, a, columnCount));
        }

        private Type GetTypeForColumn(IEnumerable<Cell> cells, Column column, int columnCount)
        {
            Type type = typeof(string);
            // first column is always from the row axis, make it string
            if (column.Ordinal == 0)
            {
                return type;
            }

            // If the user passed in a dbtape as part of the parms, use that
            var columnMap = GetColumnMapFromOrdinalOrName(column);
            if (null != columnMap && null != columnMap.Type)
            {
                return  GetTypeFromDbType(columnMap.Type);
            }
            

            int columnPosition = column.Ordinal;
            if (column.Ordinal == columnCount)
            {
                columnPosition = 0;
            }
            // using the ordinal of the cell along with the column count we can determine which column a cell belongs too.
            // this gets all the cells for the current column and gets the distinct types
            var x = cells.Where(c => ((c.Ordinal + 1) % columnCount) == columnPosition).Select(t => t.Type).Distinct();
            
            if (x.Count() > 1)
            {
                // if a non number comes back, the type is null, so non numbers are null
                // on counts of greater than 1 and no nulls, we have multiple number types, make them all double to accommodate the differences
                // TODO: find a test case for nulls and see how this works
                if ( !x.Contains(null) )
                {
                    // mix of numbers not doubles, default to int
                    if (!x.Contains("xsd:double"))
                    {
                        type = typeof(int);
                    }
                    else
                    {
                        type = typeof(double);
                    }
                }
                else
                {
                    type = typeof(string);
                }
            }
            else
            {
                // entire column maybe null, default to string, otherwise check
                if (x.Count() == 1)
                {
                    type = ConvertXmlTypeToType(x.First());
                }               
            }

            return type;
        }

        private Type ConvertXmlTypeToType(string type)
        {
            Type t = typeof(string);

            switch (type)
            {
                case "xsd:int":
                    t = typeof(int);
                    break;
                case "xsd:double":
                    t = typeof(double);
                    break;
                case "xsd:long":
                    t = typeof(long);
                    break;
                case "xsd:short":
                    t = typeof(short);
                    break;
                case "xsd:integer":
                    t = typeof(int);
                    break;
                case "xsd:decimal":
                    t = typeof(decimal);
                    break;
                case "xsd:float":
                    t = typeof(float);
                    break;
                default:
                    t = typeof(string);
                    break;
            }

            return t;
        }               

        private int convertIt(string item)
        {
            int value = -1;
            if (int.TryParse(item, out value))
            {

            }
            return value;
        }

        private ColumnMap GetColumnMapFromOrdinalOrName(Column c)
        {
            return _columnMap.Where(a => convertIt(a.NameWithoutPrefixes) == c.Ordinal || a.Value.ToString() == c.Name).SingleOrDefault();
        }

        private void AdjustValueFromColumnType(Cell cell, int columnIndex, ResultSet crs)
        {
            // change type was giving odd results when a culture was passed in on the thread, for example German 5.324145E1 came out as 5324145 instead of 53.24145
            // we give it invariant culture to fix this.  It will be up to the end user to apply formatting.
            cell.Value = Convert.ChangeType(cell.Value, crs.Columns[columnIndex].Type ?? ConvertXmlTypeToType(cell.Type), CultureInfo.InvariantCulture);            
        }

        private void AddRows(IEnumerable<Tuple> rows, List<Cell> cells, int rowColumnCount, ResultSet crs)
        {

            int start = 0;
            int columnCountFromColumnAxis = crs.Columns.Count - rowColumnCount;
            int finish = columnCountFromColumnAxis - 1;
            
            int cellsIndexer = 0;
            int cellsCount = cells.Count();
            int ordinal = 0;

            if (0 == rows.Count() && cells.Count > 0)
            {
                // data coming back from the cube only has cell for actual data, nulls are not represented.  We need to fill in those cells so that the data appears
                // in the correct columns
                var ordinals = cells.Select(a => a.Ordinal);
                Enumerable.Range(0, columnCountFromColumnAxis)
                          .Except(ordinals)
                          .ToList()
                          .ForEach(a => cells.Add(new Cell() { Ordinal = a}));

                cells.ForEach(a => AdjustValueFromColumnType(a, a.Ordinal, crs));

                crs.Rows.Add(new Row() { Cells = cells.OrderBy(a => a.Ordinal).ToList() });
            }
            else
            {
                // tokened maps that get properties from members in the row
                var memberProperties = _columnMap.GetMemberProperties();
                var columnsAdded = memberProperties.Count();
                AddColumnsFromRowProperties(crs, memberProperties);

                foreach (var row in rows)
                {
                    Row r = new Row();
                    foreach (var member in row.Members)
                    {
                        Cell c = new Cell() { FormattedValue = member.Caption, Value = member.Caption, Ordinal = ordinal++ };

                        // TODO: Logic for dimension property
                        r.Cells.Add(c);
                    }

                    //ordinal = GetOrdinalForCell(rowIndex++, rowColumnCount, crs.Columns.Count, rowColumnCount);

                    // cells are in a single dimension array, have to determine which row it belongs to to intermix the row data correctly
                    for (int i = start; i <= finish; i++)
                    {
                        Cell cellToAdd = new Cell();

                        // cell indexer can go past its range, only try to get values while in range
                        if (!(cellsIndexer >= cellsCount))
                        {
                            // get this cell
                            var cell = cells[cellsIndexer];

                            // if the ordinal of the cell is the column we are looking at, add it's values, otherwise an empty cell is added.
                            // this is done because the xml coming back does not include nulls/empty data.  We have to fill in the gap or the subsequent objects will throw the data off
                            if (Convert.ToInt32(cell.Ordinal) == i)
                            {
                                cellToAdd = cell;
                                AdjustValueFromColumnType(cellToAdd, cell.Ordinal % (columnCountFromColumnAxis) + rowColumnCount, crs);
                                cellsIndexer++;
                            }
                        }
                        cellToAdd.Ordinal = ordinal++;
                        r.Cells.Add(cellToAdd);
                    }

                    // go threw the members again, this time for the special columns, this is done here since the columns for them are done at the end
                    // if we tried to adding the cells in the for above, it would through the ordinals off for all cells.
                    if (columnsAdded > 0)
                    {
                        for (int i = 0; i < row.Members.Count; i++ )
                        {
                            int previousCount = r.Cells.Count;
                            r.Cells.AddRange(AddCellsFromMemberProperties(memberProperties, row.Members[i], ordinal, i));
                            if (previousCount != r.Cells.Count)
                            {
                                // advance the ordinal since extra columns were added, the normal process would only take into consideration
                                // the cells visible in ssms
                                ordinal = r.Cells.Last().Ordinal + 1;
                            }
                        }                        
                    }

                    crs.Rows.Add(r);

                    start += columnCountFromColumnAxis;
                    finish += columnCountFromColumnAxis;
                }
            }
        }

        private IEnumerable<Cell> AddCellsFromMemberProperties(IEnumerable<ColumnMap> specialColumns, Member member, int cellOrdinal, int columnOrdinal)
        {
            return specialColumns
                   .Where(a => a.DoesMapMatchMember(member, columnOrdinal))
                   .Select(a => new Cell()
                    {
                        Ordinal = cellOrdinal++,
                        FormattedValue = a.GetMemberProperty(member),
                        //is there a good way not to run that code twice?
                        Value = a.GetMemberProperty(member),
                        Type = "string"
                    });
        }       

        /// <summary>
        /// find the tokened column map
        /// </summary>
        /// <param name="crs"></param>
        /// <param name="columnMap"></param>
        private void AddColumnsFromRowProperties(ResultSet crs, IEnumerable<ColumnMap> columnMap)
        {
            var max = crs.Columns.Select(a => a.Ordinal).Max();
            foreach(var map in columnMap)
            {
                crs.Columns.Add(new Column() { Ordinal = ++max, Name = map.Value.ToString(), Type = typeof(string) });
            }
        }                

        private int GetOrdinalForCell(int rowIndex, int columnOrdinal, int columnCount, int rowColumnCount)
        {
            return ((rowIndex - 1) * (columnCount + rowColumnCount) + columnOrdinal);
        }

        private ColumnMap GetColumnMap(string nameFromMdx)
        {            
            return _columnMap.Where(a => a.NameWithoutPrefixes == nameFromMdx).SingleOrDefault();
        }

        protected override void Dispose(bool disposing)
        {
            if (null != _command.Connection && _command.Connection.State == ConnectionState.Open)
            {
                _command.Connection.Close();
            }

            _command.Dispose();

            base.Dispose(disposing);
        }
    }

        #endregion
}
