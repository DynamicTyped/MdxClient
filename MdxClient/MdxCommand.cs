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
using System.Diagnostics;

namespace MdxClient
{
    /// <summary>
    /// Represents an MDX statement to execute against a SQL Server Analysis Services database.
    /// </summary>
    public class MdxCommand : DbCommand
    {

        #region private variables
        
        private readonly AdomdCommand _command;
        private MdxConnection _connection;
        private MdxTransaction _transaction;
        private MdxParameterCollection _parameters;
        private readonly XNamespace _namespace = "urn:schemas-microsoft-com:xml-analysis:mddataset";
        private readonly XNamespace _xsiNs = "http://www.w3.org/2001/XMLSchema-instance";
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
        }

        /// <summary>
        /// Initializes a new instance of the MdxCommand class with the text of the query.
        /// </summary>
        /// <param name="commandText">The text of the query.</param>
        public MdxCommand(string commandText) 
        {
            _command = new AdomdCommand(commandText);                      
        }

        /// <summary>
        /// Initializes a new instance of the MdxCommand class with the text of the query and a MdxConnection.
        /// </summary>
        /// <param name="commandText">The text of the query.</param>
        /// <param name="connection">An MdxConnection representing the connection to SQL Server Analysis Services.</param>
        public MdxCommand(string commandText, MdxConnection connection) 
        {
            if (null == connection)
                throw new ArgumentNullException("connection");

            _command = new AdomdCommand(commandText, connection.Connection);
            _connection = connection;
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
				_command.Connection = _connection.Connection;
            }
        }

        protected override DbParameterCollection DbParameterCollection
        {
            get { return _parameters ?? (_parameters = new MdxParameterCollection()); }
        }
                
        protected override DbTransaction DbTransaction
        {
            get
            {
                return _transaction;
            }
            set
            {
                if (null == value)
                {
                    _transaction = null;
                    return;
                }
                if (_transaction.Connection != _connection)
                {
                    throw new InvalidOperationException("MdxCommand.Connection and MdxTransaction.Connection must be the same MdxConnection.");
                }
                _transaction = (MdxTransaction)value;
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
                if (value)
                    throw new ArgumentException("", "value");
            }
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            var allParameters = GetParams();
            _columnMap = allParameters.Where(a => a.Name.StartsWith(ColumnMap.ColumnRename, StringComparison.OrdinalIgnoreCase));

            var parameters = allParameters.Except(_columnMap, new ColumnMapComparer());

            foreach (var parameter in parameters)
            {
                // dapper 1.7+ rips off the @, this allows for either @ or no prefix to be found and replaced
                var name = parameter.Name.StartsWith(ColumnMap.Parameter, StringComparison.OrdinalIgnoreCase) ? parameter.Name : ColumnMap.Parameter + parameter.Name;
                _command.CommandText = _command.CommandText.Replace(name, parameter.Value.ToString());
                
            }

            var trace = new System.Diagnostics.TraceSource("mdx");
            trace.TraceData(System.Diagnostics.TraceEventType.Information, 0, _command.CommandText);

            var results = PopulateFromXml(_command.ExecuteXmlReader());

            return behavior == CommandBehavior.CloseConnection ? new MdxDataReader(results, _connection) : new MdxDataReader(results);
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
            using (var ds = ExecuteReader())
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
            var crs = new ResultSet();

            using (xmlReader)
            {
                var doc = XDocument.Load(xmlReader);

                var axis = GetAxis(doc);
                var rows = axis.Where(a => a.Axis == "Axis1");
                var columns = axis.Where(a => a.Axis == "Axis0");
                var cells = GetCellData(doc);

                var rowColumnCount = AddColumnsFromRowAxis(rows, crs);
                AddColumnsFromColumnAxis(columns, cells, crs);

                ProcessOrdinalColumns(crs);

                AddRows(rows, cells, rowColumnCount, crs);
            }

            return crs;
        }

        private void ProcessOrdinalColumns(ResultSet crs)
        {
            foreach (var column in _columnMap)
            {
                int ordinal;
                if (!Int32.TryParse(column.NameWithoutPrefixes, out ordinal)) continue;
                if (ordinal >= 0 && ordinal < crs.Columns.Count)
                {
                    crs.Columns[ordinal].Name = column.Value.ToString();
                }
            }
        }

        private List<Tuple> GetAxis(XDocument doc)
        {
            var excludedDimensionProperties = new [] {  "UName", "Caption",  "LName",  "LNum",  "DisplayInfo" };
            
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
                               LevelNumber = member.Element(_namespace + "LNum").Value,
                               DimensionProperties = (from property in member.Elements()
                                                      where ! excludedDimensionProperties.Contains(property.Name.LocalName) 
                                                      //where property.Name.LocalName.StartsWith("_x005B_", StringComparison.OrdinalIgnoreCase)
                                                      select new DimensionProperty()
                                                      {
                                                          UniqueName = System.Xml.XmlConvert.DecodeName(property.Name.LocalName),
                                                          Value =  XmlConvert.DecodeName(property.Value)
                                                      }).ToList()
                           }
                       ).ToList()
                   };

            return x.ToList();

        }

        private void SetColumnNameAndType(Column column, string defaultName, Type defalutType = null)
        {            
            var cm = GetColumnMap(column, defaultName);

            if (null != cm)
            {
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

        private static Type GetTypeFromDbType(DbType? dbType)
        {
            return null == dbType ? typeof(string) : TypeConverter.ToNetType(dbType.Value);
        }

        private int AddColumnsFromRowAxis(IEnumerable<Tuple> rows, ResultSet crs)
        {
            var dimensionProperties = new List<string>();
            var columnCount = 0;
            if (null != rows && rows.Any())
            {
                foreach (var member in rows.First().Members)
                {
                    var column = new Column {ColumnOrdinal = columnCount++};
                    SetColumnNameAndType(column, member.LevelName, typeof(string));                    
                    column.Items.Add(column.Name);                   
                    crs.Columns.Add(column);
                    
                }

                var dimensionPropertyColumns = rows.SelectMany(
                           (row) => row.Members.SelectMany(
                               // Project the dimension properties so we also get the member's index within the row for each:
                                  (member, memberIndex) => member.DimensionProperties.Select(
                                         (dimensionProp) => new
                                         {
                                             DimensionProperty = dimensionProp,
                                             MemberIndex = memberIndex
                                         }),
                               // Turn all this business into what we're really looking for:
                                  (member, x) => new
                                  {
                                      //ParentColumn = member.LevelName,
                                      ChildColumn = x.DimensionProperty.UniqueName,
                                      MemberIndex = x.MemberIndex
                                  })).Distinct();


                // dimension properties are looked at for all rows where the columns above is just the first row
                // it is very possible to get data in further down rows for a dimension properties that doesn't exist on the first row
                // an example is in org with parent child where a property may exist for only one level
                foreach (var dimensionProperty in dimensionPropertyColumns)
                {
                    var propertyColumn = new Column() {ColumnOrdinal = columnCount};
                    //only add column if not already added
                    // each column can have a dimension property that may already be present
                    var columnName = dimensionProperty.MemberIndex.ToString(CultureInfo.InvariantCulture) + dimensionProperty.ChildColumn;
                    if (!dimensionProperties.Any(a => string.Equals(a, columnName)))
                    {
                        SetColumnNameAndType(propertyColumn, columnName, typeof(string));
                        columnCount++;
                        crs.Columns.Add(propertyColumn);
                        propertyColumn.Items.Add(propertyColumn.Name);
                        dimensionProperties.Add(dimensionProperty.ChildColumn);
                    }
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
            var cellOrdinal = 0;
            var columnOrdinal = crs.Columns.Count;
            foreach (var tuple in columns)
            {
                var sb = new StringBuilder();
                var column = new Column();

                foreach (var member in tuple.Members)
                {
                    sb.Append(member.UniqueName);
                    column.Items.Add(member.Caption);
                }

                column.CellOrdinal = ++cellOrdinal;
                column.ColumnOrdinal = columnOrdinal++;
                SetColumnNameAndType(column, sb.ToString());                
                crs.Columns.Add(column);                            
            }
            
            // this is done after all the columns are added because we need to know the total column count for any of the modus math to work correctly
            // at least any of the modulus math i could think of so far :)
            crs.Columns.Where(a => a.Type == null).ToList().ForEach(a => a.Type = GetTypeForColumn(cells, a, cellOrdinal));
        }

        private Type GetTypeForColumn(IEnumerable<Cell> cells, Column column, int columnCount)
        {
            var type = typeof(string);
            // all columns from row axis, make it string
            if (column.CellOrdinal == 0)
            {
                return type;
            }

            // If the user passed in a dbtype as part of the parms, use that
            var columnMap = GetColumnMap(column);
            if (null != columnMap && null != columnMap.Type)
            {
                return  GetTypeFromDbType(columnMap.Type);
            }

            var columnPosition = column.CellOrdinal;
            if (column.CellOrdinal == columnCount)
            {
                columnPosition = 0;
            }
            // using the ordinal of the cell along with the column count we can determine which column a cell belongs too.
            // this gets all the cells for the current column and gets the distinct types
            var x = cells.Where(c => ((c.Ordinal + 1) % columnCount) == columnPosition).Select(t => t.Type).Distinct().ToList();
            
            if (x.Count() > 1)
            {
                // if a non number comes back, the type is null, so non numbers are null
                // on counts of greater than 1 and no nulls, we have multiple number types, make them all double to accommodate the differences
                // TODO: find a test case for nulls and see how this works
                if ( !x.Contains(null) )
                {
                    // mix of numbers not doubles, default to int
                    type = !x.Contains("xsd:double") ? typeof(int) : typeof(double);
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

        private static Type ConvertXmlTypeToType(string type)
        {
            Type t;

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

        private static void AdjustValueFromColumnType(Cell cell, int columnIndex, ResultSet crs)
        {
            // change type was giving odd results when a culture was passed in on the thread, for example German 5.324145E1 came out as 5324145 instead of 53.24145
            // we give it invariant culture to fix this.  It will be up to the end user to apply formatting.
            cell.Value = Convert.ChangeType(cell.Value, crs.Columns[columnIndex].Type ?? ConvertXmlTypeToType(cell.Type), CultureInfo.InvariantCulture);            
        }

        private void AddRows(IEnumerable<Tuple> rows, List<Cell> cells, int rowColumnCount, ResultSet crs)
        {

            var start = 0;
            var columnCountFromColumnAxis = crs.Columns.Count - rowColumnCount;
            var finish = columnCountFromColumnAxis - 1;
            
            var cellsIndexer = 0;
            var cellsCount = cells.Count();
            var ordinal = 0;

            if (!rows.Any() && cells.Count > 0)
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
                    var r = new Row();

                    // main row data
                    foreach (var member in row.Members)
                    {
                        r.Cells.Add(new Cell() { FormattedValue = member.Caption, Value = member.Caption, Ordinal = ordinal++ });
                        
                    }

                    // dimension property row data
                    // this is done as another pass since dimension properties columns are added at the end of normal columns from row
                    foreach (var property in row.Members.Where(member => member.DimensionProperties != null).SelectMany(member => member.DimensionProperties))
                    {
                        r.Cells.Add(new Cell() { FormattedValue = property.Value, Value = property.Value, Ordinal = ordinal++ });
                    }

                    
                    // cells are in a single dimension array, have to determine which row it belongs to to intermix the row data correctly
                    for (var i = start; i <= finish; i++)
                    {
                        var cellToAdd = new Cell();

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
                        for (var i = 0; i < row.Members.Count; i++ )
                        {
                            var previousCount = r.Cells.Count;
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

        private static IEnumerable<Cell> AddCellsFromMemberProperties(IEnumerable<ColumnMap> specialColumns, Member member, int cellOrdinal, int columnOrdinal)
        {
            Func<ColumnMap, Cell> newCell = (map) =>
                {
                    object formattedValue = map.GetMemberProperty(member);
                    var value = formattedValue;
                    var type = "string";
                    if (map.IsLevelNumber)
                    {
                        type = "xsd:int";
                        value = Convert.ChangeType(formattedValue, typeof (int), CultureInfo.InvariantCulture);
                    }
                    var cell = new Cell()
                        { 
                            Ordinal = cellOrdinal++,
                            FormattedValue = formattedValue,
                            Type = type,
                            Value = value
                        };

                    return cell;
                };

            return specialColumns
                   .Where(a => a.DoesMapMatchMember(member, columnOrdinal))
                   .Select(newCell);
        }       

        /// <summary>
        /// find the tokened column map
        /// </summary>
        /// <param name="crs"></param>
        /// <param name="columnMap"></param>
        private static void AddColumnsFromRowProperties(ResultSet crs, IEnumerable<ColumnMap> columnMap)
        {
            int max;
            var cels = crs.Columns.Select(a => a.CellOrdinal);
            if (cels.Any())
            {
                max = cels.Max();
            }
            else
            {
                max = 0;
            }
            foreach(var map in columnMap)
            {
                var type = map.IsLevelNumber ? typeof(int) : typeof (string);
                
                crs.Columns.Add(new Column() { CellOrdinal = ++max, Name = map.Value.ToString(), Type = type });
            }
        }                

       private static int ConvertIt(string item)
       {
           int value;
           if (int.TryParse(item, out value))
           {
               return value;
           }
           return -1;
        }

        private ColumnMap GetColumnMap(Column column, string columnName = null)
        {
            var name = columnName ?? column.Name;

            var nameMatch =_columnMap.SingleOrDefault(a => string.Equals(a.NameWithoutPrefixes, name));
            var valueMatch = _columnMap.SingleOrDefault(a => string.Equals(a.Value.ToString(), name));
            var ordinalMatch = _columnMap.SingleOrDefault(a => ConvertIt(a.NameWithoutPrefixes) == column.ColumnOrdinal);

            return nameMatch ?? valueMatch ?? ordinalMatch;
        }

        protected override void Dispose(bool disposing)
		{
			if (disposing)
			{
				_command.Dispose();
			}

			base.Dispose(disposing);
		}

		#endregion
	}
}
