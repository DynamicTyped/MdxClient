### What is MdxClient?

        MdxClient is an implementation of a [.NET Framework Data Provider](http://msdn.microsoft.com/en-us/library/a6cd7c08.aspx), 
        with objects such as Connection, Command, DataReader, DataAdapter, etc.  MdxClient is specifically designed to 
        execute MDX queries against SQL Server Analysis Services. It is very similar to the 
        [ADOMD.NET Data Provider](http://msdn.microsoft.com/en-us/library/ms123483.aspx), with some specialization.

        MdxClient was designed to allow MDX queries to be executed and used by ORM (Object Relational Mapping) solutions.  
        Specifically, it has been used against the [Dapper](http://code.google.com/p/dapper-dot-net/) Micro ORM.  
        Additionally, support has been provided for profiling the execution of MDX queries using 
        [MVC Mini Profiler](http://code.google.com/p/mvc-mini-profiler/).

        Although designed for ORM solutions, since MdxClient is a functional .NET Framework Data Provider, it can be utilized 
        by any .NET application wanting to use the functionality provided to execute MDX queries and return data by using standard 
        code against a data provider.

### Features

#### "Tilde Parameters"

        Columns returned from ADOMD.NET queries are not friendly for mapping to class properties in .NET.  For example, 
        [Organization].[Organization].[Level 06] or [Measures].[Collection Count].  MdxClient has specialized parameters 
        ("Tilde Parameters") that can be passed in to map columns returned from an MDX query to more friendly column names.

##### Syntax

            Parameter name: Tilde + MDX column

		    Parameter value: Friendly name

##### Example

```csharp
MdxParameter parameter = new MdxParameter();
parameter.ParameterName = "~[Organization].[Organization].[Level 06]";
parameter.Value = "Zone";

##### Ordinal

            In addition to using the column name, an ordinal can be used.

##### Example

```csharp
MdxParameter parameter = new MdxParameter();
parameter.ParameterName = "~0";
parameter.Value = "Zone";

##### Member properties

            Member properties are special attributes of a given member.  They allow you to access these values from the member without having to specific them in your MDX query.
            Currently there are three that are supported; Caption, LevelName, UniqueName.  These properties can be applied to both types of tilde parameters(named and ordinal).
            To utilize them, the are pre and post appended with `##`.  Caption is generally not needed, as just asking for the column by name returns the caption.  
            These member properties are only applied to members on rows.

##### Example

```csharp
MdxParameter parameter = new MdxParameter();
parameter.ParameterName = "~0##UniqueName##";
parameter.Value = "Zone";

#### Different handling of Parameters

        Rather than limiting where parameters can be used in an MDX query, MdxClient allows parameters to be placed anywhere 
        in the body of the query.  Text replacement is used for generating the final query that gets executed. Take care on your parameter names.
        If you would have one parameter @@Unit and another @@UnitName.  The replacement could replace part of the second token and thus not replace
        the intended value.

        For example, note the following MDX query with parameters:

```sql
WITH 
SET EntitiesWithScores AS 
FILTER([Employee].[Employee Guid].[Employee Guid],[Measures].[Response Count] > 0) 
MEMBER DisplayScore AS 
ROUND([Measures].[Response Computation], @@DisplayPrecision) 
SELECT {DisplayScore} on 0, 
([EntitiesWithScores], @@ReportPeriod:@@ReportPeriod.lead(@@trendLag) ) on 1 
FROM [Report] 
WHERE 
( 
     @@Entity, 
     @@Question, 
     @@ComputationType, 
     @@SummaryPeriod 
)

#### Usage

##### Dapper

```csharp
using(MdxConnection connection = new MdxConnection(connectionString))
{
    connection.Open();
    string query = @@"SELECT {[Measures].[Computation]} on 0,
            ([EmployeesWithScores], {[Questionnaire].[Question Short Text].&[Facility: Appearance]}) on 1
            FROM [CUBE]
            where (@@Unit,
            [Computation].[Computation Name].&[Mean],              
            [Report Period].[Report Period Name].&[Dec-10],
                [Report Period].[Report Period Type].&[Month]        
            )";
    DynamicParameters parms = new DynamicParameters();
    parms.Add("@@Unit", "[Unit].[Organization].[Region].&[Central].&[Toledo Store]");
    parms.Add("~0", "label");
    parms.Add("~2", "score");

    var x = connection.Query&lt;StandardScore&gt;(query, parms);                 
}

##### DataReader

```csharp
DataTable dataTable = new DataTable();

MdxConnection connection = new MdxConnection(connectionString);
using (connection)
{
    connection.Open();
    string query = @@"SELECT [Measures].[Computation] on 0,
                    [Unit].[Unit].&[Toledo Store] on 1
                    FROM [CUBE]";

    using (MdxCommand command = connection.CreateCommand())
    {
        command.CommandText = query;
        command.Parameters.Add(new MdxParameter("~[Measures].[Computation]", "Score"));
        command.Parameters.Add(new MdxParameter("~[Unit].[Unit].[Unit]", "Store"));
        using (MdxDataAdapter dataAdapter = new MdxDataAdapter())
        {
            dataAdapter.SelectCommand = command;
            dataAdapter.Fill(dataTable);
        }
    }    
}