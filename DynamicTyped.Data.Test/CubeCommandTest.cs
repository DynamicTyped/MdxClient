using System.Data;
using Dapper;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using DynamicTyped.Data.MdxClient;
using System.Collections.Generic;
using System.Linq;

namespace Test
{
    
    
    /// <summary>
    ///This is a test class for CubeCommandTest and is intended
    ///to contain all CubeCommandTest Unit Tests
    ///</summary>
    [TestClass()]
    public class CubeCommandTest
    {
        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Additional test attributes
        // 
        //You can use the following additional attributes as you write your tests:
        //
        //Use ClassInitialize to run code before running the first test in the class
        //[ClassInitialize()]
        //public static void MyClassInitialize(TestContext testContext)
        //{
        //}
        //
        //Use ClassCleanup to run code after all tests in a class have run
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}
        //
        //Use TestInitialize to run code before running each test
        //[TestInitialize()]
        //public void MyTestInitialize()
        //{
        //}
        //
        //Use TestCleanup to run code after each test has run
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}
        //
        #endregion

        [TestMethod()]
        public void CustomParmsTest()
        {
            MdxConnection connection = UnitTestHelpers.GetCeCubeConnection();
            using (connection)
            {
                connection.Open();
                string query = @"SELECT [Measures].[Computation] on 0,
                                [Unit].[Unit].&[Toledo Store] on 1
                                FROM [CUBE]";

                DynamicParameters d = new DynamicParameters();
                d.Add("~[Measures].[Computation]", "Score", DbType.Double);

                dynamic x = connection.Query(query, param: d);

                foreach (dynamic y in x)
                {                    
                    Assert.AreEqual(96, y.Score);
                    break;
                }
            }
        }

        [TestMethod()]
        public void DataSetTestWithColumnMap()
        {
            DataSet dataSet = new DataSet();

            MdxConnection connection = UnitTestHelpers.GetCeCubeConnection();
            using (connection)
            {
                connection.Open();
                string query = @"SELECT [Measures].[Computation] on 0,
                                [Unit].[Unit].&[Toledo Store] on 1
                                FROM [CUBE]";


                using (MdxCommand command = connection.CreateCommand())
                {
                    command.CommandText = query;
                    command.Parameters.Add(new MdxParameter("~[Measures].[Computation]", "Score"));
                    command.Parameters.Add(new MdxParameter("~[Unit].[Unit].[Unit]", "Store"));
                    MdxDataAdapter dataAdapter = new MdxDataAdapter();
                    dataAdapter.SelectCommand = command;
                    dataAdapter.Fill(dataSet);
                }

                // Verify there is a table
                Assert.AreEqual(1, dataSet.Tables.Count);

                // Verify data
                foreach (DataRow item in dataSet.Tables[0].Rows)
                {
                    Assert.AreEqual("Toledo Store", item["Store"].ToString());
                    Assert.AreEqual(96.0, (double) item["Score"]);
                    break;
                }
            }
        }

        [TestMethod]
        public void NothingOnRowsTest()
        {
            MdxConnection connection = UnitTestHelpers.GetCeCubeConnection();
            using (connection)
            {
                connection.Open();

                string query = @"SELECT [Measures].[Computation] on 0 FROM [CUBE]";

                var x = connection.Query(query);

                Assert.AreEqual(1, x.Count());
            }
        }

        [TestMethod()]
        public void DataSetTestWithoutColumnMap()
        {
            DataSet dataSet = new DataSet();

            MdxConnection connection = UnitTestHelpers.GetCeCubeConnection();
            using (connection)
            {
                connection.Open();
                string query = @"SELECT [Measures].[Computation] on 0,
                                [Unit].[Unit].&[Toledo Store] on 1
                                FROM [CUBE]";

                using (MdxCommand command = connection.CreateCommand())
                {
                    command.CommandText = query;
                    MdxDataAdapter dataAdapter = new MdxDataAdapter();
                    dataAdapter.SelectCommand = command;
                    dataAdapter.Fill(dataSet);
                }

                // Verify there is a table
                Assert.AreEqual(1, dataSet.Tables.Count);

                // Verify data
                foreach (DataRow item in dataSet.Tables[0].Rows)
                {
                    Assert.AreEqual("Toledo Store", item[0].ToString());
                    Assert.AreEqual(96.0, (double) item[1]);
                    break;
                }
            }
        }

        [TestMethod()]
        public void DataTableTest()
        {
            DataTable dataTable = new DataTable();

            MdxConnection connection = UnitTestHelpers.GetCeCubeConnection();
            using (connection)
            {
                connection.Open();
                string query = @"SELECT [Measures].[Computation] on 0,
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

                // Verify data
                foreach (DataRow item in dataTable.Rows)
                {
                    Assert.AreEqual("Toledo Store", item["Store"].ToString());
                    Assert.AreEqual(96, int.Parse(item["Score"].ToString()));
                    break;
                }
            }
        }

        [TestMethod]
        public void ParmTest()
        {
            using (MdxConnection connection = UnitTestHelpers.GetCeCubeConnection())
            {
                connection.Open();
                string query = @"WITH 

                        MEMBER [Measures].[Employee Value] AS
                        [Employee].[Employee Full Name].currentmember.uniquename

                        MEMBER [Measures].[Question Value] AS
                        [Questionnaire].[Question Category].currentmember.uniquename

                        SELECT {[Measures].[Employee Value], [Measures].[Question Value], [Measures].[Computation]} on 0,
                        ([EmployeesWithScores], {[Questionnaire].[Question Short Text].&[Facility: Appearance]}) on 1
                        FROM [CUBE]
                        where ([Unit].[Organization].[Region].&[Central].&[Toledo Store],
                       [Computation].[Computation Name].&[Mean],              
                       @ReportPeriod,
                           [Report Period].[Report Period Type].&[12 Month]        
                       )";

                DynamicParameters parms = new DynamicParameters();
                parms.Add("@ReportPeriod", "[Report Period].[Report Period Name].&[Dec-10]");
                parms.Add("~[Measures].[Computation]", "score");

                var x = connection.Query(query, parms);
                Assert.IsNotNull(x.First().score);
                Assert.AreEqual(28, x.Count());

            }
        }

        [TestMethod]
        public void NoResultsTest()
        {
            using (MdxConnection connection = UnitTestHelpers.GetCeCubeConnection())
            {
                connection.Open();
                string query = @"WITH 

                        MEMBER [Measures].[Employee Value] AS
                        [Employee].[Employee Full Name].currentmember.uniquename

                        MEMBER [Measures].[Question Value] AS
                        [Questionnaire].[Question Category].currentmember.uniquename

                        SELECT {[Measures].[Employee Value], [Measures].[Question Value], [Measures].[Computation]} on 0,
                        ([EmployeesWithScores], {[Questionnaire].[Question Short Text].&[Facility: Appearance]}) on 1
                        FROM [CUBE]
                        where ([Unit].[Organization].[Region].&[Central].&[1],
                       [Computation].[Computation Name].&[Mean],              
                       [Report Period].[Report Period Name].&[Dec-10],
                           [Report Period].[Report Period Type].&[12 Month]        
                       )";

                var x = connection.Query(query);
                Assert.AreEqual(0, x.Count());
                
            }
        }

        [TestMethod]
        public void ColumnMappingOrdinalOnlyTest()
        {
            using(MdxConnection connection = UnitTestHelpers.GetCeCubeConnection())
            {
                connection.Open();
                string query = @"SELECT {[Measures].[Computation]} on 0,
                        ([EmployeesWithScores], {[Questionnaire].[Question Short Text].&[Facility: Appearance]}) on 1
                        FROM [CUBE]
                        where ([Unit].[Organization].[Region].&[Central].&[Toledo Store],
                       [Computation].[Computation Name].&[Mean],              
                       [Report Period].[Report Period Name].&[Dec-10],
                           [Report Period].[Report Period Type].&[Month]        
                       )";
                DynamicParameters parms = new DynamicParameters();
                parms.Add("~0", "label");
                parms.Add("~2", "score");

                var x = connection.Query<StandardScore>(query, parms).ToList();
                var specificItem = x.First();
                Assert.AreEqual(16, x.Count, "item count");
                Assert.AreEqual(62, specificItem.Score, "score");
                Assert.AreEqual("Barbara Suiter", specificItem.Label, "label");
            }
        }

        [TestMethod]
        public void ColumnMappingMixingTildaAndOrdinalTest()
        {
            using (MdxConnection connection = UnitTestHelpers.GetCeCubeConnection())
            {
                connection.Open();
                string query = @"SELECT {[Measures].[Computation]} on 0,
                        ([EmployeesWithScores], {[Questionnaire].[Question Short Text].&[Facility: Appearance]}) on 1
                        FROM [CUBE]
                        where ([Unit].[Organization].[Region].&[Central].&[Toledo Store],
                       [Computation].[Computation Name].&[Mean],              
                       [Report Period].[Report Period Name].&[Dec-10],
                           [Report Period].[Report Period Type].&[3 Month]        
                       )";
                DynamicParameters parms = new DynamicParameters();
                parms.Add("~0", "label");
                parms.Add("~[Measures].[Computation]", "score");

                var x = connection.Query<StandardScore>(query, parms).ToList();
                var specificItem = x.First();
                Assert.AreEqual(24, x.Count, "item count");
                Assert.AreEqual(44, specificItem.Score);
                Assert.AreEqual("Barbara Suiter", specificItem.Label, "label");
            }
        }

        [TestMethod]
        public void ColumnMappingOrdinalOverrideTest()
        {
            using (MdxConnection connection = UnitTestHelpers.GetCeCubeConnection())
            {
                connection.Open();
                string query = @"SELECT {[Measures].[Computation]} on 0,
                        ([EmployeesWithScores], {[Questionnaire].[Question Short Text].&[Facility: Appearance]}) on 1
                        FROM [CUBE]
                        where ([Unit].[Organization].[Region].&[Central].&[Toledo Store],
                       [Computation].[Computation Name].&[Mean],              
                       [Report Period].[Report Period Name].&[Dec-10],
                           [Report Period].[Report Period Type].&[12 Month]        
                       )";
                DynamicParameters parms = new DynamicParameters();
                parms.Add("~[Employee].[Employee Full Name].[Employee Full Name]", "label");
                parms.Add("~[Measures].[Computation]", "score");
                parms.Add("~0", "OverrideLabel");


                var x = connection.Query<StandardScore>(query, parms).ToList();                
                Assert.AreEqual(28, x.Count, "item count");
                var specificItem = x.First();
                Assert.AreEqual(67, specificItem.Score);
                Assert.AreEqual("Barbara Suiter", specificItem.OverrideLabel, "override label");
                Assert.IsNull(specificItem.Label);
            }
        }

        [TestMethod]
        public void ExecuteScalarTest()
        {
            object scalar;
            using (MdxConnection connection = UnitTestHelpers.GetCeCubeConnection())
            {
                connection.Open();
                string query = @"SELECT [Measures].[Computation] on 0,
                                [Unit].[Unit].&[Toledo Store] on 1
                                FROM [CUBE]";
                using (MdxCommand command = connection.CreateCommand())
                {
                    command.CommandText = query;
                    command.Parameters.Add(new MdxParameter("~[Measures].[Computation]", "Score"));
                    command.Parameters.Add(new MdxParameter("~[Unit].[Unit].[Unit]", "Store"));
                    scalar = command.ExecuteScalar();
                }
            }
            Assert.AreEqual("Toledo Store", scalar.ToString());
        }

        [TestMethod]
        public void SettingDataTypeTest()
        {
            using (MdxConnection connection = UnitTestHelpers.GetCapellaDataTestConnection())
            {
                connection.Open();
                string query = @"WITH 
                            MEMBER DisplayScore AS
                            [Measures].[Response Computation]

                            MEMBER CalcScore AS
                            [Measures].[Response Computation]

                            SET EntitiesWithScores AS
                            ORDER(Filter([Employee].[Employee Guid].&[{2B10EE84-084E-1EEE-63C5-D54007BAF192}].siblings, [Measures].[Response Count]> 0), CalcScore, BDESC)


                            MEMBER [Rank] AS
                            RANK([Employee].[Employee Guid].currentmember, EntitiesWithScores, DisplayScore)

                            MEMBER MaxRank AS
                            MAX(EntitiesWithScores, [Rank])


                            SELECT {DisplayScore,  [Measures].[Response Computation], [Measures].[Response Count]} on 0,
                            ([Employee].[Employee Guid].&[{2B10EE84-084E-1EEE-63C5-D54007BAF192}]) on 1
                            FROM [Report]
                            WHERE (
		                               [Questionnaire].[Response Value Type].&[code],
                                       [Computation].[Computation].&[Mean], 
                                       [Organization].[Organization Hierarchy Name].&[Sales Demo],
                                       [Report Period].[Report Period].&[February 2012],
                                       [Report Period].[Report Period Type].&[3 Month Roll],
                                       [Organization].[Organization].&[356],
                                       [Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].[Question].&[SalesDemo]&[2]&[Repurchase]
                                      )    ";

                DynamicParameters dp = new DynamicParameters();
                dp.Add("~0", "Label");
                dp.Add("~[Measures].[DisplayScore]", "Score", DbType.Double);
                dp.Add("~[Measures].[Response Computation]", "RawScore", DbType.Double);
                var actual = connection.Query<Metric>(query,dp).SingleOrDefault();
                Assert.AreEqual("77.2727272727273", actual.Score.ToString());
                Assert.AreEqual("{2B10EE84-084E-1EEE-63C5-D54007BAF192}", actual.Label);
                Assert.AreEqual("77.2727272727273", actual.RawScore.ToString());
            }
        }

        [TestMethod]
        public void GermanMetricTest()
        {
            using (MdxConnection connection = UnitTestHelpers.GetCapellaDataTestConnection())
            { 
                string query = @"WITH 
                                                            MEMBER DisplayScore AS
                                                            ROUND([Measures].[Response Computation], 0)

                                                            MEMBER CalcScore AS
                                                            ROUND([Measures].[Response Computation], 0)

                                                            SET EntitiesWithScores AS
                                                            ORDER(Filter([Organization].[Organization].&[71].siblings, [Measures].[Response Count]> 0), CalcScore, BDESC)

                                                            MEMBER [Type] AS
                                                            IIF([Questionnaire].[Question Id].currentmember is [Questionnaire].[Question Id].[All], 'Category', 'Question')

                                                            MEMBER Label AS
                                                            [Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].currentmember.member_caption

                                                            MEMBER MdxValue AS
                                                            [Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].currentmember.uniquename
                                                   
                                                            MEMBER SqlValue AS
                                                            IIF([Type] = 'Category', [Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].currentmember.name, [Questionnaire].[Question Id].currentmember.name)

                                                            MEMBER ParentMdxValue AS
                                                            IIF([Type] = 'Category', null, [Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].currentmember.parent.uniquename)
                                                   
                                                            MEMBER ParentLabel AS
                                                            IIF([Type] = 'Category', NULL, [Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].currentmember.member_caption)                            

                                                            MEMBER [Rank] AS
                                                            RANK([Organization].[Organization].currentmember, EntitiesWithScores, DisplayScore)

                                                            MEMBER MaxRank AS
                                                            MAX(EntitiesWithScores, [Rank])

                                                            MEMBER ComparatorScore AS
                                                                                    ([Organization].[Organization].&[7], DisplayScore)

                                                            MEMBER Range1 AS
                                                            IIF([Type] = 'Category', ([Organization].[Organization].currentmember.datamember, [Measures].[Organization Question Category Threshold Low Score]), ([Organization].[Organization].currentmember.datamember, [Measures].[Organization Question Threshold Low Score]))

                                                            MEMBER Range2 AS
                                                            IIF([Type] = 'Category', ([Organization].[Organization].currentmember.datamember, [Measures].[Organization Question Category Threshold High Score]), ([Organization].[Organization].currentmember.datamember, [Measures].[Organization Question Threshold High Score]))

                                                            MEMBER Range3 AS
                                                            IIF([Type] = 'Category', ([Organization].[Organization].currentmember.datamember, [Measures].[Organization Question Category Threshold Maximum Score]), ([Organization].[Organization].currentmember.datamember, [Measures].[Organization Question Threshold Maximum Score]))

                                                            SELECT {[Measures].[Label], DisplayScore, [Rank], MaxRank, [Measures].[MdxValue], [Measures].[SqlValue], [Measures].[Response Computation], [Measures].[Response Count], ParentLabel, ParentMdxValue, Range1, Range2, Range3 , [Measures].[ComparatorScore] } on 0,
                                                            ([Organization].[Organization].&[71], [Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].[Question Category]) on 1
                                                            FROM [Report]
                                                            WHERE (
                                                                       [Questionnaire].[Response Value Type].&[code],
                                                                       [Computation].[Computation].&[Mean], 
                                                                       [Organization].[Organization Hierarchy Name].&[Hierarchy 1],
                                                                       [Report Period].[Report Period].&[December 2010],
                                                                       [Report Period].[Report Period Type].&[3 Month Roll]
                                                                      )   ";
                connection.Open();
                DynamicParameters parms = new DynamicParameters();
                parms.Add("~2", "Label");
                parms.Add("~3", "Score");
                parms.Add("~8", "RawScore");
                parms.Add("~12", "Range1");
                parms.Add("~13", "Range2");
                parms.Add("~14", "Range3");
                parms.Add("~15", "ComparatorScore");

                System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("de-DE");
                System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("de-DE");

                var scores = connection.Query<Metric>(query, parms);
                var y = scores.Count();
                
            }

            System.Threading.Thread.CurrentThread.CurrentUICulture = new System.Globalization.CultureInfo("en-US");
            System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("en-US");
        }


        [TestMethod]
        public void ExtraColumnsTest()
        {
            using (MdxConnection connection = UnitTestHelpers.GetCapellaDataTestConnection())
            {
                connection.Open();
                string query = @"WITH 
                                MEMBER DisplayScore AS
                                ROUND([Measures].[Response Computation], 0)

                                MEMBER CalcScore AS
                                ROUND([Measures].[Response Computation], 0)

                                SET EntitiesWithScores AS
                                ORDER(Filter([Organization].[Organization].&[407].siblings,[Measures].[Response Count]> 0), CalcScore, BDESC)

                                MEMBER [Type] AS
                                IIF([Questionnaire].[Question Id].currentmember is [Questionnaire].[Question Id].[All], 'Category', 'Question')

                                MEMBER ParentMdxValue AS
                                IIF([Type] = 'Category', null, [Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].currentmember.parent.uniquename)

                                MEMBER ParentLabel AS
                                IIF([Type] = 'Category', NULL, [Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].currentmember.member_caption)                            

                                MEMBER [Rank] AS
                                RANK([Organization].[Organization].currentmember, EntitiesWithScores, DisplayScore)

                                MEMBER MaxRank AS
                                MAX(EntitiesWithScores, [Rank])

                                MEMBER ComparatorScore AS
			                                ([Organization].[Organization].&[1357].parent, DisplayScore)

                                MEMBER Range1 AS
                                IIF([Type] = 'Category', ([Organization].[Organization].currentmember.datamember,[Measures].[Organization Question Category Threshold Low Score]), ([Organization].[Organization].currentmember.datamember,[Measures].[Organization Question Threshold Low Score]))

                                MEMBER Range2 AS
                                IIF([Type] = 'Category', ([Organization].[Organization].currentmember.datamember,[Measures].[Organization Question Category Threshold High Score]), ([Organization].[Organization].currentmember.datamember,[Measures].[Organization Question Threshold High Score]))

                                MEMBER Range3 AS
                                IIF([Type] = 'Category', ([Organization].[Organization].currentmember.datamember,[Measures].[Organization Question Category Threshold Maximum Score]), ([Organization].[Organization].currentmember.datamember,[Measures].[Organization Question Threshold Maximum Score]))

                                SELECT {DisplayScore, [Rank], MaxRank, [Measures].[Response Computation], [Measures].[Response Count], ParentLabel, ParentMdxValue, Range1, Range2, Range3 ,[Measures].[ComparatorScore] } on 0,
                                ([Organization].[Organization].&[407], [Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].[Question]) on 1
                                FROM [Report]
                                WHERE (
	                                    [Questionnaire].[Response Value Type].&[code],
	                                    [Computation].[Computation].&[Mean], 
	                                    [Organization].[Organization Hierarchy Name].&[Sales Demo],
	                                    [Report Period].[Report Period].&[November 2012],
	                                    [Report Period].[Report Period Type].&[3 Month Roll]
	                                    )";
                DynamicParameters parms = new DynamicParameters();
                parms.Add("~1", "Label");
                parms.Add("~1##UniqueName##", "MdxValue");
                parms.Add("~[Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].[Question]##UniqueName##", "SqlValue");
                parms.Add("~2", "score");

                var x = connection.Query<Metric>(query, parms);
                
                Assert.AreEqual(23, x.Count(), "item count");
                var specificItem = x.Where(a => a.Label == "Product knowledge").SingleOrDefault();
                Assert.AreEqual(76, specificItem.Score, "score");
                Assert.AreEqual("Product knowledge", specificItem.Label, "label");
                Assert.AreEqual("[Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].[Question].&[SalesDemo]&[2]&[Q4]", specificItem.MdxValue, "mdx value");
                Assert.AreEqual("[Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].[Question].&[SalesDemo]&[2]&[Q4]", specificItem.SqlValue, "sql value");

            }
        }

        [TestMethod]
        public void ColumnOnlyNullsOnEndTest()
        {
            using (MdxConnection connection = UnitTestHelpers.GetCapellaDataTestConnection())
            {
                connection.Open();
                string query = @"WITH
                MEMBER O AS 
                IIF([Measures].[Open] = NULL, 0, [Measures].[Open])

                MEMBER C AS
                IIF([Measures].[Closed] = NULL, 0, [Measures].[Closed])

                MEMBER New AS
                [Measures].[Collection Alert Distinct Count] - O - C

                SELECT {[Measures].[O] , [Measures].[C], new, [Measures].[Collection Count] } ON 0

                FROM Report
                WHERE (LASTPERIODS(30,StrToMember('[Date].[Date].&['+Format(Now(),""yyyy-MM-ddT00:00:00"")+']'))
                ,[Organization].[Organization].&[1442]
                ,[Questionnaire].[Questionnaire Version - Questionnaire - Question Category].[Questionnaire].&[1]&[1]
                )";

                DynamicParameters dp = new DynamicParameters();
                dp.Add("~[Measures].[O]", "Open");
                dp.Add("~[Measures].[New]", "New");
                dp.Add("~[Measures].[C]", "Closed");
                dp.Add("~[Measures].[Collection Count]", "Total");

                var result = connection.Query<AlertState>(query, dp);
                Assert.IsNotNull(result);
            }
        }

        [TestMethod]
        public void ColumnOnlyTest()
        {
            using (MdxConnection connection = UnitTestHelpers.GetCapellaDataTestConnection())
            {
                connection.Open();
                string query = @"WITH
                
                MEMBER New AS
                [Measures].[Collection Alert Distinct Count] - [Measures].[Open] - [Measures].[Closed]

                SELECT {[Measures].[Open] , [Measures].[Closed], new, [Measures].[Collection Count] } ON 0

                FROM Report
                WHERE (LASTPERIODS(30,StrToMember('[Date].[Date].&['+Format(Now(),""yyyy-MM-ddT00:00:00"")+']'))
                ,[Organization].[Organization].&[413]
                ,[Questionnaire].[Questionnaire Version - Questionnaire - Question Category - Question].[Questionnaire].&[SalesDemo]&[2]
                )";

                DynamicParameters dp = new DynamicParameters();
                dp.Add("~[Measures].[O]", "Open");
                dp.Add("~[Measures].[New]", "New");
                dp.Add("~[Measures].[C]", "Closed");
                dp.Add("~[Measures].[Collection Count]", "Total");

                var result = connection.Query<AlertState>(query, dp);
                Assert.IsNotNull(result);
            }
        }

    }
}
