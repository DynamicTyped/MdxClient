using System.Configuration;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using MdxClient;

namespace Test
{
    public static class UnitTestHelpers
    {
        public static string GetCapellaDataTestConnectionString()
        {
            return ConfigurationManager.ConnectionStrings["CapellaDataTest"].ConnectionString;
        }

        public static MdxConnection GetCapellaDataTestConnection()
        {
            return new MdxConnection(GetCapellaDataTestConnectionString());
        }
    }
}
