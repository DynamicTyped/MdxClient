using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;

namespace MdxClient
{
    internal class ColumnMap
    {
        public const string ColumnRename = "~";
        public const string Parameter = "@";
        public const string CaptionToken = "##Caption##";
        public const string LevelNameToken = "##LevelName##";
        public const string UniqueNameToken = "##UniqueName##";
        public string Name { get; set; }
        public object Value { get; set; }
        public DbType? Type { get; set; }

        public string NameWithoutPrefixes 
        { 
            get
            {
                return Name.Replace(Parameter, "")
                           .Replace(ColumnRename, "");
            }
        }

        public string NameWithoutPrefixesOrSuffixes
        {
            get
            {
                return NameWithoutPrefixes.Replace(CaptionToken, "")
                                          .Replace(LevelNameToken, "")
                                          .Replace(UniqueNameToken, "");
            }
        }

    }       

    internal static class ColumnMapExtentions
    {
        internal static IEnumerable<ColumnMap> GetMemberProperties(this IEnumerable<ColumnMap> maps)
        {
            return maps.Where(a => a.Name.EndsWith(ColumnMap.LevelNameToken, StringComparison.OrdinalIgnoreCase) ||
                                   a.Name.EndsWith(ColumnMap.UniqueNameToken, StringComparison.OrdinalIgnoreCase) ||
                                   a.Name.EndsWith(ColumnMap.CaptionToken, StringComparison.OrdinalIgnoreCase));
        }

        internal static string GetMemberProperty(this ColumnMap map, Member member)
        {
            if (map.Name.EndsWith(ColumnMap.CaptionToken, StringComparison.OrdinalIgnoreCase))
            {
                return member.Caption;
            }
            else if (map.Name.EndsWith(ColumnMap.LevelNameToken, StringComparison.OrdinalIgnoreCase))
            {
                return member.LevelName;
            }
            else if (map.Name.EndsWith(ColumnMap.UniqueNameToken, StringComparison.OrdinalIgnoreCase))
            {
                return member.UniqueName;
            }

            return null;
        }

        internal static bool DoesMapMatchMember(this ColumnMap map, Member member, int ordinal)
        {
            // check to see if name is a number and if that number matches the ordinal   
            int number;
            if (Int32.TryParse(map.NameWithoutPrefixesOrSuffixes, out number))
            {
                return number == ordinal;
            }

            return member.LevelName == map.NameWithoutPrefixesOrSuffixes;
        }
    }

    class ColumnMapComparer : IEqualityComparer<ColumnMap>
    {
        public bool Equals(ColumnMap x, ColumnMap y)
        {
            //Check whether the compared objects reference the same data.
            if (Object.ReferenceEquals(x, y)) return true;

            //Check whether any of the compared objects is null.
            if (Object.ReferenceEquals(x, null) || Object.ReferenceEquals(y, null))
            return false;

            //Check whether the products' properties are equal.
            return x.NameWithoutPrefixes == y.NameWithoutPrefixes;
        }

        public int GetHashCode(ColumnMap obj)
        {
            //Check whether the object is null
            if (Object.ReferenceEquals(obj, null)) return 0;

            //Get hash code for the Name field if it is not null.
            return obj.Name == null ? 0 : obj.NameWithoutPrefixes.GetHashCode();

        }
    }

}
