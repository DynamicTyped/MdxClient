using System;
using System.Collections.Generic;
using System.Linq;
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
        public const string LevelNumberToken = "##LevelNumber##";
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
                                          .Replace(UniqueNameToken, "")
                                          .Replace(LevelNumberToken, "");
            }
        }

        public bool IsLevelName { get { return IsTokenName(LevelNumberToken); } }
        public bool IsUniqueName { get { return IsTokenName(UniqueNameToken); } }
        public bool IsCaption { get { return IsTokenName(CaptionToken); } }
        public bool IsLevelNumber { get { return IsTokenName(LevelNumberToken); } }

        private bool IsTokenName(string token)
        {
            return Name.EndsWith(token, StringComparison.OrdinalIgnoreCase);
        }
    }       

    internal static class ColumnMapExtentions
    {
        internal static IEnumerable<ColumnMap> GetMemberProperties(this IEnumerable<ColumnMap> maps)
        {
            return maps.Where(a => a.IsLevelName ||
                                   a.IsUniqueName ||
                                   a.IsCaption ||
                                   a.IsLevelName);
        }

        internal static string GetMemberProperty(this ColumnMap map, Member member)
        {
            if (map.Name.EndsWith(ColumnMap.CaptionToken, StringComparison.OrdinalIgnoreCase))
            {
                return member.Caption;
            }
            if (map.Name.EndsWith(ColumnMap.LevelNameToken, StringComparison.OrdinalIgnoreCase))
            {
                return member.LevelName;
            }
            if (map.Name.EndsWith(ColumnMap.UniqueNameToken, StringComparison.OrdinalIgnoreCase))
            {
                return member.UniqueName;
            }
            if (map.Name.EndsWith(ColumnMap.LevelNumberToken, StringComparison.OrdinalIgnoreCase))
            {
                return member.LevelNumber;
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
            if (ReferenceEquals(x, y)) return true;

            //Check whether any of the compared objects is null.
            if (ReferenceEquals(x, null) || ReferenceEquals(y, null))
            return false;

            //Check whether the products' properties are equal.
            return x.NameWithoutPrefixes == y.NameWithoutPrefixes;
        }

        public int GetHashCode(ColumnMap obj)
        {
            //Check whether the object is null
            if (ReferenceEquals(obj, null)) return 0;

            //Get hash code for the Name field if it is not null.
            return obj.Name == null ? 0 : obj.NameWithoutPrefixes.GetHashCode();

        }
    }

}
