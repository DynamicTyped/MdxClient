using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MdxClient
{
    internal class Member
    {
        public string Caption { get; set; }
        public string UniqueName { get; set; }
        public string LevelName { get; set; }
        public string LevelNumber { get; set; }
        public List<DimensionProperty> DimensionProperties { get; set; }

    }
}
