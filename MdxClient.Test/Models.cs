using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Test
{
    public class StandardScore
    {
        public string Label { get; set; }
        public double Score { get; set; }
        public string OverrideLabel { get; set; }
    }

    public partial class Metric
    {        
        public virtual string Label { get; set; }
     
        public virtual string MdxValue { get; set; }
     
        public virtual string SqlValue { get; set; }
     
        public virtual double? Score { get; set; }
     
        public virtual double? RawScore { get; set; }
     
        public virtual double? ComparatorScore { get; set; }
     
        public virtual Int64? Rank { get; set; }
        
        public virtual Int64? MaxRank { get; set; }
        
        public virtual int? Count { get; set; }
        
        public virtual double? Range1 { get; set; }
        
        public virtual double? Range2 { get; set; }
        
        public virtual double? Range3 { get; set; }
        
        public virtual string ParentMdxValue { get; set; }
        
        public virtual string ParentSqlValue { get; set; }
                
        public virtual string SupplementalLabel { get; set; }
        
        public virtual string SupplementalMdxValue { get; set; }
        
        public virtual string SupplementalSqlValue { get; set; }
        
        public virtual string SupplementalParentMdxValue { get; set; }
        
        public int? HierarchyLevel { get; set; }
        
        public int? SupplementalHiearchyLevel { get; set; }
        
        public double? Weight { get; set; }
    }

    public class AlertState
    {
        public virtual int? New { get; set; }
        public virtual Int16? Open { get; set; }
        public virtual Int16? Closed { get; set; }
        public virtual int? Total { get; set; }

    }
}
