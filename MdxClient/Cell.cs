using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace MdxClient
{
    [Serializable()]
    internal class Cell : ISerializable
    {
        public object FormattedValue { get; set; }
        public object Value { get; set; }
        public int Ordinal { get; set; }
        public string Type { get; set; }

        public Cell()
        {
        }

        public Cell(SerializationInfo info, StreamingContext context)
        {
            FormattedValue = (string)info.GetValue("FormattedValue", typeof(string));
            Value = info.GetValue("Value", typeof(object));
            Ordinal = (int)info.GetValue("Ordinal", typeof(int));
        }

        #region ISerializable Members

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("FormattedValue", FormattedValue);
            info.AddValue("Value", Value);
            info.AddValue("Ordinal", Ordinal);
        }

        #endregion
    }
}
