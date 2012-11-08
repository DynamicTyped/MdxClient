using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.Runtime.Serialization;

namespace MdxClient
{
    [Serializable()]
    internal class Column : ISerializable
    {

        public string Name { get; set; }
        public List<string> Items { get; set; }
        public Type Type { get; set; }
        public int Ordinal { get; set; }

        public Column()
        {
            Items = new List<string>();
        }

        public Column(SerializationInfo info, StreamingContext context)
        {
            Name = (string)info.GetValue("Name", typeof(string));
            Items = (List<string>)info.GetValue("Items", typeof(List<string>));
            Type = (Type)info.GetValue("DefaultType", typeof(Type));
        }

        #region ISerializable Members

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("Name", Name);
            info.AddValue("Items", Items);
            info.AddValue("DefaultType", Type);
        }

        #endregion
    }
}
