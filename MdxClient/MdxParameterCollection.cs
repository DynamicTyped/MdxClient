using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Collections;
using System.ComponentModel;
using System.Globalization;
using System.Data.Common;

namespace MdxClient
{
    public class MdxParameterCollection : DbParameterCollection
	{
		private MdxCommand _parent;
		private ArrayList _items;

        internal MdxParameterCollection(MdxCommand parent)
        {
            this._parent = parent;
            this._items = new ArrayList();
        }

        /// <summary>
        /// Adds an MdxParameter object to the MdxParameterCollection collection.
        /// </summary>
        /// <param name="value">The MdxParameter object to be added.</param>
        public MdxParameter Add(MdxParameter value)
        {
            this.Validate(-1, value);
            value.Parent = this;
            this._items.Add(value);
            return value;
        }

        /// <summary>
        /// Creates a new MdxParameter object with the specified name and value and adds it to the collection.
        /// </summary>
        /// <param name="parameterName">The name of the MdxParameter to add.</param>
        /// <param name="value">The value of the MdxParameter object to be created.</param>
        public MdxParameter Add(string parameterName, object value)
        {
            return this.Add(new MdxParameter(parameterName, value));
        }

        /// <summary>
        /// Creates a new MdxParameter object with the specified name and value and adds it to the collection.
        /// </summary>
        /// <param name="value">The MdxParameter object to be added.</param>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public override int Add(object value)
        {
            this.ValidateType(value);
            this.Add((MdxParameter)value);
            return this.Count - 1;
        }

        /// <summary>
        /// Adds an array of items with the specified values to the MdxParameterCollection.
        /// </summary>
        /// <param name="values">An array of values of type MdxParameter to add to the collection.</param>
        public void AddRange(MdxParameter[] values)
        {
            this.AddRange(values);
        }

        /// <summary>
        /// Adds an array of items with the specified values to the MdxParameterCollection.
        /// </summary>
        /// <param name="values">An array of values of type MdxParameter to add to the collection.</param>
        public override void AddRange(Array values)
        {
            if (null != values)
            {
                foreach (object item in values)
                {
                    this.Add(item);
                }
            }
        }

        /// <summary>
        /// Removes all the MdxParameter objects from the MdxParameterCollection.
        /// </summary>
        public override void Clear()
        {
            int count = this._items.Count;
            for (int i = 0; i < count; i++)
            {
                ((MdxParameter)this._items[i]).Parent = null;
            }
            this._items.Clear();
        }

        /// <summary>
        /// Indicates whether an MdxParameter with the specified property exists in the collection.
        /// </summary>
        /// <param name="value">The value of the MdxParameter to look for in the collection.</param>
        public bool Contains(MdxParameter value)
        {
            return -1 != this.IndexOf(value);
        }

        /// <summary>
        /// Indicates whether an MdxParameter with the specified property exists in the collection.
        /// </summary>
        /// <param name="value">The value of the MdxParameter to look for in the collection.</param>
        public override bool Contains(object value)
        {
            return this.Contains((MdxParameter)value);
        }

        /// <summary>
        /// Indicates whether an MdxParameter with the specified name exists in the collection.
        /// </summary>
        /// <param name="value">The name of the MdxParameter to look for in the collection.</param>
        public override bool Contains(string value)
        {
            return -1 != this.IndexOf(value);
        }

        /// <summary>
        /// Copies an array of items to the collection starting at the specified index.
        /// </summary>
        /// <param name="array">The array of items to copy to the collection.</param>
        /// <param name="index">The index in the collection to copy the items.</param>
        public void CopyTo(MdxParameter[] array, int index)
        {
            this._items.CopyTo(array, index);
        }

        /// <summary>
        /// Copies an array of items to the collection starting at the specified index.
        /// </summary>
        /// <param name="array">The array of items to copy to the collection.</param>
        /// <param name="index">The index in the collection to copy the items.</param>
        public override void CopyTo(Array array, int index)
        {
            this._items.CopyTo(array, index);
        }

        /// <summary>
        /// Gets the number of MdxParameter objects in the MdxParameterCollection collection.
        /// </summary>
        public override int Count
        {
            get
            {
                return this._items.Count;
            }
        }

        /// <summary>
        /// Exposes the GetEnumerator method, which supports a simple iteration over a collection by a .NET Framework data provider.
        /// </summary>
        public override IEnumerator GetEnumerator()
        {
            return this._items.GetEnumerator();
        }

        protected override DbParameter GetParameter(string parameterName)
        {
            MdxParameter mdxParameter = this.Find(parameterName);
            if (mdxParameter == null)
                throw new ArgumentException(parameterName, "parameterName");
            
            return mdxParameter;
        }

        protected override DbParameter GetParameter(int index)
        {
            
            if (this._items.IndexOf(index) < 0)
                throw new ArgumentException("Index does not exist", "index");

            return (DbParameter) this._items[index];
        }

        /// <summary>
        /// Returns the index of the specified MdxParameter object.
        /// </summary>
        /// <param name="parameterName">The name of the MdxParameter object in the collection.</param>
        public override int IndexOf(string parameterName)
        {
            int count = this._items.Count;
            for (int i = 0; i < count; i++)
            {
                if (parameterName == ((MdxParameter)this._items[i]).ParameterName)
                {
                    return i;
                }
            }
            return -1;
        }

        /// <summary>
        /// Returns the index of the specified MdxParameter object.
        /// </summary>
        /// <param name="value">The MdxParameter object in the collection.</param>
        public int IndexOf(MdxParameter value)
        {
            return this._items.IndexOf(value);
        }

        /// <summary>
        /// Returns the index of the specified MdxParameter object.
        /// </summary>
        /// <param name="value">The MdxParameter object in the collection.</param>
        public override int IndexOf(object value)
        {
            return this.IndexOf((MdxParameter)value);
        }


        public void Insert(int index, MdxParameter value)
        {
            this.Validate(-1, value);
            value.Parent = this;
            this._items.Insert(index, value);
        }

        /// <summary>
        /// Inserts the specified index of the MdxParameter object with the specified name into the collection at the specified index.
        /// </summary>
        /// <param name="index">The index at which to insert the MdxParameter object.</param>
        /// <param name="value">The MdxParameter object to insert into the collection.</param>
        public override void Insert(int index, object value)
        {
            this.ValidateType(value);
            this.Insert(index, (MdxParameter)value);
        }

        /// <summary>
        /// Specifies whether the collection is a fixed size.  This always returns false.
        /// </summary>
        public override bool IsFixedSize
        {
            get { return false; }
        }

        /// <summary>
        /// Specifies whether the collection is read-only.  This always returns false.
        /// </summary>
        public override bool IsReadOnly
        {
            get { return false; }
        }

        /// <summary>
        /// Specifies whether the collection is synchronized.  This always retuns false.
        /// </summary>
        public override bool IsSynchronized
        {
            get { return false; }
        }

        /// <summary>
        /// Removes the specified MdxParameter object from the collection.
        /// </summary>
        /// <param name="value">The MdxParameter object to remove.</param>
        public void Remove(MdxParameter value)
        {
            int num = this.IndexOf(value);
            if (-1 != num)
            {
                this.RemoveIndex(num);
                return;
            }
            throw new ArgumentException("Property does not exist", "value");
        }

        /// <summary>
        /// Removes the specified MdxParameter object from the collection.
        /// </summary>
        /// <param name="value">The MdxParameter object to remove.</param>
        public override void Remove(object value)
        {
            this.ValidateType(value);
            this.Remove((MdxParameter)value);
        }

        /// <summary>
        /// Removes a specified MdxParameter object from the collection.
        /// </summary>
        /// <param name="parameterName">The name of the MdxParameter object to remove.</param>
        public override void RemoveAt(string parameterName)
        {
            int index = this.RangeCheck(parameterName);
            this.RemoveIndex(index);
        }

        /// <summary>
        /// Removes the MdxParameter object at the specified from the collection.
        /// </summary>
        /// <param name="index">The index where the MdxParameter object is located.</param>
        public override void RemoveAt(int index)
        {
            this.RangeCheck(index);
            this.RemoveIndex(index);
        }

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            int index = this.RangeCheck(parameterName);
            this.ValidateType(value);
            this.Replace(index, (MdxParameter)value);
        }

        protected override void SetParameter(int index, DbParameter value)
        {
            this.ValidateType(value);
            this[index] = (MdxParameter)value;
        }

        /// <summary>
        /// Specifies the object to be used to synchronize access to the collection.
        /// </summary>
		public override object SyncRoot
		{
			get { return this; }
		}

		private Type ItemType
		{
			get
			{
				return typeof(MdxParameter);
			}
		}

        private MdxParameter Find(string parameterName)
		{
			if (parameterName == null)
			{
				throw new ArgumentNullException("parameterName");
			}
			int num = this.IndexOf(parameterName);
			if (num < 0)
			{
				return null;
			}
            return (MdxParameter)this._items[num];
		}

		private void RangeCheck(int index)
		{
			if (index < 0 || this.Count <= index)
			{
				throw new ArgumentOutOfRangeException("index");
			}
		}

		private int RangeCheck(string parameterName)
		{
			int num = this.IndexOf(parameterName);
			if (num < 0)
			{
				throw new ArgumentOutOfRangeException("parameterName");
			}
			return num;
		}

        private void Replace(int index, MdxParameter newValue)
		{
			this.Validate(index, newValue);
            ((MdxParameter)this._items[index]).Parent = null;
			newValue.Parent = this;
			this._items[index] = newValue;
		}

        internal void Validate(int index, MdxParameter value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			if (value.Parent != null)
			{
				if (this != value.Parent)
				{
					throw new ArgumentException("mismatch", "value");
				}
				if (index != this.IndexOf(value.ParameterName))
				{
					throw new ArgumentException("already exists", "value");
				}
			}
			string text = value.ParameterName;
			if (text.Length == 0)
			{
				index = 1;
				int num = 0;
				while (index < 2147483647 && num != -1)
				{
					text = "Parameter" + index.ToString(CultureInfo.InvariantCulture);
					num = this.IndexOf(text);
					index++;
				}
				if (-1 != num)
				{
					text = "Parameter" + Guid.NewGuid().ToString();
				}
				value.ParameterName = text;
			}
		}

		private void ValidateType(object value)
		{
			if (value == null)
			{
				throw new ArgumentNullException("value");
			}
			if (!this.ItemType.IsInstanceOfType(value))
			{
				throw new ArgumentException("wrong type", "value");
			}
		}

        private void RemoveIndex(int index)
        {
            ((MdxParameter)this._items[index]).Parent = null;
            this._items.RemoveAt(index);
        }
	}
}
