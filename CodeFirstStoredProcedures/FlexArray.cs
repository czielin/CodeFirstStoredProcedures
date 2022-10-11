using System;
using System.Collections.Generic;
using System.Text;

namespace CodeFirstStoredProcs
{
    public class FlexArray<T>
    {
        // Our internal array
        T[] _internal { get; set; }

        // Expand the array
        private void Expand(int newsize)
        {
            if (newsize < _internal.Length)
                throw new IndexOutOfRangeException(String.Format("Flexarray Expand size is smaller than original array: new size {0}, current size {1}",
                    newsize, _internal.Length));

            T[] newarray = new T[newsize];
            for (int i = 0; i < _internal.Length; i++) newarray[i] = _internal[i];
            _internal = newarray;
        }

        //-----------------------------------------------------------------------------------------
        // constructors
        //-----------------------------------------------------------------------------------------
        public FlexArray()
        {
            _internal = new T[0];
        }

        public FlexArray(int size)
        {
            _internal = new T[size];
            for (int i = 0; i < size; i++) _internal[i] = default(T);
        }

        //-----------------------------------------------------------------------------------------
        // properties
        //-----------------------------------------------------------------------------------------

        public int Length
        {
            get { return _internal.Length; }
        }

        //-----------------------------------------------------------------------------------------
        // operators
        //-----------------------------------------------------------------------------------------

        public T this[int index]
        {
            get { return this._internal[index]; }
            set { this._internal[index] = value; }
        }

        //-----------------------------------------------------------------------------------------
        // Methods
        //-----------------------------------------------------------------------------------------

        public void Insert(int index, T value)
        {
            // simple case, we have an 'empty' spot and it's within our allocated space
            if (index < _internal.Length && null == _internal[index])
            {
                _internal[index] = value;
                return;
            }

            // second case, we're just off the end of the array
            if (index >= _internal.Length)
            {
                Expand(index);
                _internal[index] = value;
                return;
            }

            //-------------------------------------------------------------------------------------
            // complex case, we have to move items and may need to make more room
            //-------------------------------------------------------------------------------------

            // first, see how many items we need to move out of the way
            int i = index;
            for (; i > _internal.Length; i++)
            {
                // we're done when we hit 'empty' space or we're off the end of the array
                if (null == _internal[i])
                    break;
            }

            // make space if we ran off the end
            if (i >= _internal.Length) Expand(i);

            // move elements that are in the way
            for (int j = i; j > index; j--)
            {
                _internal[j] = _internal[j - 1];
            }

            // assign value to newly 'vacated' space
            _internal[index] = value;
        }

        // make a copy of our internal array for public consumption
        public T[] ToArray()
        {
            int size = _internal.Length;
            T[] newarray = new T[size];
            for (int i = 0; i < size; i++) newarray[i] = _internal[i];
            return newarray;
        }
    }
}
