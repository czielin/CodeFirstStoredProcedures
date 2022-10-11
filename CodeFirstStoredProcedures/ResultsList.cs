using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace CodeFirstStoredProcs
{
    /// <summary>
    /// Holds multiple Result Sets returned from a Stored Procedure call. 
    /// </summary>
    public class ResultsList : IEnumerable
    {
        // our internal object that is the list of results lists
        List<List<object>> thelist = new List<List<object>>();

        /// <summary>
        /// Add a results list to the results set
        /// </summary>
        /// <param name="list"></param>
        public void Add(List<object> list)
        {
            thelist.Add(list);
        }

        /// <summary>
        /// Return an enumerator over the internal list
        /// </summary>
        /// <returns>Enumerator over List<object> that make up the result sets </returns>
        public IEnumerator GetEnumerator()
        {
            return thelist.GetEnumerator();
        }

        /// <summary>
        /// Return the count of result sets
        /// </summary>
        public Int32 Count
        {
            get { return thelist.Count; }
        }

        /// <summary>
        /// Get the nth results list item
        /// </summary>
        /// <param name="index"></param>
        /// <returns>List of objects that make up the result set</returns>
        public List<object> this[int index]
        {
            get { return thelist[index]; }
        }

        /// <summary>
        /// Return the result set that contains a particular type and does a cast to that type.
        /// </summary>
        /// <typeparam name="T">Type that was listed in StoredProc object as a possible return type for the stored procedure</typeparam>
        /// <returns>List of T; if no results match, returns an empty list</returns>
        public List<T> ToList<T>()
        {
            // search each non-empty results list 
            foreach (List<object> list in thelist.Where(p => p.Count > 0).Select(p => p))
            {
                // compare types of the first element - this is why we filter for non-empty results
                if (list[0] is T)
                {
                    // do cast to return type
                    return list.Cast<T>().Select(p => p).ToList();
                }
            }

            // no matches? return empty list
            return new List<T>();
        }

        /// <summary>
        /// Return the result set that contains a particular type and does a cast to that type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns>Array of T; if no results match, returns an empty array</returns>
        public T[] ToArray<T>()
        {
            // search each non-empty results list 
            foreach (List<object> list in thelist.Where(p => p.Count > 0).Select(p => p))
            {
                // compare types of the first element - this is why we filter for non-empty results
                if (typeof(T) == list[0].GetType())
                {
                    // do cast to return type
                    return list.Cast<T>().Select(p => p).ToArray();
                }
            }

            // no matches? return empty array
            return new T[0];
        }
    }
}
