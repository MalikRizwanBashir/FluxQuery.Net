using System.Linq;

namespace Flux.Net
{
    public class Functions
    {
        internal string _Functions = string.Empty;
        public Functions()
        {
            _Functions = string.Empty;
        }

        public Functions First()
        {
            _Functions = @$"{_Functions} 
|> first() ";
            return this;
        }

        public Functions Last()
        {
            _Functions = @$"{_Functions} 
|> last() ";
            return this;
        }

        public Functions Fill(string column, object value)
        {
            string val;
            if (value.GetType() == typeof(string))
            {
                val = @$"""{value}""";
            }
            else
            {
                val = @$"{value}";
            }
            _Functions = @$"{_Functions} 
|> fill(column: ""{ column }"", value: {val}) ";
            return this;
        }

        public Functions FillPrevious(string column)
        {
            _Functions = @$"{_Functions} 
|> fill(column: column: ""{ column }"", usePrevious: true) ";
            return this;
        }

        public Functions Fill(object value)
        {
            string val;
            if (value.GetType() == typeof(string))
            {
                val = @$"""{value}""";
            }
            else
            {
                val = @$"{value}";
            }
            _Functions = @$"{_Functions} 
|> fill(value: {val}) ";
            return this;
        }

        public Functions FillPrevious()
        {
            _Functions = @$"{_Functions} 
|> fill(usePrevious: true) ";
            return this;
        }

        public Functions Unique(string column)
        {
            _Functions = @$"{_Functions} 
|> unique(column: ""{ column }"") ";
            return this;
        }

        public Functions Distinct(string column)
        {
            _Functions = @$"{_Functions} 
|> distinct(column: ""{ column }"") ";
            return this;
        }

        public Functions Group(params string[] columns)
        {
            _Functions = @$"{_Functions} 
|> group(columns: [{ string.Join(@" ,", columns.Select(s => { return $@"""{s}"""; })) } ]) ";
            return this;
        }

        /// <summary>
        /// select only columns from source (reverse of drop)
        /// </summary>
        /// <param name="columns"></param>
        /// <returns></returns>
        public Functions KeepColumns(params string[] columns)
        {
            _Functions = @$"{_Functions} 
|> keep(columns: [{ string.Join(@" ,", columns.Select(s => { return $@"""{s}"""; })) } ]) ";
            return this;
        }

        /// <summary>
        /// drop columns from output
        /// </summary>
        /// <param name="columns"></param>
        /// <returns></returns>
        public Functions DropColumns(params string[] columns)
        {
            _Functions = @$"{_Functions} 
|> drop(columns: [{ string.Join(@" ,", columns.Select(s => { return $@"""{s}"""; })) } ]) ";
            return this;
        }

        /// <summary>
        /// Sort and take n record
        /// </summary>
        /// <param name="n"></param>
        /// <param name="columns">sorts on columns before taking n records</param>
        /// <returns></returns>
        public Functions Top(int n, params string[] columns)
        {
            if (columns != null && columns.Length > 0)
            {
                _Functions = @$"{_Functions} 
|> top(n:{n}, columns: [{ string.Join(@" ,", columns.Select(s => { return $@"""{s}"""; })) } ]) ";
            }
            else
            {
                _Functions = @$"{_Functions} 
|> top(n:{n}) ";
            }
            return this;
        }

        /// <summary>
        /// Sort and take n record
        /// </summary>
        /// <param name="n"></param>
        /// <param name="columns">sorts on columns before taking n records</param>
        /// <returns></returns>
        public Functions Bottom(int n, params string[] columns)
        {
            if (columns != null && columns.Length > 0)
            {
                _Functions = @$"{_Functions} 
|> bottom(n:{n}, columns: [{ string.Join(@" ,", columns.Select(s => { return $@"""{s}"""; })) } ]) ";
            }
            else
            {
                _Functions = @$"{_Functions} 
|> bottom(n:{n}) ";
            }
            return this;
        }

        /// <summary>
        /// duration of some value for column
        /// </summary>
        /// <param name="column"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public Functions StateDuration(string column, object value)
        {
            string val;
            if (value.GetType() == typeof(string))
            {
                val = @$"""{value}""";
            }
            else
            {
                val = @$"{value}";
            }
            _Functions = @$"{_Functions} 
|> stateDuration(fn: (r) => r.{column} == {val}, column: ""{column}"") ";
            return this;
        }

        /// <summary>
        /// count of some value for column
        /// </summary>
        /// <param name="column"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public Functions StateCount(string column, object value)
        {
            string val;
            if (value.GetType() == typeof(string))
            {
                val = @$"""{value}""";
            }
            else
            {
                val = @$"{value}";
            }
            _Functions = @$"{_Functions} 
|> stateCount(fn: (r) => r.{column} == {val}, column: ""{column}"") ";
            return this;
        }

        /// <summary>
        /// converts into table, instead of single point
        /// </summary>
        /// <param name="tags"></param>
        /// <returns></returns>
        public Functions Pivot(params string[] tags)
        {
            string columns = string.Empty;
            if (tags != null && tags.Length > 0)
            {
                columns = string.Join(" ,", tags.Select(s => { return $@"""{s}"""; }));
                columns = @$"""_time"", {columns} ";
            }
            else
                columns = @"""_time""";
            _Functions = @$"{_Functions} 
|> pivot(rowKey:[{columns}], columnKey:[""_field""], valueColumn:""_value"") ";
            return this;
        }
    }
}
