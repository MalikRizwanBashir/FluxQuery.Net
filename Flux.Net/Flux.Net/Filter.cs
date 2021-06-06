using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Flux.Net
{
    public class FluxFilter
    {
        internal string FilterQuery = string.Empty;
        internal string SelectQuery = string.Empty;
        internal string MeasurementName = string.Empty;
        internal string Aggregates = string.Empty;
        internal string Functions = string.Empty;
        public FluxFilter()
        {
        }

        public FluxFilter(string select, string filter)
        {
            SelectQuery = select;
            SelectQuery = filter;
        }

        public FluxFilter Filter(string filters)
        {
            FilterQuery = filters;
            return this;
        }

        public FluxFilter Measurement(string measurement)
        {
            MeasurementName = measurement;
            return this;
        }

        public FluxFilter Select(Action<FluxSelect> filter)
        {
            var selectFilter = new FluxSelect();
            filter.Invoke(selectFilter);
            SelectQuery = $" And ( {selectFilter.Select} )";
            return this;
        }
    }

    public class FluxSelect
    {
        internal string Select = string.Empty;
        public FluxSelect()
        {
            Select = string.Empty;
        }

        public FluxSelect Fields(params string[] fields)
        {
            var sp = "r._field == " + string.Join(@" or r._field == ", fields.Select(s => { return $@"'{s}'"; }));
            Select = $"{Select} and {sp} ";
            return this;
        }

        public FluxSelect Tags(params string[] fields)
        {
            var sp = "r.tag == " + string.Join(@" or r.tag == ", fields.Select(s => { return $@"'{s}'"; }));
            Select = $"{Select} and {sp} ";
            return this;
        }
    }
}
