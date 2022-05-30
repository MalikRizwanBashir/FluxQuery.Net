using System;
using System.Collections.Generic;
using System.Linq;

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

        public FluxFilter Where(string filters)
        {
            FilterQuery = $"and {filters}";
            return this;
        }

        public FluxFilter Where(IDictionary<string, string> tagFilters)
        {
            if (tagFilters == null || tagFilters.Count() == 0)
                return this;

            var conditions = tagFilters
                .Select(kvp => $"r.{kvp.Key} == \"{kvp.Value}\"");

            return Where(string.Join(" and ", conditions));
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
            SelectQuery = $" and ({selectFilter.Select})";
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
            var sp = "r._field == " + string.Join(@" or r._field == ", fields.Select(s => { return $@"""{s}"""; }));
            if (string.IsNullOrEmpty(Select))
                Select = sp;
            else
                Select = $"{Select} and {sp} ";
            return this;
        }

        public FluxSelect Tags(params string[] fields)
        {
            var sp = "r.tag == " + string.Join(@" or r.tag == ", fields.Select(s => { return $@"""{s}"""; }));
            if (string.IsNullOrEmpty(Select))
                Select = sp;
            else
                Select = $"{Select} and {sp} ";
            return this;
        }
    }
}
