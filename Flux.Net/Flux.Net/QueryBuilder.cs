using NodaTime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Flux.Net
{
    public class QueryBuilder
    {
        public static FluxQuery From(string dataSource, string retentionPolicy = "autogen")
        {
            return new FluxQuery(dataSource, retentionPolicy);
        }
    }

    public class FluxQuery
    {
        private string query = string.Empty;
        private string limitRecords = string.Empty;
        private string sortRecords = string.Empty;
        private string window = string.Empty;
        private FluxFilter filter = new FluxFilter();
        private Aggregates Aggregate = new Aggregates();
        private Functions Function = new Functions();
        public FluxQuery(string dataSource, string retentionPolicy = "autogen")
        {
            query = $@"from(bucket:""{dataSource}/{retentionPolicy}"") ";
        }

        #region time range
        public FluxQuery RelativeTimeRange(KeyValuePair<TimeUnit, double> start, KeyValuePair<TimeUnit, double>? end = null)
        {
            var startUnit = GetTimeUnit(start.Key);
            if (end == null)
            {
                query = @$"{query}
                        |> range(start: {start.Value}{startUnit})";
            }
            else
            {
                var endUnit = GetTimeUnit(start.Key);
                query = @$"{query}
                        |> range(start: {start.Value}{startUnit}, stop: {end.Value}{endUnit}) ";
            }
            return this;
        }

        public FluxQuery AbsoluteTimeRange(Instant start, Instant? end = null)
        {
            string endDateTime = DateTime.UtcNow.ToInfluxDateTime();
            if (end.HasValue)
            {
                endDateTime = end.Value.ToInfluxDateTime();
                AbsoluteTimeRange(start.ToInfluxDateTime(), endDateTime);
            }
            else
                AbsoluteTimeRange(start.ToInfluxDateTime());
            return this;
        }

        public FluxQuery AbsoluteTimeRange(OffsetDateTime start, OffsetDateTime? end = null)
        {
            string endDateTime = DateTime.UtcNow.ToInfluxDateTime();
            if (end.HasValue)
            {
                endDateTime = end.Value.ToInfluxDateTime();
                AbsoluteTimeRange(start.ToInfluxDateTime(), endDateTime);
            }
            else
                AbsoluteTimeRange(start.ToInfluxDateTime());
            return this;
        }

        public FluxQuery AbsoluteTimeRange(ZonedDateTime start, ZonedDateTime? end = null)
        {
            string endDateTime = DateTime.UtcNow.ToInfluxDateTime();
            if (end.HasValue)
            {
                endDateTime = end.Value.ToInfluxDateTime();
                AbsoluteTimeRange(start.ToInfluxDateTime(), endDateTime);
            }
            else
                AbsoluteTimeRange(start.ToInfluxDateTime());

            return this;
        }

        public FluxQuery AbsoluteTimeRange(DateTime start, DateTime? end = null)
        {
            string endDateTime = DateTime.UtcNow.ToInfluxDateTime();
            if (end.HasValue)
            {
                endDateTime = end.Value.ToInfluxDateTime();
                AbsoluteTimeRange(start.ToInfluxDateTime(), endDateTime);
            }
            else
                AbsoluteTimeRange(start.ToInfluxDateTime());
            return this;
        }

        private FluxQuery AbsoluteTimeRange(string start, string end)
        {
            query = @$"{query}
                        |> range(start: {start}, stop: {end}) ";
            return this;
        }

        private FluxQuery AbsoluteTimeRange(string start)
        {
            query = @$"{query}
                        |> range(start: {start}) ";
            return this;
        }
        #endregion


        #region filter

        public FluxQuery Filter(Action<FluxFilter> filterAction)
        {
            filter = new FluxFilter();
            filterAction.Invoke(filter);

            return this;
        }

        #endregion

        public FluxQuery Aggregates(Action<Aggregates> filterAction)
        {
            Aggregate = new Aggregates();
            filterAction.Invoke(Aggregate);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="interval">The frequency of time windows. example 1m, 1h, 1d, mo, 1y</param>
        /// <param name="filterAction"></param>
        /// <returns></returns>
        public FluxQuery Window(string interval, Action<Aggregates> filterAction = null)
        {
            window = @$"window(every: {interval})";
            if (filterAction != null)
            {
                Aggregate = new Aggregates();
                filterAction.Invoke(Aggregate);
            }
            return this;
        }

        public FluxQuery Functions(Action<Functions> filterAction)
        {
            Function = new Functions();
            filterAction.Invoke(Function);
            return this;
        }

        public FluxQuery Count()
        {
            limitRecords = @$"|> count() ";
            return this;
        }

        public FluxQuery Sort(bool desc, params string[] columns)
        {
            sortRecords = @$"|> sort(columns: [{ string.Join(@" ,", columns.Select(s => { return $@"""{s}"""; })) } ], desc: {desc}) ";
            return this;
        }

        public FluxQuery Limit(int limit, int offset = 0)
        {
            limitRecords = @$"|> limit(n: {limit}, offset: {offset}) ";
            return this;
        }

        public string ToQuery()
        {
            var select = filter?.SelectQuery;
            var filt = filter?.FilterQuery;
            var m = filter?.MeasurementName;
            var aggr = Aggregate?._Aggregates;
            var fun = Function?._Functions;
            string queryString = string.Empty;
            string filterQuery = string.Empty;

            if (!string.IsNullOrEmpty(m))
            {
                filterQuery = @$"{filterQuery} r._measurement == ""{m}"" ";
            }

            if (!string.IsNullOrEmpty(select))
            {
                filterQuery = @$"{filterQuery} 
                                 {select} ";
            }

            if (!string.IsNullOrEmpty(filt))
            {
                filterQuery = @$"{filterQuery} 
                                 {filt} ";
            }
            if (!string.IsNullOrEmpty(filterQuery))
            {
                query = @$"|> filter(fn: (r) => {filterQuery} )";
            }

            if (!string.IsNullOrEmpty(fun))
            {
                queryString = @$"{queryString} 
                                 {fun} ";
            }

            if (!string.IsNullOrEmpty(window))
            {
                queryString = @$"{queryString} 
                                 {window} ";
            }

            if (!string.IsNullOrEmpty(aggr))
            {
                queryString = @$"{queryString} 
                                 {aggr} ";
            }

            if (!string.IsNullOrEmpty(sortRecords))
            {
                queryString = @$"{queryString} 
                                 {sortRecords} ";
            }

            if (!string.IsNullOrEmpty(limitRecords))
            {
                queryString = @$"{queryString} 
                                 {limitRecords} ";
            }

            query = @$"{query}
                         {queryString}";
            return query;
        }


        private string GetTimeUnit(TimeUnit unit)
        {
            string un = string.Empty;
            switch (unit)
            {
                case TimeUnit.Seconds:
                    un = "s";
                    break;
                case TimeUnit.Minutes:
                    un = "m";
                    break;
                case TimeUnit.Hours:
                    un = "h";
                    break;
                case TimeUnit.Days:
                    un = "d";
                    break;
                case TimeUnit.Months:
                    un = "m";
                    break;
                case TimeUnit.Years:
                    un = "y";
                    break;
                default:
                    un = "d";
                    break;
            }
            return un;
        }
    }
}
