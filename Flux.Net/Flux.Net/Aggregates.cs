namespace Flux.Net
{
    public class Aggregates
    {
        internal string _Aggregates = string.Empty;
        public Aggregates()
        {
            _Aggregates = string.Empty;
        }

        public Aggregates Aggregate(string methodName, string column = "_value")
        {
            _Aggregates = $@"{_Aggregates} 
|> {methodName}(column: ""{ column }"") ";
            return this;
        }

        public Aggregates Mean(string column = "_value")
        {
            _Aggregates = @$"{_Aggregates} 
|> mean(column: ""{ column }"") ";
            return this;
        }

        public Aggregates Min(string column = "_value")
        {
            _Aggregates = @$"{_Aggregates} 
|> min(column: ""{ column }"") ";
            return this;
        }

        public Aggregates Max(string column = "_value")
        {
            _Aggregates = @$"{_Aggregates} 
|> max(column: ""{ column }"") ";
            return this;
        }

        public Aggregates Sum(string column = "_value")
        {
            _Aggregates = @$"{_Aggregates} 
|> sum(column: ""{ column }"") ";
            return this;
        }

        public Aggregates Mode(string column = "_value")
        {
            _Aggregates = @$"{_Aggregates} 
|> mode(column: ""{ column }"") ";
            return this;
        }
        /// <summary>
        /// Max-Min
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
        public Aggregates Spread(string column = "_value")
        {
            _Aggregates = @$"{_Aggregates} 
|> spread(column: ""{ column }"") ";
            return this;
        }

        /// <summary>
        /// The average over a period populated by n values is equal to their algebraic mean.
        /// The average over a period populated by only null values is null.
        /// Moving averages skip null values.
        /// If n is less than the number of records in a table, movingAverage returns the average of the available values.
        /// </summary>
        /// <param name="nRecords"></param>
        /// <returns></returns>
        public Aggregates MovingAverage(int nRecords)
        {
            _Aggregates = @$"{_Aggregates} 
|> movingAverage(n: ""{ nRecords }"") ";
            return this;
        }

        /// <summary>
        /// calculates the mean of values in a defined time range at a specified frequency
        /// </summary>
        /// <param name="interval">The frequency of time windows. example 1m, 1h, 1d, mo, 1y</param>
        /// <param name="duration">The length of averaged time window</param>
        /// <param name="column"></param>
        /// <returns></returns>
        public Aggregates TimedMovingAverage(string interval, string duration, string column = "_value")
        {
            _Aggregates = @$"{_Aggregates} 
|> timedMovingAverage(every: { interval }, period: { duration }, column: ""{ column }"") ";
            return this;
        }

        /// <summary>
        /// calculates the mean of values in a defined time range at a specified frequency
        /// </summary>
        /// <param name="interval">The frequency of time windows. example 1m, 1h, 1d, mo, 1y</param>
        /// <param name="aggregateMethod">example count, sum, mean</param>
        /// <returns></returns>
        public Aggregates AggregateWindow(string interval, string aggregateMethod)
        {
            _Aggregates = @$"{_Aggregates} 
|> aggregateWindow(every: { interval }, fn: { aggregateMethod }) ";
            return this;
        }
    }
}
