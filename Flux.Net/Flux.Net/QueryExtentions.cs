using NodaTime;
using System;
using System.Collections.Generic;
using System.Text;

namespace Flux.Net
{
    public static class QueryExtentions
    {
        public static string ToInfluxDateTime(this DateTime date)
        {
            // "yyyy-MM-ddTHH:mm:ss.fffZ";
            return date.ToUniversalTime().ToString("o");
        }

        public static string ToInfluxDateTime(this Instant date)
        {
            // "yyyy-MM-ddTHH:mm:ss.fffZ";
            return date.ToDateTimeUtc().ToString("o");
        }

        public static string ToInfluxDateTime(this OffsetDateTime date)
        {
            // "yyyy-MM-ddTHH:mm:ss.fffZ";
            return date.ToInstant().ToDateTimeUtc().ToString("o");
        }

        public static string ToInfluxDateTime(this ZonedDateTime date)
        {
            // "yyyy-MM-ddTHH:mm:ss.fffZ";
            return date.ToInstant().ToDateTimeUtc().ToString("o");
        }

        public static string ToInfluxDateTime(this LocalDateTime date)
        {
            // "yyyy-MM-ddTHH:mm:ss.fffZ";
            return date.InUtc().ToDateTimeUtc().ToString("o");
        }
    }
}
