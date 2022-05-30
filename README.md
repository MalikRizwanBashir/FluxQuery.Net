# PW.FluxQueryNet

Flux query builder fluent API for C#.
Forked from [this project](https://github.com/MalikRizwanBashir/FluxQuery.Net) and customized.

## Example:

```csharp
QueryBuilder.From("datasource", "retention")
                .Filter(f => f.Measurement("measurementName")
                  .Select(s => s.Tags("tag1", "tag2", "tag3")
                    .Fields("field1", "field2"))
                  .Where("tag1 == 'tagevalue'"))
                .AbsoluteTimeRange(DateTime.Now.AddDays(-1), DateTime.Now)
                .ToQuery();
```
