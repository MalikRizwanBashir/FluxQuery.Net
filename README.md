# Flux.Net
[![Nuget](https://img.shields.io/nuget/v/FluxQuery.Net)](https://www.nuget.org/packages/FluxQuery.Net/)

Flux query builder fluent api for C#

## Example:

```csharp
QueryBuilder.From("datasource", "retention")
                .Filter(f => f.Measurement("measurementName")
                  .Select(s => s.Tags("tag1", "tag2", "tag3")
                    .Fields("field1", "field2"))
                  .Filter("tag1==tagevalue"))
                .AbsoluteTimeRange(DateTime.Now.AddDays(-1), DateTime.Now);
```
