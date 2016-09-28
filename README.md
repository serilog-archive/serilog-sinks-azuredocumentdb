# Serilog.Sinks.AzureDocumentDB
A Serilog sink that writes to Azure DocumentDB.

## Getting started
Install [Serilog.Sinks.AzureDocumentDB](https://www.nuget.org/packages/serilog.sinks.azuredocumentdb) from NuGet

```PowerShell
Install-Package Serilog.Sinks.AzureDocumentDB
```

Configure logger by calling `WriteTo.AzureDocumentDB(<uri>, <key>)`

```C#
var log = new LoggerConfiguration()
    .WriteTo.AzureDocumentDB(<uri>, <secure-key>)
    .CreateLogger();
```
## Performance
Sink buffers log internally and flush to Azure DocumentDB in batches using dedicated thread. However, it highly depends on type of Azure DocumentDB subscription you have. 


[![Build status](https://ci.appveyor.com/api/projects/status/p9elqjetu6vsnmxw/branch/master?svg=true)](https://ci.appveyor.com/project/serilog/serilog-sinks-azuredocumentdb/branch/master)

