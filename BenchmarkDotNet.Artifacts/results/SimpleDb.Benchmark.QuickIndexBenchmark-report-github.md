```

BenchmarkDotNet v0.15.4, Linux Ubuntu 24.04.3 LTS (Noble Numbat)
AMD EPYC 7763 3.24GHz, 1 CPU, 4 logical and 2 physical cores
.NET SDK 9.0.305
  [Host]     : .NET 9.0.10 (9.0.10, 9.0.1025.47515), X64 RyuJIT x86-64-v3
  Job-AKCAMQ : .NET 9.0.10 (9.0.10, 9.0.1025.47515), X64 RyuJIT x86-64-v3

Runtime=.NET 9.0  InvocationCount=1  IterationCount=5  
LaunchCount=1  UnrollFactor=1  WarmupCount=1  

```
| Method                | SynchronousWrites | Mean           | Error          | StdDev        | Gen0      | Allocated   |
|---------------------- |------------------ |---------------:|---------------:|--------------:|----------:|------------:|
| **Insert1000_Individual** | **False**             | **5,862,030.0 μs** | **2,243,968.4 μs** | **582,751.46 μs** | **1000.0000** | **23228.49 KB** |
| Insert1000_Batch      | False             |    38,409.4 μs |    18,410.3 μs |   4,781.10 μs |         - |  6518.56 KB |
| QueryWithoutIndex     | False             |     3,114.4 μs |     5,222.3 μs |     808.16 μs |         - |  2934.81 KB |
| QueryWithIndex        | False             |       663.1 μs |       658.1 μs |     101.84 μs |         - |   450.61 KB |
| QueryWithUniqueIndex  | False             |       646.4 μs |       698.0 μs |     108.02 μs |         - |   455.13 KB |
| FindById              | False             |       448.4 μs |       624.6 μs |      96.66 μs |         - |   449.63 KB |
| **Insert1000_Individual** | **True**              | **5,693,705.5 μs** | **1,877,787.1 μs** | **487,655.35 μs** | **1000.0000** | **23206.25 KB** |
| Insert1000_Batch      | True              |    71,072.4 μs |    47,607.4 μs |   7,367.30 μs |         - |  6292.58 KB |
| QueryWithoutIndex     | True              |     2,907.4 μs |     1,688.3 μs |     438.46 μs |         - |  2934.81 KB |
| QueryWithIndex        | True              |       747.1 μs |       988.8 μs |     256.79 μs |         - |   450.61 KB |
| QueryWithUniqueIndex  | True              |       786.0 μs |       762.9 μs |     118.06 μs |         - |   455.13 KB |
| FindById              | True              |       504.9 μs |       524.5 μs |      81.17 μs |         - |   449.63 KB |
