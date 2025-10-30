```

BenchmarkDotNet v0.15.4, Linux Ubuntu 24.04.3 LTS (Noble Numbat)
AMD EPYC 7763 3.05GHz, 1 CPU, 4 logical and 2 physical cores
.NET SDK 9.0.305
  [Host]     : .NET 9.0.10 (9.0.10, 9.0.1025.47515), X64 RyuJIT x86-64-v3
  Job-AKCAMQ : .NET 9.0.10 (9.0.10, 9.0.1025.47515), X64 RyuJIT x86-64-v3

Runtime=.NET 9.0  InvocationCount=1  IterationCount=5  
LaunchCount=1  UnrollFactor=1  WarmupCount=1  

```
| Method                | SynchronousWrites | Mean           | Error          | StdDev        | Gen0      | Allocated   |
|---------------------- |------------------ |---------------:|---------------:|--------------:|----------:|------------:|
| **Insert1000_Individual** | **False**             | **5,486,274.0 μs** | **1,461,983.0 μs** | **379,672.35 μs** | **1000.0000** | **23238.09 KB** |
| Insert1000_Batch      | False             |    44,393.3 μs |    29,080.6 μs |   7,552.15 μs |         - |  6389.87 KB |
| QueryWithoutIndex     | False             |     2,545.5 μs |     1,418.6 μs |     219.53 μs |         - |  2934.81 KB |
| QueryWithIndex        | False             |       657.1 μs |       860.4 μs |     133.14 μs |         - |   450.61 KB |
| QueryWithUniqueIndex  | False             |       690.2 μs |       618.7 μs |      95.74 μs |         - |   455.13 KB |
| FindById              | False             |       441.0 μs |       515.8 μs |      79.82 μs |         - |   449.63 KB |
| **Insert1000_Individual** | **True**              | **5,913,163.0 μs** |   **789,081.2 μs** | **122,111.19 μs** | **1000.0000** | **23370.44 KB** |
| Insert1000_Batch      | True              |    75,912.9 μs |    56,955.8 μs |  14,791.25 μs |         - |  6376.51 KB |
| QueryWithoutIndex     | True              |     3,278.7 μs |     3,416.7 μs |     887.30 μs |         - |  2934.97 KB |
| QueryWithIndex        | True              |       649.9 μs |       778.3 μs |     120.44 μs |         - |   450.61 KB |
| QueryWithUniqueIndex  | True              |       693.7 μs |       827.4 μs |     128.04 μs |         - |   455.13 KB |
| FindById              | True              |       398.6 μs |       707.5 μs |     109.48 μs |         - |   449.63 KB |
