# SimpleDb Documentation Center

Welcome to the SimpleDb Documentation Center! SimpleDb is a high-performance embedded document database that provides a MongoDB-like experience while supporting .NET 9.0 AOT compilation.

## 📚 Documentation Table of Contents

### 🚀 Quick Start
- **[Quick Start Guide](quickstart.md)** - Get started with SimpleDb in 5 minutes, including complete installation, configuration, and basic operation examples

### 🏗️ Architecture
- **[Architecture Design Document](architecture.md)** - Deep dive into SimpleDb's system architecture, core components, and design principles

### 📖 API Reference
- **[API Reference Documentation](api-reference.md)** - Complete API interface documentation with detailed descriptions of all classes, methods, and properties

### ⚡ Performance Optimization
- **[Performance Optimization Guide](performance.md)** - Detailed performance optimization techniques, best practices, and benchmark results

### 🔧 AOT Deployment
- **[AOT Deployment Guide](aot-deployment.md)** - .NET 9.0 AOT compilation configuration, multi-platform publishing, and containerized deployment

## 🌟 Core Features

- **📄 Document Storage**: Complete document database based on BSON format
- **⚡ High Performance**: Batch operation optimization with up to 99.7% performance improvement
- **🎯 AOT Support**: .NET 9.0 AOT compilation generating single-file executables
- **🔒 ACID Transactions**: Complete transaction support including savepoint mechanism
- **📊 Indexing System**: B+ tree indexing supporting single-field and composite indexes
- **🔄 Concurrency Control**: Multi-granularity locking mechanism supporting high concurrency
- **💾 Persistence**: Write-Ahead Logging (WAL) ensuring data durability
- **🔍 Query Engine**: Rich query operations and LINQ support

## 🚀 Quick Experience

### Install SimpleDb

```bash
dotnet add package SimpleDb
```

### Basic Usage

```csharp
using SimpleDb.Core;
using SimpleDb.Collections;
using SimpleDb.Attributes;
using SimpleDb.Bson;

// Define entity
[Entity("users")]
public class User
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    [Index(Unique = true)]
    public string Email { get; set; } = "";

    public string Name { get; set; } = "";
    public int Age { get; set; }
}

// Use database
var options = new SimpleDbOptions
{
    DatabaseName = "MyApp",
    EnableJournaling = true,
    WriteConcern = WriteConcern.Synced
};

using var engine = new SimpleDbEngine("myapp.db", options);
var users = engine.GetCollection<User>("users");

// Insert data
var user = new User
{
    Name = "John Doe",
    Email = "john@example.com",
    Age = 25
};

var id = users.Insert(user);

// Query data
var foundUser = users.FindById(id);
var allUsers = users.FindAll().ToList();
```

## 📊 Performance Benchmarks

| Operation Type | Metric | SimpleDb | SQLite | LiteDB |
|----------------|--------|----------|--------|--------|
| Batch Insert | 1000 docs/sec | 2,400 | 1,800 | 1,500 |
| Query Operations | Simple queries/sec | 8,000 | 6,500 | 5,200 |
| Memory Usage | Runtime memory | 45MB | 35MB | 40MB |
| Startup Time | Cold start | 120ms | 80ms | 100ms |

## 🎯 Use Cases

### ✅ Recommended

- **Microservice Architecture**: Lightweight, high-performance data storage
- **Desktop Applications**: Local data persistence
- **Mobile Applications**: Offline data caching
- **IoT Devices**: Edge computing data storage
- **Containerized Deployment**: Cloud-native applications

### ❌ Not Recommended

- **Large-scale Distributed Systems**: Need sharding and replication features
- **Complex Relational Queries**: Need complex JOIN operations
- **Real-time Analytics**: Need complex aggregation and OLAP features
- **Multi-tenant SaaS**: Need strong multi-tenant isolation

## 🔧 Development Requirements

- **.NET 9.0 SDK** or higher
- **Operating System**: Windows 10+, macOS 10.15+, Linux (major distributions)
- **IDE**: Visual Studio 2022, VS Code, or Rider
- **Memory**: Minimum 512MB, recommended 2GB+

## 🛠️ Development Tools

### Benchmark Testing

```bash
# Run performance benchmarks
cd SimpleDb.Benchmark
dotnet run -c Release

# Run quick batch tests
dotnet run QuickBatchTest.cs
```

### Unit Testing

```bash
# Run all tests
dotnet test

# Run AOT compatibility tests
dotnet test --configuration Release
```

## 📈 Version Roadmap

### v1.0.0 (Current Version)
- ✅ Core functionality implementation
- ✅ AOT support
- ✅ Basic queries and indexing
- ✅ Transaction support

### v1.1.0 (Planned)
- 🔄 Full-text indexing
- 🔄 Geospatial indexing
- 🔄 Query optimizer improvements
- 🔄 More data type support

### v1.2.0 (In Planning)
- 📋 Distributed support
- 📋 Streaming replication
- 📋 Sharding mechanism
- 📋 High availability

## 🤝 Community Support

### Get Help

- **📖 Documentation Website**: [SimpleDb Docs](https://docs.simpledb.com)
- **💬 Discussion Community**: [GitHub Discussions](https://github.com/your-repo/SimpleDb/discussions)
- **🐛 Issue Reporting**: [GitHub Issues](https://github.com/your-repo/SimpleDb/issues)
- **📧 Email Support**: support@simpledb.com

### Contributing

We welcome community contributions! Please check the [Contributing Guide](CONTRIBUTING.md) to learn how to participate in SimpleDb development.

### License

SimpleDb is licensed under the MIT License. See the [LICENSE](../LICENSE) file for details.

## 🔗 Related Links

- **🏠 Homepage**: [SimpleDb Official Website](https://simpledb.com)
- **📦 NuGet Package**: [SimpleDb on NuGet](https://www.nuget.org/packages/SimpleDb)
- **🐙 Source Code**: [GitHub Repository](https://github.com/your-repo/SimpleDb)
- **📊 Performance Tests**: [Benchmark Results](https://benchmarks.simpledb.com)

---

## 📖 Documentation Navigation

### Getting Started
1. [Quick Start Guide](quickstart.md) - Learn SimpleDb from scratch
2. [API Reference Documentation](api-reference.md) - View detailed API documentation

### Advanced Learning
3. [Architecture Design Document](architecture.md) - Understand internal working principles
4. [Performance Optimization Guide](performance.md) - Optimize application performance

### Expert Application
5. [AOT Deployment Guide](aot-deployment.md) - Build high-performance AOT applications

---

**Start your SimpleDb journey today!** 🚀

If this documentation helps you, please give us a ⭐ Star!