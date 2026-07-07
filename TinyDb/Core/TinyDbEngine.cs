using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using TinyDb.Bson;
using TinyDb.Security;
using TinyDb.Collections;
using TinyDb.Index;
using TinyDb.Query;
using TinyDb.Serialization;
using TinyDb.Storage;
using TinyDb.Attributes;
using TinyDb.Utils;

namespace TinyDb.Core;

/// <summary>
/// TinyDb 核心存储引擎。
/// 该引擎管理数据库的完整生命周期，包括文件 I/O、页面管理、写前日志 (WAL)、
/// 事务协调、索引管理以及文档的 CRUD 操作。
/// </summary>
/// <remarks>
/// 架构层次：
/// 1. 存储层：IDiskStream -> PageManager -> WriteAheadLog (WAL)
/// 2. 逻辑层：CollectionMetaStore -> CollectionState -> IDocumentCollection
/// 3. 操作层：TransactionManager -> IndexManager -> QueryExecutor
///
/// 引擎确保在崩溃后通过 WAL 自动恢复数据一致性。
/// </remarks>
public sealed partial class TinyDbEngine : IDisposable
{
    private readonly string _filePath;
    private readonly TinyDbOptions _options;
    private IDiskStream _diskStream = null!;
    private PageManager _pageManager = null!;
    private WriteAheadLog _writeAheadLog = null!;
    private FlushScheduler _flushScheduler = null!;
    private TinyDb.Metadata.MetadataManager _metadataManager = null!;
    private readonly ConcurrentDictionary<string, IDocumentCollection> _collections;
    internal CollectionMetaStore _collectionMetaStore = null!;
    private readonly ConcurrentDictionary<string, IndexManager> _indexManagers;
    private readonly TransactionManager _transactionManager;
    private readonly ConcurrentDictionary<string, CollectionState> _collectionStates;
    private readonly object _collectionRegistryLock = new();
    private readonly object _collectionStateInitLock = new();
    private readonly object _identitySequenceLock = new();
    private readonly ConcurrentDictionary<string, object> _indexCreationLocks;
    private readonly ConcurrentDictionary<string, IdentitySequenceState> _identitySequences;
    private LargeDocumentStorage _largeDocumentStorage = null!;
    private DataPageAccess _dataPageAccess = null!;
    private readonly object _lock = new();
    private readonly AsyncLocal<ITransaction?> _currentTransaction = new();
    private readonly Action<TinyDbLogLevel, string, Exception?> _log;
    private DatabaseHeader _header;
    private EncryptionContext? _encryptionContext;
    private int _disposed;
    private Exception? _corruptionException;
    private bool _isInitialized;
    private long _findByIdFullScanCount;
    private long _findByIdFullScanHitCount;
    private long _findByIdStaleIndexHitCount;

    private const int DocumentLengthPrefixSize = sizeof(int);
    private const int MinimumFreeSpaceThreshold = DocumentLengthPrefixSize + 64;
    private const string IndexMetadataKey = "__indexes";
    private const string IndexNameKey = "n";
    private const string IndexFieldsKey = "f";
    private const string IndexUniqueKey = "u";
    private const string IndexSparseKey = "s";
    private const string IndexRootPageKey = "r";
    private const string IndexMaxKeysKey = "m";
    private const int IdentitySequenceReservationSize = 1024;

    /// <summary>
    /// 获取数据库文件路径。
    /// </summary>
    public string FilePath => _filePath;

    internal PageManager PageManager => _pageManager;

    internal WriteAheadLog WriteAheadLog => _writeAheadLog;

    internal TransactionManager TransactionManager => _transactionManager;

    internal LargeDocumentStorage LargeDocumentStorage => _largeDocumentStorage;

    /// <summary>
    /// 获取此数据库实例使用的选项。
    /// </summary>
    public TinyDbOptions Options => _options;

    /// <summary>
    /// 获取 FindById 查询回退到全集合扫描的次数。
    /// </summary>
    public long FindByIdFullScanCount => Interlocked.Read(ref _findByIdFullScanCount);

    /// <summary>
    /// 获取 FindById 全扫描回退实际找回已存在文档的次数。
    /// </summary>
    public long FindByIdFullScanHitCount => Interlocked.Read(ref _findByIdFullScanHitCount);

    /// <summary>
    /// 获取 FindById 主键索引命中但指向记录失效或不匹配的次数。
    /// </summary>
    public long FindByIdStaleIndexHitCount => Interlocked.Read(ref _findByIdStaleIndexHitCount);

    public TinyDb.Metadata.MetadataManager MetadataManager => _metadataManager;

    /// <summary>
    /// 获取数据库头部信息。
    /// </summary>
    public DatabaseHeader Header => _header;

    /// <summary>
    /// 获取数据库是否已初始化的值。
    /// </summary>
    public bool IsInitialized => _isInitialized;

    public bool IsCorrupted => Volatile.Read(ref _corruptionException) != null;

    /// <summary>
    /// 获取数据库中集合的数量。
    /// </summary>
    public int CollectionCount => GetCollectionNames().Count();

    /// <summary>
    /// 初始化 <see cref="TinyDbEngine"/> 类的新实例。
    /// </summary>
    /// <param name="f">数据库文件路径。</param>
    /// <param name="o">可选的配置选项。</param>
    [DynamicDependency(DynamicallyAccessedMemberTypes.All, typeof(TinyDbEngine))]
    public TinyDbEngine(string f, TinyDbOptions? o = null) : this(f, o, null) { }

    internal TinyDbEngine(string f, TinyDbOptions? o, IDiskStream? ds)
    {
        _filePath = f ?? throw new ArgumentNullException();
        _options = o ?? new TinyDbOptions();
        _options.Validate();
        _log = _options.Logger ?? TinyDbLogging.NoopLogger;

        _collections = new ConcurrentDictionary<string, IDocumentCollection>(StringComparer.Ordinal);
        _indexManagers = new ConcurrentDictionary<string, IndexManager>(StringComparer.Ordinal);
        _transactionManager = new TransactionManager(this, _options.MaxTransactions, _options.TransactionTimeout);
        _collectionStates = new ConcurrentDictionary<string, CollectionState>(StringComparer.Ordinal);
        _indexCreationLocks = new ConcurrentDictionary<string, object>(StringComparer.Ordinal);
        _identitySequences = new ConcurrentDictionary<string, IdentitySequenceState>(StringComparer.Ordinal);

        InitializeComponents(ds);
    }

}
