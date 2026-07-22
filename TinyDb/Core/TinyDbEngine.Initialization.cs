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

public sealed partial class TinyDbEngine
{

    private void InitializeComponents(IDiskStream? ds)
    {
        if (ds != null)
        {
            _diskStream = ds;
        }
        else
        {
            var dir = Path.GetDirectoryName(_filePath);
            if (!_options.ReadOnly && !string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            {
                Directory.CreateDirectory(dir);
            }

            if (_options.ReadOnly && !File.Exists(_filePath))
            {
                throw new FileNotFoundException("Database file was not found.", _filePath);
            }

            AcquireConnectionLock();

            // Multiple readers may coexist, but readers and writers must not overlap without snapshot isolation.
            _diskStream = _options.ReadOnly
                ? new DiskStream(_filePath, FileAccess.Read, FileShare.Read)
                : new DiskStream(_filePath, FileAccess.ReadWrite, FileShare.Read);
        }

        var isNewDatabase = _diskStream.Size == 0;
        if (isNewDatabase && _options.ReadOnly)
        {
            throw new InvalidOperationException("Cannot create a new database in read-only mode.");
        }

        var hasBootstrapHeader = TryReadBootstrapHeader(_diskStream, out var bootstrapHeader);
        if (hasBootstrapHeader && bootstrapHeader.Magic == DatabaseHeader.MagicNumber && IsSupportedPageSize(bootstrapHeader.PageSize))
        {
            _options.PageSize = bootstrapHeader.PageSize;
        }

        _encryptionContext = ResolveEncryptionContext(isNewDatabase, hasBootstrapHeader, bootstrapHeader);
        var pageCodec = _encryptionContext?.PageCodec ?? new NoOpPageCodec(_options.PageSize);
        var walCodec = _encryptionContext?.WalCodec ?? new NoOpWalCodec();

        _pageManager = new PageManager(_diskStream, _options.PageSize, _options.CacheSize, _log, pageCodec);
        _writeAheadLog = new WriteAheadLog(
            _filePath,
            (int)_options.PageSize,
            _options.EnableJournaling,
            _options.WalFileNameFormat,
            _log,
            walCodec,
            _options.ReadOnly);

        _pageManager.RegisterWAL(
            (page, beforeImage, ctx) => _writeAheadLog.AppendPage(page, beforeImage, ctx),
            (lsn, ctx) => _writeAheadLog.FlushToLSN(lsn, ctx),
            () => _writeAheadLog.RequiresBeforeImage);
        _pageManager.RegisterDeferredWAL((page, beforeImage, _) => _writeAheadLog.AppendPageDeferred(page, beforeImage));
        _writeAheadLog.DeferredTransactionPageLogged = _pageManager.MarkDeferredWalPageLogged;
        _pageManager.RegisterWAL((page, beforeImage, ctx, ct) => _writeAheadLog.AppendPageAsync(page, beforeImage, ctx, ct));
        _pageManager.RegisterWAL((lsn, ctx, ct) => _writeAheadLog.FlushToLSNAsync(lsn, ctx, ct));

        var flushInterval = _options.EnableAutoCheckpoint && !_options.ReadOnly
            ? NormalizeInterval(_options.BackgroundFlushInterval)
            : TimeSpan.Zero;
        _flushScheduler = new FlushScheduler(_pageManager, _writeAheadLog, flushInterval, _log);
        _largeDocumentStorage = new LargeDocumentStorage(_pageManager, (int)_options.PageSize, _log);
        _dataPageAccess = new DataPageAccess(_pageManager, _largeDocumentStorage, _writeAheadLog);
        _metadataManager = new TinyDb.Metadata.MetadataManager(this);

        InitializeDatabase();
    }

    private void AcquireConnectionLock()
    {
        if (_connectionLockStream != null)
        {
            return;
        }

        var lockFilePath = Path.GetFullPath(_filePath) + ".writer.lock";
        _connectionLockStream = new FileStream(
            lockFilePath,
            FileMode.OpenOrCreate,
            _options.ReadOnly ? FileAccess.Read : FileAccess.ReadWrite,
            _options.ReadOnly ? FileShare.Read : FileShare.None);
    }

    private void ReleaseConnectionLock()
    {
        Interlocked.Exchange(ref _connectionLockStream, null)?.Dispose();
    }


    private static bool TryReadBootstrapHeader(IDiskStream diskStream, out DatabaseHeader header)
    {
        header = default;
        if (diskStream.Size < Page.DataStartOffset + DatabaseHeader.Size)
        {
            return false;
        }

        try
        {
            var headerBytes = diskStream.ReadPage(Page.DataStartOffset, DatabaseHeader.Size);
            header = DatabaseHeader.FromByteArray(headerBytes);
            return true;
        }
        catch
        {
            return false;
        }
    }


    private EncryptionContext? ResolveEncryptionContext(bool isNewDatabase, bool hasBootstrapHeader, DatabaseHeader bootstrapHeader)
    {
        if (hasBootstrapHeader &&
            bootstrapHeader.Magic == DatabaseHeader.MagicNumber &&
            IsSupportedPageSize(bootstrapHeader.PageSize) &&
            EncryptionMetadataStore.TryReadFromDisk(_diskStream, bootstrapHeader.PageSize, out var metadata) &&
            metadata != null)
        {
            if (metadata.LogicalPageSize != bootstrapHeader.PageSize)
            {
                throw new SecurityCorruptedException("Encryption metadata page size does not match database header.");
            }

            _options.PageSize = metadata.LogicalPageSize;
            _options.EnableEncryption = true;
            var context = EncryptionContext.OpenExisting(metadata, _options, _options.ReadOnly);
            try
            {
                if (!_options.ReadOnly)
                {
                    EncryptionMetadataStore.WriteToDisk(_diskStream, metadata.LogicalPageSize, context.Metadata);
                }
                return context;
            }
            catch
            {
                context.Dispose();
                throw;
            }
        }

        if (isNewDatabase)
        {
            return _options.EnableEncryption ? EncryptionContext.CreateNew(_options) : null;
        }

        if (_options.EnableEncryption)
        {
            throw new InvalidOperationException("Existing unencrypted databases are not encrypted implicitly. Use an explicit migration or compact-to-encrypted workflow.");
        }

        return null;
    }


    private static bool IsSupportedPageSize(uint pageSize)
    {
        return TinyDbOptions.IsSupportedPageSize(pageSize);
    }

    private sealed class IdentitySequenceState
    {
        public IdentitySequenceState(string collectionName, string metadataKey, long current)
        {
            CollectionName = collectionName;
            MetadataKey = metadataKey;
            Current = current;
            ReservedUntil = current;
        }

        public string CollectionName { get; }
        public string MetadataKey { get; }
        public long Current { get; set; }
        public long ReservedUntil { get; set; }
        public bool HasUnflushedExactValue { get; set; }
    }


    /// <summary>
    /// 初始化数据库。
    /// 此方法是引擎启动的核心，负责：
    /// 1. 执行 WAL 重放以实现崩溃恢复。
    /// 2. 检查数据库文件完整性或初始化新文件。
    /// 3. 同步内存中的页面管理器状态与磁盘 Header。
    /// 4. 加载系统页和集合元数据。
    /// </summary>
    private void InitializeDatabase()
    {
        lock (_lock)
        {
            if (_isInitialized) return;
            try
            {
                var isNewDatabase = _diskStream.Size == 0;

                // 步骤 1: 崩溃恢复。
                // 只有当 WAL 中的 LSN 大于磁盘页面的 LSN 时，才应用恢复逻辑（幂等恢复）。
                if (_writeAheadLog.IsEnabled)
                {
                    if (isNewDatabase)
                    {
                        // If the main database file was deleted but a stale WAL remains, replaying that WAL
                        // would resurrect pages from a different database generation and can corrupt the new header.
                        _writeAheadLog.Truncate();
                    }
                    else
                    {
                        _writeAheadLog.Replay((id, data) =>
                        {
                            if (_pageManager.TryReadLogicalPageSnapshot(id, out var diskData))
                            {
                                var diskHeader = PageHeader.FromByteArray(diskData);
                                var walHeader = PageHeader.FromSpan(data.Span);

                                if (walHeader.LSN <= diskHeader.LSN && diskHeader.IsValid() && diskHeader.PageID == id && diskHeader.VerifyChecksum(diskData))
                                {
                                    // 磁盘版本已经是最新的，跳过此条日志
                                    return;
                                }
                            }

                            var replayData = ProtectEncryptionMetadataForWalReplay(id, data);
                            _pageManager.RestorePage(id, replayData);
                        }, (id, data) => _pageManager.RestorePage(id, ProtectEncryptionMetadataForWalReplay(id, data)));
                    }
                }

                // 步骤 2: 文件结构初始化。
                if (isNewDatabase)
                {
                    // 空文件：初始化 Header 页。
                    _header = new DatabaseHeader();
                    _header.Initialize(_options.PageSize, _options.DatabaseName, _options.EnableJournaling);
                    var p1 = _pageManager.NewPage(PageType.Header);
                    p1.WriteData(0, _header.ToByteArray());
                    if (_encryptionContext != null)
                    {
                        EncryptionMetadataStore.WriteToPage(p1, _encryptionContext.Metadata);
                    }
                    _pageManager.SavePage(p1, true);
                    // 初始化 PageManager (新数据库)
                    _pageManager.Initialize(1, 0);
                }
                else
                {
                    // 已有文件：加载 Header 并验证。
                    ReadHeader();
                    if (!_header.IsValid()) throw new InvalidOperationException(CreateInvalidHeaderMessage(_header));

                    // 初始化 PageManager (现有数据库)
                    _pageManager.Initialize(
                        _header.TotalPages,
                        _header.FirstFreePage,
                        _header.FreePageCount,
                        _header.HasFreePageCount,
                        _options.ReadOnly);

                    // 步骤 3: 状态一致性同步。
                    // 关键修复：如果在崩溃前分配了页面但 Header 未更新，
                    // WAL 重放后 PageManager 知道最新状态，需反向同步回 Header
                    if (!_options.ReadOnly &&
                        (_pageManager.TotalPages > _header.TotalPages ||
                        _pageManager.FirstFreePageID != _header.FirstFreePage ||
                        !_header.HasFreePageCount ||
                        _pageManager.FreePageCount != _header.FreePageCount))
                    {
                        WriteHeader();
                    }
                }

                // 步骤 4: 加载核心系统组件。
                _header.EnableJournaling = _writeAheadLog.IsEnabled;
                InitializeSystemPages();
                _collectionMetaStore = new CollectionMetaStore(_pageManager, () => _header.CollectionInfoPage, id => _header.CollectionInfoPage = id);
                _collectionMetaStore.LoadCollections();
                _isInitialized = true;

                // 步骤 5: 安全检查。
                EnsureDatabaseSecurity();
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                _isInitialized = false;
                Dispose();
                throw;
            }
        }
    }


    private ReadOnlyMemory<byte> ProtectEncryptionMetadataForWalReplay(uint pageId, ReadOnlyMemory<byte> pageData)
    {
        if (_encryptionContext == null || pageId != 1)
        {
            return pageData;
        }

        if (!EncryptionMetadataStore.TryReadFromLogicalPage(pageData.Span, out var recoveredMetadata) ||
            recoveredMetadata == null)
        {
            return pageData;
        }

        var protectedMetadata = _encryptionContext.ProtectRecoveredMetadata(recoveredMetadata, out var metadataChanged);
        if (!metadataChanged)
        {
            return pageData;
        }

        var patchedPage = pageData.ToArray();
        EncryptionMetadataStore.WriteToLogicalPage(patchedPage, protectedMetadata);
        return patchedPage;
    }


    private static string CreateInvalidHeaderMessage(DatabaseHeader header)
    {
        return "Invalid database header " +
               $"(magic=0x{header.Magic:X8}, version=0x{header.DatabaseVersion:X8}, pageSize={header.PageSize}, " +
               $"totalPages={header.TotalPages}, usedPages={header.UsedPages}, " +
               $"createdAt={header.CreatedAt}, modifiedAt={header.ModifiedAt}).";
    }


    private void ReadHeader()
    {
        var p = _pageManager.GetPage(1);
        _header = DatabaseHeader.FromByteArray(p.ReadBytes(0, DatabaseHeader.Size));
        if (!_header.IsValid())
        {
            throw new InvalidOperationException(CreateInvalidHeaderMessage(_header));
        }

        if (_header.Checksum != 0 && !_header.VerifyChecksum())
        {
            throw new InvalidDataException("Database header checksum verification failed.");
        }
    }


    private void WriteHeader(bool forceFlush = false)
    {
        lock (_lock)
        {
            _header.TotalPages = _pageManager.TotalPages;
            _header.FirstFreePage = _pageManager.FirstFreePageID;
            _header.FreePageCount = _pageManager.FreePageCount;
            _header.UpdateModification();
            var p = _pageManager.GetPage(1);
            p.WriteData(0, _header.ToByteArray());
            _pageManager.SavePage(p, forceFlush);
        }
    }


    private void DecrementUsedPagesAndWriteHeader()
    {
        lock (_lock)
        {
            if (_header.UsedPages <= 1)
            {
                throw new InvalidOperationException("Database header used-page count cannot be decremented below the header page.");
            }

            _header.UsedPages--;
            WriteHeader();
        }
    }


    private void InitializeSystemPages()
    {
        if (_options.ReadOnly)
        {
            if (_header.CollectionInfoPage == 0 || _header.IndexInfoPage == 0)
            {
                throw new InvalidDataException(
                    "Read-only database is missing required system pages and cannot be repaired without write access.");
            }

            return;
        }

        var headerChanged = false;
        if (_header.CollectionInfoPage == 0)
        {
            _header.CollectionInfoPage = AllocateSystemPage(PageType.Collection, "Cols");
            headerChanged = true;
        }

        if (_header.IndexInfoPage == 0)
        {
            _header.IndexInfoPage = AllocateSystemPage(PageType.Index, "Idxs");
            headerChanged = true;
        }

        if (headerChanged)
        {
            WriteHeader(forceFlush: true);
        }
    }


    private uint AllocateSystemPage(PageType t, string n)
    {
        var p = _pageManager.NewPage(t);
        _pageManager.SavePage(p, true);
        return p.PageID;
    }

}
