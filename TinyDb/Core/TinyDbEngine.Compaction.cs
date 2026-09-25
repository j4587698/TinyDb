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

    private void DisposeComponents()
    {
        // 顺序很重要：页管理器的后台写回会调用 WAL（追加日志、刷到 LSN），
        // 必须在释放 WAL 之前停止并等待它结束；原先要到 _pageManager.Dispose 才等待，那时 WAL 已关闭。
        _pageManager.StopBackgroundWriteback(TimeSpan.FromSeconds(5));
        _flushScheduler.Dispose();
        _writeAheadLog.Dispose();
        _pageManager.Dispose();
        _diskStream.Dispose();
        _encryptionContext?.Dispose();
        _encryptionContext = null;
    }


    private void ResetRuntimeStateForReinitialize()
    {
        _collectionStates.Clear();
        DisposeIndexManagers();
        _collectionMetaStore = null!;
        _isInitialized = false;
    }


    /// <summary>
    /// 压缩数据库（碎片整理）
    /// 注意：此操作会阻塞所有其他操作，并重建数据库文件。
    /// </summary>
    public void CompactDatabase()
    {
        EnsureWritable();
        var tempFile = _filePath + ".compact";
        var collectionNames = GetCollectionNames(includeSystemCollections: true)
            .Distinct(StringComparer.Ordinal)
            .OrderBy(static name => name, StringComparer.Ordinal)
            .ToArray();

        using var collectionLocks = EnterCollectionCommitGates(collectionNames);

        lock (_lock)
        {
            FlushCore();
        }

        if (File.Exists(tempFile)) File.Delete(tempFile);

        using (var tempEngine = new TinyDbEngine(tempFile, _options))
        {
            CopyCollectionsTo(tempEngine, collectionNames);
        }

        // 释放当前组件以解除文件锁定，并替换文件。
        lock (_lock)
        {
            DisposeComponents();

            try
            {
                File.Move(tempFile, _filePath, overwrite: true);
            }
            catch (Exception ex) when (ex is not OutOfMemoryException)
            {
                // 移动失败，尝试恢复组件
                ResetRuntimeStateForReinitialize();
                InitializeComponents(null);
                throw;
            }

            ResetRuntimeStateForReinitialize();
            InitializeComponents(null);
        }
    }

}
