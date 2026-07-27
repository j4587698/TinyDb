using System.Buffers;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Threading;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

public sealed partial class PageManager
{
    /// <summary>
    /// 从空闲链表摘取一页。
    /// </summary>
    /// <param name="pinned">是否以 pinned 方式取页。</param>
    /// <param name="freePage">摘到的空闲页。</param>
    /// <returns>成功摘到可复用的空闲页则为 true。</returns>
    /// <remarks>
    /// <see cref="FreePage"/> 一定是先把页改成 <see cref="PageType.Empty"/> 再挂入链表的，
    /// 所以链表头若不是 Empty，就说明链表与页内容已经不一致（例如崩溃时页头与页内容
    /// 落盘次序不一致）。这种页很可能仍被索引或数据结构引用，一旦被清空改作他用，
    /// 就会把“父指针指向的页已被占用”这种损坏写进库里，表现为
    /// <c>Invalid B-tree page type for page N. Expected Index, found Data.</c>
    /// 此时整条链表都不再可信（链接字段本身就存在这个不可信的页头里），干脆整条丢弃，
    /// 后续分配改为追加新页。宁可少回收一些空间，也不能复用来路不明的页。
    /// </remarks>
    private bool TryTakeFreePage(bool pinned, out Page freePage)
    {
        freePage = null!;

        uint freePageId;
        lock (_stateLock)
        {
            freePageId = _firstFreePageID;
        }

        if (freePageId == 0) return false;

        var candidate = pinned ? GetPagePinned(freePageId) : GetPage(freePageId);
        if (candidate.PageType != PageType.Empty)
        {
            Log(TinyDbLogLevel.Warning,
                $"Free page list head {freePageId} has page type {candidate.PageType} instead of Empty. " +
                "The free list is inconsistent with page contents and will be discarded to avoid reusing a page that may still be referenced.");

            if (pinned) candidate.Unpin();

            lock (_stateLock)
            {
                _firstFreePageID = 0;
                _freePageCount = 0;
            }

            PersistAllocatorState();
            return false;
        }

        lock (_stateLock)
        {
            _firstFreePageID = candidate.Header.NextPageID;
            if (_freePageCount > 0)
            {
                _freePageCount--;
            }
        }

        PersistAllocatorState();

        freePage = candidate;
        return true;
    }

    public Page NewPage(PageType pageType)
    {
        ThrowIfDisposed();

        lock (_freeListLock)
        {
            if (TryTakeFreePage(pinned: false, out var freePage))
            {
                freePage.ClearData();
                freePage.UpdatePageType(pageType);
                freePage.SetLinks(0, 0);

                return freePage;
            }
        }

        uint pageID;
        lock (_stateLock)
        {
            pageID = ++_nextPageID;
        }

        return CreateNewPage(pageID, pageType);
    }

    internal Page NewPagePinned(PageType pageType)
    {
        ThrowIfDisposed();

        lock (_freeListLock)
        {
            if (TryTakeFreePage(pinned: true, out var freePage))
            {
                freePage.ClearData();
                freePage.UpdatePageType(pageType);
                freePage.SetLinks(0, 0);

                return freePage;
            }
        }

        uint pageID;
        lock (_stateLock)
        {
            pageID = ++_nextPageID;
        }

        return CreateNewPage(pageID, pageType, pinned: true);
    }

    /// <summary>
    /// 创建指定ID的新页面
    /// </summary>
    /// <param name="pageID">页面ID</param>
    /// <param name="pageType">页面类型</param>
    /// <returns>新页面</returns>
    private Page CreateNewPage(uint pageID, PageType pageType, bool pinned = false)
    {
        var page = new Page(pageID, (int)_pageSize, pageType);
        page.UpdateStats((ushort)Math.Min(page.DataSize, ushort.MaxValue), 0);
        page = AddToCache(page, pinned);

        // 计算新的文件大小
        var newFileSize = CalculatePageOffset(pageID) + _physicalPageSize;

        EnsureFileLength(newFileSize);

        return page;
    }

    private void EnsureFileLength(long newFileSize)
    {
        lock (_fileSizeLock)
        {
            if (newFileSize > ReadFileSize())
            {
                _diskStream.SetLength(newFileSize);
                SetFileSize(newFileSize);
            }
        }
    }
}
