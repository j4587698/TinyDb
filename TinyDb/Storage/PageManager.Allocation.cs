using System.Buffers;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Threading;
using TinyDb.Core;
using TinyDb.Utils;

namespace TinyDb.Storage;

public sealed partial class PageManager
{
    public Page NewPage(PageType pageType)
    {
        ThrowIfDisposed();

        lock (_freeListLock)
        {
            var freePageId = _firstFreePageID;
            if (freePageId != 0)
            {
                var freePage = GetPage(freePageId);
                lock (_stateLock)
                {
                    _firstFreePageID = freePage.Header.NextPageID;
                    if (_freePageCount > 0)
                    {
                        _freePageCount--;
                    }
                }

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
            var freePageId = _firstFreePageID;
            if (freePageId != 0)
            {
                var freePage = GetPagePinned(freePageId);
                lock (_stateLock)
                {
                    _firstFreePageID = freePage.Header.NextPageID;
                    if (_freePageCount > 0)
                    {
                        _freePageCount--;
                    }
                }

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
