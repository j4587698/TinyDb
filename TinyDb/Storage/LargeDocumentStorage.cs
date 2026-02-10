using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;

namespace TinyDb.Storage;

/// <summary>
/// 大文档存储管理器，负责将超过单页容量限制的文档拆分存储到多个页面中。
/// </summary>
public class LargeDocumentStorage
{
    private readonly PageManager _pageManager;
    private readonly int _dataPayloadSize;

    private const int LargeDocumentHeaderSize =
        sizeof(int) + sizeof(int) + sizeof(int) + sizeof(uint);

    private const int DataPageHeaderSize =
        sizeof(int) + sizeof(uint);

    /// <summary>
    /// 初始化大文档存储管理器。
    /// </summary>
    /// <param name="pageManager">页面管理器。</param>
    /// <param name="pageSize">页面大小。</param>
    public LargeDocumentStorage(PageManager pageManager, int pageSize)
    {
        _pageManager = pageManager;
        // 使用 Page.DataStartOffset (41) + 边距 (4)
        _dataPayloadSize = pageSize - Page.DataStartOffset - DataPageHeaderSize - 4;
    }

    /// <summary>
    /// 检查文档是否需要使用大文档存储。
    /// </summary>
    /// <param name="documentSize">文档大小。</param>
    /// <param name="maxSinglePageSize">单页最大可存储空间。</param>
    /// <returns>如果需要则为 true。</returns>
    public static bool RequiresLargeDocumentStorage(int documentSize, int maxSinglePageSize)
    {
        return documentSize > maxSinglePageSize;
    }

    /// <summary>
    /// 存储大文档。
    /// </summary>
    /// <param name="document">BSON 文档。</param>
    /// <param name="collectionName">所属集合名称。</param>
    /// <returns>大文档索引页面的 ID。</returns>
    public uint StoreLargeDocument(BsonDocument document, string collectionName)
    {
        var documentBytes = BsonSerializer.SerializeDocument(document);
        return StoreLargeDocument(documentBytes, collectionName);
    }

    /// <summary>
    /// 存储大文档（字节数组形式）。
    /// </summary>
    /// <param name="documentBytes">文档的二进制数据。</param>
    /// <param name="collectionName">所属集合名称。</param>
    /// <returns>大文档索引页面的 ID。</returns>
    public uint StoreLargeDocument(byte[] documentBytes, string collectionName)
    {
        var totalLength = documentBytes.Length;
        var requiredPages = CalculateRequiredPages(totalLength);

        var indexPage = _pageManager.NewPage(PageType.LargeDocumentIndex);
        var firstDataPageId = CreateDataPagesChain(documentBytes, requiredPages);

        WriteIndexPageHeader(indexPage, totalLength, requiredPages, firstDataPageId);

        var collectionBytes = System.Text.Encoding.UTF8.GetBytes(collectionName);
        if (collectionBytes.Length > 0 && indexPage.DataSize > LargeDocumentHeaderSize + collectionBytes.Length)
        {
            indexPage.WriteData(LargeDocumentHeaderSize, collectionBytes);
        }

        _pageManager.SavePage(indexPage);
        return indexPage.PageID;
    }

    /// <summary>
    /// 读取大文档数据。
    /// </summary>
    /// <param name="indexPageId">大文档索引页面的 ID。</param>
    /// <returns>文档的二进制数据。</returns>
    public byte[] ReadLargeDocument(uint indexPageId)
    {
        var indexPage = _pageManager.GetPage(indexPageId);
        if (indexPage.PageType != PageType.LargeDocumentIndex)
            throw new InvalidOperationException($"Page {indexPageId} is not a large document index page");

        var header = ReadIndexPageHeader(indexPage);
        var result = new byte[header.TotalLength];
        var offset = 0;

        var currentPageId = header.FirstDataPageId;
        var visitedPages = 0;
        for (int i = 0; i < header.PageCount && currentPageId != 0; i++)
        {
            var dataPage = _pageManager.GetPage(currentPageId);
            var dataHeader = ReadDataPageHeader(dataPage);

            if (dataHeader.PageNumber != i)
                throw new InvalidOperationException($"Data page number mismatch");

            var dataToRead = Math.Min(_dataPayloadSize, header.TotalLength - offset);
            // ReadData 期望相对于 DataStartOffset 的偏移
            // 我们在 DataPageHeaderSize (8) 处开始写入。
            var dataBytes = dataPage.ReadBytes(DataPageHeaderSize, dataToRead);

            Buffer.BlockCopy(dataBytes, 0, result, offset, dataToRead);
            offset += dataToRead;

            currentPageId = dataHeader.NextPageId;
            visitedPages++;
        }

        return result;
    }

    public async Task<byte[]> ReadLargeDocumentAsync(uint indexPageId, CancellationToken cancellationToken = default)
    {
        var indexPage = await _pageManager.GetPageAsync(indexPageId, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (indexPage.PageType != PageType.LargeDocumentIndex)
            throw new InvalidOperationException($"Page {indexPageId} is not a large document index page");

        var header = ReadIndexPageHeader(indexPage);
        var result = new byte[header.TotalLength];
        var offset = 0;

        var currentPageId = header.FirstDataPageId;
        for (int i = 0; i < header.PageCount && currentPageId != 0; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var dataPage = await _pageManager.GetPageAsync(currentPageId, cancellationToken: cancellationToken).ConfigureAwait(false);
            var dataHeader = ReadDataPageHeader(dataPage);

            if (dataHeader.PageNumber != i)
                throw new InvalidOperationException("Data page number mismatch");

            var dataToRead = Math.Min(_dataPayloadSize, header.TotalLength - offset);
            var dataBytes = dataPage.ReadBytes(DataPageHeaderSize, dataToRead);

            Buffer.BlockCopy(dataBytes, 0, result, offset, dataToRead);
            offset += dataToRead;

            currentPageId = dataHeader.NextPageId;
        }

        return result;
    }

    /// <summary>
    /// 删除大文档及其关联的数据页。
    /// </summary>
    /// <param name="indexPageId">大文档索引页面的 ID。</param>
    public void DeleteLargeDocument(uint indexPageId)
    {
        try {
            var indexPage = _pageManager.GetPage(indexPageId);
            if (indexPage.PageType != PageType.LargeDocumentIndex) return;

            var header = ReadIndexPageHeader(indexPage);
            var currentPageId = header.FirstDataPageId;
            for (int i = 0; i < header.PageCount && currentPageId != 0; i++)
            {
                var dataPage = _pageManager.GetPage(currentPageId);
                currentPageId = ReadDataPageHeader(dataPage).NextPageId;
                _pageManager.FreePage(dataPage.PageID);
            }
            _pageManager.FreePage(indexPageId);
        } catch {}
    }

    /// <summary>
    /// 验证大文档数据的完整性。
    /// </summary>
    /// <param name="indexPageId">大文档索引页面的 ID。</param>
    /// <returns>如果完整则为 true。</returns>
    public bool ValidateLargeDocument(uint indexPageId)
    {
        try
        {
            var indexPage = _pageManager.GetPage(indexPageId);
            if (indexPage.PageType != PageType.LargeDocumentIndex) return false;

            var header = ReadIndexPageHeader(indexPage);
            var currentPageId = header.FirstDataPageId;
            
            for (int i = 0; i < header.PageCount; i++)
            {
                if (currentPageId == 0) return false;

                var dataPage = _pageManager.GetPage(currentPageId);
                if (dataPage.PageType != PageType.LargeDocumentData) return false;

                var dataHeader = ReadDataPageHeader(dataPage);
                if (dataHeader.PageNumber != i) return false;

                currentPageId = dataHeader.NextPageId;
            }

            // 应该到达链条末尾 (NextPageId == 0)
            if (currentPageId != 0) return false;

            return true;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// 获取大文档存储统计信息。
    /// </summary>
    /// <param name="indexPageId">大文档索引页面的 ID。</param>
    /// <returns>统计信息对象。</returns>
    public LargeDocumentStatistics GetStatistics(uint indexPageId)
    {
        var indexPage = _pageManager.GetPage(indexPageId);
        if (indexPage.PageType != PageType.LargeDocumentIndex)
            throw new InvalidOperationException($"Page {indexPageId} is not a large document index page");

        var header = ReadIndexPageHeader(indexPage);
        return new LargeDocumentStatistics
        {
            IndexPageId = indexPageId,
            TotalLength = header.TotalLength,
            PageCount = header.PageCount,
            FirstDataPageId = header.FirstDataPageId
        };
    }

    private uint CreateDataPagesChain(byte[] documentBytes, int pageCount)
    {
        uint firstPageId = 0;
        uint previousPageId = 0;

        for (int i = 0; i < pageCount; i++)
        {
            var page = _pageManager.NewPage(PageType.LargeDocumentData);

            var startOffset = i * _dataPayloadSize;
            var endOffset = Math.Min(startOffset + _dataPayloadSize, documentBytes.Length);
            var chunkSize = endOffset - startOffset;

            if (chunkSize > 0)
            {
                var chunk = new byte[chunkSize];
                Buffer.BlockCopy(documentBytes, startOffset, chunk, 0, chunkSize);

                WriteDataPageHeader(page, i, 0); 
                page.WriteData(DataPageHeaderSize, chunk);
            }

            if (previousPageId != 0)
            {
                var previousPage = _pageManager.GetPage(previousPageId);
                UpdateDataPageHeaderNextPage(previousPage, page.PageID);
                _pageManager.SavePage(previousPage);
            }
            else
            {
                firstPageId = page.PageID;
            }

            _pageManager.SavePage(page);
            previousPageId = page.PageID;
        }

        return firstPageId;
    }

    private int CalculateRequiredPages(int documentSize)
    {
        return (documentSize + _dataPayloadSize - 1) / _dataPayloadSize;
    }

    private void WriteIndexPageHeader(Page page, int totalLength, int pageCount, uint firstDataPageId)
    {
        var header = new byte[LargeDocumentHeaderSize];
        var offset = 0;
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(offset, 4), -1); offset += 4;
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(offset, 4), totalLength); offset += 4;
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(offset, 4), pageCount); offset += 4;
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(offset, 4), (int)firstDataPageId);
        page.WriteData(0, header);
    }

    private LargeDocumentIndexHeader ReadIndexPageHeader(Page page)
    {
        var headerBytes = page.ReadBytes(0, LargeDocumentHeaderSize);
        var offset = 0;
        var documentIdentifier = BinaryPrimitives.ReadInt32LittleEndian(headerBytes.AsSpan(offset, 4)); offset += 4;
        var totalLength = BinaryPrimitives.ReadInt32LittleEndian(headerBytes.AsSpan(offset, 4)); offset += 4;
        var pageCount = BinaryPrimitives.ReadInt32LittleEndian(headerBytes.AsSpan(offset, 4)); offset += 4;
        var firstDataPageId = (uint)BinaryPrimitives.ReadInt32LittleEndian(headerBytes.AsSpan(offset, 4));
        return new LargeDocumentIndexHeader { DocumentIdentifier = documentIdentifier, TotalLength = totalLength, PageCount = pageCount, FirstDataPageId = firstDataPageId };
    }

    private void WriteDataPageHeader(Page page, int pageNumber, uint nextPageId)
    {
        var header = new byte[DataPageHeaderSize];
        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(0, 4), pageNumber);
        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(4, 4), nextPageId);
        page.WriteData(0, header);
    }

    private void UpdateDataPageHeaderNextPage(Page page, uint nextPageId)
    {
        var nextPageIdBytes = new byte[4];
        BinaryPrimitives.WriteUInt32LittleEndian(nextPageIdBytes, nextPageId);
        page.WriteData(4, nextPageIdBytes);
    }

    private LargeDocumentDataHeader ReadDataPageHeader(Page page)
    {
        var headerBytes = page.ReadBytes(0, DataPageHeaderSize);
        var pageNumber = BinaryPrimitives.ReadInt32LittleEndian(headerBytes.AsSpan(0, 4));
        var nextPageId = BinaryPrimitives.ReadUInt32LittleEndian(headerBytes.AsSpan(4, 4));
        return new LargeDocumentDataHeader { PageNumber = pageNumber, NextPageId = nextPageId };
    }
}

internal class LargeDocumentIndexHeader { public int DocumentIdentifier; public int TotalLength; public int PageCount; public uint FirstDataPageId; }
internal class LargeDocumentDataHeader { public int PageNumber; public uint NextPageId; }
public class LargeDocumentStatistics { public uint IndexPageId { get; init; } public int TotalLength { get; init; } public int PageCount { get; init; } public uint FirstDataPageId { get; init; } 
    public override string ToString() => $"LargeDoc[Index={IndexPageId}, Size={TotalLength:N0} bytes, Pages={PageCount}]";
}
