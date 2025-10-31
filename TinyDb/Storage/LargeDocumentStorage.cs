using System.Buffers.Binary;
using System.Collections.Generic;
using TinyDb.Bson;
using TinyDb.Serialization;

namespace TinyDb.Storage;

/// <summary>
/// 大文档跨页面存储管理器
/// </summary>
internal class LargeDocumentStorage
{
    private readonly PageManager _pageManager;
    private readonly int _dataPayloadSize;

    // 大文档索引页面头部格式
    private const int LargeDocumentHeaderSize =
        sizeof(int) +  // 文档标识符 (0xFFFFFFFF = 大文档标识)
        sizeof(int) +  // 文档总长度
        sizeof(int) +  // 页面数量
        sizeof(uint);  // 第一个数据页面ID

    // 大文档数据页面头部格式
    private const int DataPageHeaderSize =
        sizeof(int) +  // 页面序号
        sizeof(uint);  // 下一页面ID (0 = 最后一个页面)

    public LargeDocumentStorage(PageManager pageManager, int pageSize)
    {
        _pageManager = pageManager;
        // 计算每个数据页面实际可用载荷大小
        _dataPayloadSize = pageSize - PageHeader.Size - DataPageHeaderSize;
    }

    /// <summary>
    /// 检查是否需要使用大文档存储
    /// </summary>
    /// <param name="documentSize">文档大小</param>
    /// <param name="maxSinglePageSize">单页面最大容量</param>
    /// <returns>是否需要大文档存储</returns>
    public static bool RequiresLargeDocumentStorage(int documentSize, int maxSinglePageSize)
    {
        return documentSize > maxSinglePageSize;
    }

    /// <summary>
    /// 存储大文档
    /// </summary>
    /// <param name="document">文档</param>
    /// <param name="collectionName">集合名称</param>
    /// <returns>索引页面ID</returns>
    public uint StoreLargeDocument(BsonDocument document, string collectionName)
    {
        var documentBytes = BsonSerializer.SerializeDocument(document);
        return StoreLargeDocument(documentBytes, collectionName);
    }

    /// <summary>
    /// 存储大文档（字节版本）
    /// </summary>
    /// <param name="documentBytes">文档字节数据</param>
    /// <param name="collectionName">集合名称</param>
    /// <returns>索引页面ID</returns>
    public uint StoreLargeDocument(byte[] documentBytes, string collectionName)
    {
        var totalLength = documentBytes.Length;
        var requiredPages = CalculateRequiredPages(totalLength);

        // 创建索引页面
        var indexPage = _pageManager.NewPage(PageType.LargeDocumentIndex);
        var firstDataPageId = CreateDataPagesChain(documentBytes, requiredPages);

        // 写入索引页面头部
        WriteIndexPageHeader(indexPage, totalLength, requiredPages, firstDataPageId);

        // 设置集合名称（可选，用于调试）
        var collectionBytes = System.Text.Encoding.UTF8.GetBytes(collectionName);
        if (collectionBytes.Length > 0 && indexPage.DataSize > LargeDocumentHeaderSize + collectionBytes.Length)
        {
            indexPage.WriteData(LargeDocumentHeaderSize, collectionBytes);
        }

        _pageManager.SavePage(indexPage);
        return indexPage.PageID;
    }

    /// <summary>
    /// 读取大文档
    /// </summary>
    /// <param name="indexPageId">索引页面ID</param>
    /// <returns>文档字节数据</returns>
    public byte[] ReadLargeDocument(uint indexPageId)
    {
        var indexPage = _pageManager.GetPage(indexPageId);
        if (indexPage.PageType != PageType.LargeDocumentIndex)
        {
            throw new InvalidOperationException($"Page {indexPageId} is not a large document index page");
        }

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
            {
                throw new InvalidOperationException($"Data page number mismatch: expected {i}, found {dataHeader.PageNumber}");
            }

            var dataToRead = Math.Min(_dataPayloadSize, header.TotalLength - offset);
            var dataBytes = dataPage.ReadData(DataPageHeaderSize, dataToRead);

            if (dataBytes.Length != dataToRead)
            {
                throw new InvalidOperationException($"Insufficient data in page {currentPageId}");
            }

            Buffer.BlockCopy(dataBytes, 0, result, offset, dataToRead);
            offset += dataToRead;
            currentPageId = dataHeader.NextPageId;
            visitedPages++;
        }

        if (offset != header.TotalLength)
        {
            throw new InvalidOperationException($"Data length mismatch: expected {header.TotalLength}, read {offset}");
        }

        if (visitedPages != header.PageCount)
        {
            throw new InvalidOperationException($"Data page count mismatch: expected {header.PageCount}, visited {visitedPages}");
        }

        return result;
    }

    /// <summary>
    /// 删除大文档
    /// </summary>
    /// <param name="indexPageId">索引页面ID</param>
    public void DeleteLargeDocument(uint indexPageId)
    {
        var indexPage = _pageManager.GetPage(indexPageId);
        if (indexPage.PageType != PageType.LargeDocumentIndex)
        {
            throw new InvalidOperationException($"Page {indexPageId} is not a large document index page");
        }

        var header = ReadIndexPageHeader(indexPage);

        // 释放所有数据页面
        var currentPageId = header.FirstDataPageId;
        for (int i = 0; i < header.PageCount && currentPageId != 0; i++)
        {
            var dataPage = _pageManager.GetPage(currentPageId);
            currentPageId = ReadDataPageHeader(dataPage).NextPageId;
            _pageManager.FreePage(dataPage.PageID);
        }

        // 释放索引页面
        _pageManager.FreePage(indexPageId);
    }

    /// <summary>
    /// 验证大文档完整性
    /// </summary>
    /// <param name="indexPageId">索引页面ID</param>
    /// <returns>是否有效</returns>
    public bool ValidateLargeDocument(uint indexPageId)
    {
        try
        {
            var indexPage = _pageManager.GetPage(indexPageId);
            if (indexPage.PageType != PageType.LargeDocumentIndex)
                return false;

            var header = ReadIndexPageHeader(indexPage);

            // 验证索引页面头部
            if (header.DocumentIdentifier != -1 || header.TotalLength <= 0 || header.PageCount <= 0)
                return false;

            // 验证数据页面链
            var currentPageId = header.FirstDataPageId;
            var visitedPages = 0;
            for (int i = 0; i < header.PageCount && currentPageId != 0; i++)
            {
                var dataPage = _pageManager.GetPage(currentPageId);
                var dataHeader = ReadDataPageHeader(dataPage);

                if (dataPage.PageType != PageType.LargeDocumentData || dataHeader.PageNumber != i)
                    return false;

                // 最后一个页面的NextPageId应该为0
                var isLastPage = (i == header.PageCount - 1);
                if (isLastPage && dataHeader.NextPageId != 0)
                    return false;
                if (!isLastPage && dataHeader.NextPageId == 0)
                    return false;

                currentPageId = dataHeader.NextPageId;
                visitedPages++;
            }

            // 检查是否正好遍历完所有页面
            return currentPageId == 0 &&
                   visitedPages == header.PageCount &&
                   header.PageCount > 0 &&
                   header.FirstDataPageId != 0;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// 获取大文档统计信息
    /// </summary>
    /// <param name="indexPageId">索引页面ID</param>
    /// <returns>统计信息</returns>
    public LargeDocumentStatistics GetStatistics(uint indexPageId)
    {
        var indexPage = _pageManager.GetPage(indexPageId);
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

                WriteDataPageHeader(page, i, 0); // NextPageId will be set later
                page.WriteData(DataPageHeaderSize, chunk);
            }

            if (previousPageId != 0)
            {
                // 更新前一页面的NextPageId
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

        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(offset, sizeof(int)), -1); // DocumentIdentifier (0xFFFFFFFF)
        offset += sizeof(int);

        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(offset, sizeof(int)), totalLength);
        offset += sizeof(int);

        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(offset, sizeof(int)), pageCount);
        offset += sizeof(int);

        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(offset, sizeof(int)), (int)firstDataPageId);

        page.WriteData(0, header);
    }

    private LargeDocumentIndexHeader ReadIndexPageHeader(Page page)
    {
        var headerBytes = page.ReadData(0, LargeDocumentHeaderSize);
        if (headerBytes.Length < LargeDocumentHeaderSize)
            throw new InvalidOperationException("Invalid large document index page header");

        var offset = 0;
        var documentIdentifier = BinaryPrimitives.ReadInt32LittleEndian(headerBytes.AsSpan(offset, sizeof(int)));
        offset += sizeof(int);

        var totalLength = BinaryPrimitives.ReadInt32LittleEndian(headerBytes.AsSpan(offset, sizeof(int)));
        offset += sizeof(int);

        var pageCount = BinaryPrimitives.ReadInt32LittleEndian(headerBytes.AsSpan(offset, sizeof(int)));
        offset += sizeof(int);

        var firstDataPageId = (uint)BinaryPrimitives.ReadInt32LittleEndian(headerBytes.AsSpan(offset, sizeof(int)));

        return new LargeDocumentIndexHeader
        {
            DocumentIdentifier = documentIdentifier,
            TotalLength = totalLength,
            PageCount = pageCount,
            FirstDataPageId = firstDataPageId
        };
    }

    private void WriteDataPageHeader(Page page, int pageNumber, uint nextPageId)
    {
        var header = new byte[DataPageHeaderSize];
        var offset = 0;

        BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(offset, sizeof(int)), pageNumber);
        offset += sizeof(int);

        BinaryPrimitives.WriteUInt32LittleEndian(header.AsSpan(offset, sizeof(uint)), nextPageId);

        page.WriteData(0, header);
    }

    private void UpdateDataPageHeaderNextPage(Page page, uint nextPageId)
    {
        // 更新NextPageId字段（偏移量为sizeof(int)）
        var nextPageIdBytes = new byte[sizeof(uint)];
        BinaryPrimitives.WriteUInt32LittleEndian(nextPageIdBytes, nextPageId);
        page.WriteData(sizeof(int), nextPageIdBytes);
    }

    private LargeDocumentDataHeader ReadDataPageHeader(Page page)
    {
        var headerBytes = page.ReadData(0, DataPageHeaderSize);
        if (headerBytes.Length < DataPageHeaderSize)
            throw new InvalidOperationException("Invalid large document data page header");

        var pageNumber = BinaryPrimitives.ReadInt32LittleEndian(headerBytes.AsSpan(0, sizeof(int)));
        var nextPageId = BinaryPrimitives.ReadUInt32LittleEndian(headerBytes.AsSpan(sizeof(int), sizeof(uint)));

        return new LargeDocumentDataHeader
        {
            PageNumber = pageNumber,
            NextPageId = nextPageId
        };
    }
}

/// <summary>
/// 大文档索引页面头部
/// </summary>
internal class LargeDocumentIndexHeader
{
    public int DocumentIdentifier { get; set; }
    public int TotalLength { get; set; }
    public int PageCount { get; set; }
    public uint FirstDataPageId { get; set; }
}

/// <summary>
/// 大文档数据页面头部
/// </summary>
internal class LargeDocumentDataHeader
{
    public int PageNumber { get; set; }
    public uint NextPageId { get; set; }
}

/// <summary>
/// 大文档统计信息
/// </summary>
public class LargeDocumentStatistics
{
    public uint IndexPageId { get; init; }
    public int TotalLength { get; init; }
    public int PageCount { get; init; }
    public uint FirstDataPageId { get; init; }

    public override string ToString()
    {
        return $"LargeDoc[Index={IndexPageId}, Size={TotalLength:N0} bytes, Pages={PageCount}]";
    }
}
