using System.Diagnostics.CodeAnalysis;
using System.Linq;
using TinyDb.Collections;
using TinyDb.Core;

namespace TinyDb.Metadata;

/// <summary>
/// 元数据管理器，负责元数据的存储和查询
/// 这是核心库的一部分，仅提供基础的元数据查询功能
/// </summary>
public class MetadataManager
{
    private readonly TinyDbEngine _engine;
    private const string METADATA_COLLECTION_PREFIX = "__metadata_";

    /// <summary>
    /// 创建元数据管理器
    /// </summary>
    /// <param name="engine">数据库引擎</param>
    public MetadataManager(TinyDbEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
    }

    /// <summary>
    /// 保存实体类型元数据
    /// </summary>
    /// <param name="entityType">实体类型</param>
    public void SaveEntityMetadata([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type entityType)
    {
        var metadata = MetadataExtractor.ExtractEntityMetadata(entityType);
        var collectionName = GetMetadataCollectionName(entityType);

        // 使用MetadataDocument包装类存储元数据
        var collection = _engine.GetCollectionWithName<MetadataDocument>(collectionName);

        // 转换为MetadataDocument并保存
        var metadataDoc = MetadataDocument.FromEntityMetadata(metadata);
        var existing = collection.FindOne(doc => doc.TypeName == metadataDoc.TypeName);

        if (existing != null)
        {
            metadataDoc.Id = existing.Id;
            metadataDoc.CreatedAt = existing.CreatedAt;
            metadataDoc.UpdatedAt = DateTime.Now;
            collection.Update(metadataDoc);
        }
        else
        {
            collection.Insert(metadataDoc);
        }
    }

    /// <summary>
    /// 获取实体类型元数据
    /// </summary>
    /// <param name="entityType">实体类型</param>
    /// <returns>实体元数据</returns>
    public EntityMetadata? GetEntityMetadata([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type entityType)
    {
        var collectionName = GetMetadataCollectionName(entityType);
        var collection = _engine.GetCollectionWithName<MetadataDocument>(collectionName);

        var documents = collection.FindAll().ToList();

        if (documents.Count == 0)
            return null;

        // 如果存在多个文档，返回最近更新的文档
        var metadataDoc = documents
            .OrderByDescending(d => d.UpdatedAt)
            .First();
        return metadataDoc.ToEntityMetadata();
    }

    /// <summary>
    /// 获取所有已注册的实体类型名称
    /// </summary>
    /// <returns>实体类型名称列表</returns>
    public List<string> GetRegisteredEntityTypes()
    {
        var collectionNames = _engine.GetCollectionNames()
            .Where(name => name.StartsWith(METADATA_COLLECTION_PREFIX))
            .ToList();

        return collectionNames.Select(name => name.Substring(METADATA_COLLECTION_PREFIX.Length)).ToList();
    }

    /// <summary>
    /// 检查实体类型是否有元数据
    /// </summary>
    /// <param name="entityType">实体类型</param>
    /// <returns>是否有元数据</returns>
    public bool HasMetadata([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type entityType)
    {
        var collectionName = GetMetadataCollectionName(entityType);
        return _engine.CollectionExists(collectionName);
    }

    /// <summary>
    /// 获取元数据集合名称。
    /// </summary>
    /// <param name="entityType">实体类型。</param>
    /// <returns>元数据集合名称。</returns>
    private static string GetMetadataCollectionName([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] Type entityType)
    {
        return $"{METADATA_COLLECTION_PREFIX}{entityType.FullName ?? entityType.Name}";
    }
}
