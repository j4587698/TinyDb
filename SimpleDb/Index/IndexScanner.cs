using System.Reflection;
using SimpleDb.Attributes;
using SimpleDb.Bson;
using SimpleDb.Core;

namespace SimpleDb.Index;

/// <summary>
/// 索引扫描器，用于自动发现和创建基于属性的索引
/// </summary>
public static class IndexScanner
{
    /// <summary>
    /// 扫描实体类型并创建相应的索引
    /// </summary>
    /// <param name="engine">数据库引擎</param>
    /// <param name="entityType">实体类型</param>
    /// <param name="collectionName">集合名称</param>
    public static void ScanAndCreateIndexes(SimpleDbEngine engine, Type entityType, string collectionName)
    {
        if (engine == null) throw new ArgumentNullException(nameof(engine));
        if (entityType == null) throw new ArgumentNullException(nameof(entityType));
        if (string.IsNullOrEmpty(collectionName)) throw new ArgumentException("Collection name cannot be null or empty", nameof(collectionName));

        var indexManager = engine.GetIndexManager(collectionName);

        // 自动创建主键索引（_id字段）
        CreatePrimaryKeyIndex(indexManager);

        // 扫描单个属性索引
        ScanPropertyIndexes(indexManager, entityType);

        // 扫描复合索引
        ScanCompositeIndexes(indexManager, entityType);
    }

    /// <summary>
    /// 扫描属性上的索引标记
    /// </summary>
    /// <param name="indexManager">索引管理器</param>
    /// <param name="entityType">实体类型</param>
    private static void ScanPropertyIndexes(IndexManager indexManager, Type entityType)
    {
        var indexedProperties = new List<(PropertyInfo Property, IndexAttribute Attribute)>();

        // 收集所有带有IndexAttribute的属性
        foreach (var property in entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var indexAttr = property.GetCustomAttribute<IndexAttribute>();
            if (indexAttr != null)
            {
                indexedProperties.Add((property, indexAttr));
            }
        }

        // 按优先级排序
        indexedProperties.Sort((a, b) => a.Attribute.Priority.CompareTo(b.Attribute.Priority));

        // 为每个属性创建索引
        foreach (var (property, indexAttr) in indexedProperties)
        {
            var indexName = GenerateIndexName(indexAttr, property.Name);
            var fields = new[] { property.Name };

            if (!indexManager.IndexExists(indexName))
            {
                var created = indexManager.CreateIndex(indexName, fields, indexAttr.Unique);
                if (created)
                {
                    Console.WriteLine($"✅ 自动创建索引: {indexName} on {entityType.Name}.{property.Name} (Unique={indexAttr.Unique})");
                }
            }
        }
    }

    /// <summary>
    /// 扫描复合索引标记
    /// </summary>
    /// <param name="indexManager">索引管理器</param>
    /// <param name="entityType">实体类型</param>
    private static void ScanCompositeIndexes(IndexManager indexManager, Type entityType)
    {
        var compositeIndexes = entityType.GetCustomAttributes<CompositeIndexAttribute>();

        foreach (var compositeAttr in compositeIndexes)
        {
            if (!indexManager.IndexExists(compositeAttr.Name))
            {
                var created = indexManager.CreateIndex(compositeAttr.Name, compositeAttr.Fields, compositeAttr.Unique);
                if (created)
                {
                    Console.WriteLine($"✅ 自动创建复合索引: {compositeAttr.Name} on [{string.Join(", ", compositeAttr.Fields)}] (Unique={compositeAttr.Unique})");
                }
            }
        }
    }

    /// <summary>
    /// 生成索引名称
    /// </summary>
    /// <param name="indexAttr">索引属性</param>
    /// <param name="propertyName">属性名称</param>
    /// <returns>索引名称</returns>
    private static string GenerateIndexName(IndexAttribute indexAttr, string propertyName)
    {
        if (!string.IsNullOrEmpty(indexAttr.Name))
        {
            return indexAttr.Name;
        }

        // 自动生成索引名称
        var prefix = indexAttr.Unique ? "uidx" : "idx";
        return $"{prefix}_{propertyName.ToLower()}";
    }

    /// <summary>
    /// 获取实体的所有索引信息
    /// </summary>
    /// <param name="entityType">实体类型</param>
    /// <returns>索引信息列表</returns>
    public static List<EntityIndexInfo> GetEntityIndexes(Type entityType)
    {
        var indexes = new List<EntityIndexInfo>();

        // 收集单属性索引
        foreach (var property in entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            var indexAttr = property.GetCustomAttribute<IndexAttribute>();
            if (indexAttr != null)
            {
                indexes.Add(new EntityIndexInfo
                {
                    Name = GenerateIndexName(indexAttr, property.Name),
                    Fields = new[] { property.Name },
                    IsUnique = indexAttr.Unique,
                    SortDirection = indexAttr.SortDirection,
                    Priority = indexAttr.Priority,
                    IsComposite = false
                });
            }
        }

        // 收集复合索引
        foreach (var compositeAttr in entityType.GetCustomAttributes<CompositeIndexAttribute>())
        {
            indexes.Add(new EntityIndexInfo
            {
                Name = compositeAttr.Name,
                Fields = compositeAttr.Fields,
                IsUnique = compositeAttr.Unique,
                SortDirection = IndexSortDirection.Ascending, // 复合索引默认升序
                Priority = 0,
                IsComposite = true
            });
        }

        // 按优先级排序
        indexes.Sort((a, b) => a.Priority.CompareTo(b.Priority));

        return indexes;
    }

    /// <summary>
    /// 创建主键索引
    /// </summary>
    /// <param name="indexManager">索引管理器</param>
    private static void CreatePrimaryKeyIndex(IndexManager indexManager)
    {
        const string primaryKeyIndexName = "pk__id";

        // 检查主键索引是否已存在
        if (indexManager.IndexExists(primaryKeyIndexName))
        {
            return; // 主键索引已存在，无需重复创建
        }

        try
        {
            // 创建主键索引，_id字段是唯一的
            indexManager.CreateIndex(primaryKeyIndexName, new[] { "_id" }, true);
            Console.WriteLine($"✅ 自动创建主键索引: {primaryKeyIndexName} on _id (Unique=True)");
        }
        catch (Exception ex)
        {
            // 主键索引创建失败不应阻止系统启动，但需要记录警告
            Console.WriteLine($"Warning: Failed to create primary key index {primaryKeyIndexName}: {ex.Message}");
        }
    }
}

/// <summary>
/// 实体索引信息
/// </summary>
public sealed class EntityIndexInfo
{
    /// <summary>
    /// 索引名称
    /// </summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>
    /// 索引字段
    /// </summary>
    public string[] Fields { get; init; } = Array.Empty<string>();

    /// <summary>
    /// 是否为唯一索引
    /// </summary>
    public bool IsUnique { get; init; }

    /// <summary>
    /// 排序方向
    /// </summary>
    public IndexSortDirection SortDirection { get; init; }

    /// <summary>
    /// 优先级
    /// </summary>
    public int Priority { get; init; }

    /// <summary>
    /// 是否为复合索引
    /// </summary>
    public bool IsComposite { get; init; }

    /// <summary>
    /// 重写 ToString 方法
    /// </summary>
    public override string ToString()
    {
        var type = IsComposite ? "Composite" : "Single";
        var unique = IsUnique ? "Unique" : "Non-Unique";
        return $"{type}Index[{Name}]: [{string.Join(", ", Fields)}] ({unique})";
    }
}