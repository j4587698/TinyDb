using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.UI.Models;
using TinyDb.UI.Services;
using TinyDb.Bson;
using TinyDb.Metadata;

namespace TinyDb.UI.Services;


/// <summary>
/// 表结构管理服务 - 基于TinyDb实体系统 + 自动元数据生成
/// </summary>
public class TableStructureService
{
    private readonly DatabaseService _databaseService;
    private TinyDbEngine? _engine;
    private DynamicEntityGenerator? _dynamicGenerator;

    public TableStructureService(DatabaseService databaseService)
    {
        _databaseService = databaseService;
    }

    /// <summary>
    /// 设置数据库引擎
    /// </summary>
    public void SetEngine(TinyDbEngine engine)
    {
        _engine = engine;
        _dynamicGenerator = new DynamicEntityGenerator(engine);
    }

    /// <summary>
    /// 获取所有集合结构（基于TinyDb原生机制）
    /// </summary>
    public async Task<List<TableStructure>> GetAllTablesAsync()
    {
        if (_engine == null)
            throw new InvalidOperationException("数据库未连接");

        var tables = new List<TableStructure>();
        var collectionNames = _engine.GetCollectionNames()
            .Where(name => !name.StartsWith("__"))
            .ToList();

        foreach (var collectionName in collectionNames)
        {
            try
            {
                var table = await GetTableStructureAsync(collectionName);
                if (table != null)
                {
                    tables.Add(table);
                }
            }
            catch
            {
                // 忽略无法访问的集合
            }
        }

        return tables;
    }

    /// <summary>
    /// 获取集合结构（混合策略：优先原生元数据，回退到数据驱动）
    /// </summary>
    public async Task<TableStructure?> GetTableStructureAsync(string collectionName)
    {
        if (_engine == null)
            throw new InvalidOperationException("数据库未连接");

        try
        {
            Console.WriteLine($"[DEBUG] 获取集合结构: {collectionName}");

            // 1. 优先尝试从TinyDb原生元数据获取结构
            var metadataStructure = await TryGetStructureFromMetadata(collectionName);
            if (metadataStructure != null)
            {
                Console.WriteLine($"[INFO] ✅ 从原生元数据加载集合结构: {collectionName}");
                return metadataStructure;
            }

            // 2. 回退到纯数据驱动推断
            Console.WriteLine($"[INFO] 📊 从实际数据推断集合结构: {collectionName}");
            return await InferCollectionStructureFromData(collectionName);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] 获取集合结构失败: {ex.Message}");
            throw new InvalidOperationException($"获取集合结构失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 尝试从原生元数据获取结构
    /// </summary>
    private async Task<TableStructure?> TryGetStructureFromMetadata(string collectionName)
    {
        try
        {
            // 直接查找所有元数据集合，寻找匹配的
            var allCollections = _engine.GetCollectionNames();
            var metadataCollections = allCollections.Where(name => name.StartsWith("__metadata_")).ToList();

            // 寻找匹配的元数据集合
            string? matchingMetadataCollection = null;
            foreach (var metaCollection in metadataCollections)
            {
                var typeName = metaCollection.Substring("__metadata_".Length);
                if (typeName.Contains(collectionName) || collectionName.Contains(typeName))
                {
                    matchingMetadataCollection = metaCollection;
                    break;
                }
            }

            if (matchingMetadataCollection == null)
            {
                Console.WriteLine($"[DEBUG] 未找到匹配的元数据集合: {collectionName}");
                return null;
            }

            Console.WriteLine($"[DEBUG] 找到匹配的元数据集合: {matchingMetadataCollection}");

            if (!_engine.CollectionExists(matchingMetadataCollection))
            {
                Console.WriteLine($"[DEBUG] 元数据集合不存在: {matchingMetadataCollection}");
                return null;
            }

            var metadataCollection = _engine.GetCollection<MetadataDocument>(matchingMetadataCollection);
            var metadataDoc = metadataCollection.FindAll().FirstOrDefault();

            if (metadataDoc == null)
            {
                Console.WriteLine($"[DEBUG] 元数据集合为空: {matchingMetadataCollection}");
                return null;
            }

            var entityMetadata = metadataDoc.ToEntityMetadata();
            if (entityMetadata == null)
            {
                Console.WriteLine($"[DEBUG] 无法解析实体元数据");
                return null;
            }

            // 转换为TableStructure
            var table = new TableStructure
            {
                TableName = collectionName,
                DisplayName = entityMetadata.DisplayName,
                Description = entityMetadata.Description,
                RecordCount = GetCollectionRecordCount(collectionName),
                CreatedAt = DateTime.Now,
                UpdatedAt = DateTime.Now,
                HasMetadata = true
            };

            foreach (var prop in entityMetadata.Properties.OrderBy(p => p.Order))
            {
                var tableField = new TableField
                {
                    FieldName = prop.PropertyName,
                    DisplayName = prop.DisplayName,
                    FieldType = ConvertStringToTableFieldType(prop.PropertyType),
                    Description = prop.Description,
                    Order = prop.Order,
                    IsRequired = prop.Required,
                    IsPrimaryKey = IsPrimaryKeyField(prop.PropertyName),
                    IsUnique = false,
                    IsIndexed = false
                };

                table.Fields.Add(tableField);
            }

            Console.WriteLine($"[DEBUG] 从元数据成功转换 {table.Fields.Count} 个字段");
            return table;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[DEBUG] 从元数据获取结构失败: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// 获取集合记录数
    /// </summary>
    private long GetCollectionRecordCount(string collectionName)
    {
        try
        {
            // 首先检查集合是否存在
            if (!_engine.CollectionExists(collectionName))
            {
                Console.WriteLine($"[DEBUG] GetCollectionRecordCount: 集合不存在 {collectionName}");
                return 0;
            }

            try
            {
                var collection = _engine.GetCollection<BsonDocument>(collectionName);
                return collection.Count();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[DEBUG] GetCollectionRecordCount: BsonDocument读取失败 {collectionName} - {ex.Message}");

                try
                {
                    var dynamicCollection = _engine.GetCollection<DynamicEntity>(collectionName);
                    return dynamicCollection.Count();
                }
                catch (Exception ex2)
                {
                    Console.WriteLine($"[DEBUG] GetCollectionRecordCount: DynamicEntity读取失败 {collectionName} - {ex2.Message}");

                    try
                    {
                        var tempDocCollection = _engine.GetCollection<TinyDb.UI.Services.TempDocument>(collectionName);
                        return tempDocCollection.Count();
                    }
                    catch (Exception ex3)
                    {
                        Console.WriteLine($"[DEBUG] GetCollectionRecordCount: TempDocument读取失败 {collectionName} - {ex3.Message}");
                        return 1; // 集合存在但无法读取，返回至少1表示存在
                    }
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] GetCollectionRecordCount: 检查集合存在性失败 {collectionName} - {ex.Message}");
            return 0;
        }
    }

    /// <summary>
    /// 判断是否为主键字段
    /// </summary>
    private bool IsPrimaryKeyField(string fieldName)
    {
        return fieldName.Equals("Id", StringComparison.OrdinalIgnoreCase) ||
               fieldName.Equals("_id", StringComparison.OrdinalIgnoreCase) ||
               fieldName.EndsWith("Id", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// 将字符串类型转换为TableFieldType
    /// </summary>
    private TableFieldType ConvertStringToTableFieldType(string typeString)
    {
        return typeString switch
        {
            "System.String" => TableFieldType.String,
            "System.Int32" => TableFieldType.Integer,
            "System.Int64" => TableFieldType.Long,
            "System.Double" => TableFieldType.Double,
            "System.Decimal" => TableFieldType.Decimal,
            "System.Boolean" => TableFieldType.Boolean,
            "System.DateTime" => TableFieldType.DateTime,
            "System.DateTimeOffset" => TableFieldType.DateTimeOffset,
            "System.Guid" => TableFieldType.Guid,
            _ => TableFieldType.String
        };
    }

    /// <summary>
    /// 从集合数据推断结构（基于TinyDb原生机制）
    /// </summary>
    private async Task<TableStructure?> InferCollectionStructureFromData(string collectionName)
    {
        try
        {
            // 获取记录数量
            long recordCount = 0;
            ITinyCollection<BsonDocument>? bsonCollection = null;
            ITinyCollection<DynamicEntity>? dynamicCollection = null;
            ITinyCollection<TinyDb.UI.Services.TempDocument>? tempDocCollection = null;

            // 尝试获取记录数（支持多种集合类型）
            try
            {
                // 首先检查集合是否存在
                if (!_engine.CollectionExists(collectionName))
                {
                    Console.WriteLine($"[DEBUG] 集合不存在: {collectionName}");
                    return null;
                }

                // 尝试作为BsonDocument读取
                try
                {
                    bsonCollection = _engine.GetCollection<BsonDocument>(collectionName);
                    recordCount = bsonCollection.Count();
                    Console.WriteLine($"[DEBUG] 作为BsonDocument成功读取集合 {collectionName}，记录数: {recordCount}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[DEBUG] 作为BsonDocument读取失败: {collectionName} - {ex.Message}");

                    // 如果BsonDocument失败，尝试DynamicEntity
                    try
                    {
                        dynamicCollection = _engine.GetCollection<DynamicEntity>(collectionName);
                        recordCount = dynamicCollection.Count();
                        Console.WriteLine($"[DEBUG] 作为DynamicEntity成功读取集合 {collectionName}，记录数: {recordCount}");
                    }
                    catch (Exception ex2)
                    {
                        Console.WriteLine($"[DEBUG] 作为DynamicEntity读取失败: {collectionName} - {ex2.Message}");

                        // 如果DynamicEntity也失败，尝试TempDocument
                        try
                        {
                            tempDocCollection = _engine.GetCollection<TinyDb.UI.Services.TempDocument>(collectionName);
                            recordCount = tempDocCollection.Count();
                            Console.WriteLine($"[DEBUG] 作为TempDocument成功读取集合 {collectionName}，记录数: {recordCount}");
                        }
                        catch (Exception ex3)
                        {
                            Console.WriteLine($"[DEBUG] 作为TempDocument读取失败: {collectionName} - {ex3.Message}");

                            // 最后尝试使用原始方法获取集合
                            try
                            {
                                var allCollectionNames = _engine.GetCollectionNames().ToList();
                                if (allCollectionNames.Contains(collectionName))
                                {
                                    Console.WriteLine($"[DEBUG] 集合存在于列表中，使用通用方法: {collectionName}");
                                    // 使用通用方法，假设它有数据
                                    recordCount = 1; // 至少表示集合存在
                                }
                                else
                                {
                                    Console.WriteLine($"[DEBUG] 集合不在集合列表中: {collectionName}");
                                    return null;
                                }
                            }
                            catch
                            {
                                Console.WriteLine($"[DEBUG] 无法读取集合 {collectionName}");
                                return null;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] 检查集合存在性失败: {collectionName} - {ex.Message}");
                return null;
            }

            Console.WriteLine($"[DEBUG] 集合 {collectionName} 记录数: {recordCount}");

            var table = new TableStructure
            {
                TableName = collectionName,
                DisplayName = collectionName,
                RecordCount = recordCount,
                CreatedAt = DateTime.Now,
                UpdatedAt = DateTime.Now
            };

            if (recordCount == 0)
            {
                Console.WriteLine($"[DEBUG] 集合 {collectionName} 为空，无法推断字段结构");
                // 为空集合添加提示字段
                var hintField = new TableField
                {
                    FieldName = "_empty_collection_hint",
                    DisplayName = "提示",
                    FieldType = TableFieldType.String,
                    Order = 0,
                    IsRequired = false,
                    IsPrimaryKey = false,
                    IsUnique = false,
                    Description = "此集合为空，添加数据后将自动显示字段结构"
                };
                table.Fields.Add(hintField);
                return table;
            }

            // 尝试多种方式读取数据来推断结构
            var fields = new Dictionary<string, (TableFieldType, int, bool)>();
            bool foundData = false;
            int totalDocuments = 0;

            // 首先尝试作为BsonDocument读取（外部数据）
            if (bsonCollection != null)
            {
                try
                {
                    var documents = bsonCollection.FindAll().Take(100).ToList();
                    Console.WriteLine($"[DEBUG] 分析 {documents.Count} 个BsonDocument以推断结构");

                    foreach (var doc in documents)
                    {
                        foreach (var kvp in doc)
                        {
                            ProcessFieldForInference(kvp.Key, kvp.Value, fields);
                        }
                    }
                    totalDocuments += documents.Count;
                    foundData = documents.Any();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[DEBUG] 作为BsonDocument读取失败: {ex.Message}");
                }
            }

            // 如果BsonDocument方式没有数据，尝试作为DynamicEntity读取（UI创建的数据）
            if (!foundData && dynamicCollection != null)
            {
                try
                {
                    var entities = dynamicCollection.FindAll().Take(100).ToList();
                    Console.WriteLine($"[DEBUG] 分析 {entities.Count} 个DynamicEntity以推断结构");

                    foreach (var entity in entities)
                    {
                        // 处理Id字段
                        if (!string.IsNullOrEmpty(entity.Id))
                        {
                            ProcessFieldForInference("Id", entity.Id, fields);
                        }

                        // 处理Data字典中的字段
                        foreach (var kvp in entity.Data)
                        {
                            ProcessFieldForInference(kvp.Key, kvp.Value, fields);
                        }
                    }
                    totalDocuments += entities.Count;
                    foundData = entities.Any();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[DEBUG] 作为DynamicEntity读取失败: {ex.Message}");
                }
            }

            // 如果BsonDocument和DynamicEntity方式都没有数据，尝试作为TempDocument读取
            if (!foundData)
            {
                try
                {
                    Console.WriteLine($"[DEBUG] 尝试作为TempDocument读取集合: {collectionName}");
                    tempDocCollection = _engine.GetCollection<TinyDb.UI.Services.TempDocument>(collectionName);
                    var tempDocuments = tempDocCollection.FindAll().Take(100).ToList();
                    Console.WriteLine($"[DEBUG] 分析 {tempDocuments.Count} 个TempDocument以推断结构");

                    foreach (var tempDoc in tempDocuments)
                    {
                        // 处理Id字段 - 这个总是有值，是主键
                        if (!string.IsNullOrEmpty(tempDoc.Id))
                        {
                            ProcessFieldForInference("Id", tempDoc.Id, fields);
                        }

                        // 处理Name字段 - 只有非空且有实际内容才处理
                        if (!string.IsNullOrEmpty(tempDoc.Name))
                        {
                            ProcessFieldForInference("Name", tempDoc.Name, fields);
                        }

                        // 处理Description字段 - 只有非空且有实际内容才处理
                        if (!string.IsNullOrEmpty(tempDoc.Description))
                        {
                            ProcessFieldForInference("Description", tempDoc.Description, fields);
                        }

                        // 处理Created字段 - 只有不是默认值才处理
                        if (tempDoc.Created != default(DateTime))
                        {
                            ProcessFieldForInference("Created", tempDoc.Created, fields);
                        }

                        // 处理IsTemporary字段 - 只有不是默认值才处理
                        if (tempDoc.IsTemporary)
                        {
                            ProcessFieldForInference("IsTemporary", tempDoc.IsTemporary, fields);
                        }
                    }
                    totalDocuments += tempDocuments.Count;
                    foundData = tempDocuments.Any();
                    Console.WriteLine($"[DEBUG] TempDocument分析完成，找到 {tempDocuments.Count} 个文档");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[DEBUG] 作为TempDocument读取失败: {collectionName} - {ex.Message}");
                }
            }

            if (foundData && fields.Any())
            {
                var order = 0;
                foreach (var kvp in fields.OrderBy(f => f.Key))
                {
                    var field = new TableField
                    {
                        FieldName = kvp.Key,
                        DisplayName = kvp.Key,
                        FieldType = kvp.Value.Item1,
                        Order = order++,
                        IsRequired = totalDocuments > 0 && kvp.Value.Item2 == totalDocuments, // 如果所有文档都有这个字段，则认为是必需的
                        IsPrimaryKey = kvp.Value.Item3, // 基于字段名推断是否为主键
                        IsUnique = kvp.Value.Item3 && kvp.Value.Item1 == TableFieldType.String
                    };

                    table.Fields.Add(field);
                }

                Console.WriteLine($"[DEBUG] 推断出 {table.Fields.Count} 个字段");
            }

            return table;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] 推断集合结构失败: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// 从元数据加载字段结构定义
    /// </summary>
    private TableStructure? LoadFieldStructureFromMetadata(string collectionName)
    {
        try
        {
            var metadataCollection = _engine.GetCollection<BsonDocument>("__ui_table_metadata");
            var metadataDoc = metadataCollection.FindById((BsonValue)collectionName);

            if (metadataDoc == null)
            {
                Console.WriteLine($"[DEBUG] 未找到表 {collectionName} 的字段结构元数据");
                return null;
            }

            Console.WriteLine($"[DEBUG] 找到表 {collectionName} 的字段结构元数据");

            var table = new TableStructure
            {
                TableName = collectionName,
                DisplayName = metadataDoc.Contains("displayName") ? metadataDoc["displayName"].ToString() : collectionName,
                Description = metadataDoc.Contains("description") ? metadataDoc["description"].ToString() : null,
                Fields = new System.Collections.ObjectModel.ObservableCollection<TableField>()
            };

            if (metadataDoc.Contains("fieldsJson"))
            {
                var fieldsJson = metadataDoc["fieldsJson"].ToString();
                var fieldData = System.Text.Json.JsonSerializer.Deserialize<List<Dictionary<string, object>>>(fieldsJson);

                if (fieldData != null)
                {
                    foreach (var data in fieldData)
                    {
                        var field = new TableField
                        {
                            FieldName = data.GetValueOrDefault("FieldName", "")?.ToString() ?? "",
                            DisplayName = data.GetValueOrDefault("DisplayName", "")?.ToString() ?? "",
                            Description = data.GetValueOrDefault("Description")?.ToString(),
                            FieldType = Enum.TryParse<TableFieldType>(data.GetValueOrDefault("FieldType", "String")?.ToString() ?? "String", out var ft) ? ft : TableFieldType.String,
                            IsRequired = data.GetValueOrDefault("IsRequired", false)?.ToString()?.ToLower() == "true",
                            DefaultValue = data.GetValueOrDefault("DefaultValue")?.ToString(),
                            Order = int.TryParse(data.GetValueOrDefault("Order", "0")?.ToString() ?? "0", out var order) ? order : 0,
                            IsPrimaryKey = data.GetValueOrDefault("IsPrimaryKey", false)?.ToString()?.ToLower() == "true",
                            IsUnique = data.GetValueOrDefault("IsUnique", false)?.ToString()?.ToLower() == "true",
                            IsIndexed = data.GetValueOrDefault("IsIndexed", false)?.ToString()?.ToLower() == "true"
                        };

                        // 处理数值类型字段
                        if (data.ContainsKey("MaxLength") && int.TryParse(data["MaxLength"]?.ToString(), out var maxLen))
                            field.MaxLength = maxLen;
                        if (data.ContainsKey("MinValue") && double.TryParse(data["MinValue"]?.ToString(), out var minVal))
                            field.MinValue = minVal;
                        if (data.ContainsKey("MaxValue") && double.TryParse(data["MaxValue"]?.ToString(), out var maxVal))
                            field.MaxValue = maxVal;

                        table.Fields.Add(field);
                    }
                }

                Console.WriteLine($"[DEBUG] 成功加载 {table.Fields.Count} 个字段定义");
                return table;
            }

            return null;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] 加载字段结构元数据失败: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// 处理字段用于结构推断
    /// </summary>
    private static void ProcessFieldForInference(string fieldName, object? value, Dictionary<string, (TableFieldType, int, bool)> fields)
    {
        if (!fields.ContainsKey(fieldName))
        {
            var fieldType = InferFieldValueType(value);
            var isId = IsLikelyIdField(fieldName);
            fields[fieldName] = (fieldType, 1, isId);
        }
        else
        {
            var (existingType, count, isId) = fields[fieldName];
            var newFieldType = InferFieldValueType(value);
            // 如果推断的类型不同，使用更通用的类型
            var newType = GetCommonFieldType(existingType, newFieldType);
            fields[fieldName] = (newType, count + 1, isId);
        }
    }

    /// <summary>
    /// 推断字段值类型（从object值）
    /// </summary>
    private static TableFieldType InferFieldValueType(object? value)
    {
        return value switch
        {
            null => TableFieldType.String,
            string => TableFieldType.String,
            int => TableFieldType.Integer,
            long => TableFieldType.Long,
            double => TableFieldType.Double,
            decimal => TableFieldType.Decimal,
            bool => TableFieldType.Boolean,
            DateTime => TableFieldType.DateTime,
            Guid => TableFieldType.Guid,
            byte[] => TableFieldType.Binary,
            // 如果是BsonValue，使用原有的推断逻辑
            BsonValue bsonValue => InferFieldType(bsonValue),
            _ => TableFieldType.String
        };
    }

    /// <summary>
    /// 判断字段是否可能是ID字段
    /// </summary>
    private static bool IsLikelyIdField(string fieldName)
    {
        var lowerFieldName = fieldName.ToLowerInvariant();
        return lowerFieldName == "id" ||
               lowerFieldName == "_id" ||
               lowerFieldName.EndsWith("id") ||
               lowerFieldName.Contains("uuid") ||
               lowerFieldName.Contains("guid");
    }

    /// <summary>
    /// 推断字段类型
    /// </summary>
    private static TableFieldType InferFieldType(BsonValue value)
    {
        return value.BsonType switch
        {
            BsonType.String => TableFieldType.String,
            BsonType.Int32 => TableFieldType.Integer,
            BsonType.Int64 => TableFieldType.Long,
            BsonType.Double => TableFieldType.Double,
            BsonType.Decimal128 => TableFieldType.Decimal,
            BsonType.Boolean => TableFieldType.Boolean,
            BsonType.DateTime => TableFieldType.DateTime,
            BsonType.ObjectId => TableFieldType.Guid,
            BsonType.Binary => TableFieldType.Binary,
            BsonType.Document => TableFieldType.Object,
            BsonType.Array => TableFieldType.Array,
            BsonType.Null => TableFieldType.String,
            _ => TableFieldType.String
        };
    }

    /// <summary>
    /// 获取通用字段类型
    /// </summary>
    private static TableFieldType GetCommonFieldType(TableFieldType type1, TableFieldType type2)
    {
        // 如果类型相同，返回该类型
        if (type1 == type2) return type1;

        // 定义类型优先级（数字 > 字符串 > 其他）
        var priority = new Dictionary<TableFieldType, int>
        {
            { TableFieldType.String, 1 },
            { TableFieldType.Integer, 2 },
            { TableFieldType.Long, 3 },
            { TableFieldType.Double, 4 },
            { TableFieldType.Decimal, 5 },
            { TableFieldType.Boolean, 6 },
            { TableFieldType.DateTime, 7 },
            { TableFieldType.DateTimeOffset, 8 },
            { TableFieldType.Guid, 9 },
            { TableFieldType.Binary, 10 },
            { TableFieldType.Json, 11 },
            { TableFieldType.Array, 12 },
            { TableFieldType.Object, 13 }
        };

        return priority[type1] >= priority[type2] ? type1 : type2;
    }

    /// <summary>
    /// 创建集合（基于TinyDb原生机制 + 自动元数据生成）
    /// </summary>
    public async Task<bool> CreateTableAsync(TableStructure table)
    {
        if (_engine == null)
            throw new InvalidOperationException("数据库未连接");

        try
        {
            // 验证集合名
            if (string.IsNullOrWhiteSpace(table.TableName))
                throw new ArgumentException("集合名不能为空");

            Console.WriteLine($"[DEBUG] 创建集合: {table.TableName}");

            // 检查集合是否已存在
            if (_engine.CollectionExists(table.TableName))
            {
                throw new InvalidOperationException($"集合 '{table.TableName}' 已存在");
            }

            // 使用EntityFactory为集合名称创建对应的实体类型
            var entityType = EntityFactory.GetOrCreateEntityType(table.TableName);
            Console.WriteLine($"[EntityFactory] 为集合 '{table.TableName}' 创建实体类型: {entityType.Name}");

            // 使用反射获取集合，类型安全
            var getCollectionMethod = _engine.GetType()
                .GetMethods()
                .FirstOrDefault(m => m.Name == "GetCollection" && m.IsGenericMethod);

            if (getCollectionMethod == null)
            {
                throw new InvalidOperationException("无法找到GetCollection方法");
            }

            var genericMethod = getCollectionMethod.MakeGenericMethod(entityType);
            dynamic collection = genericMethod.Invoke(_engine, new object[] { table.TableName })!;
            Console.WriteLine($"[DEBUG] 创建类型化空集合: {table.TableName}, 类型: {entityType.Name}");

            // 创建一个类型化的实体来初始化集合
            dynamic tempEntity = Activator.CreateInstance(entityType)!;

            // 使用反射设置Id属性
            var idProperty = entityType.GetProperty("Id");
            if (idProperty != null)
            {
                idProperty.SetValue(tempEntity, "_temp_init_" + Guid.NewGuid().ToString("N")[..8]);
            }

            // 使用反射访问Data字典来设置初始数据
            var dataProperty = entityType.GetProperty("Data");
            if (dataProperty != null && dataProperty.PropertyType == typeof(Dictionary<string, object?>))
            {
                var data = (Dictionary<string, object?>)dataProperty.GetValue(tempEntity)!;
                data["_created"] = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
                data["_is_temp"] = true;
                data["_entity_type"] = entityType.Name;
                data["_table_structure"] = true;
            }

            // 插入临时实体
            var tempId = await Task.Run(() => collection.Insert(tempEntity));
            Console.WriteLine($"[DEBUG] 类型化临时实体插入成功，ID: {tempId}, 类型: {entityType.Name}");

            // 立即删除临时实体，但集合会保留
            await Task.Run(() => collection.Delete(tempId));
            Console.WriteLine($"[DEBUG] 类型化临时实体已删除，集合保留");

            // 自动生成并保存元数据
            if (_dynamicGenerator != null && table.Fields.Count > 0)
            {
                Console.WriteLine($"[DEBUG] 开始自动生成元数据...");
                var metadataSuccess = _dynamicGenerator.CreateEntityAndSaveMetadata(table);
                if (metadataSuccess)
                {
                    Console.WriteLine($"[INFO] ✅ 已为表 '{table.TableName}' 自动生成元数据");
                    Console.WriteLine($"[INFO] 🎯 包含 {table.Fields.Count} 个字段的完整定义");
                }
                else
                {
                    Console.WriteLine($"[WARNING] ⚠️ 元数据生成失败，将使用纯数据驱动模式");
                }
            }
            else
            {
                Console.WriteLine($"[INFO] 📝 表 '{table.TableName}' 无字段定义，跳过元数据生成");
            }

            Console.WriteLine($"[DEBUG] 集合创建成功: {table.TableName}");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] 创建集合失败: {ex.Message}");
            throw new InvalidOperationException($"创建集合失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 更新集合结构（重新生成元数据）
    /// </summary>
    public async Task<bool> UpdateTableAsync(TableStructure table)
    {
        if (_engine == null)
            throw new InvalidOperationException("数据库未连接");

        try
        {
            Console.WriteLine($"[DEBUG] 更新表结构并重新生成元数据: {table.TableName}, 字段数: {table.Fields.Count}");

            // 使用DynamicEntityGenerator重新生成元数据
            // 这会更新现有的__metadata_集合中的元数据
            var generator = new DynamicEntityGenerator(_engine);
            var success = generator.CreateEntityAndSaveMetadata(table);

            if (success)
            {
                Console.WriteLine($"[DEBUG] 元数据更新成功: {table.TableName}");
            }
            else
            {
                Console.WriteLine($"[ERROR] 元数据更新失败: {table.TableName}");
            }

            return success;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] 更新表结构失败: {ex.Message}");
            throw new InvalidOperationException($"更新表结构失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 删除表结构
    /// </summary>
    public async Task<bool> DropTableAsync(string tableName)
    {
        if (_engine == null)
            throw new InvalidOperationException("数据库未连接");

        try
        {
            // 删除集合
            var result = await Task.Run(() => _engine.DropCollection(tableName));

            // 删除我们的表结构文档
            try
            {
                var metadataCollection = _engine.GetCollection<BsonDocument>("__table_structures");
                var structureDoc = metadataCollection.FindById((BsonValue)tableName);
                if (structureDoc != null)
                {
                    metadataCollection.Delete((BsonValue)tableName);
                }
            }
            catch
            {
                // 忽略删除表结构文档的错误
            }

            return result;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"删除表失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 从实体系统加载表结构
    /// </summary>
    private TableStructure? LoadTableStructureFromEntity(string tableName)
    {
        if (_engine == null) return null;

        try
        {
            Console.WriteLine($"[DEBUG] 从实体系统加载表结构: {tableName}");

            var metadataCollection = _engine.GetCollection<TableEntity>("__table_structures");
            var tableEntity = metadataCollection.FindById((BsonValue)tableName);

            if (tableEntity == null)
            {
                Console.WriteLine($"[DEBUG] 未找到表结构实体: {tableName}");
                return null;
            }

            Console.WriteLine($"[DEBUG] 找到表结构实体: {tableEntity.TableName}");
            Console.WriteLine($"[DEBUG] 字段JSON长度: {tableEntity.FieldsJson.Length}");

            var table = TableMetadataManager.FromTableEntity(tableEntity);
            Console.WriteLine($"[DEBUG] 转换后字段数量: {table.Fields.Count}");

            if (table.Fields.Count > 0)
            {
                var fieldNames = string.Join(", ", table.Fields.Select(f => f.FieldName));
                Console.WriteLine($"[DEBUG] 字段列表: {fieldNames}");
            }

            return table;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] 从实体系统加载表结构失败: {ex.Message}");
            Console.WriteLine($"[ERROR] 错误详情: {ex.StackTrace}");
            return null;
        }
    }

    
    /// <summary>
    /// 创建动态类型
    /// </summary>
    private static Type CreateDynamicType(TableStructure table)
    {
        // 这里简化处理，使用一个通用的动态类型
        return typeof(DynamicTableEntity);
    }

    /// <summary>
    /// 创建与表名匹配的类型名称
    /// </summary>
    private static string CreateEntityTypeName(string tableName)
    {
        // 确保类型名是有效的C#标识符
        var typeName = tableName.Replace(" ", "_").Replace("-", "_");
        if (char.IsDigit(typeName[0]))
        {
            typeName = "_" + typeName;
        }
        return typeName;
    }

    /// <summary>
    /// 为结构推断创建简单的示例实体
    /// </summary>
    private static DynamicEntity CreateSampleEntityForInference(TableStructure table)
    {
        try
        {
            var entity = new DynamicEntity();
            entity.Id = $"sample_{table.TableName}_{DateTime.UtcNow.Ticks}";

            // 只添加基本的字段，避免复杂的类型转换
            foreach (var field in table.Fields.Where(f => f.IsRequired || f.IsPrimaryKey))
            {
                if (field.FieldType == TableFieldType.String)
                {
                    entity.Set(field.FieldName, $"sample_{field.FieldName}");
                }
                else if (field.FieldType == TableFieldType.Integer)
                {
                    entity.Set(field.FieldName, 1);
                }
                else if (field.FieldType == TableFieldType.Boolean)
                {
                    entity.Set(field.FieldName, true);
                }
                else if (field.FieldType == TableFieldType.DateTime)
                {
                    entity.Set(field.FieldName, DateTime.UtcNow);
                }
                else if (field.FieldType == TableFieldType.Long)
                {
                    entity.Set(field.FieldName, 1L);
                }
                else if (field.FieldType == TableFieldType.Double)
                {
                    entity.Set(field.FieldName, 1.0);
                }
                else if (field.FieldType == TableFieldType.Decimal)
                {
                    entity.Set(field.FieldName, 1.0m);
                }
                else
                {
                    // 其他类型都用字符串表示
                    entity.Set(field.FieldName, $"sample_{field.FieldName}");
                }
            }

            // 添加示例标识
            entity.Set("_isSample", true);
            entity.Set("_sampleCreated", DateTime.UtcNow);
            entity.Set("_tableName", table.TableName);

            return entity;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] 创建示例实体失败: {ex.Message}");
            // 返回一个最简单的实体
            return new DynamicEntity
            {
                Id = $"sample_{DateTime.UtcNow.Ticks}",
                Data = new Dictionary<string, object?>
                {
                    { "_isSample", true },
                    { "_sampleCreated", DateTime.UtcNow }
                }
            };
        }
    }

    /// <summary>
    /// 创建示例文档
    /// </summary>
    private static BsonDocument CreateSampleDocument(TableStructure table)
    {
        var doc = new BsonDocument();

        foreach (var field in table.Fields.Where(f => !f.IsPrimaryKey))
        {
            var value = GetDefaultValueForFieldType(field.FieldType);
            var bsonValue = CreateBsonValue(value);
            doc = doc.Set(field.FieldName, bsonValue);
        }

        doc = doc.Set("_created", DateTime.UtcNow);
        doc = doc.Set("_isSample", true);

        return doc;
    }

    /// <summary>
    /// 获取字段类型的默认值
    /// </summary>
    private static object GetDefaultValueForFieldType(TableFieldType fieldType)
    {
        return fieldType switch
        {
            TableFieldType.String => "",
            TableFieldType.Integer => 0,
            TableFieldType.Long => 0L,
            TableFieldType.Double => 0.0,
            TableFieldType.Decimal => 0.0m,
            TableFieldType.Boolean => false,
            TableFieldType.DateTime => DateTime.UtcNow,
            TableFieldType.DateTimeOffset => DateTime.UtcNow,
            TableFieldType.Guid => Guid.NewGuid(),
            TableFieldType.Binary => Array.Empty<byte>(),
            TableFieldType.Json => "{}",
            TableFieldType.Array => new object[0],
            TableFieldType.Object => new { },
            TableFieldType.Reference => "",
            _ => ""
        };
    }

    /// <summary>
    /// 创建BsonValue
    /// </summary>
    private static BsonValue CreateBsonValue(object value)
    {
        return value switch
        {
            string s => (BsonValue)s,
            int i => (BsonValue)i,
            long l => (BsonValue)l,
            double d => (BsonValue)d,
            decimal dm => (BsonValue)dm,
            bool b => (BsonValue)b,
            DateTime dt => (BsonValue)dt,
            Guid g => new BsonString(g.ToString()),
            byte[] => new BsonString(""),
            _ => (BsonValue)(value.ToString() ?? "")
        };
    }
}

/// <summary>
/// 动态表实体类
/// </summary>
public class DynamicTableEntity
{
    public string? Id { get; set; }
    public string? Data { get; set; }
}