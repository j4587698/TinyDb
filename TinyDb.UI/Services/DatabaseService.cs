using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.UI.Models;
using System.Text.Json;

namespace TinyDb.UI.Services;

/// <summary>
/// 临时文档类，用于创建集合
/// </summary>
public class TempDocument
{
    public string? Id { get; set; }
    public string? Name { get; set; }
    public string? Description { get; set; }
    public DateTime Created { get; set; }
    public bool IsTemporary { get; set; }
}

/// <summary>
/// 简单文档类，用于通用集合访问
/// </summary>
public class SimpleDocument
{
    public string? Id { get; set; }
    public string? Data { get; set; }
}

/// <summary>
/// TinyDb数据库服务
/// </summary>
public class DatabaseService : IDisposable
{
    private TinyDbEngine? _engine;
    private bool _isConnected;

    public bool IsConnected => _isConnected && _engine != null;
    public TinyDbEngine? Engine => _engine;

    // 静态属性用于调试信息
    public static string? LastDebugInfo { get; set; }

    /// <summary>
    /// 生成WAL文件路径，使用默认格式
    /// </summary>
    private static string GenerateDefaultWalPath(string dbPath)
    {
        var directory = System.IO.Path.GetDirectoryName(dbPath) ?? string.Empty;
        var fileNameWithoutExt = System.IO.Path.GetFileNameWithoutExtension(dbPath);
        var extension = System.IO.Path.GetExtension(dbPath).TrimStart('.');
        return System.IO.Path.Combine(directory, $"{fileNameWithoutExt}-wal.{extension}");
    }

    /// <summary>
    /// 连接到数据库文件
    /// </summary>
    public async Task<bool> ConnectAsync(string filePath, string? password = null)
    {
        try
        {
            var options = new TinyDbOptions
            {
                Password = password,
                EnableJournaling = false,
                ReadOnly = false
            };

            // 检查文件是否存在，如果不存在则创建新数据库
            bool fileExists = System.IO.File.Exists(filePath);

            if (!fileExists)
            {
                // 确保目录存在
                var directory = System.IO.Path.GetDirectoryName(filePath);
                if (!string.IsNullOrEmpty(directory) && !System.IO.Directory.Exists(directory))
                {
                    System.IO.Directory.CreateDirectory(directory);
                }
            }

            _engine = new TinyDbEngine(filePath, options);
            _isConnected = true;

            return true;
        }
        catch (Exception ex)
        {
            // 如果是文件损坏错误，尝试备份并重新创建
            if (ex.Message.Contains("checksum") || ex.Message.Contains("corrupt"))
            {
                try
                {
                    // 备份损坏的文件
                    string backupPath = filePath + ".corrupt." + DateTime.Now.ToString("yyyyMMddHHmmss");
                    if (System.IO.File.Exists(filePath))
                    {
                        System.IO.File.Move(filePath, backupPath);
                    }

                    // 重新创建数据库
                    var options = new TinyDbOptions
                    {
                        Password = password,
                        EnableJournaling = false,
                        ReadOnly = false
                    };

                    _engine = new TinyDbEngine(filePath, options);
                    _isConnected = true;

                    return true;
                }
                catch (Exception backupEx)
                {
                    throw new InvalidOperationException($"数据库文件损坏，备份失败: {backupEx.Message}", backupEx);
                }
            }

            throw new InvalidOperationException($"连接数据库失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 断开数据库连接
    /// </summary>
    public void Disconnect()
    {
        if (_engine != null)
        {
            _engine.Dispose();
            _engine = null;
        }
        _isConnected = false;
    }

    /// <summary>
    /// 清理损坏的数据库文件并重新创建
    /// </summary>
    public async Task<bool> RecreateDatabaseAsync(string filePath, string? password = null)
    {
        try
        {
            // 断开现有连接
            Disconnect();

            // 备份现有文件
            if (System.IO.File.Exists(filePath))
            {
                string backupPath = filePath + ".backup." + DateTime.Now.ToString("yyyyMMddHHmmss");
                System.IO.File.Move(filePath, backupPath);
            }

            // 删除可能的WAL文件（支持新旧格式）
            string oldWalPath = filePath + ".wal";
            string newWalPath = GenerateDefaultWalPath(filePath);
            if (System.IO.File.Exists(oldWalPath))
            {
                System.IO.File.Delete(oldWalPath);
            }
            if (System.IO.File.Exists(newWalPath))
            {
                System.IO.File.Delete(newWalPath);
            }

            // 确保目录存在
            var directory = System.IO.Path.GetDirectoryName(filePath);
            if (!string.IsNullOrEmpty(directory) && !System.IO.Directory.Exists(directory))
            {
                System.IO.Directory.CreateDirectory(directory);
            }

            // 重新创建数据库
            var options = new TinyDbOptions
            {
                Password = password,
                EnableJournaling = false,
                ReadOnly = false
            };

            _engine = new TinyDbEngine(filePath, options);
            _isConnected = true;

            return true;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"重新创建数据库失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 获取数据库信息
    /// </summary>
    public DatabaseInfo GetDatabaseInfo()
    {
        if (!IsConnected || _engine == null)
            throw new InvalidOperationException("数据库未连接");

        var stats = _engine.GetStatistics();
        return new DatabaseInfo
        {
            FilePath = stats.FilePath,
            DatabaseName = stats.DatabaseName,
            Version = $"v{stats.Version:X8}",
            CreatedAt = stats.CreatedAt,
            ModifiedAt = stats.ModifiedAt,
            FileSize = stats.FileSize,
            CollectionCount = stats.CollectionCount,
            IsReadOnly = stats.IsReadOnly,
            EnableJournaling = stats.EnableJournaling,
            TotalPages = stats.TotalPages,
            UsedPages = stats.UsedPages,
            CacheHitRatio = stats.CacheHitRatio,
            CachedPages = stats.CachedPages
        };
    }

    /// <summary>
    /// 获取所有集合信息
    /// </summary>
    public List<CollectionInfo> GetCollections()
    {
        if (!IsConnected || _engine == null)
            throw new InvalidOperationException("数据库未连接");

        var collections = new List<CollectionInfo>();
        var collectionNames = _engine.GetCollectionNames();

        // 过滤系统集合，只显示用户表
        var userCollectionNames = collectionNames.Where(name => !name.StartsWith("__")).ToList();
        var debugInfo = $"引擎返回集合总数: {collectionNames.Count()}, 用户表数量: {userCollectionNames.Count()}. ";
        foreach (var name in collectionNames)
        {
            debugInfo += $"集合: {name}, 系统集合: {name.StartsWith("__")}. ";
        }

        // 如果引擎返回0个集合但数据库有数据，尝试恢复
        var stats = _engine.GetStatistics();
        if (collectionNames.Count() == 0 && stats.UsedPages > 0)
        {
            debugInfo += $"数据库有{stats.UsedPages}页数据但无集合，尝试恢复... ";

            // 尝试创建一个测试集合来检查数据完整性
            try
            {
                var testCollection = _engine.GetCollection<TempDocument>("__recovery_test__");
                var testDoc = new TempDocument { Name = "test", Created = DateTime.UtcNow, IsTemporary = true };
                var testId = testCollection.Insert(testDoc);
                testCollection.Delete(testId);
                _engine.DropCollection("__recovery_test__");
                debugInfo += "数据完整性正常，可能是集合元数据丢失。 ";
            }
            catch (Exception ex)
            {
                debugInfo += $"数据完整性检查失败: {ex.Message}。 ";
            }
        }

        // 将调试信息存储在静态变量中，供UI显示
        LastDebugInfo = debugInfo;

        foreach (var name in userCollectionNames) // 只处理用户集合
        {
            try
            {
                // 尝试多种方式获取集合信息
                long count = 0;
                long cachedCount = 0;

                try
                {
                    // 首先尝试使用TempDocument类型
                    var tempCollection = _engine.GetCollection<TempDocument>(name);
                    count = tempCollection.Count();
                    cachedCount = _engine.GetCachedDocumentCount(name);
                }
                catch
                {
                    try
                    {
                        // 如果失败，尝试使用一个通用的文档类型
                        var genericCollection = _engine.GetCollection<SimpleDocument>(name);
                        count = genericCollection.Count();
                        cachedCount = _engine.GetCachedDocumentCount(name);
                    }
                    catch
                    {
                        // 最后尝试直接获取集合统计信息
                        try
                        {
                            // 这里我们至少知道集合存在，设置为0计数
                            count = 0;
                            cachedCount = 0;
                        }
                        catch
                        {
                            continue; // 跳过这个集合
                        }
                    }
                }

                collections.Add(new CollectionInfo
                {
                    Name = name,
                    DocumentType = name.StartsWith("__") ? "系统集合" : "用户集合",
                    DocumentCount = count,
                    CachedDocumentCount = cachedCount,
                    Indexes = new List<string>()
                });
            }
            catch (Exception ex)
            {
                // 记录错误但继续处理其他集合
                System.Diagnostics.Debug.WriteLine($"无法访问集合 {name}: {ex.Message}");
            }
        }

        return collections;
    }

    /// <summary>
    /// 获取集合中的所有文档
    /// </summary>
    public async Task<List<DocumentItem>> GetDocumentsAsync(string collectionName)
    {
        if (!IsConnected || _engine == null)
            throw new InvalidOperationException("数据库未连接");

        try
        {
            Console.WriteLine($"[DEBUG] GetDocumentsAsync called for collection: {collectionName}");

            // 检查集合是否存在
            if (!_engine.CollectionExists(collectionName))
            {
                Console.WriteLine($"[DEBUG] Collection {collectionName} does not exist");
                return new List<DocumentItem>();
            }

            var documents = new List<DocumentItem>();

            await Task.Run(() =>
            {
                try
                {
                    Console.WriteLine($"[DEBUG] Trying to load documents from {collectionName}");

                    // 首先尝试使用类型化实体（新的架构）
                    try
                    {
                        var entityType = EntityFactory.GetOrCreateEntityType(collectionName);
                        Console.WriteLine($"[DEBUG] Using entity type: {entityType.Name} for collection: {collectionName}");

                        var getCollectionMethod = _engine.GetType()
                            .GetMethods()
                            .FirstOrDefault(m => m.Name == "GetCollection" && m.IsGenericMethod);

                        if (getCollectionMethod != null)
                        {
                            var genericMethod = getCollectionMethod.MakeGenericMethod(entityType);
                            dynamic collection = genericMethod.Invoke(_engine, new object[] { collectionName })!;
                            Console.WriteLine($"[DEBUG] Got typed collection, document count: {collection.Count()}");

                            foreach (dynamic entity in collection.FindAll())
                            {
                                if (entity != null)
                                {
                                    Console.WriteLine($"[DEBUG] Found typed entity: ID={entity.Id}");
                                    documents.Add(ConvertTypedEntityToDocumentItem(entity, collectionName, entityType));
                                }
                            }
                            Console.WriteLine($"[DEBUG] Added {documents.Count} documents from typed collection");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[DEBUG] Typed collection failed: {ex.Message}");

                        // 回退到BsonDocument类型（兼容性）
                        try
                        {
                            var collection = _engine.GetCollection<BsonDocument>(collectionName);
                            Console.WriteLine($"[DEBUG] Got BsonDocument collection, document count: {collection.Count()}");

                            foreach (var doc in collection.FindAll())
                            {
                                if (doc != null)
                                {
                                    Console.WriteLine($"[DEBUG] Found BsonDocument: {doc}");
                                    documents.Add(ConvertToDocumentItem(doc, collectionName));
                                }
                            }
                            Console.WriteLine($"[DEBUG] Added {documents.Count} documents from BsonDocument collection");
                        }
                        catch (Exception ex2)
                        {
                            Console.WriteLine($"[DEBUG] BsonDocument collection failed: {ex2.Message}");

                            // 最后尝试TempDocument类型（兼容旧数据）
                            try
                            {
                                var tempCollection = _engine.GetCollection<TempDocument>(collectionName);
                                Console.WriteLine($"[DEBUG] Got TempDocument collection, document count: {tempCollection.Count()}");

                                foreach (var doc in tempCollection.FindAll())
                                {
                                    if (doc != null)
                                    {
                                        Console.WriteLine($"[DEBUG] Found TempDocument: ID={doc.Id}, Name={doc.Name}");
                                        documents.Add(ConvertTempDocumentToItem(doc, collectionName));
                                    }
                                }
                                Console.WriteLine($"[DEBUG] Added {documents.Count} documents from TempDocument collection");
                            }
                            catch (InvalidCastException)
                            {
                                Console.WriteLine($"[DEBUG] TempDocument conversion failed");

                                // 最后尝试使用SimpleDocument类型
                                try
                                {
                                    var simpleCollection = _engine.GetCollection<SimpleDocument>(collectionName);
                                    Console.WriteLine($"[DEBUG] Got SimpleDocument collection, document count: {simpleCollection.Count()}");

                                    foreach (var doc in simpleCollection.FindAll())
                                    {
                                        if (doc != null)
                                        {
                                            Console.WriteLine($"[DEBUG] Found SimpleDocument: {doc}");
                                            documents.Add(ConvertSimpleDocumentToItem(doc, collectionName));
                                        }
                                    }
                                    Console.WriteLine($"[DEBUG] Added {documents.Count} documents from SimpleDocument collection");
                                }
                                catch (Exception ex3)
                                {
                                    Console.WriteLine($"[DEBUG] SimpleDocument also failed: {ex3.Message}");
                                    throw new InvalidOperationException($"无法读取集合 {collectionName} 中的文档，可能存在不兼容的文档类型: {ex3.Message}");
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[DEBUG] Exception in GetDocumentsAsync: {ex}");
                    throw;
                }
            });

            Console.WriteLine($"[DEBUG] Returning {documents.Count} documents total");
            return documents;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[DEBUG] Outer exception in GetDocumentsAsync: {ex}");
            throw new InvalidOperationException($"获取文档失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 根据ID查找文档
    /// </summary>
    public DocumentItem? GetDocumentById(string collectionName, string id)
    {
        if (!IsConnected || _engine == null)
            throw new InvalidOperationException("数据库未连接");

        try
        {
            var collection = _engine.GetCollection<BsonDocument>(collectionName);
            var bsonId = (BsonValue)id;
            var doc = collection.FindById(bsonId);

            return doc != null ? ConvertToDocumentItem(doc, collectionName) : null;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"查找文档失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 插入文档
    /// </summary>
    public async Task<string> InsertDocumentAsync(string collectionName, string jsonContent)
    {
        Console.WriteLine($"[DEBUG] InsertDocumentAsync called for collection: {collectionName}");
        if (!IsConnected || _engine == null)
            throw new InvalidOperationException("数据库未连接");

        try
        {
            Console.WriteLine($"[DEBUG] Getting collection: {collectionName}");

            // 获取或创建集合对应的实体类型
            var entityType = EntityFactory.GetOrCreateEntityType(collectionName);
            Console.WriteLine($"[DEBUG] Using entity type: {entityType.Name} for collection: {collectionName}");

            // 使用反射获取类型化集合
            var getCollectionMethod = _engine.GetType()
                .GetMethods()
                .FirstOrDefault(m => m.Name == "GetCollection" && m.IsGenericMethod);

            if (getCollectionMethod == null)
            {
                throw new InvalidOperationException("无法找到GetCollection方法");
            }

            var genericMethod = getCollectionMethod.MakeGenericMethod(entityType);
            dynamic collection = genericMethod.Invoke(_engine, new object[] { collectionName })!;
            Console.WriteLine($"[DEBUG] Type-safe collection obtained successfully");

            // 解析JSON
            Console.WriteLine($"[DEBUG] Parsing JSON content");
            using var jsonDoc = JsonDocument.Parse(jsonContent);
            Console.WriteLine($"[DEBUG] JSON parsed successfully");

            // 创建类型化实体并填充数据
            Console.WriteLine($"[DEBUG] Creating typed entity...");
            dynamic entity = Activator.CreateInstance(entityType)!;

            // 使用反射设置Id属性（如果JSON中有id）
            var idProperty = entityType.GetProperty("Id");
            if (idProperty != null && jsonDoc.RootElement.TryGetProperty("id", out var idElement))
            {
                idProperty.SetValue(entity, idElement.GetString() ?? Guid.NewGuid().ToString());
            }
            else if (idProperty != null)
            {
                idProperty.SetValue(entity, Guid.NewGuid().ToString());
            }

            // 使用反射访问Data字典并填充JSON数据
            var dataProperty = entityType.GetProperty("Data");
            if (dataProperty != null && dataProperty.PropertyType == typeof(Dictionary<string, object?>))
            {
                var data = (Dictionary<string, object?>)dataProperty.GetValue(entity)!;

                // 将JSON属性复制到Data字典中
                foreach (var property in jsonDoc.RootElement.EnumerateObject())
                {
                    if (property.Name.ToLower() != "id") // Id已经单独处理
                    {
                        data[property.Name] = ConvertJsonElementToObject(property.Value);
                    }
                }

                Console.WriteLine($"[DEBUG] Entity data populated with {data.Count} fields");
            }

            Console.WriteLine($"[DEBUG] Inserting typed entity into collection");
            var result = await Task.Run(() => collection.Insert(entity));
            Console.WriteLine($"[DEBUG] Document inserted with ID: {result}");

            return result.ToString();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"插入文档失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 更新文档
    /// </summary>
    public async Task<bool> UpdateDocumentAsync(string collectionName, string id, string jsonContent)
    {
        if (!IsConnected || _engine == null)
            throw new InvalidOperationException("数据库未连接");

        try
        {
            var collection = _engine.GetCollection<BsonDocument>(collectionName);

            // 解析JSON为BsonDocument
            using var jsonDoc = JsonDocument.Parse(jsonContent);
            var bsonDoc = ConvertJsonToBson(jsonDoc.RootElement);

            // 确保文档有正确的ID
            bsonDoc = bsonDoc.Set("_id", (BsonValue)id);

            var result = await Task.Run(() => collection.Update(bsonDoc));
            return result > 0;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"更新文档失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 删除文档
    /// </summary>
    public async Task<bool> DeleteDocumentAsync(string collectionName, string id)
    {
        if (!IsConnected || _engine == null)
            throw new InvalidOperationException("数据库未连接");

        try
        {
            var collection = _engine.GetCollection<BsonDocument>(collectionName);
            var bsonId = (BsonValue)id;
            var result = await Task.Run(() => collection.Delete(bsonId));
            return result > 0;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"删除文档失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 创建集合 - 使用类型化实体架构
    /// 实现用户要求的：每个集合对应具体实体类型
    /// </summary>
    public async Task<bool> CreateCollectionAsync(string collectionName)
    {
        if (!IsConnected || _engine == null)
            throw new InvalidOperationException("数据库未连接");

        try
        {
            Console.WriteLine($"[DEBUG] 创建集合: {collectionName}");

            // 使用EntityFactory为集合名称创建对应的实体类型
            var entityType = EntityFactory.GetOrCreateEntityType(collectionName);
            Console.WriteLine($"[EntityFactory] 为集合 '{collectionName}' 使用实体类型: {entityType.Name}");

            // 使用反射获取集合，类型安全
            var getCollectionMethod = _engine.GetType()
                .GetMethods()
                .FirstOrDefault(m => m.Name == "GetCollection" && m.IsGenericMethod);

            if (getCollectionMethod == null)
            {
                throw new InvalidOperationException("无法找到GetCollection方法");
            }

            var genericMethod = getCollectionMethod.MakeGenericMethod(entityType);
            dynamic collection = genericMethod.Invoke(_engine, new object[] { collectionName })!;

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
            }

            // 插入临时实体
            var id = await Task.Run(() => collection.Insert(tempEntity));
            Console.WriteLine($"[DEBUG] 类型化临时实体插入成功，ID: {id}, 类型: {entityType.Name}");

            // 立即删除临时实体，但集合会保留
            await Task.Run(() => collection.Delete(id));
            Console.WriteLine($"[DEBUG] 类型化临时实体已删除，集合保留");

            // 验证集合是否真的被创建了
            var collectionExists = _engine.CollectionExists(collectionName);
            Console.WriteLine($"[DEBUG] 集合创建验证: {collectionName} - {collectionExists}, 实体类型: {entityType.Name}");

            return collectionExists;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] 创建集合失败: {ex.Message}");
            throw new InvalidOperationException($"创建集合失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 删除集合
    /// </summary>
    public async Task<bool> DropCollectionAsync(string collectionName)
    {
        if (!IsConnected || _engine == null)
            throw new InvalidOperationException("数据库未连接");

        try
        {
            var result = await Task.Run(() => _engine.DropCollection(collectionName));
            return result;
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException($"删除集合失败: {ex.Message}", ex);
        }
    }

    /// <summary>
    /// 执行查询
    /// </summary>
    public async Task<QueryResult> ExecuteQueryAsync(string collectionName, string queryJson)
    {
        var result = new QueryResult { Query = queryJson };
        var startTime = DateTime.UtcNow;

        try
        {
            if (!IsConnected || _engine == null)
                throw new InvalidOperationException("数据库未连接");

            var collection = _engine.GetCollection<BsonDocument>(collectionName);

            // 简单实现：获取所有文档然后进行过滤
            // 实际项目中可以实现更复杂的查询解析
            var allDocs = await Task.Run(() => collection.FindAll().ToList());

            if (!string.IsNullOrWhiteSpace(queryJson))
            {
                // 这里可以实现JSON查询过滤逻辑
                // 暂时返回所有文档
                result.Documents = allDocs.Select(doc => ConvertToDocumentItem(doc, collectionName)).ToList();
            }
            else
            {
                result.Documents = allDocs.Select(doc => ConvertToDocumentItem(doc, collectionName)).ToList();
            }

            result.TotalCount = result.Documents.Count;
            result.ExecutionTime = DateTime.UtcNow - startTime;
        }
        catch (Exception ex)
        {
            result.ErrorMessage = ex.Message;
            result.ExecutionTime = DateTime.UtcNow - startTime;
        }

        return result;
    }

    private static DocumentItem ConvertToDocumentItem(BsonDocument doc, string collectionName)
    {
        var id = doc.TryGetValue("_id", out var idValue) ? idValue.ToString() : string.Empty;
        var content = BsonSerializer.SerializeDocument(doc);

        return new DocumentItem
        {
            Id = id,
            Type = "BsonDocument",
            Content = System.Text.Encoding.UTF8.GetString(content),
            CollectionName = collectionName,
            CreatedAt = DateTime.UtcNow, // 可以从文档中获取创建时间
            ModifiedAt = DateTime.UtcNow, // 可以从文档中获取修改时间
            Size = content.Length
        };
    }

    /// <summary>
    /// 将类型化动态实体转换为DocumentItem
    /// </summary>
    private static DocumentItem ConvertTypedEntityToDocumentItem(dynamic entity, string collectionName, Type entityType)
    {
        try
        {
            // 使用反射获取实体的属性
            var idProperty = entityType.GetProperty("Id");
            var dataProperty = entityType.GetProperty("Data");

            var entityId = idProperty?.GetValue(entity)?.ToString() ?? Guid.NewGuid().ToString();
            var entityData = dataProperty?.GetValue(entity) as Dictionary<string, object?> ?? new Dictionary<string, object?>();

            // 构建包含所有数据的字典
            var fullData = new Dictionary<string, object?>(entityData);
            fullData["id"] = entityId;
            fullData["_entity_type"] = entityType.Name;

            // 序列化为JSON
            var content = System.Text.Json.JsonSerializer.Serialize(fullData, new System.Text.Json.JsonSerializerOptions
            {
                WriteIndented = true,
                PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase
            });

            var documentItem = new DocumentItem
            {
                Id = entityId,
                Type = entityType.Name,
                Content = content,
                CollectionName = collectionName,
                CreatedAt = DateTime.UtcNow,
                ModifiedAt = DateTime.UtcNow,
                Size = System.Text.Encoding.UTF8.GetByteCount(content)
            };

            // 添加调试日志
            Console.WriteLine($"[DEBUG] ConvertTypedEntityToDocumentItem:");
            Console.WriteLine($"[DEBUG]   - Entity Type: {entityType.Name}");
            Console.WriteLine($"[DEBUG]   - Entity ID: {entityId}");
            Console.WriteLine($"[DEBUG]   - Data Fields: {entityData.Count}");
            Console.WriteLine($"[DEBUG]   - DocumentItem Size: {documentItem.Size}");

            return documentItem;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] ConvertTypedEntityToDocumentItem failed: {ex.Message}");

            // 回退到简单的序列化
            var fallbackContent = System.Text.Json.JsonSerializer.Serialize(entity);
            return new DocumentItem
            {
                Id = Guid.NewGuid().ToString(),
                Type = entityType.Name,
                Content = fallbackContent,
                CollectionName = collectionName,
                CreatedAt = DateTime.UtcNow,
                ModifiedAt = DateTime.UtcNow,
                Size = System.Text.Encoding.UTF8.GetByteCount(fallbackContent)
            };
        }
    }

    private static DocumentItem ConvertTempDocumentToItem(TempDocument doc, string collectionName)
    {
        var content = System.Text.Json.JsonSerializer.Serialize(doc);
        var documentItem = new DocumentItem
        {
            Id = doc.Id ?? Guid.NewGuid().ToString(), // 使用TempDocument的实际ID，如果没有则生成新ID
            Type = "TempDocument",
            Content = content,
            CollectionName = collectionName,
            CreatedAt = DateTime.UtcNow,
            ModifiedAt = DateTime.UtcNow,
            Size = System.Text.Encoding.UTF8.GetByteCount(content)
        };

        // 添加调试日志
        Console.WriteLine($"[DEBUG] ConvertTempDocumentToItem:");
        Console.WriteLine($"[DEBUG]   - TempDocument ID: {doc.Id}");
        Console.WriteLine($"[DEBUG]   - TempDocument Name: {doc.Name}");
        Console.WriteLine($"[DEBUG]   - TempDocument Created: {doc.Created}");
        Console.WriteLine($"[DEBUG]   - TempDocument IsTemporary: {doc.IsTemporary}");
        Console.WriteLine($"[DEBUG]   - DocumentItem ID: {documentItem.Id}");
        Console.WriteLine($"[DEBUG]   - DocumentItem Content: {content}");
        Console.WriteLine($"[DEBUG]   - DocumentItem Size: {documentItem.Size}");

        return documentItem;
    }

    private static DocumentItem ConvertSimpleDocumentToItem(SimpleDocument doc, string collectionName)
    {
        var content = System.Text.Json.JsonSerializer.Serialize(doc);
        return new DocumentItem
        {
            Id = doc.Id ?? Guid.NewGuid().ToString(),
            Type = "SimpleDocument",
            Content = content,
            CollectionName = collectionName,
            CreatedAt = DateTime.UtcNow,
            ModifiedAt = DateTime.UtcNow,
            Size = System.Text.Encoding.UTF8.GetByteCount(content)
        };
    }

    private static BsonDocument ConvertJsonToBson(JsonElement jsonElement)
    {
        var doc = new BsonDocument();

        if (jsonElement.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in jsonElement.EnumerateObject())
            {
                doc = doc.Set(property.Name, ConvertJsonElementToBsonValue(property.Value));
            }
        }

        return doc;
    }

    private static BsonValue ConvertJsonElementToBsonValue(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => new BsonString(element.GetString()!),
            JsonValueKind.Number => element.TryGetInt64(out var longVal)
                ? new BsonInt64(longVal)
                : new BsonDouble(element.GetDouble()),
            JsonValueKind.True => new BsonBoolean(true),
            JsonValueKind.False => new BsonBoolean(false),
            JsonValueKind.Null => BsonNull.Value,
            JsonValueKind.Object => ConvertJsonToBson(element),
            JsonValueKind.Array => ConvertJsonArrayToBsonArray(element),
            _ => new BsonString(element.GetRawText())
        };
    }

    private static BsonArray ConvertJsonArrayToBsonArray(JsonElement arrayElement)
    {
        BsonArray array = new BsonArray();
        foreach (var item in arrayElement.EnumerateArray())
        {
            array = array.AddValue(ConvertJsonElementToBsonValue(item));
        }
        return array;
    }

    /// <summary>
    /// 将JsonElement转换为object，用于动态实体Data字典
    /// </summary>
    private static object? ConvertJsonElementToObject(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var longVal)
                ? (object)longVal
                : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            JsonValueKind.Object => ConvertJsonElementToDictionary(element),
            JsonValueKind.Array => ConvertJsonElementToList(element),
            _ => element.GetRawText()
        };
    }

    /// <summary>
    /// 将JsonElement对象转换为Dictionary
    /// </summary>
    private static Dictionary<string, object?> ConvertJsonElementToDictionary(JsonElement element)
    {
        var dict = new Dictionary<string, object?>();
        foreach (var property in element.EnumerateObject())
        {
            dict[property.Name] = ConvertJsonElementToObject(property.Value);
        }
        return dict;
    }

    /// <summary>
    /// 将JsonElement数组转换为List
    /// </summary>
    private static List<object?> ConvertJsonElementToList(JsonElement element)
    {
        var list = new List<object?>();
        foreach (var item in element.EnumerateArray())
        {
            list.Add(ConvertJsonElementToObject(item));
        }
        return list;
    }

    private static TempDocument ConvertBsonToTempDocument(BsonDocument bsonDoc)
    {
        var tempDoc = new TempDocument();

        if (bsonDoc.Contains("_id"))
        {
            tempDoc.Id = bsonDoc["_id"].ToString();
        }

        if (bsonDoc.Contains("name"))
        {
            tempDoc.Name = bsonDoc["name"].ToString();
        }

        if (bsonDoc.Contains("description"))
        {
            tempDoc.Description = bsonDoc["description"].ToString();
        }

        if (bsonDoc.Contains("created_at"))
        {
            if (DateTime.TryParse(bsonDoc["created_at"].ToString(), out var createdDate))
                tempDoc.Created = createdDate;
        }

        if (bsonDoc.Contains("is_temporary"))
        {
            tempDoc.IsTemporary = bsonDoc["is_temporary"].ToBoolean();
        }

        return tempDoc;
    }

    public void Dispose()
    {
        Disconnect();
    }
}