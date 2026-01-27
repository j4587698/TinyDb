using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text.Json;
using TinyDb.Core;
using TinyDb.Metadata;
using TinyDb.UI.Models;
using TinyDb.Attributes;

namespace TinyDb.UI.Services;

/// <summary>
/// 动态实体生成器 - 根据TableStructure动态创建实体类并保存元数据
/// </summary>
public class DynamicEntityGenerator
{
    private readonly TinyDbEngine _engine;
    private readonly MetadataManager _metadataManager;
    private readonly AssemblyBuilder _assemblyBuilder;
    private readonly ModuleBuilder _moduleBuilder;
    private int _typeCounter = 0;

    public DynamicEntityGenerator(TinyDbEngine engine)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _metadataManager = new MetadataManager(_engine);

        // 创建动态程序集
        var assemblyName = new AssemblyName($"DynamicEntities_{Guid.NewGuid():N}");
        _assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(
            assemblyName,
            AssemblyBuilderAccess.Run);
        _moduleBuilder = _assemblyBuilder.DefineDynamicModule("MainModule");
    }

    /// <summary>
    /// 根据TableStructure创建实体类型并保存元数据
    /// </summary>
    /// <param name="table">表结构</param>
    /// <returns>是否成功</returns>
    public bool CreateEntityAndSaveMetadata(TableStructure table)
    {
        try
        {
            Console.WriteLine($"[DEBUG] 开始为表 {table.TableName} 创建动态实体和元数据");

            // 1. 动态创建实体类型
            var entityType = CreateDynamicEntityType(table);

            // 2. 直接创建并保存元数据（无需实例化实体类型）
            var entityMetadata = CreateEntityMetadataFromTable(table);
            SaveMetadataDirectly(table.TableName, entityMetadata);

            Console.WriteLine($"[DEBUG] 成功为表 {table.TableName} 保存元数据");
            return true;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] 创建动态实体和元数据失败: {ex.Message}");
            Console.WriteLine($"[ERROR] 堆栈: {ex.StackTrace}");
            return false;
        }
    }

    /// <summary>
    /// 动态创建实体类型
    /// </summary>
    private Type CreateDynamicEntityType(TableStructure table)
    {
        var typeName = GetValidTypeName(table.TableName);

        var typeBuilder = _moduleBuilder.DefineType(
            typeName,
            TypeAttributes.Public | TypeAttributes.Class,
            typeof(object));

        // 添加Entity特性
        var entityAttributeConstructor = typeof(EntityAttribute).GetConstructor(new[] { typeof(string) });
        var entityAttributeBuilder = new CustomAttributeBuilder(
            entityAttributeConstructor!,
            new object[] { table.DisplayName });
        typeBuilder.SetCustomAttribute(entityAttributeBuilder);

        // 为每个字段创建属性
        foreach (var field in table.Fields.OrderBy(f => f.Order))
        {
            CreateProperty(typeBuilder, field);
        }

        return typeBuilder.CreateType()!;
    }

    /// <summary>
    /// 创建属性
    /// </summary>
    private void CreateProperty(TypeBuilder typeBuilder, TableField field)
    {
        var propertyName = GetValidPropertyName(field.FieldName);
        var propertyType = GetClrType(field.FieldType);

        // 定义字段
        var fieldBuilder = typeBuilder.DefineField(
            $"_{propertyName}",
            propertyType,
            FieldAttributes.Private);

        // 定义属性
        var propertyBuilder = typeBuilder.DefineProperty(
            propertyName,
            System.Reflection.PropertyAttributes.None,
            propertyType,
            null);

        // 创建getter方法
        var getterMethod = typeBuilder.DefineMethod(
            $"get_{propertyName}",
            System.Reflection.MethodAttributes.Public | System.Reflection.MethodAttributes.SpecialName | System.Reflection.MethodAttributes.HideBySig,
            propertyType,
            Type.EmptyTypes);

        var getterIL = getterMethod.GetILGenerator();
        getterIL.Emit(OpCodes.Ldarg_0);
        getterIL.Emit(OpCodes.Ldfld, fieldBuilder);
        getterIL.Emit(OpCodes.Ret);

        // 创建setter方法
        var setterMethod = typeBuilder.DefineMethod(
            $"set_{propertyName}",
            System.Reflection.MethodAttributes.Public | System.Reflection.MethodAttributes.SpecialName | System.Reflection.MethodAttributes.HideBySig,
            null,
            new[] { propertyType });

        var setterIL = setterMethod.GetILGenerator();
        setterIL.Emit(OpCodes.Ldarg_0);
        setterIL.Emit(OpCodes.Ldarg_1);
        setterIL.Emit(OpCodes.Stfld, fieldBuilder);
        setterIL.Emit(OpCodes.Ret);

        // 设置属性的getter和setter
        propertyBuilder.SetGetMethod(getterMethod);
        propertyBuilder.SetSetMethod(setterMethod);

        // 添加PropertyMetadata特性（如果不是Id字段）
        if (!field.IsPrimaryKey)
        {
            var metadataAttributeConstructor = typeof(PropertyMetadataAttribute).GetConstructor(
                new[] { typeof(string) });

            var metadataAttributeBuilder = new CustomAttributeBuilder(
                metadataAttributeConstructor!,
                new object[] { field.DisplayName ?? field.FieldName });

            propertyBuilder.SetCustomAttribute(metadataAttributeBuilder);
        }
        else
        {
            // 添加Id特性
            var idAttributeConstructor = typeof(IdAttribute).GetConstructor(Type.EmptyTypes);
            var idAttributeBuilder = new CustomAttributeBuilder(idAttributeConstructor!, Array.Empty<object>());
            propertyBuilder.SetCustomAttribute(idAttributeBuilder);
        }
    }

    /// <summary>
    /// 从TableStructure创建EntityMetadata
    /// </summary>
    private EntityMetadata CreateEntityMetadataFromTable(TableStructure table)
    {
        var metadata = new EntityMetadata
        {
            TypeName = GetValidTypeName(table.TableName),
            DisplayName = table.DisplayName,
            Description = table.Description
        };

        foreach (var field in table.Fields.OrderBy(f => f.Order))
        {
            var propertyMetadata = new PropertyMetadata
            {
                PropertyName = GetValidPropertyName(field.FieldName),
                PropertyType = GetClrType(field.FieldType).FullName ?? typeof(string).FullName!,
                DisplayName = field.DisplayName ?? field.FieldName,
                Description = field.Description,
                Order = field.Order,
                Required = field.IsRequired
            };

            metadata.Properties.Add(propertyMetadata);
        }

        return metadata;
    }

    /// <summary>
    /// 直接保存元数据到数据库（绕过动态类型实例化）
    /// </summary>
    private void SaveMetadataDirectly(string tableName, EntityMetadata metadata)
    {
        try
        {
            var collectionName = $"__metadata_{GetValidTypeName(tableName)}";
            var collection = _engine.GetCollection<MetadataDocument>(collectionName);

            // 转换为MetadataDocument并保存
            var metadataDoc = MetadataDocument.FromEntityMetadata(metadata);
            var existing = collection.FindOne(doc => doc.TypeName == metadata.TypeName);

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

            Console.WriteLine($"[DEBUG] 元数据已保存到集合: {collectionName}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] 保存元数据失败: {ex.Message}");
            throw;
        }
    }

    /// <summary>
    /// 获取有效的类型名称
    /// </summary>
    private string GetValidTypeName(string tableName)
    {
        // 清理表名，使其符合C#类型命名规范
        var typeName = tableName
            .Replace(" ", "_")
            .Replace("-", "_")
            .Replace(".", "_");

        // 确保以字母开头
        if (char.IsDigit(typeName[0]))
        {
            typeName = "Table_" + typeName;
        }

        return $"DynamicEntity_{typeName}_{_typeCounter++}";
    }

    /// <summary>
    /// 获取有效的属性名称
    /// </summary>
    private string GetValidPropertyName(string fieldName)
    {
        var propertyName = fieldName
            .Replace(" ", "_")
            .Replace("-", "_")
            .Replace(".", "_");

        if (char.IsDigit(propertyName[0]))
        {
            propertyName = "Field_" + propertyName;
        }

        return propertyName;
    }

    /// <summary>
    /// 将TableFieldType转换为CLR类型
    /// </summary>
    private Type GetClrType(TableFieldType fieldType)
    {
        return fieldType switch
        {
            TableFieldType.String => typeof(string),
            TableFieldType.Integer => typeof(int),
            TableFieldType.Long => typeof(long),
            TableFieldType.Double => typeof(double),
            TableFieldType.Decimal => typeof(decimal),
            TableFieldType.Boolean => typeof(bool),
            TableFieldType.DateTime => typeof(DateTime),
            TableFieldType.DateTimeOffset => typeof(DateTimeOffset),
            TableFieldType.Guid => typeof(Guid),
            _ => typeof(string)
        };
    }
}