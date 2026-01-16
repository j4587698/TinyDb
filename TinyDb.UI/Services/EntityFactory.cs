using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using TinyDb.Attributes;

namespace TinyDb.UI.Services;

/// <summary>
/// 动态实体类型工厂 - 为NoSQL集合创建对应的实体类型
/// 实现用户要求的：每个集合对应具体实体类型的架构
/// </summary>
public class EntityFactory
{
    private static readonly Dictionary<string, Type> _entityTypes = new();
    private static readonly AssemblyBuilder _assemblyBuilder;
    private static readonly ModuleBuilder _moduleBuilder;
    private static int _typeCounter = 0;

    static EntityFactory()
    {
        var assemblyName = new AssemblyName($"DynamicEntityAssembly_{Guid.NewGuid():N}");
        _assemblyBuilder = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
        _moduleBuilder = _assemblyBuilder.DefineDynamicModule("MainModule");
    }

    /// <summary>
    /// 根据集合名称获取或创建对应的实体类型
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <returns>对应的实体类型</returns>
    public static Type GetOrCreateEntityType(string collectionName)
    {
        if (string.IsNullOrWhiteSpace(collectionName))
            throw new ArgumentException("集合名称不能为空", nameof(collectionName));

        // 标准化集合名称，确保符合C#类型命名规范
        var typeName = NormalizeCollectionName(collectionName);

        // 如果已存在该类型，直接返回
        if (_entityTypes.TryGetValue(collectionName, out var existingType))
            return existingType;

        // 创建新的实体类型
        var newType = CreateEntityType(typeName, collectionName);
        _entityTypes[collectionName] = newType;

        Console.WriteLine($"[EntityFactory] 为集合 '{collectionName}' 创建实体类型: {newType.Name}");

        return newType;
    }

    /// <summary>
    /// 创建实体类型
    /// </summary>
    /// <param name="typeName">类型名称</param>
    /// <param name="collectionName">集合名称</param>
    /// <returns>创建的实体类型</returns>
    private static Type CreateEntityType(string typeName, string collectionName)
    {
        var typeBuilder = _moduleBuilder.DefineType(
            typeName,
            TypeAttributes.Public | TypeAttributes.Class,
            typeof(object));

        // 添加Entity特性
        var entityAttributeConstructor = typeof(EntityAttribute).GetConstructor(Type.EmptyTypes);
        var entityAttributeBuilder = new CustomAttributeBuilder(entityAttributeConstructor!, Array.Empty<object>());
        typeBuilder.SetCustomAttribute(entityAttributeBuilder);

        // 添加Id属性
        CreateIdProperty(typeBuilder);

        // 添加动态数据字典属性
        CreateDataProperty(typeBuilder);

        // 添加索引器
        CreateIndexerProperty(typeBuilder);

        // 添加Set方法
        CreateSetMethod(typeBuilder);

        // 添加Get方法
        CreateGetMethod(typeBuilder);

        // 添加Contains方法
        CreateContainsMethod(typeBuilder);

        // 添加GetFieldNames方法
        CreateGetFieldNamesMethod(typeBuilder);

        return typeBuilder.CreateType()!;
    }

    /// <summary>
    /// 创建Id属性
    /// </summary>
    /// <param name="typeBuilder">类型构建器</param>
    private static void CreateIdProperty(TypeBuilder typeBuilder)
    {
        var idField = typeBuilder.DefineField("_id", typeof(string), FieldAttributes.Private);

        var idProperty = typeBuilder.DefineProperty(
            "Id",
            PropertyAttributes.HasDefault,
            typeof(string),
            null);

        // 添加Id特性
        var idAttributeConstructor = typeof(IdAttribute).GetConstructor(Type.EmptyTypes);
        var idAttributeBuilder = new CustomAttributeBuilder(idAttributeConstructor!, Array.Empty<object>());
        idProperty.SetCustomAttribute(idAttributeBuilder);

        // 创建getter方法
        var getterMethod = typeBuilder.DefineMethod(
            "get_Id",
            MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
            typeof(string),
            Type.EmptyTypes);

        var getterIL = getterMethod.GetILGenerator();
        getterIL.Emit(OpCodes.Ldarg_0);
        getterIL.Emit(OpCodes.Ldfld, idField);
        getterIL.Emit(OpCodes.Ret);

        // 创建setter方法
        var setterMethod = typeBuilder.DefineMethod(
            "set_Id",
            MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
            typeof(void),
            new[] { typeof(string) });

        var setterIL = setterMethod.GetILGenerator();
        setterIL.Emit(OpCodes.Ldarg_0);
        setterIL.Emit(OpCodes.Ldarg_1);
        setterIL.Emit(OpCodes.Stfld, idField);
        setterIL.Emit(OpCodes.Ret);

        // 关联getter和setter到属性
        idProperty.SetGetMethod(getterMethod);
        idProperty.SetSetMethod(setterMethod);
    }

    /// <summary>
    /// 创建Data属性
    /// </summary>
    /// <param name="typeBuilder">类型构建器</param>
    private static void CreateDataProperty(TypeBuilder typeBuilder)
    {
        var dataField = typeBuilder.DefineField(
            "_data",
            typeof(Dictionary<string, object?>),
            FieldAttributes.Private);

        var dataProperty = typeBuilder.DefineProperty(
            "Data",
            PropertyAttributes.None,
            typeof(Dictionary<string, object?>),
            null);

        // 创建getter方法
        var getterMethod = typeBuilder.DefineMethod(
            "get_Data",
            MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
            typeof(Dictionary<string, object?>),
            Type.EmptyTypes);

        var getterIL = getterMethod.GetILGenerator();
        getterIL.Emit(OpCodes.Ldarg_0);
        getterIL.Emit(OpCodes.Ldfld, dataField);
        getterIL.Emit(OpCodes.Ret);

        // 创建setter方法
        var setterMethod = typeBuilder.DefineMethod(
            "set_Data",
            MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
            typeof(void),
            new[] { typeof(Dictionary<string, object?>) });

        var setterIL = setterMethod.GetILGenerator();
        setterIL.Emit(OpCodes.Ldarg_0);
        setterIL.Emit(OpCodes.Ldarg_1);

        // 如果value为null，创建新字典
        var notNullLabel = getterIL.DefineLabel();
        setterIL.Emit(OpCodes.Brtrue_S, notNullLabel);
        setterIL.Emit(OpCodes.Ldarg_0);
        setterIL.Emit(OpCodes.Newobj, typeof(Dictionary<string, object?>).GetConstructor(Type.EmptyTypes)!);
        setterIL.Emit(OpCodes.Stfld, dataField);
        setterIL.Emit(OpCodes.Ret);

        setterIL.MarkLabel(notNullLabel);
        setterIL.Emit(OpCodes.Stfld, dataField);
        setterIL.Emit(OpCodes.Ret);

        // 关联getter和setter到属性
        dataProperty.SetGetMethod(getterMethod);
        dataProperty.SetSetMethod(setterMethod);

        // 在构造函数中初始化Data字段
        CreateConstructor(typeBuilder, dataField);
    }

    /// <summary>
    /// 创建构造函数
    /// </summary>
    /// <param name="typeBuilder">类型构建器</param>
    /// <param name="dataField">数据字段</param>
    private static void CreateConstructor(TypeBuilder typeBuilder, FieldBuilder dataField)
    {
        var constructor = typeBuilder.DefineConstructor(
            MethodAttributes.Public,
            CallingConventions.Standard,
            Type.EmptyTypes);

        var constructorIL = constructor.GetILGenerator();
        constructorIL.Emit(OpCodes.Ldarg_0);
        constructorIL.Emit(OpCodes.Call, typeof(object).GetConstructor(Type.EmptyTypes)!);
        constructorIL.Emit(OpCodes.Ldarg_0);
        constructorIL.Emit(OpCodes.Newobj, typeof(Dictionary<string, object?>).GetConstructor(Type.EmptyTypes)!);
        constructorIL.Emit(OpCodes.Stfld, dataField);
        constructorIL.Emit(OpCodes.Ret);
    }

    /// <summary>
    /// 创建索引器属性
    /// </summary>
    /// <param name="typeBuilder">类型构建器</param>
    private static void CreateIndexerProperty(TypeBuilder typeBuilder)
    {
        var indexerProperty = typeBuilder.DefineProperty(
            "Item",
            PropertyAttributes.None,
            typeof(object),
            new[] { typeof(string) });

        // 创建getter方法
        var getterMethod = typeBuilder.DefineMethod(
            "get_Item",
            MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
            typeof(object),
            new[] { typeof(string) });

        var getterIL = getterMethod.GetILGenerator();
        getterIL.Emit(OpCodes.Ldarg_0);
        getterIL.Emit(OpCodes.Ldfld, typeBuilder.GetField("_data", BindingFlags.NonPublic | BindingFlags.Instance)!);
        getterIL.Emit(OpCodes.Ldarg_1);

        // 调用Dictionary.TryGetValue
        var tryGetValueMethod = typeof(Dictionary<string, object?>).GetMethod(
            "TryGetValue",
            BindingFlags.Public | BindingFlags.Instance)!;
        getterIL.DeclareLocal(typeof(bool));
        getterIL.Emit(OpCodes.Callvirt, tryGetValueMethod);

        var valueLabel = getterIL.DefineLabel();
        getterIL.Emit(OpCodes.Brtrue_S, valueLabel);
        getterIL.Emit(OpCodes.Ldnull);
        getterIL.Emit(OpCodes.Ret);

        getterIL.MarkLabel(valueLabel);
        getterIL.Emit(OpCodes.Ldloc_0);
        getterIL.Emit(OpCodes.Ret);

        // 创建setter方法
        var setterMethod = typeBuilder.DefineMethod(
            "set_Item",
            MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig,
            typeof(void),
            new[] { typeof(string), typeof(object) });

        var setterIL = setterMethod.GetILGenerator();
        setterIL.Emit(OpCodes.Ldarg_0);
        setterIL.Emit(OpCodes.Ldfld, typeBuilder.GetField("_data", BindingFlags.NonPublic | BindingFlags.Instance)!);
        setterIL.Emit(OpCodes.Ldarg_1);
        setterIL.Emit(OpCodes.Ldarg_2);

        var setItemMethod = typeof(Dictionary<string, object?>).GetMethods()!
            .First(m => m.Name == "set_Item" && m.GetParameters().Length == 2);
        setterIL.Emit(OpCodes.Callvirt, setItemMethod);
        setterIL.Emit(OpCodes.Ret);

        // 关联getter和setter到属性
        indexerProperty.SetGetMethod(getterMethod);
        indexerProperty.SetSetMethod(setterMethod);
    }

    /// <summary>
    /// 创建Set方法
    /// </summary>
    /// <param name="typeBuilder">类型构建器</param>
    private static void CreateSetMethod(TypeBuilder typeBuilder)
    {
        var method = typeBuilder.DefineMethod(
            "Set",
            MethodAttributes.Public,
            typeof(object), // 返回自身以支持链式调用
            new[] { typeof(string), typeof(object) });

        var il = method.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, typeBuilder.GetField("_data", BindingFlags.NonPublic | BindingFlags.Instance)!);
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Ldarg_2);

        var setItemMethod = typeof(Dictionary<string, object?>).GetMethods()!
            .First(m => m.Name == "set_Item" && m.GetParameters().Length == 2);
        il.Emit(OpCodes.Callvirt, setItemMethod);
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ret);
    }

    /// <summary>
    /// 创建Get方法
    /// </summary>
    /// <param name="typeBuilder">类型构建器</param>
    private static void CreateGetMethod(TypeBuilder typeBuilder)
    {
        var method = typeBuilder.DefineMethod(
            "Get",
            MethodAttributes.Public,
            typeof(object),
            new[] { typeof(string) });

        var il = method.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, typeBuilder.GetField("_data", BindingFlags.NonPublic | BindingFlags.Instance)!);
        il.Emit(OpCodes.Ldarg_1);

        var tryGetValueMethod = typeof(Dictionary<string, object?>).GetMethod(
            "TryGetValue",
            BindingFlags.Public | BindingFlags.Instance)!;
        il.DeclareLocal(typeof(object));
        il.DeclareLocal(typeof(bool));
        il.Emit(OpCodes.Callvirt, tryGetValueMethod);

        var hasValueLabel = il.DefineLabel();
        il.Emit(OpCodes.Brtrue_S, hasValueLabel);
        il.Emit(OpCodes.Ldnull);
        il.Emit(OpCodes.Stloc_0);
        il.Emit(OpCodes.Br_S, il.DefineLabel());

        il.MarkLabel(hasValueLabel);
        il.Emit(OpCodes.Ldloc_1);

        var endLabel = il.DefineLabel();
        il.Emit(OpCodes.Br_S, endLabel);
        il.MarkLabel(endLabel);
        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ret);
    }

    /// <summary>
    /// 创建Contains方法
    /// </summary>
    /// <param name="typeBuilder">类型构建器</param>
    private static void CreateContainsMethod(TypeBuilder typeBuilder)
    {
        var method = typeBuilder.DefineMethod(
            "Contains",
            MethodAttributes.Public,
            typeof(bool),
            new[] { typeof(string) });

        var il = method.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, typeBuilder.GetField("_data", BindingFlags.NonPublic | BindingFlags.Instance)!);
        il.Emit(OpCodes.Ldarg_1);

        var containsKeyMethod = typeof(Dictionary<string, object?>).GetMethod(
            "ContainsKey",
            BindingFlags.Public | BindingFlags.Instance)!;
        il.Emit(OpCodes.Callvirt, containsKeyMethod);
        il.Emit(OpCodes.Ret);
    }

    /// <summary>
    /// 创建GetFieldNames方法
    /// </summary>
    /// <param name="typeBuilder">类型构建器</param>
    private static void CreateGetFieldNamesMethod(TypeBuilder typeBuilder)
    {
        var method = typeBuilder.DefineMethod(
            "GetFieldNames",
            MethodAttributes.Public,
            typeof(IEnumerable<string>),
            Type.EmptyTypes);

        var il = method.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, typeBuilder.GetField("_data", BindingFlags.NonPublic | BindingFlags.Instance)!);

        var keysProperty = typeof(Dictionary<string, object?>).GetProperty(
            "Keys",
            BindingFlags.Public | BindingFlags.Instance)!;
        il.Emit(OpCodes.Callvirt, keysProperty.GetMethod!);
        il.Emit(OpCodes.Ret);
    }

    /// <summary>
    /// 标准化集合名称为有效的C#类型名称
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <returns>标准化的类型名称</returns>
    private static string NormalizeCollectionName(string collectionName)
    {
        // 移除无效字符并确保以字母开头
        var typeName = collectionName.Trim();

        // 如果以数字开头，添加下划线
        if (typeName.Length > 0 && char.IsDigit(typeName[0]))
        {
            typeName = "_" + typeName;
        }

        // 替换无效字符
        var validChars = new List<char>();
        foreach (var c in typeName)
        {
            if (char.IsLetterOrDigit(c) || c == '_')
            {
                validChars.Add(c);
            }
            else
            {
                validChars.Add('_');
            }
        }

        var result = new string(validChars.ToArray());

        // 确保不为空
        if (string.IsNullOrEmpty(result))
        {
            result = $"DynamicEntity_{++_typeCounter}";
        }

        // 添加唯一后缀以避免冲突
        result += $"_{++_typeCounter:D3}";

        return result;
    }

    /// <summary>
    /// 获取所有已创建的实体类型
    /// </summary>
    /// <returns>集合名称到实体类型的映射</returns>
    public static Dictionary<string, Type> GetAllEntityTypes()
    {
        return new Dictionary<string, Type>(_entityTypes);
    }

    /// <summary>
    /// 检查集合是否已有对应的实体类型
    /// </summary>
    /// <param name="collectionName">集合名称</param>
    /// <returns>是否已存在实体类型</returns>
    public static bool HasEntityType(string collectionName)
    {
        return _entityTypes.ContainsKey(collectionName);
    }

    /// <summary>
    /// 清除所有缓存的实体类型（主要用于测试）
    /// </summary>
    public static void ClearCache()
    {
        _entityTypes.Clear();
        _typeCounter = 0;
    }
}