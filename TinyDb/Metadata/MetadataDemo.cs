using TinyDb.Core;

namespace TinyDb.Metadata;

/// <summary>
/// 元数据系统演示程序
/// 展示核心库如何提供元数据，UI层如何使用这些信息
/// </summary>
public static class MetadataDemo
{
    /// <summary>
    /// 运行元数据演示
    /// </summary>
    public static Task RunAsync()
    {
        Console.WriteLine("=== TinyDb 元数据系统演示 ===");
        Console.WriteLine();

        // 创建数据库引擎
        using var engine = new TinyDbEngine("metadata_demo.db");
        var metadataManager = new MetadataManager(engine);

        Console.WriteLine("1. 保存实体元数据到数据库");
        Console.WriteLine(new string('-', 50));

        // 保存示例实体元数据
        metadataManager.SaveEntityMetadata(typeof(UserEntity));
        metadataManager.SaveEntityMetadata(typeof(ProductEntity));

        Console.WriteLine("✅ 已保存 UserEntity 和 ProductEntity 的元数据");
        Console.WriteLine();

        Console.WriteLine("2. 查询已注册的实体类型");
        Console.WriteLine(new string('-', 50));

        var registeredTypes = metadataManager.GetRegisteredEntityTypes();
        foreach (var typeName in registeredTypes)
        {
            Console.WriteLine($"📋 {typeName}");
        }
        Console.WriteLine();

        Console.WriteLine("3. 核心库API演示 - 仅提供基础信息");
        Console.WriteLine(new string('-', 50));

        // 获取用户实体的基础信息
        var userDisplayName = metadataManager.GetEntityDisplayName(typeof(UserEntity));
        Console.WriteLine($"实体显示名称: {userDisplayName}");

        var userProperties = metadataManager.GetEntityProperties(typeof(UserEntity));
        Console.WriteLine("属性列表 (名称 + 类型):");
        foreach (var (name, type) in userProperties)
        {
            var required = metadataManager.IsPropertyRequired(typeof(UserEntity), name) ? " (必需)" : "";
            Console.WriteLine($"  • {name}: {type}{required}");
        }
        Console.WriteLine();

        Console.WriteLine("4. UI层使用示例 - 基于类型信息处理显示");
        Console.WriteLine(new string('-', 50));

        // 这部分展示UI层如何根据类型信息决定如何处理
        demonstrateUiLogic(metadataManager);
        Console.WriteLine();

        Console.WriteLine("5. 完整合并演示");
        Console.WriteLine(new string('-', 50));

        demonstrateCompleteWorkflow(metadataManager);

        Console.WriteLine();
        Console.WriteLine("✅ 元数据演示完成！");
        Console.WriteLine("🎯 核心库仅提供名称和类型，UI层负责显示逻辑");

        return Task.CompletedTask;
    }

    /// <summary>
    /// 演示UI层如何根据类型信息处理显示逻辑
    /// 这部分代码属于UI层，不在核心库中
    /// </summary>
    private static void demonstrateUiLogic(MetadataManager metadataManager)
    {
        Console.WriteLine("UI层逻辑演示 (这部分代码不在核心库中):");

        var userProperties = metadataManager.GetEntityProperties(typeof(UserEntity));

        foreach (var (propertyName, propertyType) in userProperties)
        {
            var displayName = metadataManager.GetPropertyDisplayName(typeof(UserEntity), propertyName);
            var required = metadataManager.IsPropertyRequired(typeof(UserEntity), propertyName);
            var order = metadataManager.GetPropertyOrder(typeof(UserEntity), propertyName);

            // UI层根据类型决定如何处理
            var uiControl = GetUiControlByType(propertyType);
            var validation = required ? "required" : "optional";

            Console.WriteLine($"  [{order:D2}] {displayName} ({propertyType})");
            Console.WriteLine($"       UI控件: {uiControl}, 验证: {validation}");
        }
    }

    /// <summary>
    /// UI层根据类型选择合适的控件
    /// 这是UI层的逻辑，不在核心库中
    /// </summary>
    private static string GetUiControlByType(string propertyType)
    {
        return propertyType switch
        {
            "System.String" => "TextBox",
            "System.Int32" => "NumberInput",
            "System.DateTime" => "DateTimePicker",
            "System.Boolean" => "CheckBox",
            "System.Decimal" => "DecimalInput",
            _ => "TextBox" // 默认
        };
    }

    /// <summary>
    /// 演示完整的工作流程
    /// </summary>
    private static void demonstrateCompleteWorkflow(MetadataManager metadataManager)
    {
        Console.WriteLine("完整工作流程演示:");

        // 1. 核心库：获取元数据
        var userMetadata = metadataManager.GetEntityMetadata(typeof(UserEntity));
        if (userMetadata != null)
        {
            Console.WriteLine($"📊 实体: {userMetadata.DisplayName}");
            Console.WriteLine($"📝 描述: {userMetadata.Description}");

            // 2. UI层：生成表单结构
            Console.WriteLine("🎨 UI层生成的表单结构:");
            foreach (var prop in userMetadata.Properties.OrderBy(p => p.Order))
            {
                var control = GetUiControlByType(prop.PropertyType);
                var requiredMark = prop.Required ? "*" : "";
                Console.WriteLine($"   {prop.DisplayName}{requiredMark}: {control}");
                if (!string.IsNullOrEmpty(prop.Description))
                {
                    Console.WriteLine($"   └─ {prop.Description}");
                }
            }
        }

        Console.WriteLine();
        Console.WriteLine("💡 架构优势:");
        Console.WriteLine("   • 核心库只关心数据存储和基础类型信息");
        Console.WriteLine("   • UI层完全独立，可以灵活定制显示逻辑");
        Console.WriteLine("   • 类型信息驱动UI生成，避免硬编码");
    }
}
