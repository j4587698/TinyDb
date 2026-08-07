using System;
using System.Text;

namespace TinyDb.SourceGenerator;

public partial class TinyDbSourceGenerator
{

    /// <summary>
    /// 生成 BSON 值转换的辅助方法
    /// </summary>
    private static void AppendBsonConversionHelpers(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 转换为 BSON 值");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"value\">值</param>");
        sb.AppendLine("        /// <returns>BSON 值</returns>");
        sb.AppendLine("        private static BsonValue ConvertToBsonValue(object? value)");
        sb.AppendLine("        {");
        sb.AppendLine("            return value == null");
        sb.AppendLine("                ? BsonNull.Value");
        sb.AppendLine("                : global::TinyDb.Serialization.BsonConversion.ConvertToBsonValue(value);");
        sb.AppendLine("        }");
        sb.AppendLine();
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 辅助方法：将BSON值转换为目标类型");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        private static TGeneratedTinyDbValue ConvertFromBsonValue<TGeneratedTinyDbValue>(BsonValue value)");
        sb.AppendLine("        {");
        sb.AppendLine("            return global::TinyDb.Serialization.BsonConversion.FromBsonValue<TGeneratedTinyDbValue>(value)!;");
        sb.AppendLine("        }");
        sb.AppendLine();

        // 如果有依赖的复杂类型，生成专用的内联序列化方法
        if (classInfo.DependentComplexTypes.Count > 0)
        {
            // 生成带类型检查的 SerializeComplexObject 方法
            GenerateSerializeComplexObjectWithInline(sb, classInfo);

            // 生成带类型检查的 DeserializeComplexObject 方法
            GenerateDeserializeComplexObjectWithInline(sb, classInfo);

            // 为每个依赖类型生成专用的序列化/反序列化方法
            foreach (var depType in classInfo.DependentComplexTypes)
            {
                GenerateInlineSerializerForDependentType(sb, depType);
                GenerateInlineDeserializerForDependentType(sb, depType);
            }
        }
        else
        {
            // 没有依赖类型时，使用原来的通用方法
            GenerateGenericSerializeComplexObject(sb);
            GenerateGenericDeserializeComplexObject(sb);
        }
    }

    /// <summary>
    /// 生成带内联方法调用的 SerializeComplexObject
    /// </summary>
    private static void GenerateSerializeComplexObjectWithInline(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 序列化复杂对象为 BSON 文档（AOT兼容，使用内联序列化器）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"TGeneratedTinyDbObject\">对象类型</typeparam>");
        sb.AppendLine("        /// <param name=\"obj\">要序列化的对象</param>");
        sb.AppendLine("        /// <returns>BSON 文档</returns>");
        sb.AppendLine("        private static BsonDocument SerializeComplexObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] TGeneratedTinyDbObject>(TGeneratedTinyDbObject obj)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (obj == null) return new BsonDocument();");
        sb.AppendLine();

        // 为每个依赖类型生成类型检查
        foreach (var depType in classInfo.DependentComplexTypes)
        {
            sb.AppendLine($"            // 检查是否是 {depType.ShortName}");
            sb.AppendLine($"            if (obj is {depType.FullyQualifiedName} typed_{depType.SafeMethodName})");
            sb.AppendLine("            {");
            sb.AppendLine($"                return Serialize_{depType.SafeMethodName}(typed_{depType.SafeMethodName});");
            sb.AppendLine("            }");
            sb.AppendLine();
        }

        sb.AppendLine("            // 通过 AotBsonMapper.ToDocument 来序列化，以支持循环引用检测");
        sb.AppendLine("            return global::TinyDb.Serialization.AotBsonMapper.ToDocument(obj);");
        sb.AppendLine("        }");
        sb.AppendLine();
    }

    /// <summary>
    /// 生成带内联方法调用的 DeserializeComplexObject
    /// </summary>
    private static void GenerateDeserializeComplexObjectWithInline(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 从 BSON 文档反序列化复杂对象（AOT兼容，使用内联反序列化器）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"TGeneratedTinyDbObject\">目标类型</typeparam>");
        sb.AppendLine("        /// <param name=\"document\">BSON 文档</param>");
        sb.AppendLine("        /// <returns>反序列化后的对象</returns>");
        sb.AppendLine("        private static TGeneratedTinyDbObject DeserializeComplexObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] TGeneratedTinyDbObject>(BsonDocument document)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (document == null) return default!;");
        sb.AppendLine();

        // 为每个依赖类型生成类型检查
        foreach (var depType in classInfo.DependentComplexTypes)
        {
            sb.AppendLine($"            // 检查是否要反序列化为 {depType.ShortName}");
            sb.AppendLine($"            if (typeof(TGeneratedTinyDbObject) == typeof({depType.FullyQualifiedName}))");
            sb.AppendLine("            {");
            sb.AppendLine($"                return (TGeneratedTinyDbObject)(object)Deserialize_{depType.SafeMethodName}(document);");
            sb.AppendLine("            }");
            sb.AppendLine();
        }

        sb.AppendLine("            // 首先尝试使用已注册的 AOT 适配器");
        sb.AppendLine("            if (global::TinyDb.Serialization.AotHelperRegistry.TryGetAdapter<TGeneratedTinyDbObject>(out var adapter))");
        sb.AppendLine("            {");
        sb.AppendLine("                return adapter.FromDocument(document);");
        sb.AppendLine("            }");
        sb.AppendLine();
        sb.AppendLine("            // 回退到通用反序列化（可能使用反射）");
        sb.AppendLine("            return global::TinyDb.Serialization.AotBsonMapper.FromDocument<TGeneratedTinyDbObject>(document);");
        sb.AppendLine("        }");
        sb.AppendLine();
    }

    /// <summary>
    /// 生成通用的 SerializeComplexObject 方法（无内联）
    /// </summary>
    private static void GenerateGenericSerializeComplexObject(StringBuilder sb)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 序列化复杂对象为 BSON 文档（AOT兼容）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"TGeneratedTinyDbObject\">对象类型</typeparam>");
        sb.AppendLine("        /// <param name=\"obj\">要序列化的对象</param>");
        sb.AppendLine("        /// <returns>BSON 文档</returns>");
        sb.AppendLine("        private static BsonDocument SerializeComplexObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] TGeneratedTinyDbObject>(TGeneratedTinyDbObject obj)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (obj == null) return new BsonDocument();");
        sb.AppendLine();
        sb.AppendLine("            // 通过 AotBsonMapper.ToDocument 来序列化，以支持循环引用检测");
        sb.AppendLine("            return global::TinyDb.Serialization.AotBsonMapper.ToDocument(obj);");
        sb.AppendLine("        }");
        sb.AppendLine();
    }

    /// <summary>
    /// 生成通用的 DeserializeComplexObject 方法（无内联）
    /// </summary>
    private static void GenerateGenericDeserializeComplexObject(StringBuilder sb)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// 从 BSON 文档反序列化复杂对象（AOT兼容）");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"TGeneratedTinyDbObject\">目标类型</typeparam>");
        sb.AppendLine("        /// <param name=\"document\">BSON 文档</param>");
        sb.AppendLine("        /// <returns>反序列化后的对象</returns>");
        sb.AppendLine("        private static TGeneratedTinyDbObject DeserializeComplexObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] TGeneratedTinyDbObject>(BsonDocument document)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (document == null) return default!;");
        sb.AppendLine();
        sb.AppendLine("            // 首先尝试使用已注册的 AOT 适配器");
        sb.AppendLine("            if (global::TinyDb.Serialization.AotHelperRegistry.TryGetAdapter<TGeneratedTinyDbObject>(out var adapter))");
        sb.AppendLine("            {");
        sb.AppendLine("                return adapter.FromDocument(document);");
        sb.AppendLine("            }");
        sb.AppendLine();
        sb.AppendLine("            // 回退到通用反序列化（可能使用反射）");
        sb.AppendLine("            return global::TinyDb.Serialization.AotBsonMapper.FromDocument<TGeneratedTinyDbObject>(document);");
        sb.AppendLine("        }");
        sb.AppendLine();
    }

}
