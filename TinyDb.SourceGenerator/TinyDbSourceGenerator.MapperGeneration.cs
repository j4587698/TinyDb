using System;
using System.Text;

namespace TinyDb.SourceGenerator;

public partial class TinyDbSourceGenerator
{

    /// <summary>
    /// ç”Ÿæˆ BSON å€¼è½¬æ¢çš„è¾…åŠ©æ–¹æ³•
    /// </summary>
    private static void AppendBsonConversionHelpers(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// è½¬æ¢ä¸º BSON å€¼");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <param name=\"value\">å€¼</param>");
        sb.AppendLine("        /// <returns>BSON å€¼</returns>");
        sb.AppendLine("        private static BsonValue ConvertToBsonValue(object? value)");
        sb.AppendLine("        {");
        sb.AppendLine("            return value == null");
        sb.AppendLine("                ? BsonNull.Value");
        sb.AppendLine("                : global::TinyDb.Serialization.BsonConversion.ConvertToBsonValue(value);");
        sb.AppendLine("        }");
        sb.AppendLine();
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// è¾…åŠ©æ–¹æ³•ï¼šå°†BSONå€¼è½¬æ¢ä¸ºç›®æ ‡ç±»åž‹");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        private static TGeneratedTinyDbValue ConvertFromBsonValue<TGeneratedTinyDbValue>(BsonValue value)");
        sb.AppendLine("        {");
        sb.AppendLine("            return global::TinyDb.Serialization.BsonConversion.FromBsonValue<TGeneratedTinyDbValue>(value)!;");
        sb.AppendLine("        }");
        sb.AppendLine();

        // å¦‚æžœæœ‰ä¾èµ–çš„å¤æ‚ç±»åž‹ï¼Œç”Ÿæˆä¸“ç”¨çš„å†…è”åºåˆ—åŒ–æ–¹æ³•
        if (classInfo.DependentComplexTypes.Count > 0)
        {
            // ç”Ÿæˆå¸¦ç±»åž‹æ£€æŸ¥çš„ SerializeComplexObject æ–¹æ³•
            GenerateSerializeComplexObjectWithInline(sb, classInfo);

            // ç”Ÿæˆå¸¦ç±»åž‹æ£€æŸ¥çš„ DeserializeComplexObject æ–¹æ³•
            GenerateDeserializeComplexObjectWithInline(sb, classInfo);

            // ä¸ºæ¯ä¸ªä¾èµ–ç±»åž‹ç”Ÿæˆä¸“ç”¨çš„åºåˆ—åŒ–/ååºåˆ—åŒ–æ–¹æ³•
            foreach (var depType in classInfo.DependentComplexTypes)
            {
                GenerateInlineSerializerForDependentType(sb, depType);
                GenerateInlineDeserializerForDependentType(sb, depType);
            }
        }
        else
        {
            // æ²¡æœ‰ä¾èµ–ç±»åž‹æ—¶ï¼Œä½¿ç”¨åŽŸæ¥çš„é€šç”¨æ–¹æ³•
            GenerateGenericSerializeComplexObject(sb);
            GenerateGenericDeserializeComplexObject(sb);
        }
    }

    /// <summary>
    /// ç”Ÿæˆå¸¦å†…è”æ–¹æ³•è°ƒç”¨çš„ SerializeComplexObject
    /// </summary>
    private static void GenerateSerializeComplexObjectWithInline(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// åºåˆ—åŒ–å¤æ‚å¯¹è±¡ä¸º BSON æ–‡æ¡£ï¼ˆAOTå…¼å®¹ï¼Œä½¿ç”¨å†…è”åºåˆ—åŒ–å™¨ï¼‰");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"T\">å¯¹è±¡ç±»åž‹</typeparam>");
        sb.AppendLine("        /// <param name=\"obj\">è¦åºåˆ—åŒ–çš„å¯¹è±¡</param>");
        sb.AppendLine("        /// <returns>BSON æ–‡æ¡£</returns>");
        sb.AppendLine("        private static BsonDocument SerializeComplexObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] TGeneratedTinyDbObject>(TGeneratedTinyDbObject obj)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (obj == null) return new BsonDocument();");
        sb.AppendLine();

        // ä¸ºæ¯ä¸ªä¾èµ–ç±»åž‹ç”Ÿæˆç±»åž‹æ£€æŸ¥
        foreach (var depType in classInfo.DependentComplexTypes)
        {
            sb.AppendLine($"            // æ£€æŸ¥æ˜¯å¦æ˜¯ {depType.ShortName}");
            sb.AppendLine($"            if (obj is {depType.FullyQualifiedName} typed_{depType.SafeMethodName})");
            sb.AppendLine("            {");
            sb.AppendLine($"                return Serialize_{depType.SafeMethodName}(typed_{depType.SafeMethodName});");
            sb.AppendLine("            }");
            sb.AppendLine();
        }

        sb.AppendLine("            // é€šè¿‡ AotBsonMapper.ToDocument æ¥åºåˆ—åŒ–ï¼Œä»¥æ”¯æŒå¾ªçŽ¯å¼•ç”¨æ£€æµ‹");
        sb.AppendLine("            return global::TinyDb.Serialization.AotBsonMapper.ToDocument(obj);");
        sb.AppendLine("        }");
        sb.AppendLine();
    }

    /// <summary>
    /// ç”Ÿæˆå¸¦å†…è”æ–¹æ³•è°ƒç”¨çš„ DeserializeComplexObject
    /// </summary>
    private static void GenerateDeserializeComplexObjectWithInline(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// ä»Ž BSON æ–‡æ¡£ååºåˆ—åŒ–å¤æ‚å¯¹è±¡ï¼ˆAOTå…¼å®¹ï¼Œä½¿ç”¨å†…è”ååºåˆ—åŒ–å™¨ï¼‰");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"T\">ç›®æ ‡ç±»åž‹</typeparam>");
        sb.AppendLine("        /// <param name=\"document\">BSON æ–‡æ¡£</param>");
        sb.AppendLine("        /// <returns>ååºåˆ—åŒ–åŽçš„å¯¹è±¡</returns>");
        sb.AppendLine("        private static TGeneratedTinyDbObject DeserializeComplexObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] TGeneratedTinyDbObject>(BsonDocument document)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (document == null) return default!;");
        sb.AppendLine();

        // ä¸ºæ¯ä¸ªä¾èµ–ç±»åž‹ç”Ÿæˆç±»åž‹æ£€æŸ¥
        foreach (var depType in classInfo.DependentComplexTypes)
        {
            sb.AppendLine($"            // æ£€æŸ¥æ˜¯å¦è¦ååºåˆ—åŒ–ä¸º {depType.ShortName}");
            sb.AppendLine($"            if (typeof(TGeneratedTinyDbObject) == typeof({depType.FullyQualifiedName}))");
            sb.AppendLine("            {");
            sb.AppendLine($"                return (TGeneratedTinyDbObject)(object)Deserialize_{depType.SafeMethodName}(document);");
            sb.AppendLine("            }");
            sb.AppendLine();
        }

        sb.AppendLine("            // é¦–å…ˆå°è¯•ä½¿ç”¨å·²æ³¨å†Œçš„ AOT é€‚é…å™¨");
        sb.AppendLine("            if (global::TinyDb.Serialization.AotHelperRegistry.TryGetAdapter<TGeneratedTinyDbObject>(out var adapter))");
        sb.AppendLine("            {");
        sb.AppendLine("                return adapter.FromDocument(document);");
        sb.AppendLine("            }");
        sb.AppendLine();
        sb.AppendLine("            // å›žé€€åˆ°é€šç”¨ååºåˆ—åŒ–ï¼ˆå¯èƒ½ä½¿ç”¨åå°„ï¼‰");
        sb.AppendLine("            return global::TinyDb.Serialization.AotBsonMapper.FromDocument<TGeneratedTinyDbObject>(document);");
        sb.AppendLine("        }");
        sb.AppendLine();
    }

    /// <summary>
    /// ç”Ÿæˆé€šç”¨çš„ SerializeComplexObject æ–¹æ³•ï¼ˆæ— å†…è”ï¼‰
    /// </summary>
    private static void GenerateGenericSerializeComplexObject(StringBuilder sb)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// åºåˆ—åŒ–å¤æ‚å¯¹è±¡ä¸º BSON æ–‡æ¡£ï¼ˆAOTå…¼å®¹ï¼‰");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"T\">å¯¹è±¡ç±»åž‹</typeparam>");
        sb.AppendLine("        /// <param name=\"obj\">è¦åºåˆ—åŒ–çš„å¯¹è±¡</param>");
        sb.AppendLine("        /// <returns>BSON æ–‡æ¡£</returns>");
        sb.AppendLine("        private static BsonDocument SerializeComplexObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] TGeneratedTinyDbObject>(TGeneratedTinyDbObject obj)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (obj == null) return new BsonDocument();");
        sb.AppendLine();
        sb.AppendLine("            // é€šè¿‡ AotBsonMapper.ToDocument æ¥åºåˆ—åŒ–ï¼Œä»¥æ”¯æŒå¾ªçŽ¯å¼•ç”¨æ£€æµ‹");
        sb.AppendLine("            return global::TinyDb.Serialization.AotBsonMapper.ToDocument(obj);");
        sb.AppendLine("        }");
        sb.AppendLine();
    }

    /// <summary>
    /// ç”Ÿæˆé€šç”¨çš„ DeserializeComplexObject æ–¹æ³•ï¼ˆæ— å†…è”ï¼‰
    /// </summary>
    private static void GenerateGenericDeserializeComplexObject(StringBuilder sb)
    {
        sb.AppendLine("        /// <summary>");
        sb.AppendLine("        /// ä»Ž BSON æ–‡æ¡£ååºåˆ—åŒ–å¤æ‚å¯¹è±¡ï¼ˆAOTå…¼å®¹ï¼‰");
        sb.AppendLine("        /// </summary>");
        sb.AppendLine("        /// <typeparam name=\"T\">ç›®æ ‡ç±»åž‹</typeparam>");
        sb.AppendLine("        /// <param name=\"document\">BSON æ–‡æ¡£</param>");
        sb.AppendLine("        /// <returns>ååºåˆ—åŒ–åŽçš„å¯¹è±¡</returns>");
        sb.AppendLine("        private static TGeneratedTinyDbObject DeserializeComplexObject<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors | DynamicallyAccessedMemberTypes.PublicMethods | DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicFields)] TGeneratedTinyDbObject>(BsonDocument document)");
        sb.AppendLine("        {");
        sb.AppendLine("            if (document == null) return default!;");
        sb.AppendLine();
        sb.AppendLine("            // é¦–å…ˆå°è¯•ä½¿ç”¨å·²æ³¨å†Œçš„ AOT é€‚é…å™¨");
        sb.AppendLine("            if (global::TinyDb.Serialization.AotHelperRegistry.TryGetAdapter<TGeneratedTinyDbObject>(out var adapter))");
        sb.AppendLine("            {");
        sb.AppendLine("                return adapter.FromDocument(document);");
        sb.AppendLine("            }");
        sb.AppendLine();
        sb.AppendLine("            // å›žé€€åˆ°é€šç”¨ååºåˆ—åŒ–ï¼ˆå¯èƒ½ä½¿ç”¨åå°„ï¼‰");
        sb.AppendLine("            return global::TinyDb.Serialization.AotBsonMapper.FromDocument<TGeneratedTinyDbObject>(document);");
        sb.AppendLine("        }");
        sb.AppendLine();
    }

}
