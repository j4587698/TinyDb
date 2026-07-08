using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;
using System.Collections.Immutable;
using System.Text;

namespace TinyDb.SourceGenerator;

public partial class TinyDbSourceGenerator
{

    private static void GenerateForeignKeyReferences(StringBuilder sb, ClassInfo classInfo)
    {
        var references = classInfo.Properties
            .Where(p => !p.HasIgnoreAttribute && !string.IsNullOrEmpty(p.ForeignKeyCollectionName))
            .Select(p => new { ForeignKey = p, Target = ResolveForeignKeyTarget(classInfo, p) })
            .ToList();

        sb.AppendLine("        public static readonly IReadOnlyList<AotForeignKeyReference> ForeignKeyReferences = new AotForeignKeyReference[]");
        sb.AppendLine("        {");

        foreach (var reference in references)
        {
            var targetName = reference.Target?.Name is { Length: > 0 } name
                ? ToCSharpStringLiteral(name)
                : "null";
            var targetType = reference.Target != null
                ? $"typeof({reference.Target.FullyQualifiedNonNullableType})"
                : "null";

            sb.AppendLine(
                $"            new AotForeignKeyReference({ToCSharpStringLiteral(reference.ForeignKey.Name)}, {ToCSharpStringLiteral(reference.ForeignKey.ForeignKeyCollectionName!)}, {targetName}, {targetType}),");
        }

        sb.AppendLine("        };");
    }


    private static void AppendIdAdapterMetadata(StringBuilder sb, ClassInfo classInfo)
    {
        sb.AppendLine();

        if (classInfo.IdProperty == null)
        {
            sb.AppendLine("        public static string? IdPropertyName => null;");
            sb.AppendLine("        public static Type? IdPropertyType => null;");
            sb.AppendLine("        public static global::TinyDb.Attributes.IdGenerationStrategy IdGenerationStrategy => global::TinyDb.Attributes.IdGenerationStrategy.None;");
            sb.AppendLine("        public static string? IdGenerationSequenceName => null;");
            var noIdEntityParameter = classInfo.IsValueType
                ? $"ref {classInfo.FullyQualifiedTypeReference} entity"
                : $"{classInfo.FullyQualifiedTypeReference} entity";
            sb.AppendLine($"        public static bool GenerateIdIfNeeded({noIdEntityParameter}) => false;");
            return;
        }

        var idProperty = classInfo.IdProperty;
        var idType = idProperty.FullyQualifiedNonNullableType;
        var strategy = ToIdGenerationStrategyExpression(idProperty.IdGenerationStrategyValue);
        var sequenceName = idProperty.IdGenerationSequenceName == null
            ? "null"
            : ToCSharpStringLiteral(idProperty.IdGenerationSequenceName);

        sb.AppendLine($"        public static string? IdPropertyName => {ToCSharpStringLiteral(idProperty.Name)};");
        sb.AppendLine($"        public static Type? IdPropertyType => typeof({idType});");
        sb.AppendLine($"        public static global::TinyDb.Attributes.IdGenerationStrategy IdGenerationStrategy => {strategy};");
        sb.AppendLine($"        public static string? IdGenerationSequenceName => {sequenceName};");
        sb.AppendLine();
        var entityParameter = classInfo.IsValueType
            ? $"ref {classInfo.FullyQualifiedTypeReference} entity"
            : $"{classInfo.FullyQualifiedTypeReference} entity";
        sb.AppendLine($"        public static bool GenerateIdIfNeeded({entityParameter})");
        sb.AppendLine("        {");
        if (!classInfo.IsValueType)
        {
            sb.AppendLine("            if (entity == null) return false;");
        }

        if (!idProperty.CanSet)
        {
            sb.AppendLine("            return false;");
            sb.AppendLine("        }");
            return;
        }

        sb.AppendLine("            if (HasValidId(entity)) return false;");
        sb.AppendLine();
        sb.AppendLine("            if (IdGenerationStrategy != global::TinyDb.Attributes.IdGenerationStrategy.None)");
        sb.AppendLine("            {");
        AppendConfiguredIdGeneration(sb, classInfo, idProperty);
        sb.AppendLine("            }");
        sb.AppendLine();
        AppendDefaultIdGeneration(sb, classInfo, idProperty);
        sb.AppendLine("        }");
    }


    private static void AppendConfiguredIdGeneration(StringBuilder sb, ClassInfo classInfo, PropertyInfo idProperty)
    {
        var idType = idProperty.FullyQualifiedNonNullableType;
        var idAccess = idProperty.AccessName;
        var defaultSequenceName = ToCSharpStringLiteral($"{classInfo.Name}_{idProperty.Name}");
        var normalizedIdType = NormalizeTypeName(idProperty.NonNullableType);

        sb.AppendLine("                if (IdGenerationStrategy == global::TinyDb.Attributes.IdGenerationStrategy.IdentityInt ||");
        sb.AppendLine("                    IdGenerationStrategy == global::TinyDb.Attributes.IdGenerationStrategy.IdentityLong)");
        sb.AppendLine("                {");
        sb.AppendLine("                    return false;");
        sb.AppendLine("                }");

        sb.AppendLine($"                var generatedId = global::TinyDb.IdGeneration.AutoIdGenerator.CreateIdValue(typeof({idType}), IdGenerationStrategy, IdGenerationSequenceName, {defaultSequenceName});");
        sb.AppendLine("                if (generatedId == null) return false;");

        if (string.Equals(normalizedIdType, "int", StringComparison.OrdinalIgnoreCase))
        {
            sb.AppendLine($"                entity.{idAccess} = (int)generatedId;");
        }
        else if (string.Equals(normalizedIdType, "long", StringComparison.OrdinalIgnoreCase))
        {
            sb.AppendLine($"                entity.{idAccess} = (long)generatedId;");
        }
        else if (string.Equals(normalizedIdType, "Guid", StringComparison.Ordinal))
        {
            sb.AppendLine($"                entity.{idAccess} = (Guid)generatedId;");
        }
        else if (string.Equals(normalizedIdType, "string", StringComparison.OrdinalIgnoreCase))
        {
            sb.AppendLine($"                entity.{idAccess} = (string)generatedId;");
        }
        else if (IsTinyDbObjectIdTypeName(idProperty.FullyQualifiedNonNullableType))
        {
            sb.AppendLine($"                entity.{idAccess} = (ObjectId)generatedId;");
        }
        else
        {
            sb.AppendLine("                return false;");
            return;
        }

        sb.AppendLine("                return true;");
    }


    private static void AppendDefaultIdGeneration(StringBuilder sb, ClassInfo classInfo, PropertyInfo idProperty)
    {
        var normalizedIdType = NormalizeTypeName(idProperty.NonNullableType);
        var idAccess = idProperty.AccessName;

        if (string.Equals(normalizedIdType, "int", StringComparison.OrdinalIgnoreCase))
        {
            sb.AppendLine("            return false;");
        }
        else if (string.Equals(normalizedIdType, "long", StringComparison.OrdinalIgnoreCase))
        {
            sb.AppendLine("            return false;");
        }
        else if (string.Equals(normalizedIdType, "Guid", StringComparison.Ordinal))
        {
            sb.AppendLine($"            entity.{idAccess} = global::TinyDb.IdGeneration.AutoIdGenerator.CreateGuidV7();");
            sb.AppendLine("            return true;");
        }
        else if (string.Equals(normalizedIdType, "string", StringComparison.OrdinalIgnoreCase))
        {
            sb.AppendLine($"            entity.{idAccess} = global::TinyDb.IdGeneration.AutoIdGenerator.CreateGuidV7().ToString();");
            sb.AppendLine("            return true;");
        }
        else if (IsTinyDbObjectIdTypeName(idProperty.FullyQualifiedNonNullableType))
        {
            sb.AppendLine($"            entity.{idAccess} = ObjectId.NewObjectId();");
            sb.AppendLine("            return true;");
        }
        else
        {
            sb.AppendLine("            return false;");
        }
    }


    private static string ToIdGenerationStrategyExpression(int value)
    {
        return value switch
        {
            1 => "global::TinyDb.Attributes.IdGenerationStrategy.ObjectId",
            2 => "global::TinyDb.Attributes.IdGenerationStrategy.IdentityInt",
            3 => "global::TinyDb.Attributes.IdGenerationStrategy.IdentityLong",
            4 => "global::TinyDb.Attributes.IdGenerationStrategy.GuidV7",
            5 => "global::TinyDb.Attributes.IdGenerationStrategy.GuidV4",
            _ => "global::TinyDb.Attributes.IdGenerationStrategy.None"
        };
    }


    private static PropertyInfo? ResolveForeignKeyTarget(ClassInfo classInfo, PropertyInfo foreignKeyProperty)
    {
        if (CanStoreReference(foreignKeyProperty))
        {
            return foreignKeyProperty;
        }

        if (foreignKeyProperty.Name.EndsWith("Id", StringComparison.Ordinal) &&
            foreignKeyProperty.Name.Length > 2)
        {
            var navigationName = foreignKeyProperty.Name.Substring(0, foreignKeyProperty.Name.Length - 2);
            return classInfo.Properties.FirstOrDefault(p =>
                !p.HasIgnoreAttribute &&
                p.Name == navigationName &&
                CanStoreReference(p));
        }

        return null;
    }


    private static bool CanStoreReference(PropertyInfo property)
    {
        var normalized = NormalizeTypeName(property.FullyQualifiedNonNullableType);
        return normalized is "object" or "System.Object" ||
               normalized.EndsWith("BsonDocument", StringComparison.Ordinal) ||
               normalized.EndsWith("BsonValue", StringComparison.Ordinal) ||
               !IsSimpleReferenceValueType(property);
    }


    private static bool IsSimpleReferenceValueType(PropertyInfo property)
    {
        if (property.IsEnum) return true;

        var normalized = NormalizeTypeName(property.NonNullableType);
        return property.IsValueType ||
               normalized is "string" or "decimal" or "double" or "float" or "bool" or "byte" or "sbyte" or
                   "short" or "ushort" or "int" or "uint" or "long" or "ulong" or "char" or "DateTime" or
                   "DateTimeOffset" or "TimeSpan" or "Guid" ||
               IsTinyDbObjectIdTypeName(property.FullyQualifiedNonNullableType);
    }

}
