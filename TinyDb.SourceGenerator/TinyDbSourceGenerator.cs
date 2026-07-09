using System;
using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace TinyDb.SourceGenerator;

/// <summary>
/// TinyDb AOT 源代码生成器
/// </summary>
[Generator(LanguageNames.CSharp)]
public partial class TinyDbSourceGenerator : IIncrementalGenerator
{
    private const string EntityAttributeMetadataName = "TinyDb.Attributes.EntityAttribute";

    /// <summary>
    /// BsonRef 引用类型缺少 Entity 特性的错误诊断描述符
    /// </summary>
    private static readonly DiagnosticDescriptor BsonRefMissingEntityErrorDescriptor = new(
        id: "TINYDB001",
        title: "BsonRef referenced type must have [Entity] attribute",
        messageFormat: "Property '{0}' in type '{1}' uses [BsonRef] but the referenced type '{2}' does not have [Entity] attribute. Add [Entity] attribute to '{2}' or remove [BsonRef] from this property.",
        category: "TinyDb.SourceGenerator",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "Types referenced by [BsonRef] must have [Entity] attribute to ensure AOT compatibility. The Entity attribute causes the source generator to create AOT-compatible serialization code for the type.");

    /// <summary>
    /// 循环引用警告的诊断描述符（非Entity类型）
    /// </summary>
    private static readonly DiagnosticDescriptor CircularReferenceWarningDescriptor = new(
        id: "TINYDB002",
        title: "Circular reference detected in non-Entity types",
        messageFormat: "Circular reference detected in type '{0}': {1}. The circular reference will be handled by breaking the cycle, but this may cause incomplete serialization at runtime.",
        category: "TinyDb.SourceGenerator",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "A circular reference was detected among non-Entity complex types. The source generator will break the cycle to prevent infinite recursion, but properties involved in the cycle may not be fully serialized. Consider marking one of the types with [Entity] attribute or restructuring the types to avoid circular dependencies.");

    /// <summary>
    /// Entity类型间循环引用警告的诊断描述符
    /// </summary>
    private static readonly DiagnosticDescriptor EntityCircularReferenceWarningDescriptor = new(
        id: "TINYDB003",
        title: "Circular reference detected between Entity types",
        messageFormat: "Circular reference detected between Entity types in '{0}': {1}. While this is handled at runtime, it may cause performance overhead and incomplete data during serialization.",
        category: "TinyDb.SourceGenerator",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "A circular reference was detected between Entity types. The runtime will handle this by breaking the cycle during serialization (returning only the ID for circular references), but this may cause performance overhead due to circular reference tracking. Consider redesigning the data model to avoid circular dependencies between entities.");

    private static readonly DiagnosticDescriptor InvalidIdPropertyErrorDescriptor = new(
        id: "TINYDB004",
        title: "Entity IdProperty does not exist",
        messageFormat: "Entity type '{0}' specifies IdProperty '{1}', but no public mapped property or field with that name exists",
        category: "TinyDb.SourceGenerator",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "When EntityAttribute.IdProperty is specified, the name must match a mapped public property or field. TinyDb only applies automatic Id, _id, ID, or [Id] fallback when IdProperty is not specified.");

    private static readonly DiagnosticDescriptor GenericEntityRegistryWarningDescriptor = new(
        id: "TINYDB005",
        title: "Generic Entity is not registered for AOT lookup",
        messageFormat: "Generic Entity type '{0}' cannot be registered in the TinyDb AOT registry. Runtime lookups for closed generic instances may fall back to reflection.",
        category: "TinyDb.SourceGenerator",
        DiagnosticSeverity.Warning,
        isEnabledByDefault: true,
        description: "TinyDb generates helpers for generic Entity definitions, but the runtime registry is keyed by concrete closed types. Add a non-generic Entity wrapper if registry-based AOT lookup is required.");

    private static readonly DiagnosticDescriptor UnsupportedArrayRankErrorDescriptor = new(
        id: "TINYDB006",
        title: "Multidimensional arrays are not supported by TinyDb AOT generation",
        messageFormat: "Property '{0}' in type '{1}' uses array rank {2}. TinyDb AOT generation only supports single-dimensional arrays.",
        category: "TinyDb.SourceGenerator",
        DiagnosticSeverity.Error,
        isEnabledByDefault: true,
        description: "TinyDb AOT collection generation materializes arrays from BSON arrays and supports only single-dimensional CLR arrays.");

    /// <summary>
    /// 初始化生成器
    /// </summary>
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        // 查找带有 [Entity] 特性的类声明
        var classDeclarations = context.SyntaxProvider
            .ForAttributeWithMetadataName(
                EntityAttributeMetadataName,
                predicate: static (s, _) => s is ClassDeclarationSyntax || s is StructDeclarationSyntax || s is RecordDeclarationSyntax,
                transform: static (ctx, cancellationToken) => GetClassInfo(ctx, cancellationToken))
            .Where(static m => m is not null);

        var comparableClassDeclarations = classDeclarations
            .WithComparer(ClassInfoComparer.Instance);

        // 注册源代码生成
        var diagnosticClassDeclarations = comparableClassDeclarations
            .Where(static classInfo => classInfo is not null && ShouldGenerateMapper(classInfo) && HasDiagnostics(classInfo));

        var validClassDeclarations = comparableClassDeclarations
            .Where(static classInfo => classInfo is not null && ShouldGenerateMapper(classInfo) && !HasBlockingDiagnostics(classInfo));

        context.RegisterSourceOutput(diagnosticClassDeclarations, static (spc, classInfo) =>
        {
            if (classInfo == null) return;

            ReportDiagnostics(spc, classInfo);
        });

        context.RegisterSourceOutput(validClassDeclarations, static (spc, classInfo) =>
        {
            if (classInfo == null) return;

            var partialClassCode = GeneratePartialClass(classInfo);
            var partialFileName = $"{classInfo.UniqueFileName}_AotHelper.g.cs";
            spc.AddSource(partialFileName, SourceText.From(partialClassCode, Encoding.UTF8));
        });

        context.RegisterSourceOutput(validClassDeclarations.Collect(), static (spc, classes) =>
        {
            if (classes.IsDefaultOrEmpty) return;

            var validClasses = GetValidClasses(classes);
            if (validClasses.Count == 0) return;

            var registrySource = GenerateRegistrySource(validClasses);
            spc.AddSource("AotHelperRegistry.g.cs", SourceText.From(registrySource, Encoding.UTF8));
        });
    }

    private static List<ClassInfo> GetValidClasses(ImmutableArray<ClassInfo?> classes)
    {
        var validClasses = new List<ClassInfo>(classes.Length);
        var seenTypes = new HashSet<string>(StringComparer.Ordinal);

        foreach (var classInfo in classes)
        {
            if (classInfo == null) continue;

            // 嵌套类现在已支持。复杂类型的 AOT 优化机制同样适用于嵌套的 Entity 类。
            if (ShouldGenerateMapper(classInfo) &&
                seenTypes.Add(classInfo.FullyQualifiedTypeReference))
            {
                validClasses.Add(classInfo);
            }
        }

        validClasses.Sort(static (x, y) => string.CompareOrdinal(x.FullName, y.FullName));
        return validClasses;
    }

    private static void ReportDiagnostics(SourceProductionContext context, ClassInfo classInfo)
    {
        ReportInvalidIdPropertyErrors(context, classInfo);
        ReportCircularReferences(context, classInfo);
        ReportEntityCircularReferences(context, classInfo);
        ReportBsonRefMissingEntityErrors(context, classInfo);
        ReportGenericEntityRegistryWarnings(context, classInfo);
        ReportUnsupportedArrayRankErrors(context, classInfo);
    }

    private static bool HasBlockingDiagnostics(ClassInfo classInfo)
    {
        return !string.IsNullOrWhiteSpace(classInfo.InvalidIdPropertyName) ||
               classInfo.BsonRefMissingEntityErrors.Count > 0 ||
               HasUnsupportedArrayRank(classInfo);
    }

    private static bool HasDiagnostics(ClassInfo classInfo)
    {
        return HasBlockingDiagnostics(classInfo) ||
               classInfo.CircularReferences.Count > 0 ||
               classInfo.EntityCircularReferences.Count > 0 ||
               classInfo.IsGenericType;
    }

    private static void ReportGenericEntityRegistryWarnings(SourceProductionContext context, ClassInfo classInfo)
    {
        if (!classInfo.IsGenericType)
        {
            return;
        }

        var diagnostic = Diagnostic.Create(
            GenericEntityRegistryWarningDescriptor,
            classInfo.Location.ToLocation(),
            classInfo.FullName);
        context.ReportDiagnostic(diagnostic);
    }

    private static void ReportUnsupportedArrayRankErrors(SourceProductionContext context, ClassInfo classInfo)
    {
        foreach (var property in GetUnsupportedArrayRankProperties(classInfo))
        {
            var diagnostic = Diagnostic.Create(
                UnsupportedArrayRankErrorDescriptor,
                classInfo.Location.ToLocation(),
                property.Name,
                classInfo.FullName,
                property.ArrayRank);
            context.ReportDiagnostic(diagnostic);
        }

        foreach (var unsupported in GetUnsupportedDependentArrayRankProperties(classInfo))
        {
            var property = unsupported.Property;
            var diagnostic = Diagnostic.Create(
                UnsupportedArrayRankErrorDescriptor,
                classInfo.Location.ToLocation(),
                property.Name,
                unsupported.ContainingTypeName,
                property.ArrayRank);
            context.ReportDiagnostic(diagnostic);
        }
    }

    private static bool HasUnsupportedArrayRank(ClassInfo classInfo)
    {
        return GetUnsupportedArrayRankProperties(classInfo).Any() ||
               GetUnsupportedDependentArrayRankProperties(classInfo).Any();
    }

    private static IEnumerable<PropertyInfo> GetUnsupportedArrayRankProperties(ClassInfo classInfo)
    {
        return classInfo.Properties.Where(static p => p.IsArray && p.ArrayRank != 1);
    }

    private static IEnumerable<(string ContainingTypeName, DependentTypeProperty Property)> GetUnsupportedDependentArrayRankProperties(ClassInfo classInfo)
    {
        return classInfo.DependentComplexTypes
            .SelectMany(static t => t.Properties.Select(p => (ContainingTypeName: t.FullyQualifiedName, Property: p)))
            .Where(static item => item.Property.IsArray && item.Property.ArrayRank != 1);
    }

    private static void ReportInvalidIdPropertyErrors(SourceProductionContext context, ClassInfo classInfo)
    {
        if (string.IsNullOrWhiteSpace(classInfo.InvalidIdPropertyName))
        {
            return;
        }

        var diagnostic = Diagnostic.Create(
            InvalidIdPropertyErrorDescriptor,
            classInfo.InvalidIdPropertyLocation.ToLocation() ?? classInfo.Location.ToLocation(),
            classInfo.FullName,
            classInfo.InvalidIdPropertyName);
        context.ReportDiagnostic(diagnostic);
    }

    private static void ReportCircularReferences(SourceProductionContext context, ClassInfo classInfo)
    {
        foreach (var circularRef in classInfo.CircularReferences)
        {
            var diagnostic = Diagnostic.Create(
                CircularReferenceWarningDescriptor,
                classInfo.Location.ToLocation(),
                circularRef.ContainingTypeName,
                circularRef.CycleChain);
            context.ReportDiagnostic(diagnostic);
        }
    }

    private static void ReportEntityCircularReferences(SourceProductionContext context, ClassInfo classInfo)
    {
        foreach (var entityCircularRef in classInfo.EntityCircularReferences)
        {
            var diagnostic = Diagnostic.Create(
                EntityCircularReferenceWarningDescriptor,
                classInfo.Location.ToLocation(),
                entityCircularRef.CurrentEntityName,
                entityCircularRef.CycleChain);
            context.ReportDiagnostic(diagnostic);
        }
    }

    private static void ReportBsonRefMissingEntityErrors(SourceProductionContext context, ClassInfo classInfo)
    {
        foreach (var bsonRefError in classInfo.BsonRefMissingEntityErrors)
        {
            var diagnostic = Diagnostic.Create(
                BsonRefMissingEntityErrorDescriptor,
                bsonRefError.Location.ToLocation() ?? classInfo.Location.ToLocation(),
                bsonRefError.PropertyName,
                bsonRefError.ContainingTypeName,
                bsonRefError.ReferencedTypeName);
            context.ReportDiagnostic(diagnostic);
        }
    }
}
