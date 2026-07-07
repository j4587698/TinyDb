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

    private static string GetRuntimeFullName(INamedTypeSymbol typeSymbol)
    {
        var names = new Stack<string>();
        var current = typeSymbol;
        while (current != null)
        {
            names.Push(current.MetadataName);
            current = current.ContainingType;
        }

        var typePath = string.Join("+", names);
        var namespaceName = typeSymbol.ContainingNamespace is { IsGlobalNamespace: false } ns
            ? ns.ToDisplayString()
            : string.Empty;

        return string.IsNullOrEmpty(namespaceName) ? typePath : $"{namespaceName}.{typePath}";
    }

    private static string GetStableMetadataTypeName(ITypeSymbol typeSymbol)
    {
        if (typeSymbol is IArrayTypeSymbol arrayType)
        {
            return $"{GetStableMetadataTypeName(arrayType.ElementType)}[]";
        }

        if (typeSymbol is ITypeParameterSymbol typeParameter)
        {
            return typeParameter.Name;
        }

        if (typeSymbol is not INamedTypeSymbol namedType)
        {
            return typeSymbol.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat).Replace("global::", "");
        }

        if (!namedType.IsGenericType)
        {
            return GetRuntimeFullName(namedType);
        }

        var definitionName = GetRuntimeFullName(namedType.ConstructedFrom);
        var tickIndex = definitionName.IndexOf('`');
        if (tickIndex >= 0)
        {
            definitionName = definitionName.Substring(0, tickIndex);
        }

        var typeArguments = namedType.TypeArguments.Select(GetStableMetadataTypeName);
        return $"{definitionName}<{string.Join(",", typeArguments)}>";
    }

    /// <summary>
    /// 判断是否应该为类生成映射器
    /// </summary>
    private static bool ShouldGenerateMapper(ClassInfo classInfo)
    {
        // 注意：嵌套类的检查已移至 Initialize 方法中，会生成编译错误诊断

        // 排除TinyDb内部命名空间，但允许Demo项目和Tests项目
        if (!string.IsNullOrEmpty(classInfo.Namespace))
        {
            // 明确排除System命名空间
            if (classInfo.Namespace.Contains("System"))
            {
                return false;
            }

            // 已移除TinyDb核心命名空间的排除逻辑，以便 MetadataDocument 等实体能正常生成
        }

        // 排除编译生成的类
        if (classInfo.Name.StartsWith("<") || classInfo.Name.Contains("Anonymous"))
        {
            return false;
        }

        // 只处理有属性的实体类（ID将通过智能识别获得）
        return classInfo.Properties.Count > 0;
    }
}
