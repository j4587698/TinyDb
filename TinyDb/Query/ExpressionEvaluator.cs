using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Globalization;
using TinyDb.Bson;
using TinyDb.Serialization;
using TinyDb.Utils;

namespace TinyDb.Query;

public static partial class ExpressionEvaluator
{
    private const int MaxExpressionEvaluationDepth = 256;
    private const int NameCacheCapacity = 2048;
    private const int PropertyCacheCapacity = 4096;
    private static readonly LRUCache<string, string> CamelCaseNameCache = new(NameCacheCapacity);
    private static readonly LRUCache<(Type Type, string Name), PropertyInfo?> PropertyCache = new(PropertyCacheCapacity);
    [ThreadStatic]
    private static int _evaluationDepth;

    internal static (int CamelCaseNames, int Properties) GetCacheCounts()
    {
        return (CamelCaseNameCache.Count, PropertyCache.Count);
    }

    public static bool Evaluate<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(QueryExpression expression, T entity)
        where T : class
    {
        EnterEvaluation();
        try
        {
            return expression switch
            {
                ConstantExpression constExpr => EvaluateConstantExpression(constExpr),
                BinaryExpression binaryExpr => (bool?)EvaluateBinaryExpression(binaryExpr, entity) ?? false,
                MemberExpression memberExpr => EvaluateMemberExpression(memberExpr, entity),
                UnaryExpression unaryExpr => EvaluateUnaryBooleanExpression(unaryExpr, entity),
                ParameterExpression => true,
                FunctionExpression funcExpr => (bool?)EvaluateFunctionExpression(funcExpr, entity) ?? false,
                ConditionalQueryExpression conditionalExpr => EvaluateConditionalBoolean(conditionalExpr, entity),
                _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported")
            };
        }
        finally
        {
            ExitEvaluation();
        }
    }

    public static bool Evaluate(QueryExpression expression, BsonDocument document)
    {
        EnterEvaluation();
        try
        {
            return expression switch
            {
                ConstantExpression constExpr => EvaluateConstantExpression(constExpr),
                BinaryExpression binaryExpr => (bool?)EvaluateBinaryExpression(binaryExpr, document) ?? false,
                MemberExpression memberExpr => EvaluateMemberExpression(memberExpr, document),
                UnaryExpression unaryExpr => EvaluateUnaryBooleanExpression(unaryExpr, document),
                ParameterExpression => true,
                FunctionExpression funcExpr => (bool?)EvaluateFunctionExpression(funcExpr, document) ?? false,
                ConditionalQueryExpression conditionalExpr => EvaluateConditionalBoolean(conditionalExpr, (object)document),
                _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported")
            };
        }
        finally
        {
            ExitEvaluation();
        }
    }

    private static bool EvaluateConstantExpression(ConstantExpression expression)
    {
        if (expression.Value is bool boolValue)
        {
            return boolValue;
        }
        throw new InvalidOperationException($"Constant expression must be boolean, got {expression.Value?.GetType().Name}");
    }

    public static object? EvaluateValue<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(QueryExpression expression, T entity)
        where T : class
    {
        EnterEvaluation();
        try
        {
            return expression switch
            {
                ConstantExpression constExpr => constExpr.Value,
                MemberExpression memberExpr => GetMemberValue(memberExpr, entity),
                UnaryExpression unaryExpr => EvaluateUnaryExpression(unaryExpr, entity),
                ParameterExpression => entity,
                BinaryExpression binaryExpr => EvaluateBinaryExpression(binaryExpr, entity),
                FunctionExpression funcExpr => EvaluateFunctionExpression(funcExpr, entity),
                ConstructorExpression ctorExpr => EvaluateConstructorExpression(ctorExpr, entity),
                MemberInitQueryExpression memberInitExpr => EvaluateMemberInitExpression(memberInitExpr, entity),
                ConditionalQueryExpression conditionalExpr => EvaluateConditionalValue(conditionalExpr, entity),
                _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported for value evaluation")
            };
        }
        finally
        {
            ExitEvaluation();
        }
    }

    public static object? EvaluateValue(QueryExpression expression, BsonDocument document)
    {
        EnterEvaluation();
        try
        {
            return expression switch
            {
                ConstantExpression constExpr => constExpr.Value,
                MemberExpression memberExpr => GetMemberValue(memberExpr, (object)document),
                UnaryExpression unaryExpr => EvaluateUnaryExpression(unaryExpr, (object)document),
                ParameterExpression => document,
                BinaryExpression binaryExpr => EvaluateBinaryExpression(binaryExpr, (object)document),
                FunctionExpression funcExpr => EvaluateFunctionExpression(funcExpr, (object)document),
                ConstructorExpression ctorExpr => EvaluateConstructorExpression(ctorExpr, (object)document),
                MemberInitQueryExpression memberInitExpr => EvaluateMemberInitExpression(memberInitExpr, (object)document),
                ConditionalQueryExpression conditionalExpr => EvaluateConditionalValue(conditionalExpr, (object)document),
                _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported for value evaluation")
            };
        }
        finally
        {
            ExitEvaluation();
        }
    }

    public static object? EvaluateValue(QueryExpression expression, object entity)
    {
        EnterEvaluation();
        try
        {
            if (entity is BsonDocument doc) return EvaluateValue(expression, doc);

            return expression switch
            {
                ConstantExpression constExpr => constExpr.Value,
                MemberExpression memberExpr => GetMemberValue(memberExpr, entity),
                UnaryExpression unaryExpr => EvaluateUnaryExpression(unaryExpr, entity),
                ParameterExpression => entity,
                BinaryExpression binaryExpr => EvaluateBinaryExpression(binaryExpr, entity),
                FunctionExpression funcExpr => EvaluateFunctionExpression(funcExpr, entity),
                ConstructorExpression ctorExpr => EvaluateConstructorExpression(ctorExpr, entity),
                MemberInitQueryExpression memberInitExpr => EvaluateMemberInitExpression(memberInitExpr, entity),
                ConditionalQueryExpression conditionalExpr => EvaluateConditionalValue(conditionalExpr, entity),
                _ => throw new NotSupportedException($"Expression type {expression.GetType().Name} is not supported for value evaluation")
            };
        }
        finally
        {
            ExitEvaluation();
        }
    }

    private static void EnterEvaluation()
    {
        if (++_evaluationDepth > MaxExpressionEvaluationDepth)
        {
            _evaluationDepth--;
            throw new InvalidOperationException($"Expression evaluation depth exceeds {MaxExpressionEvaluationDepth}.");
        }
    }

    private static void ExitEvaluation()
    {
        _evaluationDepth--;
    }


    private static object? EvaluateConstructorExpression<T>(ConstructorExpression expression, T entity)
    {
        var args = EvaluateArguments(expression.Arguments, (object)entity!);
        return Activator.CreateInstance(expression.Type, args);
    }

    private static bool EvaluateConditionalBoolean(ConditionalQueryExpression expression, object entity)
    {
        var result = EvaluateConditionalValue(expression, entity);
        if (result is bool b) return b;
        if (result is null) return false;
        throw new InvalidOperationException("Conditional expression must evaluate to a boolean value");
    }

    private static object? EvaluateConditionalValue(ConditionalQueryExpression expression, object entity)
    {
        var testValue = EvaluateValue(expression.Test, entity);
        if (testValue is bool test)
        {
            return EvaluateValue(test ? expression.IfTrue : expression.IfFalse, entity);
        }
        if (testValue is null)
        {
            return null;
        }
        throw new InvalidOperationException("Conditional test must be a boolean value");
    }

    [UnconditionalSuppressMessage("TrimAnalysis", "IL2072", Justification = "Type and properties come from expression tree.")]
    [UnconditionalSuppressMessage("TrimAnalysis", "IL2075", Justification = "Property access via reflection for AOT projection.")]
    private static object? EvaluateMemberInitExpression<T>(MemberInitQueryExpression expression, T entity)
    {
        // Create instance using parameterless constructor
        var instance = Activator.CreateInstance(expression.Type);
        if (instance == null) return null;

        // Set each property
        foreach (var (memberName, valueExpr) in expression.Bindings)
        {
            var value = EvaluateValue(valueExpr, (object)entity!);
            var prop = GetPropertySafe(expression.Type, memberName);
            if (prop != null && prop.CanWrite)
            {
                // Handle type conversion if needed
                if (value != null && !prop.PropertyType.IsInstanceOfType(value))
                {
                    try
                    {
                        value = Convert.ChangeType(value, prop.PropertyType);
                    }
                    catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException or ArgumentException)
                    {
                        // Keep original value if conversion fails
                    }
                }
                prop.SetValue(instance, value);
            }
        }

        return instance;
    }

    private static bool EvaluateUnaryBooleanExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(UnaryExpression expression, T entity)
        where T : class
    {
        var val = EvaluateValue(expression, entity);
        return val is bool b && b;
    }

    private static bool EvaluateUnaryBooleanExpression(UnaryExpression expression, BsonDocument document)
    {
        var val = EvaluateValue(expression, document);
        return val is bool b && b;
    }

    private static object? EvaluateUnaryExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(UnaryExpression expression, T entity)
        where T : class
    {
        var value = EvaluateValue(expression.Operand, entity);
        return EvaluateUnary(expression.NodeType, value, expression.Type);
    }

    private static object? EvaluateUnaryExpression(UnaryExpression expression, object entity)
    {
        var value = EvaluateValue(expression.Operand, entity);
        return EvaluateUnary(expression.NodeType, value, expression.Type);
    }

    private static object? EvaluateUnary(ExpressionType nodeType, object? value, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicParameterlessConstructor)] Type targetType)
    {
        if (nodeType == ExpressionType.Convert)
        {
            if (value == null) return null;
            return Convert.ChangeType(value, targetType);
        }
        else if (nodeType == ExpressionType.Not)
        {
            if (value is bool b) return !b;
            throw new InvalidOperationException("Not operator requires boolean operand");
        }
        throw new NotSupportedException($"Unary operation {nodeType} is not supported");
    }

    private static bool EvaluateMemberExpression<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties)] T>(MemberExpression expression, T entity)
        where T : class
    {
        var value = GetMemberValue(expression, entity);
        return value is bool boolValue ? boolValue : value != null;
    }

    private static bool EvaluateMemberExpression(MemberExpression expression, BsonDocument document)
    {
        var value = GetMemberValue(expression, document);
        return value is bool boolValue ? boolValue : value != null;
    }

}
