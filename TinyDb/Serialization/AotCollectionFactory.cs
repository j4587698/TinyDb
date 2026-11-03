using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;

namespace TinyDb.Serialization;

/// <summary>
/// 在 AOT 模式下把已读取的元素列表物化为属性声明的集合类型。
/// </summary>
public static class AotCollectionFactory
{
    /// <summary>
    /// 将已读取的元素列表转换为目标集合类型。
    /// </summary>
    /// <typeparam name="TCollection">属性声明的集合类型。</typeparam>
    /// <typeparam name="TElement">集合元素类型。</typeparam>
    /// <param name="items">已按 BSON 顺序读取的元素。</param>
    /// <returns>目标集合类型的实例。</returns>
    public static TCollection Create<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TCollection, TElement>(List<TElement> items)
    {
        if (items is TCollection same)
        {
            return same;
        }

        if (typeof(TCollection).IsAssignableFrom(typeof(List<TElement>)))
        {
            return (TCollection)(object)items;
        }

        if (typeof(TCollection).IsAssignableFrom(typeof(HashSet<TElement>)))
        {
            return (TCollection)(object)new HashSet<TElement>(items);
        }

        // Stack<T> 的 IEnumerable 构造函数按枚举顺序压栈，会反转枚举结果；
        // 逆序压栈可保持序列化前后的枚举顺序一致。
        if (typeof(TCollection) == typeof(Stack<TElement>))
        {
            var stack = new Stack<TElement>(items.Count);
            for (var i = items.Count - 1; i >= 0; i--)
            {
                stack.Push(items[i]);
            }

            return (TCollection)(object)stack;
        }

        if (typeof(TCollection) == typeof(System.Collections.Concurrent.ConcurrentStack<TElement>))
        {
            var reversed = new List<TElement>(items.Count);
            for (var i = items.Count - 1; i >= 0; i--)
            {
                reversed.Add(items[i]);
            }

            return (TCollection)(object)new System.Collections.Concurrent.ConcurrentStack<TElement>(reversed);
        }

        if (TryCreateFromEnumerable<TCollection, TElement>(items, out var created))
        {
            return created;
        }

        throw new NotSupportedException(
            $"Collection type '{typeof(TCollection).FullName}' cannot be materialized in AOT mode. " +
            "Use List<T>, HashSet<T>, or a collection type with a public constructor that accepts IEnumerable<T>.");
    }

    /// <summary>
    /// 尝试通过构造函数或 <see cref="ICollection{T}"/> 把元素写入目标集合类型。
    /// </summary>
    internal static bool TryCreateFromEnumerable<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TCollection, TElement>(
        IEnumerable<TElement> items,
        out TCollection created)
    {
        var collectionType = typeof(TCollection);

        try
        {
            if (Activator.CreateInstance(collectionType, new object?[] { items }) is TCollection fromEnumerable)
            {
                created = fromEnumerable;
                return true;
            }
        }
        catch (Exception ex) when (IsConstructionFailure(ex))
        {
        }

        try
        {
            if (Activator.CreateInstance(collectionType) is TCollection instance &&
                instance is ICollection<TElement> collection)
            {
                foreach (var item in items)
                {
                    collection.Add(item);
                }

                created = instance;
                return true;
            }
        }
        catch (Exception ex) when (IsConstructionFailure(ex))
        {
        }

        created = default!;
        return false;
    }

    /// <summary>
    /// 尝试通过接受 <see cref="IEnumerable{T}"/> 的构造函数构造集合（运行时回退路径使用）。
    /// </summary>
    internal static bool TryCreateFromEnumerable(
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] Type collectionType,
        object items,
        out object? created)
    {
        try
        {
            created = Activator.CreateInstance(collectionType, new object?[] { items });
            return created != null;
        }
        catch (Exception ex) when (IsConstructionFailure(ex))
        {
            created = null;
            return false;
        }
    }

    private static bool IsConstructionFailure(Exception exception)
    {
        return exception is MissingMethodException or
               ArgumentException or
               NotSupportedException or
               MemberAccessException or
               AmbiguousMatchException;
    }
}
