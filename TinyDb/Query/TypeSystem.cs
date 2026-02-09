using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace TinyDb.Query;

internal static class TypeSystem
{
    public static Type GetElementType([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.Interfaces)] Type seqType)
    {
        if (seqType == typeof(string))
        {
            return seqType;
        }

        if (seqType.IsArray)
        {
            return seqType.GetElementType() ?? seqType;
        }

        var current = seqType;
        while (current != null && current != typeof(object))
        {
            if (current.IsGenericType && current.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                return current.GetGenericArguments()[0];
            }

            foreach (var iface in current.GetInterfaces())
            {
                if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                {
                    return iface.GetGenericArguments()[0];
                }
            }

            current = current.BaseType;
        }

        return seqType;
    }
}
