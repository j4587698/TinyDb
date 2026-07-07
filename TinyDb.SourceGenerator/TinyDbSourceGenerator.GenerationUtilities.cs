using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TinyDb.SourceGenerator;

public partial class TinyDbSourceGenerator
{
    private static string NormalizeTypeName(string type)
    {
        if (string.IsNullOrWhiteSpace(type)) return type;

        var normalized = type.Trim();
        if (normalized.StartsWith("global::", StringComparison.Ordinal))
        {
            normalized = normalized.Substring("global::".Length);
        }

        return normalized switch
        {
            "System.String" => "string",
            "System.Guid" => "Guid",
            "System.Int32" => "int",
            "System.Int64" => "long",
            "System.Int16" => "short",
            "System.UInt32" => "uint",
            "System.UInt64" => "ulong",
            "System.UInt16" => "ushort",
            "System.Byte" => "byte",
            "System.SByte" => "sbyte",
            "System.Boolean" => "bool",
            "System.Decimal" => "decimal",
            "System.Double" => "double",
            "System.Single" => "float",
            "System.DateTime" => "DateTime",
            "System.DateTimeOffset" => "DateTimeOffset",
            "System.TimeSpan" => "TimeSpan",
            _ => normalized
        };
    }

    internal static string ToCSharpStringLiteral(string? value)
    {
        if (value == null) return "null";

        var sb = new StringBuilder(value.Length + 2);
        sb.Append('"');

        foreach (var ch in value)
        {
            switch (ch)
            {
                case '\\':
                    sb.Append(@"\\");
                    break;
                case '"':
                    sb.Append("\\\"");
                    break;
                case '\0':
                    sb.Append(@"\0");
                    break;
                case '\a':
                    sb.Append(@"\a");
                    break;
                case '\b':
                    sb.Append(@"\b");
                    break;
                case '\f':
                    sb.Append(@"\f");
                    break;
                case '\r':
                    sb.Append(@"\r");
                    break;
                case '\n':
                    sb.Append(@"\n");
                    break;
                case '\t':
                    sb.Append(@"\t");
                    break;
                case '\v':
                    sb.Append(@"\v");
                    break;
                default:
                    if (char.IsControl(ch))
                    {
                        sb.Append(@"\u");
                        sb.Append(((int)ch).ToString("x4"));
                    }
                    else
                    {
                        sb.Append(ch);
                    }
                    break;
            }
        }

        sb.Append('"');
        return sb.ToString();
    }
}
