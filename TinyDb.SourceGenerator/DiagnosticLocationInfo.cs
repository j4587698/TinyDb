using System;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace TinyDb.SourceGenerator;

public readonly struct DiagnosticLocationInfo : IEquatable<DiagnosticLocationInfo>
{
    public string FilePath { get; }
    public TextSpan TextSpan { get; }
    public LinePositionSpan LineSpan { get; }

    private DiagnosticLocationInfo(string filePath, TextSpan textSpan, LinePositionSpan lineSpan)
    {
        FilePath = filePath;
        TextSpan = textSpan;
        LineSpan = lineSpan;
    }

    public static DiagnosticLocationInfo? From(Location? location)
    {
        if (location == null || location == Location.None || !location.IsInSource)
        {
            return null;
        }

        var lineSpan = location.GetLineSpan();
        return new DiagnosticLocationInfo(lineSpan.Path ?? string.Empty, location.SourceSpan, lineSpan.Span);
    }

    public Location? ToLocation()
    {
        return string.IsNullOrEmpty(FilePath)
            ? null
            : Location.Create(FilePath, TextSpan, LineSpan);
    }

    public bool Equals(DiagnosticLocationInfo other)
    {
        return string.Equals(FilePath, other.FilePath, StringComparison.Ordinal) &&
               TextSpan.Equals(other.TextSpan) &&
               LineSpan.Equals(other.LineSpan);
    }

    public override bool Equals(object? obj)
    {
        return obj is DiagnosticLocationInfo other && Equals(other);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            var hash = 17;
            hash = hash * 31 + StringComparer.Ordinal.GetHashCode(FilePath ?? string.Empty);
            hash = hash * 31 + TextSpan.GetHashCode();
            hash = hash * 31 + LineSpan.GetHashCode();
            return hash;
        }
    }
}

internal static class DiagnosticLocationInfoExtensions
{
    public static Location? ToLocation(this DiagnosticLocationInfo? location)
    {
        return location.HasValue ? location.Value.ToLocation() : null;
    }
}
