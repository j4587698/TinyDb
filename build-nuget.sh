#!/bin/bash

# TinyDb NuGet 包构建脚本
# 构建包含 SourceGenerator 和 AOT 支持的完整 NuGet 包

set -e

echo "🚀 开始构建 TinyDb NuGet 包..."

# 配置
CONFIGURATION=${1:-Release}
OUTPUT_DIR="nupkg-output"

# 清理之前的构建
echo "📁 清理之前的构建..."
dotnet clean TinyDb.sln -c $CONFIGURATION
rm -rf $OUTPUT_DIR
mkdir -p $OUTPUT_DIR

# 构建整个解决方案
echo "🔨 构建解决方案..."
dotnet build TinyDb.sln -c $CONFIGURATION --no-restore

# 构建 SourceGenerator (确保最新)
echo "📦 构建 SourceGenerator..."
dotnet build TinyDb.SourceGenerator/TinyDb.SourceGenerator.csproj -c $CONFIGURATION

# 构建 NuGet 包
echo "📦 构建 NuGet 包..."
dotnet pack TinyDb/TinyDb.csproj \
    -c $CONFIGURATION \
    --output $OUTPUT_DIR \
    --include-symbols \
    --include-source

# 验证包内容
echo "🔍 验证包内容..."
NUPKG_FILE=$(find $OUTPUT_DIR -name "TinyDb.*.nupkg" | head -1)
if [ -n "$NUPKG_FILE" ]; then
    echo "✅ 包文件: $NUPKG_FILE"

    # 提取并检查包内容
    TEMP_DIR=$(mktemp -d)
    unzip -q "$NUPKG_FILE" -d "$TEMP_DIR"

    # 检查关键文件
    echo "📋 检查包内容:"
    [ -f "$TEMP_DIR/lib/net8.0/TinyDb.dll" ] && echo "  ✅ .NET 8.0 库"
    [ -f "$TEMP_DIR/lib/net9.0/TinyDb.dll" ] && echo "  ✅ .NET 9.0 库"
    [ -f "$TEMP_DIR/analyzers/dotnet/cs/TinyDb.SourceGenerator.dll" ] && echo "  ✅ SourceGenerator 分析器"
    [ -f "$TEMP_DIR/content/aot-compatibility.json" ] && echo "  ✅ AOT 兼容性配置"
    [ -f "$TEMP_DIR/content/aot-example.csproj" ] && echo "  ✅ AOT 示例配置"
    [ -f "$TEMP_DIR/README.md" ] && echo "  ✅ README"
    [ -f "$TEMP_DIR/LICENSE" ] && echo "  ✅ LICENSE"

    rm -rf "$TEMP_DIR"
else
    echo "❌ 未找到包文件"
    exit 1
fi

echo ""
echo "✅ NuGet 包构建完成!"
echo "📁 输出目录: $OUTPUT_DIR"
echo ""
echo "📦 包文件列表:"
ls -la $OUTPUT_DIR

echo ""
echo "🚀 安装测试命令:"
echo "dotnet new console -n TestTinyDb"
echo "cd TestTinyDb"
echo "dotnet add package $OUTPUT_DIR/TinyDb.*.nupkg"
echo "dotnet publish -c Release --self-contained true -r linux-x64 /p:PublishAot=true"