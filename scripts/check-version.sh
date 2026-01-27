#!/bin/bash

# 版本检查脚本 - 用于本地测试版本比较逻辑

echo "🔍 测试版本检查逻辑..."

# 模拟获取版本号的逻辑
echo "📋 获取当前版本..."
CURRENT_VERSION=$(git show HEAD:TinyDb/TinyDb.csproj | grep '<Version>' | sed 's/.*<Version>\(.*\)<\/Version>.*/\1/' | xargs)
echo "当前版本: $CURRENT_VERSION"

echo "📋 获取上一个版本..."
if git rev-parse HEAD~1 >/dev/null 2>&1; then
    PREVIOUS_VERSION=$(git show HEAD~1:TinyDb/TinyDb.csproj | grep '<Version>' | sed 's/.*<Version>\(.*\)<\/Version>.*/\1/' | xargs)
else
    PREVIOUS_VERSION="0.0.0"
fi
echo "上一个版本: $PREVIOUS_VERSION"

echo "🔍 比较版本..."
if [ "$CURRENT_VERSION" != "$PREVIOUS_VERSION" ]; then
    echo "✅ 版本已变更: $PREVIOUS_VERSION → $CURRENT_VERSION"
    echo "🏷️  将创建标签: v$CURRENT_VERSION"
    echo "📦 将发布到 NuGet: TinyDb.$CURRENT_VERSION.nupkg"
else
    echo "ℹ️ 版本未变更: $CURRENT_VERSION"
    echo "🚫 跳过发布流程"
fi

echo "✅ 版本检查完成"