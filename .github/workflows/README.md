# TinyDb 自动化发布工作流程

## 🚀 工作流程概述

这个GitHub Actions工作流程实现了TinyDb的完全自动化发布，包括版本检测、构建测试、Git标签、NuGet发布和GitHub Release创建。

## 📋 工作流程文件

- **主工作流程**: `.github/workflows/release.yml`
- **测试脚本**: `scripts/check-version.sh`

## 🔧 工作流程原理

### 1. 触发条件
```yaml
on:
  push:
    branches: [ main ]
  workflow_dispatch:
```

工作流程在以下情况触发：
- 推送代码到`main`分支
- 手动触发（`workflow_dispatch`）

### 2. 版本检测机制

工作流程通过比较当前提交与上一个提交的版本来判断是否需要发布：

```bash
# 获取当前版本
CURRENT_VERSION=$(git show HEAD:TinyDb/TinyDb.csproj | grep '<Version>' | sed 's/.*<Version>\(.*\)<\/Version>.*/\1/' | xargs)

# 获取上一个版本
PREVIOUS_VERSION=$(git show HEAD~1:TinyDb/TinyDb.csproj | grep '<Version>' | sed 's/.*<Version>\(.*\)<\/Version>.*/\1/' | xargs)
```

**版本变更判断**：
- 如果 `CURRENT_VERSION != PREVIOUS_VERSION` → 执行发布流程
- 如果 `CURRENT_VERSION == PREVIOUS_VERSION` → 跳过发布

### 3. 构建和测试流程

当检测到版本变更时，执行以下步骤：

1. **构建解决方案**
   ```bash
   dotnet build TinyDb.sln -c Release
   ```

2. **运行测试**
   ```bash
   dotnet test TinyDb.Tests/TinyDb.Tests.csproj -c Release --no-build --logger "console;verbosity=minimal"
   ```

3. **构建NuGet包**
   ```bash
   dotnet pack TinyDb/TinyDb.csproj -c Release --no-build --output ./nupkg
   ```

4. **验证包内容**
   - 检查.NET 8.0库文件
   - 检查.NET 9.0库文件
   - 检查SourceGenerator分析器
   - 检查AOT兼容性配置

### 4. 版本标签管理

**创建Git标签**：
```bash
git tag -a "v$CURRENT_VERSION" -m "Release version $CURRENT_VERSION"
```

**推送Git标签**：
```bash
git push origin "v$CURRENT_VERSION"
```

### 5. NuGet发布

**配置NuGet源**：
```bash
dotnet nuget add source --name "github" --username "github-actions" --password "${{ secrets.GITHUB_TOKEN }}" --store-password-in-clear-text
```

**发布包**：
```bash
dotnet nuget push ./nupkg/TinyDb.$CURRENT_VERSION.nupkg --source "github" --skip-duplicate --no-symbols
```

### 6. GitHub Release

自动创建GitHub Release，包含：
- 版本信息
- 主要特性说明
- 技术规格
- 安装使用指南
- 相关链接

## 📦 包内容验证

工作流程会自动验证NuGet包包含以下内容：

```
📋 Package contents:
  ✅ .NET 8.0 library (lib/net8.0/TinyDb.dll)
  ✅ .NET 9.0 library (lib/net9.0/TinyDb.dll)
  ✅ SourceGenerator analyzer (analyzers/dotnet/cs/TinyDb.SourceGenerator.dll)
  ✅ AOT compatibility config (content/aot-compatibility.json)
  ✅ AOT example config (content/aot-example.csproj)
```

## 🛠️ 本地测试

在推送到GitHub之前，可以使用本地测试脚本验证版本检测逻辑：

```bash
# 运行版本检查测试
./scripts/check-version.sh
```

这个脚本会模拟版本检测流程，输出类似：
```
🔍 测试版本检查逻辑...
📋 获取当前版本...
当前版本: 1.2.3
📋 获取上一个版本...
上一个版本: 1.2.2
🔍 比较版本...
✅ 版本已变更: 1.2.2 → 1.2.3
🏷️  将创建标签: v1.2.3
📦 将发布到 NuGet: TinyDb.1.2.3.nupkg
✅ 版本检查完成
```

## 📋 使用步骤

### 1. 更新版本号

在`TinyDb/TinyDb.csproj`中更新版本号：

```xml
<PropertyGroup>
  <TargetFrameworks>net9.0;net8.0</TargetFrameworks>
  <Version>1.2.3</Version>  <!-- 更新这里 -->
  <!-- ... 其他配置 ... -->
</PropertyGroup>
```

### 2. 推送代码

```bash
git add .
git commit -m "feat: 发布版本 v1.2.3"
git push origin main
```

### 3. 自动化流程

GitHub Actions将自动：
1. 检测版本变更
2. 构建和测试
3. 创建Git标签
4. 发布到NuGet
5. 创建GitHub Release

## ⚙️ 权限配置

工作流程需要以下权限：

```yaml
permissions:
  contents: write    # 创建标签和Release
  packages: write    # 发布到NuGet
```

## 🔍 故障排除

### 常见问题

1. **版本未检测到变更**
   - 确保版本号格式正确
   - 检查`TinyDb/TinyDb.csproj`中的`<Version>`标签

2. **构建失败**
   - 检查代码是否有编译错误
   - 确保所有依赖项正确

3. **测试失败**
   - 检查测试用例是否通过
   - 确保测试环境配置正确

4. **NuGet发布失败**
   - 检查`GITHUB_TOKEN`权限
   - 确保包内容验证通过

### 调试命令

本地调试版本检测：
```bash
# 检查当前版本
git show HEAD:TinyDb/TinyDb.csproj | grep '<Version>'

# 检查上一个版本
git show HEAD~1:TinyDb/TinyDb.csproj | grep '<Version>'
```

## 📈 发布历史

每次发布都会创建：
- Git标签：`v{version}`
- GitHub Release：包含详细说明
- NuGet包：`TinyDb.{version}.nupkg`

## 🎯 最佳实践

1. **语义化版本**：遵循`MAJOR.MINOR.PATCH`格式
2. **提交信息**：使用清晰的提交信息描述变更
3. **测试覆盖**：确保所有测试通过后再发布
4. **版本说明**：在Release中详细记录变更内容

---

🤖 此工作流程由GitHub Actions自动维护，确保TinyDb的发布过程一致、可靠、自动化。