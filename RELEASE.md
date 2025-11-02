# 🚀 TinyDb 自动化发布使用指南

## 📋 快速开始

### 1. 版本发布流程

发布新版本只需要三个简单步骤：

#### 步骤一：更新版本号
在 `TinyDb/TinyDb.csproj` 中更新版本号：
```xml
<PropertyGroup>
  <Version>0.1.2</Version>  <!-- 更新为新的版本号 -->
  <!-- ... 其他配置 ... -->
</PropertyGroup>
```

#### 步骤二：提交代码
```bash
git add .
git commit -m "feat: 发布版本 v0.1.2

- 添加新功能特性
- 修复已知问题
- 性能优化"
```

#### 步骤三：推送到GitHub
```bash
git push origin main
```

就这么简单！GitHub Actions会自动处理其余所有事情。

## 🤖 自动化流程详情

推送代码后，GitHub Actions将自动执行：

1. **版本检测** - 比较当前版本与上一版本
2. **构建测试** - 编译项目并运行所有测试
3. **创建标签** - 自动生成Git标签（如 `v0.1.2`）
4. **NuGet发布** - 发布包到NuGet仓库
5. **创建Release** - 生成详细的GitHub Release页面

## 📦 发布内容验证

每个发布的NuGet包都包含：
- ✅ .NET 8.0 库文件
- ✅ .NET 9.0 库文件
- ✅ SourceGenerator 分析器
- ✅ AOT 兼容性配置
- ✅ 使用示例和文档

## 🔍 本地测试

在推送前，可以本地测试版本检测：

```bash
# 运行版本检查脚本
./scripts/check-version.sh
```

预期输出：
```
🔍 测试版本检查逻辑...
📋 获取当前版本...
当前版本: 0.1.2
📋 获取上一个版本...
上一个版本: 0.1.1
🔍 比较版本...
✅ 版本已变更: 0.1.1 → 0.1.2
🏷️  将创建标签: v0.1.2
📦 将发布到 NuGet: TinyDb.0.1.2.nupkg
✅ 版本检查完成
```

## 📈 发布历史查看

### GitHub Release
访问：https://github.com/j4587698/TinyDb/releases

### NuGet 包
访问：https://www.nuget.org/packages/TinyDb

### Git 标签
```bash
# 查看所有标签
git tag -l

# 查看特定标签信息
git show v0.1.2
```

## ⚠️ 注意事项

1. **版本号格式**：遵循语义化版本 `MAJOR.MINOR.PATCH`
2. **提交信息**：使用清晰的提交信息描述变更内容
3. **测试通过**：确保所有测试通过后再发布
4. **网络连接**：推送时确保网络连接正常

## 🛠️ 故障排除

### 如果发布失败

1. 检查GitHub Actions状态：
   - 访问：https://github.com/j4587698/TinyDb/actions

2. 查看具体错误信息：
   - 进入失败的workflow
   - 查看详细日志

3. 常见问题：
   - **版本号格式错误** - 确保 `<Version>` 标签格式正确
   - **编译失败** - 检查代码是否有语法错误
   - **测试失败** - 确保所有测试用例通过
   - **权限问题** - 检查仓库权限设置

### 手动重试

如果需要重新发布，可以：

1. **手动触发**：
   - 访问GitHub Actions页面
   - 选择"Release TinyDb to NuGet" workflow
   - 点击"Run workflow"

2. **重新推送**（如果版本号未变）：
   ```bash
   # 创建一个新的空提交来触发workflow
   git commit --allow-empty -m "trigger: 重新触发发布流程"
   git push origin main
   ```

## 🎯 最佳实践

### 版本管理
- **主版本**：不兼容的API变更
- **次版本**：向后兼容的功能新增
- **修订版本**：向后兼容的问题修正

### 提交信息规范
```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

类型示例：
- `feat`: 新功能
- `fix`: 问题修复
- `docs`: 文档更新
- `style`: 代码格式调整
- `refactor`: 代码重构
- `test`: 测试相关
- `chore`: 构建或辅助工具变更

### 发布检查清单
- [ ] 版本号已更新
- [ ] 代码编译通过
- [ ] 所有测试通过
- [ ] 更新日志已准备
- [ ] 文档同步更新

---

🎉 **恭喜！** 现在你已经掌握了TinyDb的自动化发布流程。只需更新版本号并推送代码，剩下的就交给GitHub Actions吧！