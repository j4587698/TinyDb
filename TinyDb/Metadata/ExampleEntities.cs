using TinyDb.Attributes;

namespace TinyDb.Metadata;

/// <summary>
/// 示例用户实体，展示如何使用元数据特性
/// </summary>
[EntityMetadata("用户信息", Description = "系统用户的基本信息实体")]
public class UserEntity
{
    [Id]
    [PropertyMetadata("用户ID", Description = "用户的唯一标识符", Order = 1)]
    public int Id { get; set; }

    [PropertyMetadata("用户名", Description = "登录用户名", Order = 2, Required = true)]
    public string Username { get; set; } = string.Empty;

    [PropertyMetadata("邮箱地址", Description = "用户邮箱地址", Order = 3, Required = true)]
    public string Email { get; set; } = string.Empty;

    [PropertyMetadata("年龄", Description = "用户年龄", Order = 4)]
    public int Age { get; set; }

    [PropertyMetadata("注册时间", Description = "用户注册时间", Order = 5)]
    public DateTime CreatedAt { get; set; } = DateTime.Now;

    [PropertyMetadata("是否激活", Description = "账户是否已激活", Order = 6)]
    public bool IsActive { get; set; } = true;
}

/// <summary>
/// 示例产品实体
/// </summary>
[EntityMetadata("产品信息", Description = "产品目录中的产品信息")]
public class ProductEntity
{
    [Id]
    [PropertyMetadata("产品编号", Description = "产品的唯一标识", Order = 1)]
    public int Id { get; set; }

    [PropertyMetadata("产品名称", Description = "产品名称", Order = 2, Required = true)]
    public string Name { get; set; } = string.Empty;

    [PropertyMetadata("产品价格", Description = "产品销售价格", Order = 3, Required = true)]
    public decimal Price { get; set; }

    [PropertyMetadata("库存数量", Description = "当前库存数量", Order = 4)]
    public int Stock { get; set; }

    [PropertyMetadata("产品描述", Description = "产品的详细描述", Order = 5)]
    public string? Description { get; set; }
}