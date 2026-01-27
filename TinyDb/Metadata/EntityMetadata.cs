namespace TinyDb.Metadata;

/// <summary>
/// 实体元数据信息
/// </summary>
public class EntityMetadata
{
    /// <summary>
    /// 实体类型名称
    /// </summary>
    public string TypeName { get; set; } = string.Empty;

    /// <summary>
    /// 实体集合名称
    /// </summary>
    public string CollectionName { get; set; } = string.Empty;

    /// <summary>
    /// 实体显示名称
    /// </summary>
    public string DisplayName { get; set; } = string.Empty;

    /// <summary>
    /// 实体描述
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// 属性元数据集合
    /// </summary>
    public List<PropertyMetadata> Properties { get; set; } = new();
}

/// <summary>
/// 属性元数据信息
/// </summary>
public class PropertyMetadata
{
    /// <summary>
    /// 属性名称
    /// </summary>
    public string PropertyName { get; set; } = string.Empty;

    /// <summary>
    /// 属性类型完整名称
    /// </summary>
    public string PropertyType { get; set; } = string.Empty;

    /// <summary>
    /// 属性显示名称
    /// </summary>
    public string DisplayName { get; set; } = string.Empty;

    /// <summary>
    /// 属性描述
    /// </summary>
    public string? Description { get; set; }

    /// <summary>
    /// 显示顺序
    /// </summary>
    public int Order { get; set; } = 0;

    /// <summary>
    /// 是否必需
    /// </summary>
    public bool Required { get; set; } = false;

    /// <summary>
    /// 密码相关配置（当属性是密码类型时使用）
    /// </summary>
    public PasswordMetadata? Password { get; set; }

    /// <summary>
    /// 外键关联的集合名称
    /// </summary>
    public string? ForeignKeyCollection { get; set; }
}

/// <summary>
/// 密码字段元数据信息
/// </summary>
public class PasswordMetadata
{
    /// <summary>
    /// 是否为密码字段
    /// </summary>
    public bool IsPassword { get; set; } = false;

    /// <summary>
    /// 密码强度要求
    /// </summary>
    public PasswordStrength RequiredStrength { get; set; } = PasswordStrength.Medium;

    /// <summary>
    /// 最小长度要求
    /// </summary>
    public int MinLength { get; set; } = 8;

    /// <summary>
    /// 最大长度限制
    /// </summary>
    public int MaxLength { get; set; } = 128;

    /// <summary>
    /// 是否需要特殊字符
    /// </summary>
    public bool RequireSpecialChar { get; set; } = true;

    /// <summary>
    /// 是否需要数字
    /// </summary>
    public bool RequireNumber { get; set; } = true;

    /// <summary>
    /// 是否需要大写字母
    /// </summary>
    public bool RequireUppercase { get; set; } = true;

    /// <summary>
    /// 是否需要小写字母
    /// </summary>
    public bool RequireLowercase { get; set; } = true;

    /// <summary>
    /// 密码提示信息
    /// </summary>
    public string? Hint { get; set; }

    /// <summary>
    /// 是否显示密码强度指示器
    /// </summary>
    public bool ShowStrengthIndicator { get; set; } = true;

    /// <summary>
    /// 是否支持密码显示/隐藏切换
    /// </summary>
    public bool AllowToggle { get; set; } = true;
}

/// <summary>
/// 密码强度枚举
/// </summary>
public enum PasswordStrength
{
    /// <summary>
    /// 弱密码 - 仅基本长度要求
    /// </summary>
    Weak = 1,

    /// <summary>
    /// 中等密码 - 长度+字符类型要求
    /// </summary>
    Medium = 2,

    /// <summary>
    /// 强密码 - 严格复杂度要求
    /// </summary>
    Strong = 3,

    /// <summary>
    /// 非常强密码 - 最高安全标准
    /// </summary>
    VeryStrong = 4
}