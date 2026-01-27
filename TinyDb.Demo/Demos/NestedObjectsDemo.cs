using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Bson;
using TinyDb.Attributes;

namespace TinyDb.Demo.Demos;

/// <summary>
/// 嵌套对象和复杂类型演示
/// </summary>
public static class NestedObjectsDemo
{
    public static Task RunAsync()
    {
        Console.WriteLine("=== TinyDb 嵌套对象与复杂类型演示 ===");
        Console.WriteLine();

        var dbPath = "nested_objects_demo.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var engine = new TinyDbEngine(dbPath);

        // 嵌套对象演示
        Console.WriteLine("1. 嵌套对象演示");
        Console.WriteLine(new string('-', 50));
        NestedObjectDemo(engine);
        Console.WriteLine();

        // 集合类型演示
        Console.WriteLine("2. 集合类型演示 (List/Array)");
        Console.WriteLine(new string('-', 50));
        CollectionTypeDemo(engine);
        Console.WriteLine();

        // 字典类型演示
        Console.WriteLine("3. 字典类型演示 (Dictionary)");
        Console.WriteLine(new string('-', 50));
        DictionaryTypeDemo(engine);
        Console.WriteLine();

        // 深度嵌套演示
        Console.WriteLine("4. 深度嵌套结构演示");
        Console.WriteLine(new string('-', 50));
        DeepNestingDemo(engine);
        Console.WriteLine();

        // 可空类型演示
        Console.WriteLine("5. 可空类型演示");
        Console.WriteLine(new string('-', 50));
        NullableTypeDemo(engine);
        Console.WriteLine();

        // 枚举类型演示
        Console.WriteLine("6. 枚举类型演示");
        Console.WriteLine(new string('-', 50));
        EnumTypeDemo(engine);
        Console.WriteLine();

        // 清理
        if (File.Exists(dbPath)) File.Delete(dbPath);

        Console.WriteLine("✅ 嵌套对象与复杂类型演示完成！");
        return Task.CompletedTask;
    }

    /// <summary>
    /// 嵌套对象演示
    /// </summary>
    private static void NestedObjectDemo(TinyDbEngine engine)
    {
        var customers = engine.GetCollection<Customer>();

        // 创建包含嵌套地址的客户
        var customer = new Customer
        {
            Name = "张三",
            Email = "zhangsan@example.com",
            Phone = "13800138000",
            Address = new Address
            {
                Country = "中国",
                Province = "广东省",
                City = "深圳市",
                District = "南山区",
                Street = "科技园路100号",
                PostalCode = "518000"
            },
            ShippingAddress = new Address
            {
                Country = "中国",
                Province = "广东省",
                City = "深圳市",
                District = "福田区",
                Street = "华强北路200号",
                PostalCode = "518001"
            }
        };

        customers.Insert(customer);
        Console.WriteLine($"✅ 插入客户: {customer.Name}");
        Console.WriteLine($"   账单地址: {customer.Address.Province} {customer.Address.City} {customer.Address.Street}");
        Console.WriteLine($"   配送地址: {customer.ShippingAddress?.Province} {customer.ShippingAddress?.City} {customer.ShippingAddress?.Street}");

        // 查询并验证嵌套对象
        var retrieved = customers.FindById(customer.Id);
        if (retrieved != null)
        {
            Console.WriteLine($"✅ 查询验证: 地址完整性 = {retrieved.Address != null}");
            Console.WriteLine($"   邮编: {retrieved.Address?.PostalCode}");
        }

        // 按嵌套属性查询
        var shenzhenCustomers = customers.Find(c => c.Address.City == "深圳市").ToList();
        Console.WriteLine($"📊 深圳市客户数: {shenzhenCustomers.Count}");
    }

    /// <summary>
    /// 集合类型演示
    /// </summary>
    private static void CollectionTypeDemo(TinyDbEngine engine)
    {
        var blogPosts = engine.GetCollection<BlogPost>();

        // 创建包含列表的博客文章
        var post = new BlogPost
        {
            Title = "TinyDb入门教程",
            Content = "TinyDb是一个轻量级的嵌入式数据库...",
            Author = "技术博客",
            Tags = new List<string> { "数据库", "C#", ".NET", "教程" },
            Categories = new[] { "技术", "编程" },
            Comments = new List<Comment>
            {
                new Comment { Author = "读者A", Content = "很有帮助！", CreatedAt = DateTime.Now.AddHours(-2) },
                new Comment { Author = "读者B", Content = "期待更多内容", CreatedAt = DateTime.Now.AddHours(-1) }
            },
            Ratings = new List<int> { 5, 4, 5, 5, 4 }
        };

        blogPosts.Insert(post);
        Console.WriteLine($"✅ 插入博客文章: {post.Title}");
        Console.WriteLine($"   标签: {string.Join(", ", post.Tags)}");
        Console.WriteLine($"   分类: {string.Join(", ", post.Categories)}");
        Console.WriteLine($"   评论数: {post.Comments.Count}");
        Console.WriteLine($"   评分: {post.Ratings.Average():F1}");

        // 查询并验证
        var retrieved = blogPosts.FindById(post.Id);
        if (retrieved != null)
        {
            Console.WriteLine($"✅ 查询验证:");
            Console.WriteLine($"   标签数量: {retrieved.Tags?.Count}");
            Console.WriteLine($"   评论数量: {retrieved.Comments?.Count}");

            // 显示评论
            Console.WriteLine("   评论列表:");
            foreach (var comment in retrieved.Comments ?? new List<Comment>())
            {
                Console.WriteLine($"   - {comment.Author}: {comment.Content}");
            }
        }
    }

    /// <summary>
    /// 字典类型演示
    /// </summary>
    private static void DictionaryTypeDemo(TinyDbEngine engine)
    {
        var configs = engine.GetCollection<AppConfiguration>();

        // 创建包含字典的配置
        var config = new AppConfiguration
        {
            AppName = "DemoApp",
            Version = "1.0.0",
            Settings = new Dictionary<string, string>
            {
                { "Theme", "Dark" },
                { "Language", "zh-CN" },
                { "AutoSave", "true" },
                { "MaxRetries", "3" }
            },
            FeatureFlags = new Dictionary<string, bool>
            {
                { "NewUI", true },
                { "BetaFeatures", false },
                { "AdvancedMode", true }
            },
            Metadata = new Dictionary<string, object>
            {
                { "CreatedBy", "Admin" },
                { "Priority", 1 },
                { "EnabledAt", DateTime.Now }
            }
        };

        configs.Insert(config);
        Console.WriteLine($"✅ 插入配置: {config.AppName}");
        Console.WriteLine($"   设置项数量: {config.Settings.Count}");
        Console.WriteLine($"   功能标志数量: {config.FeatureFlags.Count}");

        // 查询并验证
        var retrieved = configs.FindById(config.Id);
        if (retrieved != null)
        {
            Console.WriteLine($"✅ 查询验证:");
            Console.WriteLine("   设置项:");
            foreach (var setting in retrieved.Settings ?? new Dictionary<string, string>())
            {
                Console.WriteLine($"   - {setting.Key}: {setting.Value}");
            }

            Console.WriteLine("   功能标志:");
            foreach (var flag in retrieved.FeatureFlags ?? new Dictionary<string, bool>())
            {
                Console.WriteLine($"   - {flag.Key}: {(flag.Value ? "启用" : "禁用")}");
            }
        }
    }

    /// <summary>
    /// 深度嵌套演示
    /// </summary>
    private static void DeepNestingDemo(TinyDbEngine engine)
    {
        var companies = engine.GetCollection<Company>();

        // 创建深度嵌套结构
        var company = new Company
        {
            Name = "示例科技有限公司",
            Departments = new List<Department>
            {
                new Department
                {
                    Name = "技术部",
                    Manager = new Manager { Name = "王经理", Title = "技术总监" },
                    Teams = new List<Team>
                    {
                        new Team
                        {
                            Name = "后端团队",
                            Members = new List<TeamMember>
                            {
                                new TeamMember { Name = "张三", Role = "高级工程师", Skills = new List<string> { "C#", ".NET", "SQL" } },
                                new TeamMember { Name = "李四", Role = "工程师", Skills = new List<string> { "Java", "Spring" } }
                            }
                        },
                        new Team
                        {
                            Name = "前端团队",
                            Members = new List<TeamMember>
                            {
                                new TeamMember { Name = "王五", Role = "高级工程师", Skills = new List<string> { "React", "TypeScript" } }
                            }
                        }
                    }
                },
                new Department
                {
                    Name = "市场部",
                    Manager = new Manager { Name = "李经理", Title = "市场总监" },
                    Teams = new List<Team>
                    {
                        new Team
                        {
                            Name = "品牌团队",
                            Members = new List<TeamMember>
                            {
                                new TeamMember { Name = "赵六", Role = "品牌经理", Skills = new List<string> { "营销", "策划" } }
                            }
                        }
                    }
                }
            }
        };

        companies.Insert(company);
        Console.WriteLine($"✅ 插入公司: {company.Name}");
        Console.WriteLine($"   部门数: {company.Departments.Count}");

        // 统计总人数
        var totalMembers = company.Departments
            .SelectMany(d => d.Teams ?? new List<Team>())
            .SelectMany(t => t.Members ?? new List<TeamMember>())
            .Count();
        Console.WriteLine($"   总员工数: {totalMembers}");

        // 查询并显示结构
        var retrieved = companies.FindById(company.Id);
        if (retrieved != null)
        {
            Console.WriteLine($"✅ 组织结构:");
            foreach (var dept in retrieved.Departments ?? new List<Department>())
            {
                Console.WriteLine($"   📁 {dept.Name} (负责人: {dept.Manager?.Name})");
                foreach (var team in dept.Teams ?? new List<Team>())
                {
                    Console.WriteLine($"      📂 {team.Name}");
                    foreach (var member in team.Members ?? new List<TeamMember>())
                    {
                        Console.WriteLine($"         👤 {member.Name} ({member.Role}) - 技能: {string.Join(", ", member.Skills ?? new List<string>())}");
                    }
                }
            }
        }
    }

    /// <summary>
    /// 可空类型演示
    /// </summary>
    private static void NullableTypeDemo(TinyDbEngine engine)
    {
        var events = engine.GetCollection<CalendarEvent>();

        // 创建包含可空类型的事件
        var event1 = new CalendarEvent
        {
            Title = "项目启动会",
            StartTime = DateTime.Now.AddDays(1),
            EndTime = DateTime.Now.AddDays(1).AddHours(2),
            Location = "会议室A",
            Description = "讨论新项目的启动计划",
            MaxParticipants = 20,
            IsAllDay = false,
            ReminderMinutes = 30
        };

        var event2 = new CalendarEvent
        {
            Title = "年假",
            StartTime = DateTime.Now.AddDays(10),
            EndTime = null, // 可空
            Location = null, // 可空
            Description = null, // 可空
            MaxParticipants = null, // 可空
            IsAllDay = true,
            ReminderMinutes = null // 可空
        };

        events.Insert(event1);
        events.Insert(event2);

        Console.WriteLine($"✅ 插入事件1: {event1.Title}");
        Console.WriteLine($"   结束时间: {event1.EndTime}");
        Console.WriteLine($"   地点: {event1.Location}");
        Console.WriteLine($"   最大参与人数: {event1.MaxParticipants}");

        Console.WriteLine($"✅ 插入事件2: {event2.Title}");
        Console.WriteLine($"   结束时间: {(event2.EndTime.HasValue ? event2.EndTime.ToString() : "未指定")}");
        Console.WriteLine($"   地点: {event2.Location ?? "未指定"}");
        Console.WriteLine($"   最大参与人数: {(event2.MaxParticipants.HasValue ? event2.MaxParticipants.ToString() : "不限")}");

        // 查询可空值
        var eventsWithLocation = events.Find(e => e.Location != null).ToList();
        Console.WriteLine($"📊 有指定地点的事件: {eventsWithLocation.Count}");

        var allDayEvents = events.Find(e => e.IsAllDay == true).ToList();
        Console.WriteLine($"📊 全天事件: {allDayEvents.Count}");
    }

    /// <summary>
    /// 枚举类型演示
    /// </summary>
    private static void EnumTypeDemo(TinyDbEngine engine)
    {
        var tickets = engine.GetCollection<SupportTicket>();

        // 创建包含枚举的工单
        var ticket1 = new SupportTicket
        {
            Title = "无法登录系统",
            Description = "用户反馈无法登录，显示密码错误",
            Priority = TicketPriority.High,
            Status = TicketStatus.Open,
            Category = TicketCategory.Technical
        };

        var ticket2 = new SupportTicket
        {
            Title = "功能建议",
            Description = "建议增加批量导出功能",
            Priority = TicketPriority.Low,
            Status = TicketStatus.Pending,
            Category = TicketCategory.FeatureRequest
        };

        var ticket3 = new SupportTicket
        {
            Title = "账单问题",
            Description = "账单金额显示不正确",
            Priority = TicketPriority.Medium,
            Status = TicketStatus.InProgress,
            Category = TicketCategory.Billing
        };

        tickets.Insert(ticket1);
        tickets.Insert(ticket2);
        tickets.Insert(ticket3);

        Console.WriteLine($"✅ 插入工单:");
        foreach (var ticket in tickets.FindAll())
        {
            Console.WriteLine($"   - [{ticket.Priority}] {ticket.Title} ({ticket.Status}) - {ticket.Category}");
        }

        // 按枚举值查询
        var highPriorityTickets = tickets.Find(t => t.Priority == TicketPriority.High).ToList();
        Console.WriteLine($"📊 高优先级工单: {highPriorityTickets.Count}");

        var openTickets = tickets.Find(t => t.Status == TicketStatus.Open || t.Status == TicketStatus.Pending).ToList();
        Console.WriteLine($"📊 待处理工单: {openTickets.Count}");

        var technicalTickets = tickets.Find(t => t.Category == TicketCategory.Technical).ToList();
        Console.WriteLine($"📊 技术问题工单: {technicalTickets.Count}");
    }
}

#region 实体类定义

/// <summary>
/// 客户实体
/// </summary>
[Entity("customers")]
public class Customer
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public string Phone { get; set; } = string.Empty;
    public Address Address { get; set; } = new Address();
    public Address? ShippingAddress { get; set; }
}

/// <summary>
/// 地址
/// </summary>
public class Address
{
    public string Country { get; set; } = string.Empty;
    public string Province { get; set; } = string.Empty;
    public string City { get; set; } = string.Empty;
    public string District { get; set; } = string.Empty;
    public string Street { get; set; } = string.Empty;
    public string PostalCode { get; set; } = string.Empty;
}

/// <summary>
/// 博客文章实体
/// </summary>
[Entity("blog_posts")]
public class BlogPost
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Title { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public string Author { get; set; } = string.Empty;
    public List<string> Tags { get; set; } = new List<string>();
    public string[] Categories { get; set; } = Array.Empty<string>();
    public List<Comment> Comments { get; set; } = new List<Comment>();
    public List<int> Ratings { get; set; } = new List<int>();
    public DateTime CreatedAt { get; set; } = DateTime.Now;
}

/// <summary>
/// 评论
/// </summary>
public class Comment
{
    public string Author { get; set; } = string.Empty;
    public string Content { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.Now;
}

/// <summary>
/// 应用配置实体
/// </summary>
[Entity("app_configurations")]
public class AppConfiguration
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string AppName { get; set; } = string.Empty;
    public string Version { get; set; } = string.Empty;
    public Dictionary<string, string> Settings { get; set; } = new Dictionary<string, string>();
    public Dictionary<string, bool> FeatureFlags { get; set; } = new Dictionary<string, bool>();
    public Dictionary<string, object> Metadata { get; set; } = new Dictionary<string, object>();
}

/// <summary>
/// 公司实体 (深度嵌套)
/// </summary>
[Entity("companies")]
public class Company
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = string.Empty;
    public List<Department> Departments { get; set; } = new List<Department>();
}

public class Department
{
    public string Name { get; set; } = string.Empty;
    public Manager? Manager { get; set; }
    public List<Team> Teams { get; set; } = new List<Team>();
}

public class Manager
{
    public string Name { get; set; } = string.Empty;
    public string Title { get; set; } = string.Empty;
}

public class Team
{
    public string Name { get; set; } = string.Empty;
    public List<TeamMember> Members { get; set; } = new List<TeamMember>();
}

public class TeamMember
{
    public string Name { get; set; } = string.Empty;
    public string Role { get; set; } = string.Empty;
    public List<string> Skills { get; set; } = new List<string>();
}

/// <summary>
/// 日历事件实体 (可空类型)
/// </summary>
[Entity("calendar_events")]
public class CalendarEvent
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Title { get; set; } = string.Empty;
    public DateTime StartTime { get; set; }
    public DateTime? EndTime { get; set; }
    public string? Location { get; set; }
    public string? Description { get; set; }
    public int? MaxParticipants { get; set; }
    public bool IsAllDay { get; set; }
    public int? ReminderMinutes { get; set; }
}

/// <summary>
/// 支持工单实体 (枚举类型)
/// </summary>
[Entity("support_tickets")]
public class SupportTicket
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Title { get; set; } = string.Empty;
    public string Description { get; set; } = string.Empty;
    public TicketPriority Priority { get; set; }
    public TicketStatus Status { get; set; }
    public TicketCategory Category { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.Now;
}

public enum TicketPriority
{
    Low,
    Medium,
    High,
    Critical
}

public enum TicketStatus
{
    Open,
    Pending,
    InProgress,
    Resolved,
    Closed
}

public enum TicketCategory
{
    Technical,
    Billing,
    General,
    FeatureRequest
}

#endregion
