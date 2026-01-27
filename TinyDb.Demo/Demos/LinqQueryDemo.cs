using System;
using System.Linq;
using TinyDb.Core;
using TinyDb.Collections;
using TinyDb.Bson;
using TinyDb.Attributes;

namespace TinyDb.Demo.Demos;

/// <summary>
/// LINQ查询功能演示
/// </summary>
public static class LinqQueryDemo
{
    public static Task RunAsync()
    {
        Console.WriteLine("=== TinyDb LINQ查询功能演示 ===");
        Console.WriteLine();

        var dbPath = "linq_query_demo.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var engine = new TinyDbEngine(dbPath);
        var employees = engine.GetCollection<Employee>();

        // 准备测试数据
        Console.WriteLine("1. 准备测试数据");
        Console.WriteLine(new string('-', 50));
        PrepareTestData(employees);
        Console.WriteLine($"✅ 已插入 {employees.Count()} 条员工记录");
        Console.WriteLine();

        // 基本查询
        Console.WriteLine("2. 基本查询");
        Console.WriteLine(new string('-', 50));
        BasicQueryDemo(employees);
        Console.WriteLine();

        // 条件查询
        Console.WriteLine("3. 条件查询");
        Console.WriteLine(new string('-', 50));
        ConditionalQueryDemo(employees);
        Console.WriteLine();

        // 排序查询
        Console.WriteLine("4. 排序查询");
        Console.WriteLine(new string('-', 50));
        SortingQueryDemo(employees);
        Console.WriteLine();

        // 分页查询
        Console.WriteLine("5. 分页查询");
        Console.WriteLine(new string('-', 50));
        PaginationQueryDemo(employees);
        Console.WriteLine();

        // 聚合查询
        Console.WriteLine("6. 聚合查询");
        Console.WriteLine(new string('-', 50));
        AggregationQueryDemo(employees);
        Console.WriteLine();

        // 分组查询
        Console.WriteLine("7. 分组查询");
        Console.WriteLine(new string('-', 50));
        GroupingQueryDemo(employees);
        Console.WriteLine();

        // 投影查询
        Console.WriteLine("8. 投影查询");
        Console.WriteLine(new string('-', 50));
        ProjectionQueryDemo(employees);
        Console.WriteLine();

        // 清理
        if (File.Exists(dbPath)) File.Delete(dbPath);

        Console.WriteLine("✅ LINQ查询演示完成！");
        return Task.CompletedTask;
    }

    /// <summary>
    /// 准备测试数据
    /// </summary>
    private static void PrepareTestData(ITinyCollection<Employee> employees)
    {
        var testData = new[]
        {
            new Employee { Name = "张三", Department = "技术部", Position = "高级工程师", Salary = 25000, Age = 35, JoinDate = DateTime.Now.AddYears(-5) },
            new Employee { Name = "李四", Department = "技术部", Position = "工程师", Salary = 18000, Age = 28, JoinDate = DateTime.Now.AddYears(-3) },
            new Employee { Name = "王五", Department = "市场部", Position = "经理", Salary = 30000, Age = 40, JoinDate = DateTime.Now.AddYears(-8) },
            new Employee { Name = "赵六", Department = "市场部", Position = "专员", Salary = 12000, Age = 25, JoinDate = DateTime.Now.AddYears(-1) },
            new Employee { Name = "钱七", Department = "人事部", Position = "主管", Salary = 22000, Age = 32, JoinDate = DateTime.Now.AddYears(-4) },
            new Employee { Name = "孙八", Department = "技术部", Position = "架构师", Salary = 35000, Age = 38, JoinDate = DateTime.Now.AddYears(-6) },
            new Employee { Name = "周九", Department = "财务部", Position = "会计", Salary = 15000, Age = 30, JoinDate = DateTime.Now.AddYears(-2) },
            new Employee { Name = "吴十", Department = "技术部", Position = "实习生", Salary = 6000, Age = 22, JoinDate = DateTime.Now.AddMonths(-6) },
            new Employee { Name = "郑一", Department = "市场部", Position = "总监", Salary = 45000, Age = 45, JoinDate = DateTime.Now.AddYears(-10) },
            new Employee { Name = "陈二", Department = "人事部", Position = "专员", Salary = 10000, Age = 24, JoinDate = DateTime.Now.AddMonths(-8) },
        };

        foreach (var emp in testData)
        {
            employees.Insert(emp);
        }
    }

    /// <summary>
    /// 基本查询演示
    /// </summary>
    private static void BasicQueryDemo(ITinyCollection<Employee> employees)
    {
        // 查询所有员工
        var allEmployees = employees.FindAll().ToList();
        Console.WriteLine($"📊 所有员工数量: {allEmployees.Count}");

        // 查询单个员工
        var firstEmployee = employees.FindOne(e => e.Name == "张三");
        Console.WriteLine($"👤 查找张三: {firstEmployee?.Name} - {firstEmployee?.Position}");

        // 检查是否存在
        var exists = employees.Exists(e => e.Department == "技术部");
        Console.WriteLine($"✅ 技术部是否有员工: {exists}");
    }

    /// <summary>
    /// 条件查询演示
    /// </summary>
    private static void ConditionalQueryDemo(ITinyCollection<Employee> employees)
    {
        // 单条件查询
        var techEmployees = employees.Find(e => e.Department == "技术部").ToList();
        Console.WriteLine($"🔧 技术部员工: {techEmployees.Count}人");
        foreach (var emp in techEmployees)
        {
            Console.WriteLine($"   - {emp.Name} ({emp.Position})");
        }

        // 多条件查询 (AND)
        var highSalaryTech = employees.Find(e => e.Department == "技术部" && e.Salary > 20000).ToList();
        Console.WriteLine($"💰 技术部高薪员工(>20000): {highSalaryTech.Count}人");

        // 范围查询
        var ageRange = employees.Find(e => e.Age >= 25 && e.Age <= 35).ToList();
        Console.WriteLine($"📅 年龄在25-35之间: {ageRange.Count}人");

        // 字符串包含查询
        var managers = employees.Find(e => e.Position.Contains("经理") || e.Position.Contains("主管") || e.Position.Contains("总监")).ToList();
        Console.WriteLine($"👔 管理层: {managers.Count}人");
    }

    /// <summary>
    /// 排序查询演示
    /// </summary>
    private static void SortingQueryDemo(ITinyCollection<Employee> employees)
    {
        // 按薪资降序
        var bySalaryDesc = employees.FindAll()
            .OrderByDescending(e => e.Salary)
            .Take(3)
            .ToList();
        Console.WriteLine("💵 薪资最高的3位员工:");
        foreach (var emp in bySalaryDesc)
        {
            Console.WriteLine($"   - {emp.Name}: {emp.Salary:C}");
        }

        // 按年龄升序
        var byAgeAsc = employees.FindAll()
            .OrderBy(e => e.Age)
            .Take(3)
            .ToList();
        Console.WriteLine("👶 最年轻的3位员工:");
        foreach (var emp in byAgeAsc)
        {
            Console.WriteLine($"   - {emp.Name}: {emp.Age}岁");
        }

        // 多字段排序
        var multiSort = employees.FindAll()
            .OrderBy(e => e.Department)
            .ThenByDescending(e => e.Salary)
            .ToList();
        Console.WriteLine("📋 按部门排序，同部门按薪资降序:");
        foreach (var emp in multiSort.Take(5))
        {
            Console.WriteLine($"   - {emp.Department} | {emp.Name}: {emp.Salary:C}");
        }
    }

    /// <summary>
    /// 分页查询演示
    /// </summary>
    private static void PaginationQueryDemo(ITinyCollection<Employee> employees)
    {
        const int pageSize = 3;
        var totalCount = employees.Count();
        var totalPages = (int)Math.Ceiling((double)totalCount / pageSize);

        Console.WriteLine($"📖 总记录数: {totalCount}, 每页: {pageSize}, 总页数: {totalPages}");

        for (int page = 0; page < totalPages; page++)
        {
            var pageData = employees.FindAll()
                .OrderBy(e => e.Name)
                .Skip(page * pageSize)
                .Take(pageSize)
                .ToList();

            Console.WriteLine($"   第{page + 1}页: {string.Join(", ", pageData.Select(e => e.Name))}");
        }
    }

    /// <summary>
    /// 聚合查询演示
    /// </summary>
    private static void AggregationQueryDemo(ITinyCollection<Employee> employees)
    {
        var allEmployees = employees.FindAll().ToList();

        // 统计
        Console.WriteLine($"📊 员工总数: {allEmployees.Count}");
        Console.WriteLine($"💰 平均薪资: {allEmployees.Average(e => e.Salary):C}");
        Console.WriteLine($"💵 最高薪资: {allEmployees.Max(e => e.Salary):C}");
        Console.WriteLine($"💴 最低薪资: {allEmployees.Min(e => e.Salary):C}");
        Console.WriteLine($"💎 薪资总和: {allEmployees.Sum(e => e.Salary):C}");

        // 条件统计
        var techCount = employees.Count(e => e.Department == "技术部");
        Console.WriteLine($"🔧 技术部员工数: {techCount}");

        // 年龄统计
        Console.WriteLine($"👤 平均年龄: {allEmployees.Average(e => e.Age):F1}岁");
    }

    /// <summary>
    /// 分组查询演示
    /// </summary>
    private static void GroupingQueryDemo(ITinyCollection<Employee> employees)
    {
        var allEmployees = employees.FindAll().ToList();

        // 按部门分组
        var byDepartment = allEmployees
            .GroupBy(e => e.Department)
            .Select(g => new
            {
                Department = g.Key,
                Count = g.Count(),
                AvgSalary = g.Average(e => e.Salary),
                TotalSalary = g.Sum(e => e.Salary)
            })
            .OrderByDescending(g => g.Count);

        Console.WriteLine("📊 按部门统计:");
        foreach (var dept in byDepartment)
        {
            Console.WriteLine($"   {dept.Department}: {dept.Count}人, 平均薪资: {dept.AvgSalary:C}, 总薪资: {dept.TotalSalary:C}");
        }

        // 按年龄段分组
        var byAgeGroup = allEmployees
            .GroupBy(e => e.Age / 10 * 10)
            .Select(g => new { AgeGroup = g.Key, Count = g.Count() })
            .OrderBy(g => g.AgeGroup);

        Console.WriteLine("👥 按年龄段统计:");
        foreach (var group in byAgeGroup)
        {
            Console.WriteLine($"   {group.AgeGroup}-{group.AgeGroup + 9}岁: {group.Count}人");
        }
    }

    /// <summary>
    /// 投影查询演示
    /// </summary>
    private static void ProjectionQueryDemo(ITinyCollection<Employee> employees)
    {
        // 选择特定字段
        var nameAndSalary = employees.FindAll()
            .Select(e => new { e.Name, e.Salary })
            .OrderByDescending(e => e.Salary)
            .ToList();

        Console.WriteLine("📋 员工薪资列表:");
        foreach (var item in nameAndSalary.Take(5))
        {
            Console.WriteLine($"   {item.Name}: {item.Salary:C}");
        }

        // 计算字段
        var withBonus = employees.FindAll()
            .Select(e => new
            {
                e.Name,
                e.Salary,
                Bonus = e.Salary * 0.1m,
                TotalIncome = e.Salary * 1.1m
            })
            .OrderByDescending(e => e.TotalIncome)
            .ToList();

        Console.WriteLine("💎 包含奖金的收入 (假设奖金为薪资的10%):");
        foreach (var item in withBonus.Take(5))
        {
            Console.WriteLine($"   {item.Name}: 薪资={item.Salary:C}, 奖金={item.Bonus:C}, 总收入={item.TotalIncome:C}");
        }
    }
}

/// <summary>
/// 员工实体
/// </summary>
[Entity("employees")]
public class Employee
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = string.Empty;
    public string Department { get; set; } = string.Empty;
    public string Position { get; set; } = string.Empty;
    public decimal Salary { get; set; }
    public int Age { get; set; }
    public DateTime JoinDate { get; set; }
}
