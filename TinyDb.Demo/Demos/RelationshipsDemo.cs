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
/// 实体关系演示 - 展示如何处理一对一、一对多、多对多关系
/// </summary>
public static class RelationshipsDemo
{
    public static Task RunAsync()
    {
        Console.WriteLine("=== TinyDb 实体关系演示 ===");
        Console.WriteLine();

        var dbPath = "relationships_demo.db";
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var engine = new TinyDbEngine(dbPath);

        // 一对一关系演示
        Console.WriteLine("1. 一对一关系演示");
        Console.WriteLine(new string('-', 50));
        OneToOneDemo(engine);
        Console.WriteLine();

        // 一对多关系演示
        Console.WriteLine("2. 一对多关系演示");
        Console.WriteLine(new string('-', 50));
        OneToManyDemo(engine);
        Console.WriteLine();

        // 多对多关系演示
        Console.WriteLine("3. 多对多关系演示");
        Console.WriteLine(new string('-', 50));
        ManyToManyDemo(engine);
        Console.WriteLine();

        // 引用完整性演示
        Console.WriteLine("4. 引用完整性演示");
        Console.WriteLine(new string('-', 50));
        ReferentialIntegrityDemo(engine);
        Console.WriteLine();

        // 级联操作演示
        Console.WriteLine("5. 手动级联操作演示");
        Console.WriteLine(new string('-', 50));
        CascadeOperationsDemo(engine);
        Console.WriteLine();

        // 清理
        if (File.Exists(dbPath)) File.Delete(dbPath);

        Console.WriteLine("✅ 实体关系演示完成！");
        return Task.CompletedTask;
    }

    /// <summary>
    /// 一对一关系演示
    /// </summary>
    private static void OneToOneDemo(TinyDbEngine engine)
    {
        var users = engine.GetCollection<RelUser>();
        var profiles = engine.GetCollection<UserProfile>();

        // 创建用户和对应的详细资料
        var user = new RelUser
        {
            Username = "zhangsan",
            Email = "zhangsan@example.com"
        };
        users.Insert(user);

        var profile = new UserProfile
        {
            UserId = user.Id, // 关联到用户
            FullName = "张三",
            Bio = "热爱编程的开发者",
            Birthday = new DateTime(1990, 5, 15),
            AvatarUrl = "https://example.com/avatar/zhangsan.jpg"
        };
        profiles.Insert(profile);

        Console.WriteLine($"✅ 创建用户: {user.Username} (ID: {user.Id})");
        Console.WriteLine($"   创建资料: {profile.FullName} (关联UserId: {profile.UserId})");

        // 查询用户的资料 (通过外键关联)
        var foundUser = users.FindById(user.Id);
        if (foundUser != null)
        {
            var userProfile = profiles.FindOne(p => p.UserId == foundUser.Id);
            Console.WriteLine($"📖 查询用户资料:");
            Console.WriteLine($"   用户名: {foundUser.Username}");
            Console.WriteLine($"   全名: {userProfile?.FullName}");
            Console.WriteLine($"   简介: {userProfile?.Bio}");
        }

        // 扩展方法示例 - 获取用户和资料
        Console.WriteLine("\n📊 一对一关系查询示例:");
        var userWithProfile = from u in users.FindAll()
                              join p in profiles.FindAll() on u.Id equals p.UserId into profileGroup
                              from p in profileGroup.DefaultIfEmpty()
                              select new { User = u, Profile = p };

        foreach (var item in userWithProfile)
        {
            Console.WriteLine($"   {item.User.Username} -> {item.Profile?.FullName ?? "无资料"}");
        }
    }

    /// <summary>
    /// 一对多关系演示
    /// </summary>
    private static void OneToManyDemo(TinyDbEngine engine)
    {
        var authors = engine.GetCollection<Author>();
        var books = engine.GetCollection<Book>();

        // 创建作者
        var author1 = new Author { Name = "金庸", Country = "中国" };
        var author2 = new Author { Name = "鲁迅", Country = "中国" };
        authors.Insert(author1);
        authors.Insert(author2);

        // 创建书籍（关联到作者）
        var booksData = new[]
        {
            new Book { Title = "射雕英雄传", AuthorId = author1.Id, PublishedYear = 1957, Genre = "武侠" },
            new Book { Title = "神雕侠侣", AuthorId = author1.Id, PublishedYear = 1959, Genre = "武侠" },
            new Book { Title = "倚天屠龙记", AuthorId = author1.Id, PublishedYear = 1961, Genre = "武侠" },
            new Book { Title = "呐喊", AuthorId = author2.Id, PublishedYear = 1923, Genre = "小说" },
            new Book { Title = "彷徨", AuthorId = author2.Id, PublishedYear = 1926, Genre = "小说" }
        };
        books.Insert(booksData);

        Console.WriteLine($"✅ 创建作者和书籍:");
        Console.WriteLine($"   作者数量: {authors.Count()}");
        Console.WriteLine($"   书籍数量: {books.Count()}");

        // 查询每个作者的书籍（一对多）
        Console.WriteLine("\n📚 作者和其书籍:");
        foreach (var author in authors.FindAll())
        {
            var authorBooks = books.Find(b => b.AuthorId == author.Id).ToList();
            Console.WriteLine($"   📝 {author.Name} ({author.Country}) - {authorBooks.Count} 本书:");
            foreach (var book in authorBooks)
            {
                Console.WriteLine($"      - {book.Title} ({book.PublishedYear}) [{book.Genre}]");
            }
        }

        // 统计查询
        Console.WriteLine("\n📊 统计信息:");
        var booksByAuthor = books.FindAll()
            .GroupBy(b => b.AuthorId)
            .Select(g => new { AuthorId = g.Key, BookCount = g.Count() });

        foreach (var stat in booksByAuthor)
        {
            var author = authors.FindById(stat.AuthorId);
            Console.WriteLine($"   {author?.Name}: {stat.BookCount} 本书");
        }
    }

    /// <summary>
    /// 多对多关系演示
    /// </summary>
    private static void ManyToManyDemo(TinyDbEngine engine)
    {
        var students = engine.GetCollection<Student>();
        var courses = engine.GetCollection<Course>();
        var enrollments = engine.GetCollection<Enrollment>(); // 中间表

        // 创建学生
        var student1 = new Student { Name = "张三", StudentNo = "S001" };
        var student2 = new Student { Name = "李四", StudentNo = "S002" };
        var student3 = new Student { Name = "王五", StudentNo = "S003" };
        students.Insert(new[] { student1, student2, student3 });

        // 创建课程
        var course1 = new Course { Name = "高等数学", Credits = 4, Instructor = "陈教授" };
        var course2 = new Course { Name = "数据结构", Credits = 3, Instructor = "李教授" };
        var course3 = new Course { Name = "计算机网络", Credits = 3, Instructor = "王教授" };
        courses.Insert(new[] { course1, course2, course3 });

        // 创建选课关系（多对多）
        var enrollmentData = new[]
        {
            new Enrollment { StudentId = student1.Id, CourseId = course1.Id, Grade = 85 },
            new Enrollment { StudentId = student1.Id, CourseId = course2.Id, Grade = 92 },
            new Enrollment { StudentId = student2.Id, CourseId = course1.Id, Grade = 78 },
            new Enrollment { StudentId = student2.Id, CourseId = course2.Id, Grade = 88 },
            new Enrollment { StudentId = student2.Id, CourseId = course3.Id, Grade = 95 },
            new Enrollment { StudentId = student3.Id, CourseId = course3.Id, Grade = 90 }
        };
        enrollments.Insert(enrollmentData);

        Console.WriteLine($"✅ 创建多对多关系:");
        Console.WriteLine($"   学生数量: {students.Count()}");
        Console.WriteLine($"   课程数量: {courses.Count()}");
        Console.WriteLine($"   选课记录: {enrollments.Count()}");

        // 查询学生选修的课程
        Console.WriteLine("\n📚 学生选课情况:");
        foreach (var student in students.FindAll())
        {
            var studentEnrollments = enrollments.Find(e => e.StudentId == student.Id).ToList();
            Console.WriteLine($"   🎓 {student.Name} ({student.StudentNo}) - 选修 {studentEnrollments.Count} 门课:");
            foreach (var enrollment in studentEnrollments)
            {
                var course = courses.FindById(enrollment.CourseId);
                Console.WriteLine($"      - {course?.Name}: {enrollment.Grade}分");
            }
        }

        // 查询课程的选修学生
        Console.WriteLine("\n📖 课程选修情况:");
        foreach (var course in courses.FindAll())
        {
            var courseEnrollments = enrollments.Find(e => e.CourseId == course.Id).ToList();
            Console.WriteLine($"   📘 {course.Name} ({course.Credits}学分) - {courseEnrollments.Count} 人选修:");
            foreach (var enrollment in courseEnrollments)
            {
                var student = students.FindById(enrollment.StudentId);
                Console.WriteLine($"      - {student?.Name}: {enrollment.Grade}分");
            }
        }

        // 成绩统计
        Console.WriteLine("\n📊 课程成绩统计:");
        var courseStats = enrollments.FindAll()
            .GroupBy(e => e.CourseId)
            .Select(g => new
            {
                CourseId = g.Key,
                AvgGrade = g.Average(e => e.Grade ?? 0),
                MaxGrade = g.Max(e => e.Grade ?? 0),
                MinGrade = g.Min(e => e.Grade ?? 0)
            });

        foreach (var stat in courseStats)
        {
            var course = courses.FindById(stat.CourseId);
            Console.WriteLine($"   {course?.Name}: 平均={stat.AvgGrade:F1}, 最高={stat.MaxGrade}, 最低={stat.MinGrade}");
        }
    }

    /// <summary>
    /// 引用完整性演示
    /// </summary>
    private static void ReferentialIntegrityDemo(TinyDbEngine engine)
    {
        var departments = engine.GetCollection<RelDepartment>();
        var employees = engine.GetCollection<RelEmployee>();

        // 创建部门
        var techDept = new RelDepartment { Name = "技术部", Code = "TECH" };
        var hrDept = new RelDepartment { Name = "人事部", Code = "HR" };
        departments.Insert(new[] { techDept, hrDept });

        // 创建员工
        var emp1 = new RelEmployee { Name = "张三", DepartmentId = techDept.Id };
        var emp2 = new RelEmployee { Name = "李四", DepartmentId = techDept.Id };
        var emp3 = new RelEmployee { Name = "王五", DepartmentId = hrDept.Id };
        employees.Insert(new[] { emp1, emp2, emp3 });

        Console.WriteLine($"✅ 初始数据:");
        Console.WriteLine($"   部门: {departments.Count()}个");
        Console.WriteLine($"   员工: {employees.Count()}个");

        // 检查引用完整性 - 删除前验证
        Console.WriteLine("\n🔍 引用完整性检查:");

        // 尝试"安全"删除部门（先检查是否有关联员工）
        var deptToDelete = techDept.Id;
        var hasEmployees = employees.Exists(e => e.DepartmentId == deptToDelete);

        if (hasEmployees)
        {
            var empCount = employees.Count(e => e.DepartmentId == deptToDelete);
            Console.WriteLine($"   ⚠️ 部门 '{techDept.Name}' 有 {empCount} 名员工，不能直接删除");

            // 选项1: 先删除/转移员工
            Console.WriteLine("   🔄 转移员工到其他部门...");
            var empsToTransfer = employees.Find(e => e.DepartmentId == deptToDelete).ToList();
            foreach (var emp in empsToTransfer)
            {
                emp.DepartmentId = hrDept.Id;
            }
            employees.Update(empsToTransfer);
            Console.WriteLine($"   ✅ 已转移 {empsToTransfer.Count} 名员工到 '{hrDept.Name}'");

            // 现在可以安全删除
            departments.Delete(deptToDelete);
            Console.WriteLine($"   ✅ 部门 '{techDept.Name}' 已删除");
        }

        // 验证结果
        Console.WriteLine("\n📊 操作后状态:");
        Console.WriteLine($"   部门: {departments.Count()}个");
        Console.WriteLine($"   人事部员工: {employees.Count(e => e.DepartmentId == hrDept.Id)}人");
    }

    /// <summary>
    /// 级联操作演示
    /// </summary>
    private static void CascadeOperationsDemo(TinyDbEngine engine)
    {
        var orders = engine.GetCollection<RelOrder>();
        var orderItems = engine.GetCollection<RelOrderItem>();

        // 创建订单和订单项
        var order = new RelOrder
        {
            OrderNo = "ORD-001",
            CustomerName = "张三",
            TotalAmount = 0
        };
        orders.Insert(order);

        var items = new[]
        {
            new RelOrderItem { OrderId = order.Id, ProductName = "笔记本电脑", Quantity = 1, UnitPrice = 6999 },
            new RelOrderItem { OrderId = order.Id, ProductName = "鼠标", Quantity = 2, UnitPrice = 99 },
            new RelOrderItem { OrderId = order.Id, ProductName = "键盘", Quantity = 1, UnitPrice = 299 }
        };
        orderItems.Insert(items);

        // 计算并更新订单总金额
        var total = items.Sum(i => i.Quantity * i.UnitPrice);
        order.TotalAmount = total;
        orders.Update(order);

        Console.WriteLine($"✅ 创建订单: {order.OrderNo}");
        Console.WriteLine($"   订单项: {items.Length} 项");
        Console.WriteLine($"   总金额: ¥{total:N2}");

        // 级联删除演示
        Console.WriteLine("\n🗑️ 级联删除订单:");

        // 方法1: 手动级联删除
        var orderIdToDelete = order.Id;

        // 先删除子记录
        var deletedItems = orderItems.DeleteMany(i => i.OrderId == orderIdToDelete);
        Console.WriteLine($"   删除订单项: {deletedItems} 条");

        // 再删除主记录
        var deletedOrders = orders.Delete(orderIdToDelete);
        Console.WriteLine($"   删除订单: {deletedOrders} 条");

        // 验证删除结果
        Console.WriteLine($"\n📊 删除后验证:");
        Console.WriteLine($"   订单数: {orders.Count()}");
        Console.WriteLine($"   订单项数: {orderItems.Count()}");

        // 创建另一个订单演示级联更新
        Console.WriteLine("\n🔄 级联更新演示:");

        var order2 = new RelOrder { OrderNo = "ORD-002", CustomerName = "李四" };
        orders.Insert(order2);

        var items2 = new[]
        {
            new RelOrderItem { OrderId = order2.Id, ProductName = "显示器", Quantity = 1, UnitPrice = 1999 }
        };
        orderItems.Insert(items2);

        // 更新订单（级联重新计算）
        var newItem = new RelOrderItem { OrderId = order2.Id, ProductName = "音箱", Quantity = 1, UnitPrice = 599 };
        orderItems.Insert(newItem);

        // 重新计算总金额
        var updatedTotal = orderItems.Find(i => i.OrderId == order2.Id)
            .Sum(i => i.Quantity * i.UnitPrice);
        order2.TotalAmount = updatedTotal;
        orders.Update(order2);

        Console.WriteLine($"   添加新订单项后，订单 {order2.OrderNo} 总金额更新为: ¥{updatedTotal:N2}");
    }
}

#region 实体类定义

/// <summary>
/// 用户实体（一对一关系）
/// </summary>
[Entity("rel_users")]
public class RelUser
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Username { get; set; } = string.Empty;
    public string Email { get; set; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.Now;
}

/// <summary>
/// 用户详情（一对一关系）
/// </summary>
[Entity("user_profiles")]
public class UserProfile
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public ObjectId UserId { get; set; } // 外键
    public string FullName { get; set; } = string.Empty;
    public string Bio { get; set; } = string.Empty;
    public DateTime? Birthday { get; set; }
    public string AvatarUrl { get; set; } = string.Empty;
}

/// <summary>
/// 作者实体（一对多关系）
/// </summary>
[Entity("authors")]
public class Author
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = string.Empty;
    public string Country { get; set; } = string.Empty;
}

/// <summary>
/// 书籍实体（一对多关系）
/// </summary>
[Entity("books")]
public class Book
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public ObjectId AuthorId { get; set; } // 外键
    public string Title { get; set; } = string.Empty;
    public int PublishedYear { get; set; }
    public string Genre { get; set; } = string.Empty;
}

/// <summary>
/// 学生实体（多对多关系）
/// </summary>
[Entity("students")]
public class Student
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = string.Empty;
    public string StudentNo { get; set; } = string.Empty;
}

/// <summary>
/// 课程实体（多对多关系）
/// </summary>
[Entity("courses")]
public class Course
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = string.Empty;
    public int Credits { get; set; }
    public string Instructor { get; set; } = string.Empty;
}

/// <summary>
/// 选课记录（多对多中间表）
/// </summary>
[Entity("enrollments")]
public class Enrollment
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public ObjectId StudentId { get; set; } // 外键
    public ObjectId CourseId { get; set; } // 外键
    public int? Grade { get; set; }
    public DateTime EnrolledAt { get; set; } = DateTime.Now;
}

/// <summary>
/// 部门实体（引用完整性演示）
/// </summary>
[Entity("rel_departments")]
public class RelDepartment
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = string.Empty;
    public string Code { get; set; } = string.Empty;
}

/// <summary>
/// 员工实体（引用完整性演示）
/// </summary>
[Entity("rel_employees")]
public class RelEmployee
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string Name { get; set; } = string.Empty;
    public ObjectId DepartmentId { get; set; } // 外键
}

/// <summary>
/// 订单实体（级联操作演示）
/// </summary>
[Entity("rel_orders")]
public class RelOrder
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public string OrderNo { get; set; } = string.Empty;
    public string CustomerName { get; set; } = string.Empty;
    public decimal TotalAmount { get; set; }
    public DateTime CreatedAt { get; set; } = DateTime.Now;
}

/// <summary>
/// 订单项实体（级联操作演示）
/// </summary>
[Entity("rel_order_items")]
public class RelOrderItem
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();
    public ObjectId OrderId { get; set; } // 外键
    public string ProductName { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
}

#endregion
