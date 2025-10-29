using TinyDb.Bson;

namespace TinyDb.IdGeneration;

/// <summary>
/// ID生成器接口
/// </summary>
public interface IIdGenerator
{
    /// <summary>
    /// 生成新的ID值
    /// </summary>
    /// <param name="entityType">实体类型</param>
    /// <param name="idProperty">ID属性信息</param>
    /// <param name="sequenceName">序列名称（可选）</param>
    /// <returns>生成的ID值</returns>
    BsonValue GenerateId(Type entityType, System.Reflection.PropertyInfo idProperty, string? sequenceName = null);

    /// <summary>
    /// 检查是否支持指定的ID类型
    /// </summary>
    /// <param name="idType">ID类型</param>
    /// <returns>是否支持</returns>
    bool Supports(Type idType);
}