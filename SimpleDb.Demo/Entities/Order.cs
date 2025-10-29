using SimpleDb.Attributes;
using SimpleDb.Bson;

namespace SimpleDb.Demo.Entities;

[Entity("orders")]
public class Order
{
    public ObjectId Id { get; set; } = ObjectId.NewObjectId();

    public string OrderNumber { get; set; } = string.Empty;
    public ObjectId CustomerId { get; set; }
    public List<OrderItem> Items { get; set; } = new();
    public decimal TotalAmount { get; set; }
    public OrderStatus Status { get; set; } = OrderStatus.Pending;
    public DateTime OrderDate { get; set; } = DateTime.UtcNow;
    public DateTime? ShippedDate { get; set; }
    public string? ShippingAddress { get; set; }
}

public class OrderItem
{
    public ObjectId ProductId { get; set; }
    public string ProductName { get; set; } = string.Empty;
    public int Quantity { get; set; }
    public decimal UnitPrice { get; set; }
    public decimal LineTotal => Quantity * UnitPrice;
}

public enum OrderStatus
{
    Pending,
    Confirmed,
    Shipped,
    Delivered,
    Cancelled
}