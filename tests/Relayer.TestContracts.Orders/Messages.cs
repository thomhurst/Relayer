using Relayer;

[assembly: GenerateMessageTypes("AddOrdersContracts")]

namespace Relayer.TestContracts.Orders;

[Message("orders.shared", Version = 2)]
public sealed record Order(int MessageId);
