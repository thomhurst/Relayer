using Relayer;
using Relayer.TestContracts.Orders;

[assembly: GenerateMessageTypes("AddBillingContracts")]

namespace Relayer.TestContracts.Billing;

[Message("billing.shared", Version = 2)]
public sealed record Payment(int MessageId);

[Message("billing.order-audit")]
public sealed record OrderAudit(Order Order);
