using Relayer;

[assembly: GenerateMessageTypes("AddBillingContracts")]

namespace Relayer.TestContracts.Billing;

[Message("billing.shared", Version = 2)]
public sealed record Payment(int MessageId);
