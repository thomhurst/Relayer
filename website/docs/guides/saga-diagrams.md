---
title: Saga process diagrams
description: "Declare and export a process diagram explicitly, without executing a saga."
---

`Relayer.Sagas` provides explicit process declarations and [Mermaid state diagram](https://mermaid.js.org/syntax/stateDiagram.html) export. Declare the diagram in application code; no assembly scanning or handler discovery runs.

These declarations describe a process; they do not register consumers, persist state, invoke handlers, or schedule timeouts. [Transactional state transitions](sagas.md) are available separately through explicit handlers and the EF Core inbox/outbox boundary. Diagram metadata does not generate or enforce runtime transitions. Durable process timeout handling remains planned in [#249](https://github.com/thomhurst/Relayer/issues/249); existing [scheduled sends](sending.md) remain available.

## Declare and export

Reference `Relayer.Sagas`, then build the diagram once at startup or in a documentation tool:

```csharp
using Relayer.Sagas;

var diagram = new SagaDiagramBuilder()
    .State("Pending", initial: true)
    .State("Paid", final: true)
    .State("Cancelled", final: true)
    .Transition("Pending", "Paid", "PaymentReceived")
    .Transition("Pending", "Cancelled", "OrderExpired")
    .Build();

File.WriteAllText("order-process.mmd", diagram.ToMermaid());
```

The output is:

```text
stateDiagram-v2
    state "Cancelled" as s0
    s0 --> [*]
    state "Paid" as s1
    s1 --> [*]
    state "Pending" as s2
    [*] --> s2
    s2 --> s0 : OrderExpired
    s2 --> s1 : PaymentReceived
```

Open the `.mmd` file in a Mermaid renderer, or place its contents in a Markdown `mermaid` code fence supported by your documentation host. `ToMermaid()` returns text and does not write files or contact a service.

## Declaration rules

- Names are case-sensitive and cannot be null, empty, or whitespace-only. Trigger names are descriptive labels, not CLR type names or message registrations.
- Declare each state once and exactly one initial state. A process may have no final state; a single state may be both initial and final.
- Both transition endpoints must be declared by `Build()`. Transitions may be declared before their states.
- Final states cannot have outgoing transitions. Loops and branching from other states are supported. An identical source/destination/trigger declaration is rejected.
- States without transitions are retained. The diagram does not assert reachability or that a running process will finish.

`Build()` validates the whole declaration and returns an immutable snapshot. Later builder changes cannot change an existing diagram. `States` and `Transitions` expose read-only metadata for inspection.

Export sorts states and transitions by ordinal names and uses LF line endings. Equivalent declarations produce identical output regardless of insertion order or current culture. Generated identifiers (`s0`, `s1`, …) keep labels separate from syntax; punctuation, whitespace and Unicode are written as Mermaid numeric entities. Renderers display the labels. Export is startup/tooling work and adds no per-message work.
