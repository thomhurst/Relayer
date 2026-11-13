import {useState} from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';
import CodeBlock from '@theme/CodeBlock';
import styles from './index.module.css';

// The same logical message on two native bindings. The envelope and payload never change;
// only the native shell and the CloudEvents header prefix do. Prefixes follow the CloudEvents
// Kafka and AMQP binary bindings documented in docs/transports/envelopes.
const shells = [
  {
    id: 'kafka',
    label: 'Kafka',
    client: 'Dekaf producer record',
    native: [
      ['topic', 'orders.placed'],
      ['key', 'c-42'],
    ],
    prefix: 'ce_',
  },
  {
    id: 'servicebus',
    label: 'Azure Service Bus',
    client: 'ServiceBusMessage',
    native: [
      ['queue', 'orders.placed'],
      ['SessionId', 'c-42'],
    ],
    prefix: 'cloudEvents:',
  },
];

const envelope = [
  ['id', '0193c0de-5b1a-7c2e-9f41-6a0d2b7e1c55'],
  ['type', 'orders.placed;v=1'],
  ['source', '/orders'],
  ['time', '2026-11-01T09:30:00.0000000Z'],
  ['specversion', '1.0'],
];

const payload = '{ "orderId": "A-1001", "customerId": "c-42" }';

function Layers() {
  const [active, setActive] = useState(shells[0].id);
  const shell = shells.find((item) => item.id === active);

  const onKeyDown = (event) => {
    const index = shells.findIndex((item) => item.id === active);
    const next = {ArrowRight: index + 1, ArrowLeft: index - 1 + shells.length, Home: 0, End: shells.length - 1}[event.key];
    if (next === undefined) {
      return;
    }
    event.preventDefault();
    const target = shells[next % shells.length];
    setActive(target.id);
    document.getElementById(`shell-tab-${target.id}`)?.focus();
  };

  return (
    <figure className={styles.figure}>
      <div className={styles.tabs} role="tablist" aria-label="Native transport">
        {shells.map((item) => (
          <button
            key={item.id}
            type="button"
            role="tab"
            id={`shell-tab-${item.id}`}
            aria-selected={item.id === active}
            aria-controls="shell-panel"
            tabIndex={item.id === active ? 0 : -1}
            className={clsx(styles.tab, item.id === active && styles.tabActive)}
            onClick={() => setActive(item.id)}
            onKeyDown={onKeyDown}>
            {item.label}
          </button>
        ))}
      </div>
      <div id="shell-panel" role="tabpanel" aria-labelledby={`shell-tab-${shell.id}`} className={styles.native}>
        <p className={styles.layerName}>
          <strong>{shell.client}</strong>
          <span>sent and settled by the native client</span>
        </p>
        <dl className={styles.fields}>
          {shell.native.map(([name, value]) => (
            <div key={name}>
              <dt>{name}</dt>
              <dd>{value}</dd>
            </div>
          ))}
        </dl>
        <div className={styles.relayer}>
          <p className={styles.layerName}>
            <strong>CloudEvents envelope</strong>
            <span>written by Relayer as native headers</span>
          </p>
          <dl className={styles.fields}>
            {envelope.map(([name, value]) => (
              <div key={name}>
                <dt>
                  <span className={styles.prefix}>{shell.prefix}</span>
                  {name}
                </dt>
                <dd>{value}</dd>
              </div>
            ))}
            <div>
              <dt>traceparent</dt>
              <dd>00-4bf92f35…-00f067aa…-01</dd>
            </div>
          </dl>
          <div className={styles.payload}>
            <p className={styles.layerName}>
              <strong>OrderPlaced</strong>
              <span>your record, serialized with source-generated JSON</span>
            </p>
            <code>{payload}</code>
          </div>
        </div>
      </div>
      <figcaption>
        Switch the transport: the envelope and your message stay the same. Only the native client changes.
      </figcaption>
    </figure>
  );
}

function Hero() {
  return (
    <header className={styles.hero}>
      <div className={clsx('container', styles.heroGrid)}>
        <div className={styles.heroCopy}>
          <Heading as="h1" className={styles.wordmark}>
            Relayer
          </Heading>
          <p className={styles.lede}>
            Messaging for .NET that wraps your broker’s own client. Relayer handles retries, dead-lettering, the outbox,
            tracing and serialization. Kafka, Service Bus, SQS and RabbitMQ keep sending, ordering and settling messages
            the way they already do.
          </p>
          <div className={styles.actions}>
            <Link className={styles.primary} to="/docs/intro">
              Read the docs
            </Link>
            <Link className={styles.secondary} to="https://github.com/thomhurst/Relayer">
              View on GitHub
            </Link>
          </div>
          <p className={styles.heroStatus}>
            Early development. The core pipeline is implemented; transports and NuGet packages are not released yet.
          </p>
        </div>
        <Layers />
      </div>
    </header>
  );
}

const split = {
  relayer: [
    ['Serialization', 'System.Text.Json source generation by default; optional Protobuf and MessagePack with explicit generated contracts and Native AOT coverage, and startup-prepared Avro/schema registry (AOT/trimming currently unsupported).'],
    ['Message identity', 'Stable names like orders.placed;v=1 on the wire, never CLR type names.'],
    ['Message versioning', 'Explicit upcaster chains upgrade older contracts before single-message and batch handlers.'],
    ['Failure handling', 'Classification, immediate retries, delayed redelivery, dead-letter copies with full history.'],
    ['Outbox and inbox', 'Sends inside a transaction are enlisted automatically.'],
    ['Telemetry', 'OpenTelemetry messaging conventions, with one ActivitySource and one Meter.'],
    ['Startup validation', 'Routes, handlers, scheduling, transaction requirements and endpoint capabilities are checked before consumers start.'],
  ],
  native: [
    ['Sending and receiving', 'Dekaf, the Azure SDK, the AWS SDK and RabbitMQ.Client do the I/O.'],
    ['Settlement', 'Complete, abandon, ack, commit. Relayer only decides which one.'],
    ['Ordering', 'Partition keys, sessions and FIFO message groups, used as they are.'],
    ['Dead-letter queues', 'Native DLQs where the broker has them, with error metadata attached.'],
    ['Configuration', 'ProducerBuilder, ServiceBusClientOptions, ConnectionFactory, passed through unchanged.'],
    ['The raw message', 'Always one call away from a handler, through the feature collection.'],
  ],
};

function Split() {
  return (
    <section className={styles.section}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          Who does what
        </Heading>
        <p className={styles.sectionLede}>
          Relayer is not another broker abstraction. It owns the work that is the same on every broker and leaves the
          rest to the client you already trust.
        </p>
        <div className={styles.split}>
          <div className={clsx(styles.column, styles.columnRelayer)}>
            <Heading as="h3">Relayer</Heading>
            <dl>
              {split.relayer.map(([term, text]) => (
                <div key={term}>
                  <dt>{term}</dt>
                  <dd>{text}</dd>
                </div>
              ))}
            </dl>
          </div>
          <div className={clsx(styles.column, styles.columnNative)}>
            <Heading as="h3">The native client</Heading>
            <dl>
              {split.native.map(([term, text]) => (
                <div key={term}>
                  <dt>{term}</dt>
                  <dd>{text}</dd>
                </div>
              ))}
            </dl>
          </div>
        </div>
      </div>
    </section>
  );
}

const callSite = `[Message("orders.placed", Version = 1)]
public sealed record OrderPlaced(string OrderId, string CustomerId);

public sealed class OrderPlacedHandler(AppDbContext db) : IHandler<OrderPlaced>
{
    public async ValueTask HandleAsync(OrderPlaced message, MessageContext context,
        CancellationToken cancellationToken)
    {
        db.Invoices.Add(Invoice.For(message));
        await db.SaveChangesAsync(cancellationToken);
    }
}

services.AddRelayer(relayer =>
{
    relayer.AddMessageTypesFromThisAssembly();
    relayer.AddTransport<BrokerOptions, RouteOptions, ConsumerOptions>("events", broker =>
    {
        broker.Send<OrderPlaced>("orders.placed",
            route => route.OrderingKey(static m => m.CustomerId));
        broker.Consume("orders.placed",
            consumer => consumer.Handler<OrderPlacedHandler>().Concurrency(8));
    });
});

await sender.SendAsync(new OrderPlaced("A-1001", "c-42"), cancellationToken);`;

function CallSite() {
  return (
    <section className={clsx(styles.section, styles.sectionAlt)}>
      <div className={clsx('container', styles.callSite)}>
        <div>
          <Heading as="h2" className={styles.sectionTitle}>
            A message, a handler, a route
          </Heading>
          <p className={styles.sectionLede}>
            Registration is explicit. There is no assembly scanning: the source generator writes the registration code,
            and you can read it.
          </p>
          <ul className={styles.points}>
            <li>Analyzers catch a missing message name or an unregistered handler at compile time.</li>
            <li>Startup fails with every configuration error listed together, before a consumer runs.</li>
            <li>An ordering key on an endpoint that cannot order messages is an error, not a silent no-op.</li>
          </ul>
          <Link to="/docs/getting-started">Walk through it step by step</Link>
        </div>
        <CodeBlock language="csharp" title="Program.cs">
          {callSite}
        </CodeBlock>
      </div>
    </section>
  );
}

const status = [
  ['Saga process declarations and Mermaid export', 'Implemented diagram tooling', true],
  ['Durable saga timeouts through transactional scheduled sends', 'Implemented', true],
  ['NuGet packaging, symbols and tag release workflow', 'Implemented; packages not released yet', true],
  ['Configuration, routes and startup validation', 'Implemented', true],
  ['Source-generated handlers, messages and JSON', 'Implemented', true],
  ['Native AOT handler round trips and trace propagation in CI', 'Implemented', true],
  ['Batch dispatch and receive executor composition', 'Implemented core seam', true],
  ['Send pipeline and middleware', 'Implemented', true],
  ['Consume pipeline and receive SPI', 'Implemented', true],
  ['Message upcaster chains for single and batch handlers', 'Implemented', true],
  ['Failure classification, retries and dead-letter copies', 'Implemented', true],
  ['Service Bus consumer failure circuits', 'Implemented', true],
  ['Service Bus dead-letter management and optional authorized HTTP endpoints', 'Implemented', true],
  ['Service Bus DLQ and Kafka dead-letter inspection, filtered redrive and native purge', 'Implemented', true],
  ['Manual settlement requests with exception precedence', 'Implemented', true],
  ['Request/response with bounded state and mandatory timeouts', 'Implemented', true],
  ['Declared topology, JSON and SQS/SNS Terraform export', 'Implemented', true],
  ['Service Bus Bicep and Terraform infrastructure export', 'Implemented API and CLI', true],
  ['In-memory transport for tests and samples', 'Implemented', true],
  ['Runnable samples and migration guides', 'Implemented', true],
    ['Service Bus receive binding and manual settlement', 'Implemented', true],
    ['Service Bus sending, client ownership and topology', 'Implemented', true],
    ['Service Bus processors, sessions, telemetry and health', 'Implemented', true],
  ['Packaged transport conformance suite', 'Implemented', true],
  ['Test harness and standalone handler context', 'Implemented', true],
  ['CI performance regression gate, in-memory and native-client benchmarks', <>Implemented, including Kafka and RabbitMQ broker-backed receive comparisons; remaining coverage planned. <Link to="/docs/transports/kafka#performance-validation">Kafka comparison scope</Link></>, true],
  ['Kafka producers, serial consumers, typed tombstones, dead-letter copies, circuit breaking and offset/timestamp replay', 'Implemented', true],
  ['Named transport serializers and independent Kafka JSON schema registries', 'Implemented; explicit startup contracts', true],
  ['Optional Protobuf and MessagePack serializers', 'Implemented; explicit generated contracts and Native AOT coverage', true],
  ['Avro with generated codecs and startup-prepared schema registry', 'Implemented; Native AOT/trimming unsupported', true],
  ['Fallback partition watermarks, key scheduling, backpressure and bounded drain', 'Implemented core SPI; native runtime first', true],
  ['SQS receiving, health and SNS topic/queue/subscription topology', 'Implemented', true],
  ['RabbitMQ topology, confirmed sends, receive/settlement and health', 'Implemented', true],
  ['RabbitMQ Streams native connection and topology', 'Implemented; message execution awaits native client release', true],
  ['Google Cloud Pub/Sub native publishing, streaming pull, durable poison copies and topology', 'Implemented; prepared-client Native AOT coverage', true],
  ['NATS JetStream', 'Implemented; native comparison validated', true],
  ['Apache Pulsar transport', 'Planned; Native AOT prerequisite', false],
  ['AsyncAPI topology and JSON contract export', 'Implemented; protocol bindings unsupported', true],
  ['Kafka parallel receive via Dekaf', 'In progress', false],
  ['Health checks with application-provided adapters', 'Implemented', true],
  ['Aspire resource connections, health and telemetry', 'Implemented for Kafka and Service Bus', true],
  ['Scoped tenant header propagation', 'Implemented with durable binding and bounded PostgreSQL catalog dispatch', true],
  ['Durable outbox contracts and keyed health adapter', 'Implemented core seam', true],
  ['PostgreSQL schema scripts, CLI and EF migration path', 'Implemented schema tooling', true],
  ['PostgreSQL durable reads and fenced claims', 'Implemented store operations', true],
  ['SQL Server outbox transactions and durable stores', 'ADO.NET/Dapper enlistment, atomic consumer inbox, fenced claims and unordered commit dispatch implemented; leased recovery, poison isolation and retention implemented; ordered lane workers implemented; EF Core planned', true],
  ['MySQL store, transactions and recovery', 'Implemented ADO.NET/Dapper, atomic inbox, unordered/ordered sends, poison handling and retention', true],

  ['EF Core transaction enlistment for PostgreSQL', 'Implemented enqueue path', true],
  ['PostgreSQL ADO.NET and Dapper transactions', 'Implemented enqueue path', true],
  ['Unordered PostgreSQL direct dispatch after EF commit', 'Implemented direct path', true],
  ['EF Core consumer inbox for PostgreSQL', 'Implemented deduplication, isolation levels and bounded conflict replay', true],
  ['Saga state transitions on the EF Core inbox/outbox', 'Implemented', true],
  ['Kafka connectivity and receiver liveness health adapters', 'Implemented', true],
  ['Service Bus, SQS and RabbitMQ connectivity and receiver liveness health adapters', 'Implemented', true],
  ['Ordered PostgreSQL dispatch and lane leases', 'Implemented with consumer insert sequences', true],
  ['PostgreSQL poison-row limits, key parking and dead-letter management', 'Implemented', true],
  ['PostgreSQL outbox recovery and retention cleanup', 'Implemented', true],
  ['PostgreSQL message lookup API, CLI and bounded sent history', 'Implemented', true],
  ['PostgreSQL CDC outbox dispatch', 'Implemented; SQL Server CDC planned', true],
  ['Native scoped tenant store selection', 'Implemented with authorized durable binding and bounded PostgreSQL catalog dispatch', true],
  ['Send and delivery tracing, settlement, metrics and logs', 'Implemented', true],
  ['PostgreSQL durable local work transport', 'Implemented with native transactional completion and shared dispatch', true],
  ['PostgreSQL outbox tracing and metrics', 'Implemented', true],
  ['SQS and RabbitMQ delivery tracing, metrics and settlement telemetry', <>Implemented; process and native settlement instrumentation, no Relayer receive spans. <Link to="/docs/guides/telemetry">Telemetry scope</Link></>, true],
];

function Status() {
  return (
    <section className={styles.section}>
      <div className="container">
        <Heading as="h2" className={styles.sectionTitle}>
          Where it stands
        </Heading>
        <p className={styles.sectionLede}>
          Relayer is built in the open. This is what works on <code>main</code> today.
        </p>
        <table className={styles.statusTable}>
          <tbody>
            {status.map(([area, state, done]) => (
              <tr key={area}>
                <th scope="row">{area}</th>
                <td className={done ? styles.done : styles.pending}>{state}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p>
          Implemented benchmark cases do not establish that the 5% throughput overhead budget
          against raw native clients has passed across transports and send/receive paths.
        </p>
        <Link to="/docs/roadmap">See the full roadmap</Link>
      </div>
    </section>
  );
}

export default function Home() {
  return (
    <Layout
      title="Messaging for .NET that wraps your broker’s client"
      description="Relayer wraps the native client for Kafka, Azure Service Bus, Amazon SQS/SNS and RabbitMQ, and adds resiliency, outbox/inbox, telemetry, serialization, topology and testing.">
      <Hero />
      <main>
        <Split />
        <CallSite />
        <Status />
      </main>
    </Layout>
  );
}
