# Kafka with an EF Core outbox

This web API writes an order and its serialized outgoing event in one PostgreSQL transaction. Committing wakes the unordered direct dispatcher; a maintenance worker recovers rows left by failed sends or process death. A serial Kafka consumer logs the committed event. The consumer attaches the EF Core transaction boundary through its fluent declaration; the selected outbox context enables inbox deduplication for the stable `sample-billing` consumer identity. Logging itself is not transactional, so a crash after logging but before the inbox commit can still repeat a log line.

## Start dependencies

Use an isolated development database. For example, with Docker:

```shell
docker run --name relayer-sample-postgres -e POSTGRES_PASSWORD=sample -e POSTGRES_DB=orders -p 5432:5432 -d postgres:18
docker run --name relayer-sample-kafka -p 9092:9092 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -d apache/kafka:4.0.2
```

Wait for PostgreSQL and Kafka to become ready, then create both the source and failure topic explicitly (Kafka topology management remains planned):

```shell
docker exec relayer-sample-postgres pg_isready -U postgres -d orders
docker exec relayer-sample-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic sample-orders --partitions 1 --replication-factor 1
docker exec relayer-sample-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic sample-orders.errors --partitions 1 --replication-factor 1
```

The Docker commands use Bash/zsh line continuations; PowerShell users can put each command on one line. These fixed names and ports are for a single local run; choose unused ones if other applications use them. Use native SASL/TLS builder settings for a secured cluster.

## Run

Bash/zsh:

```shell
export ConnectionStrings__Orders='Host=localhost;Database=orders;Username=postgres;Password=sample'
export Kafka__BootstrapServers='localhost:9092'
dotnet run -c Release --project samples/KafkaEfOutbox -- --urls http://localhost:5081
```

PowerShell uses `$env:ConnectionStrings__Orders = 'Host=localhost;Database=orders;Username=postgres;Password=sample'` and `$env:Kafka__BootstrapServers = 'localhost:9092'`.

On this empty demo database, startup uses EF `EnsureCreatedAsync` for the business table, then the implemented `PostgreSqlOutboxSchema.CreateScript()` for messaging tables. This is not a production migration strategy. Do not point it at an existing application database; deploy reviewed EF migrations and Relayer schema scripts there.

```shell
curl -i -X POST http://localhost:5081/orders/order-1
```

Expect HTTP 202 and `Received committed order order-1`. Use a new ID for each request: the business primary key intentionally rejects duplicates. HTTP 202 confirms the database commit, not broker delivery. Direct dispatch sends in the background, and maintenance retries undispatched rows after its recovery delay. Ctrl+C drains/stops the host before disposing the shared data source.

Inspect committed business and pending outbox data:

```shell
docker exec relayer-sample-postgres psql -U postgres -d orders -c 'SELECT * FROM "Orders";'
docker exec relayer-sample-postgres psql -U postgres -d orders -c 'SELECT id, destination, status FROM relayer_outbox;'
```

After successful dispatch the outbox row is deleted. Delivery is at-least-once: a crash after Kafka accepts a record but before row deletion may send it again. No ordering key is configured: these are unordered rows, not ordered-lane dispatch. Kafka consumers currently process serially; parallel receive remains planned. See [EF enlistment](https://relayer.tomlonghurst.dev/docs/reference/ef-core-outbox), [direct dispatch](https://relayer.tomlonghurst.dev/docs/reference/direct-dispatch) and [recovery](https://relayer.tomlonghurst.dev/docs/reference/postgres-maintenance).

Stop the application first, then remove only these demonstration containers:

```shell
docker rm -f relayer-sample-postgres relayer-sample-kafka
```

Removing the PostgreSQL container discards its demonstration data.

The application [outbox and inbox guide](../../website/docs/guides/outbox.md) walks through this setup. The endpoint uses `SendViaOutboxAsync` to require active enlistment; the consumer configures in-process retries and a dead-letter topic through `Options.Failure`. Kafka delayed redelivery, parallel handlers and batch handlers remain unsupported.
