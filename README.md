# CDC: Setup and Implementation Documentation


## Overview

CDC is a Docker Compose stack that streams row-level changes from two MySQL databases (`ecommerce` and `hr`) into PostgreSQL in near real time. It uses Debezium, Kafka and Kafka Connect, with Prometheus and Grafana for monitoring.

- Debezium 2.7.3 reads the MySQL binary log (ROW format) and captures inserts, updates and deletes.
- Each table gets its own Kafka topic, named `transactions.<database>.<table>`.
- JDBC sink connectors write the changes into the matching PostgreSQL tables (`ecommerce.*`, `hr.*`) in upsert mode.
- Single Message Transformations (SMTs) cast types and drop or rename columns on the way.
- Prometheus scrapes MySQL, PostgreSQL, Kafka and the Debezium JVM; Grafana displays them.

| Component | Where it runs | Version and port |
| --- | --- | --- |
| MySQL (source) | Bare metal on the host, not in the compose file | 3306 |
| PostgreSQL (target) | Bare metal on the host, not in the compose file | 16 (per the setup guide), 5433 |
| Zookeeper | Docker | `cp-zookeeper:7.5.0`, 2181 |
| Kafka | Docker | `cp-kafka:7.5.0`, 9092 |
| Debezium Connect | Docker, built from `Dockerfile.debezium` | `debezium/connect:2.7.3.Final`, REST 8083, JMX metrics 9404 |
| Kafka UI | Docker | 8080 |
| Prometheus | Docker | `v2.40.0`, 9090 |
| Grafana | Docker | `9.2.0`, 3000 |
| Kafka, MySQL and PostgreSQL exporters | Docker | 9308, 9104, 9187 |

The two databases stay on the host. Containers reach them through `172.17.0.1`, the default Docker bridge gateway on Linux.

## Architecture and project structure

<img width="734" height="362" alt="image" src="https://github.com/user-attachments/assets/2d2bb5b6-3ede-46d8-ad27-5d945edc00ea" />


MySQL changes travel left to right through Debezium and Kafka; the JDBC sink applies them to PostgreSQL. The dashed lines show that one Debezium Connect worker hosts both the source and the sink connectors.

### Repository layout

| Path | Purpose |
| --- | --- |
| `docker-compose.yml` | Zookeeper, Kafka, Debezium Connect, Kafka UI, Prometheus, Grafana and the three exporters |
| `Dockerfile.debezium` | Extends `debezium/connect:2.7.3.Final` with the Prometheus JMX agent 0.20.0 on port 9404 |
| `my.cnf` | Client credentials file read by the MySQL exporter |
| `connectors/mysql_ecommerce_source.json`, `mysql_hr_source.json` | Debezium MySQL source connectors |
| `connectors/eccommerce_sink_connectors/` | Five Debezium JDBC sink configs (folder name is spelled this way in the repo) |
| `connectors/hr_sink_connectors/` | Six Confluent JDBC sink configs |
| `connectors/pg_sink_hr.json` | Older single sink for all six HR topics, using different topic names |
| `postgres/` | `create_postgres_tables.sql`, `pgwriter.sql`, `publications.sql` |
| `config/prometheus/`, `config/grafana/`, `config/jmx-exporter/` | Prometheus scrape jobs, Grafana data source, JMX exporter rules |
| `data_generator/` | Faker-based test data script and `requirements.txt` |
| `how_to_setup.md`, `monitoring.md` | The original working notes this document builds on |

### Topics and tables

Every table becomes one topic named `transactions.<database>.<table>`, for example `transactions.ecommerce.orders`. Each sink writes to the same-named table in the `ecommerce` or `hr` schema of the PostgreSQL `transactions` database.

## Setup

Setup takes five steps: prepare MySQL, prepare PostgreSQL, set credentials, start the stack, then deploy the connectors (covered under Implementation).

### Prerequisites

- A Linux host with Docker and Docker Compose v2. The `172.17.0.1` addresses in the configs assume the default Docker bridge.
- MySQL 8 on the host (port 3306) with `ecommerce` and `hr` databases already created and populated.
- PostgreSQL on the host (the guide uses version 16) listening on port 5433.
- `curl` and `jq` for the Kafka Connect REST calls.
- Python 3 only if you use the data generator.

### 1. Prepare MySQL

Create a Debezium user and, optionally, a read-only metrics user for the exporter. Choose your own passwords.

```sql
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY '<strong-password>';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES ON *.* TO 'debezium'@'%';

CREATE USER IF NOT EXISTS 'metrics'@'%' IDENTIFIED BY '<metrics-password>';
GRANT PROCESS, REPLICATION CLIENT ON *.* TO 'metrics'@'%';
GRANT SELECT ON performance_schema.* TO 'metrics'@'%';
FLUSH PRIVILEGES;
```

Enable row-level binary logging in `/etc/mysql/mysql.conf.d/mysqld.cnf` under `[mysqld]`, then restart MySQL with `sudo systemctl restart mysql`.

```ini
server-id                  = 1
log_bin                    = mysql-bin
binlog_format              = ROW
binlog_row_image           = FULL
binlog_expire_logs_seconds = 604800
bind-address               = 0.0.0.0
```

Verify with `SHOW VARIABLES LIKE 'log_bin'` and the `binlog_format`, `binlog_row_image` and `server_id` variables. Expected values are ON, ROW, FULL and 1.

### 2. Prepare PostgreSQL

Create the `transactions` database, then run the scripts in this order. The grants in `pgwriter.sql` only cover tables that already exist.

```bash
psql -p 5433 -U postgres -c 'CREATE DATABASE transactions;'
psql -p 5433 -U postgres -d transactions -f postgres/create_postgres_tables.sql
psql -p 5433 -U postgres -d transactions -f postgres/pgwriter.sql
```

`create_postgres_tables.sql` creates 13 tables and their indexes: seven in `ecommerce` and six in `hr`. `pgwriter.sql` creates the `pgwriter` role the sink connectors use, with rights on both schemas and their sequences. Replace the placeholder password `PG_PASS` in that script and in the sink configs.

For the Postgres exporter, create a `metrics` user and grant it the built-in monitoring role:

```sql
CREATE USER metrics WITH PASSWORD '<metrics-password>';
GRANT pg_monitor TO metrics;
```

### 3. Set credentials

The repo ships with placeholder or example credentials in several files. Replace them before use:

| File | What to change |
| --- | --- |
| `connectors/mysql_*_source.json` | `database.password` is a literal value committed to the repo. Rotate that MySQL password and keep the new one out of version control |
| `connectors/*sink_connectors/*.json` | `connection.password` is the placeholder `PG_PASS` |
| `docker-compose.yml` | `DATA_SOURCE_NAME` for both exporters, and `GF_SECURITY_ADMIN_PASSWORD` for Grafana (currently `admin`) |
| `my.cnf` | `password` for the `metrics` MySQL user |

`.gitignore` already excludes `.env`, so the safest route is a `.env` file that Compose reads, for example `DATA_SOURCE_NAME: "metrics:${METRICS_MYSQL_PASSWORD}@tcp(172.17.0.1:3306)/"`.

### 4. Start the stack

```bash
docker compose up -d --build
docker compose ps
curl -s http://localhost:8083/ | jq .
```

Debezium Connect waits 30 seconds for Kafka before it starts, so allow up to a couple of minutes. When `curl` returns the Connect version, the worker is ready. These ports should then answer:

| Service | URL |
| --- | --- |
| Kafka Connect REST | http://localhost:8083 |
| Kafka UI | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |
| Exporter metrics | ports 9104 (MySQL), 9187 (PostgreSQL), 9308 (Kafka), 9404 (Debezium JMX) |

## Implementation

One source connector per MySQL database publishes change events to Kafka, and one sink connector per table applies them to PostgreSQL. Connector JSON files are posted to the Kafka Connect REST API.

### Source connectors

`mysql_ecommerce_source.json` is the reference configuration. Its main settings:

| Setting | Value | Effect |
| --- | --- | --- |
| `connector.class` | `io.debezium.connector.mysql.MySqlConnector` | Reads the MySQL binlog |
| `database.include.list`, `table.include.list` | `ecommerce`, `ecommerce.*` | Captures every table in the database |
| `topic.prefix` | `transactions` | Topics are `transactions.ecommerce.<table>` |
| `database.server.id` | 5403 (ecommerce), 5404 (hr) | Must be unique among MySQL replication clients |
| `snapshot.mode` | `initial` | Copies existing rows first, then follows the binlog |
| `incremental.snapshot.enabled` | `true` | Allows re-snapshotting tables later |
| `decimal.handling.mode` | `string` | Keeps DECIMAL values exact; the sink casts them |
| `time.precision.mode`, `connect.timezone` | `connect`, `UTC` | Dates and timestamps use Kafka Connect types in UTC |
| `schema.history.internal.kafka.*` | topic `dbhistory-ecommerce` | Stores table DDL history |
| `transforms` | `unwrap`, `bit2boolean` | Flattens the Debezium envelope; casts `is_active` and `is_paid` from MySQL bit to boolean |

The `unwrap` transform (`ExtractNewRecordState`) turns each event into a plain row of the new values. In the ecommerce connector, deletes are rewritten with a `__deleted` field instead of being dropped.

The HR connector (`mysql_hr_source.json`) is similar but sets `heartbeat.interval.ms` to 5000, uses schema-less JSON (`schemas.enable` false), and drops delete events.

### Sink connectors

Every ecommerce sink follows the same pattern with Debezium's JDBC sink (`io.debezium.connector.jdbc.JdbcSinkConnector`):

- `insert.mode` is `upsert` and the primary key comes from the Kafka record key (`primary.key.mode` set to `record_key`), so replays are idempotent.
- `auto.create` and `auto.evolve` are `false`: the PostgreSQL tables from `create_postgres_tables.sql` are the schema.
- `table.name.format` sends each topic to a fixed `schema.table`.
- `ReplaceField` and `Cast` transforms make the Kafka record match the PostgreSQL table.

The transforms exist because MySQL has more columns than the PostgreSQL tables, and decimals arrive as strings.

| Sink | Primary key | Columns dropped | Renamed | Cast to float64 |
| --- | --- | --- | --- | --- |
| customers | `customer_id` | `uuid`, `created_at`, `updated_at`, `__deleted` | none | `account_balance` |
| products | `product_id` | `uuid`, `cost`, `min_stock_level`, `max_stock_level`, `is_available`, `manufacturer`, `sku`, `created_at`, `updated_at`, `rating`, `__deleted` | `product_metadata` to `tags` | `price`, `weight` |
| orders | `order_id` | `uuid`, `order_date`, `billing_address`, `shipping_cost`, `tax_amount`, `discount_amount`, `tracking_number`, `estimated_delivery`, `actual_delivery`, `order_notes`, `__deleted` | none | `total_amount` |
| order\_items | `order_item_id` | `total_price`, `discount_percentage`, `__deleted` | none | `unit_price` |
| payments | `payment_id` | `uuid`, `payment_date`, `transaction_id`, `card_last_four`, `card_brand`, `payment_gateway`, `currency`, `__deleted` | `payment_method` to `method`, `payment_status` to `status` | `amount` |

The six HR sinks in `connectors/hr_sink_connectors/` use the Confluent JDBC sink (`io.confluent.connect.jdbc.JdbcSinkConnector`) instead. Each reads `transactions.hr.<table>`, routes it to `hr.<table>` with a `RegexRouter`, and casts key columns to `int32` (and `salary` to `decimal` for salaries). They connect to `jdbc:postgresql://postgres:5432/transactions`.

The `shipments` and `reviews` tables exist in PostgreSQL and are captured by the ecommerce source, but the repo has no sink connector for them yet.

### Deploy the connectors

Deploy the source first, wait for the initial snapshot, then deploy the sinks.

```bash
curl -X POST -H 'Content-Type: application/json' \
  --data @connectors/mysql_ecommerce_source.json \
  http://localhost:8083/connectors

sleep 30
curl -s http://localhost:8083/connectors/mysql-ecommerce-source/status | jq .

for f in connectors/eccommerce_sink_connectors/*.json; do
  curl -X POST -H 'Content-Type: application/json' --data @"$f" http://localhost:8083/connectors
done
```

Each connector and each task should report `"state": "RUNNING"`. Repeat the same steps with `mysql_hr_source.json` and `connectors/hr_sink_connectors/*.json` for the HR database.

### Change a connector

A connector is changed by pausing, deleting and re-posting it. Replace `<name>` with the connector name.

```bash
curl -s http://localhost:8083/connectors/<name>/config | jq
curl -X PUT    http://localhost:8083/connectors/<name>/pause
curl -X PUT    http://localhost:8083/connectors/<name>/resume
curl -X DELETE http://localhost:8083/connectors/<name>
```

### Test data generator

`data_generator/mysql_data_generator.py` uses Faker to create both schemas, their tables and sample rows: 100 customers, 50 products, 200 orders, 500 order items, 200 payments, 200 shipments, 10 departments, 100 employees, 300 attendance rows, 100 salaries, 20 projects and 200 employee-project links. Install its dependencies with `pip install -r data_generator/requirements.txt`. As written it connects with `pyodbc` to SQL Server, not MySQL (see Troubleshooting).

## Running, testing and monitoring

A change in MySQL should appear in PostgreSQL within seconds once the sink is running. Test it with a small update, then watch the metrics.

### Test replication

1. Change a row in MySQL:

   ```sql
   UPDATE ecommerce.products SET stock_quantity = stock_quantity + 1 WHERE product_id = 1;
   ```
2. Open Kafka UI at http://localhost:8080, go to Topics and open `transactions.ecommerce.products`. A new message should appear.
3. Confirm the value in PostgreSQL:

   ```sql
   SELECT product_id, stock_quantity FROM ecommerce.products WHERE product_id = 1;
   ```
4. Compare row counts between the two databases for each table after the initial snapshot finishes.

### Monitoring

Prometheus (`config/prometheus/prometheus.yml`) scrapes five jobs. Grafana loads the Prometheus data source automatically from `config/grafana/provisioning/datasources/prometheus.yml`. Log in at http://localhost:3000 and change the default admin password straight away.

| Job | Target | Interval |
| --- | --- | --- |
| `prometheus` | `localhost:9090` | 15 s |
| `mysql-exporter` | `mysql-exporter:9104` | 10 s |
| `postgres-exporter` | `postgres-exporter:9187` | 10 s |
| `kafka-exporter` | `kafka-exporter:9308` | 10 s |
| `debezium-connect` | `debezium-connect:9404` (JMX agent) | 10 s |

Check that all targets are up with `curl -s http://localhost:9090/api/v1/targets | jq '.data.activeTargets[].labels.job'`.

The repo does not include Grafana dashboards. Compose mounts `./config/grafana/dashboards`, but that folder is not in the repo, so create dashboards or import community ones for MySQL, PostgreSQL and Kafka. Useful metrics to start with:

| Source | Metric | What it tells you |
| --- | --- | --- |
| MySQL | `mysql_global_status_questions` | Query rate |
| MySQL | `mysql_global_status_innodb_rows_inserted` | Insert volume |
| PostgreSQL | `pg_stat_user_tables_n_tup_ins` | Rows inserted per table, to confirm the sink is writing |
| PostgreSQL | `pg_stat_activity_count` | Active connections |
| Kafka | `kafka_consumer_group_lag` | Whether the sinks are keeping up |
| Kafka | `kafka_topic_partitions` | Topics created per table |
| Debezium JMX | `kafka_connect_*` and `debezium_*` series | Connector status and event counts |

### Day-to-day operations

```bash
docker compose logs -f debezium-connect     # connector errors appear here
docker compose restart debezium-connect
curl -s http://localhost:8083/connectors?expand=status | jq
docker compose down                         # stop; named volumes keep Prometheus and Grafana data
```

Kafka topic data lives in the Kafka container, which has no volume in the compose file, so `docker compose down` followed by `up` starts Kafka empty. Re-deploy the connectors after that; the connector offsets stored in Kafka are gone too, so the source connector repeats its initial snapshot.

## Troubleshooting and known issues

Start with the connector status and the Debezium Connect logs; most failures name the column or setting at fault. The second table lists mismatches found while reading the repo. I did not run the stack, so treat those as items to verify, not confirmed failures.

### Common issues

| Issues | Potential cause | Fix |
| --- | --- | --- |
| Connector or task shows `FAILED` | Bad config or unreachable database | Read the `trace` in `/connectors/<name>/status` and `docker compose logs debezium-connect` |
| Source cannot connect to MySQL | `bind-address`, grants or host address wrong | Check the `debezium` user grants and that `172.17.0.1:3306` is reachable from the container |
| Source starts but captures nothing | Binary log not in ROW format | Verify `log_bin`, `binlog_format` and `binlog_row_image` |
| Sink error: column does not exist | MySQL has columns PostgreSQL lacks and `auto.evolve` is false | Add the column to the `dropFields` list, or to the PostgreSQL table |
| Sink error on numeric columns | DECIMAL arrives as a string | Add the column to the `Cast$Value` spec (`float64`) |
| Sink error: permission denied | `pgwriter` lacks rights on a new table | Re-run `postgres/pgwriter.sql` |
| No messages for a table in Kafka UI | Source not `RUNNING`, snapshot unfinished, or table outside `table.include.list` | Check source status and wait for the snapshot |
| Config edit has no effect | Running connector keeps its old config | Delete and re-post the connector |

### Current issues

| Area | Issue | Suggested fix |
| --- | --- | --- |
| Credentials | Source connectors, `my.cnf` and the exporter settings in `docker-compose.yml` contain real-looking passwords in plain text | Rotate them and load them from `.env` or Kafka Connect config providers |
| Kafka address | Source connectors set the schema history bootstrap server to `sql-kafka-1:9092`, but the compose service is `kafka:9092` | Change it to `kafka:9092` in both source files |
| HR sinks | They use `io.confluent.connect.jdbc.JdbcSinkConnector`, which the Debezium Connect image in `Dockerfile.debezium` does not include, and point to `postgres:5432`, which is not a compose service | Install the Confluent plugin, or switch them to the Debezium JDBC sink and `172.17.0.1:5433` like the ecommerce sinks |
| HR message format | The HR source writes JSON without schemas, while JDBC sinks normally need schemas | Set `schemas.enable` to `true` for the HR source, as in the ecommerce source |
| Deletes | Ecommerce sinks drop `__deleted`, the source drops tombstones, and the HR source drops deletes, so deletes probably do not reach PostgreSQL | Test with a deleted row; if needed, keep tombstones and use a delete-aware sink setup |
| Data generator | `mysql_data_generator.py` connects to SQL Server through `pyodbc` and creates SQL Server tables | Port it to a MySQL driver, or insert test data into MySQL by hand |
| `reviews` table | PostgreSQL has a `NOT NULL` column named `void` (likely meant to be `uuid`), and no sink exists for `reviews` or `shipments` | Rename the column if it is a typo, then add sinks |
| Kafka listeners | Only `PLAINTEXT://kafka:9092` is advertised, although port 29092 is published | Add a host listener if tools outside Docker must reach Kafka |


### To be implemented

1. Fix the Kafka address and HR sink issues, then deploy all connectors and run the replication test.
2. Add sinks for `shipments` and `reviews`.
3. Add Grafana dashboards and Prometheus alert rules for connector status and consumer lag.
4. Move every secret out of the repo.
