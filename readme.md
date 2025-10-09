

---

### 🔹 1. Create the Debezium user in MySQL

Log into MySQL as root/admin and run:

```sql
CREATE USER 'debezium'@'%' IDENTIFIED BY 'your_password';

GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'debezium'@'%';

FLUSH PRIVILEGES;
```

That gives Debezium the minimal privileges it needs to read the binlog and stream changes.

---

### 🔹 2. Enable CDC (binlog) on MySQL

In your MySQL config (`/etc/mysql/my.cnf` or `/etc/my.cnf` depending on OS), under `[mysqld]`, set:


```bash limux
sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf
```

```ini
server-id         = 1    # required for replication
log_bin           = mysql-bin    #enables binary logging ,
binlog_format     = ROW           #safest for replication and cd captures row level changes
binlog_row_image  = FULL          #ensures all column data is captured
binlog_expire_logs_seconds=604800   # 7 days in seconds

bind-address = 0.0.0.0
mysqlx-bind-address = 0.0.0.0


```

Then restart MySQL:

```bash
sudo systemctl restart mysql
```

---

### 🔹 3. Verify binlog is enabled

```bash
mysql -u root -p
```

```sql
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
```

They should be `ON`, `ROW`, and `FULL`.

---

### 🔹 4. Register the Debezium MySQL Source Connector

Now that your config JSON is ready, POST it to Kafka Connect’s REST API:

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @mysql-source.json \
  http://localhost:8083/connectors
```

Where `mysql-source.json` contains your connector definition (the one you pasted earlier).

---

### 🔹 5. Register the JDBC Sink Connector for Postgres

That’s what will take the topics like `transactions.hr.employees` and write them into your bare-metal Postgres.
(We can build this config together when you’re ready — it just needs JDBC URL, user, password, and auto-create/auto-evolve enabled).

---

✅ After this, your pipeline is:
**MySQL (CDC enabled) → Debezium MySQL Source Connector → Kafka Topics → JDBC Sink Connector → Postgres**

---

connectors config

I can see exactly what’s going on with this JSON — there’s a **syntax issue**. Let me break it down clearly.

---

### **1. Comma is missing**

In JSON, each key/value pair must be separated by a comma **except the last one**.

In your snippet:

```json
"tombstones.on.delete": "false"
"key.converter": "org.apache.kafka.connect.json.JsonConverter",
```

There is **no comma after `"tombstones.on.delete": "false"`** → this makes it **invalid JSON**.

---

### **2. Corrected JSON**

Here’s the fixed, ready-to-use version:

```json
{
  "name": "mysql-ecommerce-source",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "host.docker.internal",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "@lgor1st",
    "database.server.id": "5403",
    "database.server.name": "transactions",
    "database.include.list": "ecommerce",
    "table.include.list": "ecommerce.*",
    "topic.prefix": "transactions",
    "snapshot.mode": "initial",
    "include.schema.changes": "false",
    "heartbeat.interval.ms": "5000",
    "tombstones.on.delete": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  }
}
```

✅ Notice the **comma after `"tombstones.on.delete": "false"`** — now it’s valid JSON.

---

### **3. What this config does**

* `connector.class`: Tells Kafka Connect to use the **Debezium MySQL connector**.
* `tasks.max`: Maximum parallel tasks (1 is fine for testing).
* `database.hostname/port/user/password`: Connects to your host MySQL via `host.docker.internal`.
* `database.server.id`: Must be **unique** in the MySQL replication ecosystem.
* `database.server.name`: Prefix for Kafka topics (`transactions.ecommerce_table_name`).
* `database.include.list` & `table.include.list`: Include only this schema/tables.
* `topic.prefix`: Kafka topic name prefix. Required.
* `snapshot.mode`: `initial` → take a snapshot first.
* `include.schema.changes`: Whether to send schema change events. `false` disables them.
* `heartbeat.interval.ms`: Heartbeat to detect dead connectors.
* `tombstones.on.delete`: Whether to send tombstone messages on row delete.
* **Converters** (`key.converter`, `value.converter`): Serialize messages as JSON, schemas disabled.

---

If you want, I can **also give you a Postgres sink connector JSON** configured to use the same `host.docker.internal` trick so your pipeline can stream directly from MySQL → Kafka → Postgres.

Do you want me to do that next?
***resolve connection***
grizzly@grizzly:~$ docker network inspect sql_cdc-network | grep Subnet
                    "Subnet": "172.19.0.0/16",
grizzly@grizzly:~$ sudo ufw allow from 172.19.0.0/16 to any port 3306


STEP
# Pause the connector first
curl -X PUT http://localhost:8083/connectors/mysql-ecommerce-source/pause

# Delete and recreate the connector to start fresh
curl -X DELETE http://localhost:8083/connectors/mysql-ecommerce-source

# Wait a few seconds, then recreate
curl -X POST http://localhost:8083/connectors \
     -H "Content-Type: application/json" \
     -d '{
  "name": "mysql-ecommerce-source",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "172.17.0.1",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "@lgor1st",
    "database.server.id": "5403",
    "database.server.name": "transactions",
    "database.include.list": "ecommerce",
    "table.include.list": "ecommerce.*",
    "snapshot.mode": "initial",
    "include.schema.changes": "false",
    "database.allowPublicKeyRetrieval": "true",
    "database.ssl.mode": "disabled",
    "topic.prefix": "transactions",
    "heartbeat.interval.ms": "5000",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "tasks.max": "1",
    "poll.interval.ms": "500",
    "max.batch.size": "2048",
    "max.queue.size": "8192",
    "tombstones.on.delete": "false",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.handling.mode": "drop",
    "transforms.unwrap.drop.tombstones": "false",
    "schema.history.internal": "io.debezium.storage.kafka.history.KafkaSchemaHistory",
    "schema.history.internal.kafka.bootstrap.servers": "sql-kafka-1:9092",
    "schema.history.internal.kafka.topic": "dbhistory.ecommerce"
  }
}'



STEP 2 
-- Create the user (if it doesn't exist already)
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY '@lgor1st';

-- Grant all required privileges including LOCK TABLES
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES 
ON *.* TO 'debezium'@'%';

-- Flush privileges to apply changes
FLUSH PRIVILEGES;

-- Verify the privileges
SHOW GRANTS FOR 'debezium'@'%';




***Postgres***
```bash
-- Connect to your transactions database
\c transactions

-- Create the two schemas
CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- Create the pgwriter user (if not exists)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'pgwriter') THEN
        CREATE USER pgwriter WITH PASSWORD 'PG_PASS';
    END IF;
END
$$;

-- Grant privileges for both schemas
GRANT CONNECT ON DATABASE transactions TO pgwriter;

-- HR schema privileges
GRANT USAGE, CREATE ON SCHEMA hr TO pgwriter;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA hr TO pgwriter;
ALTER DEFAULT PRIVILEGES IN SCHEMA hr GRANT ALL PRIVILEGES ON TABLES TO pgwriter;

-- Ecommerce schema privileges
GRANT USAGE, CREATE ON SCHEMA ecommerce TO pgwriter;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ecommerce TO pgwriter;
ALTER DEFAULT PRIVILEGES IN SCHEMA ecommerce GRANT ALL PRIVILEGES ON TABLES TO pgwriter;

-- Sequence privileges for both schemas
GRANT USAGE ON ALL SEQUENCES IN SCHEMA hr TO pgwriter;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA ecommerce TO pgwriter;
ALTER DEFAULT PRIVILEGES IN SCHEMA hr GRANT USAGE ON SEQUENCES TO pgwriter;
ALTER DEFAULT PRIVILEGES IN SCHEMA ecommerce GRANT USAGE ON SEQUENCES TO pgwriter;

```
sudo -u postgres psql -d transactions -f fix_publications.sql

Excellent! The publications were created successfully! 🎉 The warnings are expected since we haven't configured `wal_level = logical` yet, but the publications are ready for when we do that tomorrow.

## Summary of what was accomplished:

### ✅ **Successfully Created:**
1. **2 Publications**: `hr_pub` and `ecommerce_pub`
2. **12 Tables included**: 6 in each publication
3. **Proper schema organization**: All tables in their correct schemas

### ⚠️ **Expected Warnings:**
The warnings about `wal_level` are normal and expected. They're just reminding us that we need to configure PostgreSQL for logical replication before we can use these publications for CDC.

## Next Steps for Tomorrow (CDC Setup):

### 1. **Configure PostgreSQL** (`postgresql.conf`):
```bash
sudo nano /etc/postgresql/16/main/postgresql.conf
```

Add/change these settings:
```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
max_logical_replication_workers = 4
```

### 2. **Restart PostgreSQL**:
```bash
sudo systemctl restart postgresql
```

### 3. **Verify wal_level**:
```sql
SELECT name, setting FROM pg_settings WHERE name = 'wal_level';
```

### 4. **Create Replication Slots** (for Debezium/Kafka Connect):
```sql
SELECT pg_create_logical_replication_slot('hr_slot', 'pgoutput');
SELECT pg_create_logical_replication_slot('ecommerce_slot', 'pgoutput');
```

## Current Status Verification:

Let's run a final verification:

```sql
-- Verify publications are ready
SELECT 
    p.pubname as publication,
    COUNT(pt.tablename) as table_count,
    string_agg(pt.schemaname || '.' || pt.tablename, ', ') as tables
FROM pg_publication p
JOIN pg_publication_tables pt ON p.pubname = pt.pubname
GROUP BY p.pubname;

-- Verify pgwriter can access all tables
SELECT 
    table_schema,
    table_name,
    has_table_privilege('pgwriter', table_schema || '.' || table_name, 'SELECT') as can_select,
    has_table_privilege('pgwriter', table_schema || '.' || table_name, 'INSERT') as can_insert,
    has_table_privilege('pgwriter', table_schema || '.' || table_name, 'UPDATE') as can_update,
    has_table_privilege('pgwriter', table_schema || '.' || table_name, 'DELETE') as can_delete
FROM information_schema.tables
WHERE table_schema IN ('hr', 'ecommerce')
ORDER BY table_schema, table_name;
```

Your setup is **perfectly ready** for CDC! The publications are created and once you configure `wal_level = logical` tomorrow, you'll be able to:

1. Use Debezium for Change Data Capture
2. Stream changes to Kafka
3. Set up JDBC sink connectors
4. Enable real-time data replication

Great job! The hard part is done. Tomorrow we just need to configure PostgreSQL and set up the CDC tools. 🚀
## **PERFECT! 🎯 Your Kafka topics are working beautifully!**

Based on your screenshot, you have **2 databases with multiple tables** that need to sink to PostgreSQL with **2 separate schemas**. Here's how to proceed:

## **📋 PostgreSQL Setup**

### **1. Connect to PostgreSQL and create the schemas:**
```sql
-- Connect to your transactions database
\c transactions

-- Create the two schemas
CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS ecommerce;

-- Create the pgwriter user (if not exists)
CREATE USER pgwriter WITH PASSWORD 'PG_PASS';

-- Grant privileges for both schemas
GRANT CONNECT ON DATABASE transactions TO pgwriter;

-- For HR schema
GRANT USAGE, CREATE ON SCHEMA hr TO pgwriter;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA hr TO pgwriter;
ALTER DEFAULT PRIVILEGES IN SCHEMA hr GRANT ALL PRIVILEGES ON TABLES TO pgwriter;

-- For Ecommerce schema
GRANT USAGE, CREATE ON SCHEMA ecommerce TO pgwriter;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ecommerce TO pgwriter;
ALTER DEFAULT PRIVILEGES IN SCHEMA ecommerce GRANT ALL PRIVILEGES ON TABLES TO pgwriter;

-- Grant sequence usage for both schemas
GRANT USAGE ON ALL SEQUENCES IN SCHEMA hr TO pgwriter;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA ecommerce TO pgwriter;
ALTER DEFAULT PRIVILEGES IN SCHEMA hr GRANT USAGE ON SEQUENCES TO pgwriter;
ALTER DEFAULT PRIVILEGES IN SCHEMA ecommerce GRANT USAGE ON SEQUENCES TO pgwriter;
```

## **📝 Sink Connector Configurations**

### **1. HR Sink Connector (`pg-sink-hr.json`):**
```json
{
  "name": "pg-sink-hr",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "2",
    "topics.regex": "^hr\\.hr\\.(.*)$",
    "connection.url": "jdbc:postgresql://host.docker.internal:5432/transactions?currentSchema=hr",
    "connection.user": "pgwriter",
    "connection.password": "PG_PASS",
    "transforms": "unwrap,route",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "^hr\\.hr\\.(.*)$",
    "transforms.route.replacement": "$1",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "delete.enabled": "true",
    "auto.create": "true",
    "auto.evolve": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "dialect.name": "PostgreSqlDatabaseDialect"
  }
}
```

### **2. Ecommerce Sink Connector (`pg-sink-ecommerce.json`):**
```json
{
  "name": "pg-sink-ecommerce",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "2",
    "topics.regex": "^transactions\\.ecommerce\\.(.*)$",
    "connection.url": "jdbc:postgresql://host.docker.internal:5432/transactions?currentSchema=ecommerce",
    "connection.user": "pgwriter",
    "connection.password": "PG_PASS",
    "transforms": "unwrap,route",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "^transactions\\.ecommerce\\.(.*)$",
    "transforms.route.replacement": "$1",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "delete.enabled": "true",
    "auto.create": "true",
    "auto.evolve": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "dialect.name": "PostgreSqlDatabaseDialect"
  }
}
```

## **🚀 Deployment Steps**

### **1. Create the configuration files:**
```bash
# Create HR sink config
cat > pg-sink-hr.json << 'EOF'
{
  "name": "pg-sink-hr",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "2",
    "topics.regex": "^hr\\.hr\\.(.*)$",
    "connection.url": "jdbc:postgresql://host.docker.internal:5432/transactions?currentSchema=hr",
    "connection.user": "pgwriter",
    "connection.password": "PG_PASS",
    "transforms": "unwrap,route",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "^hr\\.hr\\.(.*)$",
    "transforms.route.replacement": "$1",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "delete.enabled": "true",
    "auto.create": "true",
    "auto.evolve": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "dialect.name": "PostgreSqlDatabaseDialect"
  }
}
EOF

# Create Ecommerce sink config
cat > pg-sink-ecommerce.json << 'EOF'
{
  "name": "pg-sink-ecommerce",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "2",
    "topics.regex": "^transactions\\.ecommerce\\.(.*)$",
    "connection.url": "jdbc:postgresql://host.docker.internal:5432/transactions?currentSchema=ecommerce",
    "connection.user": "pgwriter",
    "connection.password": "PG_PASS",
    "transforms": "unwrap,route",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "^transactions\\.ecommerce\\.(.*)$",
    "transforms.route.replacement": "$1",
    "insert.mode": "upsert",
    "pk.mode": "record_key",
    "delete.enabled": "true",
    "auto.create": "true",
    "auto.evolve": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "dialect.name": "PostgreSqlDatabaseDialect"
  }
}
EOF
```

### **2. Deploy both sink connectors:**
```bash
# Deploy HR sink
curl -X POST -H "Content-Type: application/json" \
  --data @pg-sink-hr.json \
  http://localhost:8083/connectors

# Deploy Ecommerce sink
curl -X POST -H "Content-Type: application/json" \
  --data @pg-sink-ecommerce.json \
  http://localhost:8083/connectors
```

### **3. Verify both sinks are working:**
```bash
curl -s http://localhost:8083/connectors/pg-sink-hr/status | jq .
curl -s http://localhost:8083/connectors/pg-sink-ecommerce/status | jq .
```

## **🎯 Expected Result:**

- **HR Topics** → PostgreSQL `hr` schema:
  - `hr.hr.attendance` → `attendance` table
  - `hr.hr.departments` → `departments` table
  - `hr.hr.employees` → `employees` table
  - etc.

- **Ecommerce Topics** → PostgreSQL `ecommerce` schema:
  - `transactions.ecommerce.customers` → `customers` table
  - `transactions.ecommerce.orders` → `orders` table
  - `transactions.ecommerce.products` → `products` table
  - etc.

**Perfect setup!** 🎉 You'll have all your Kafka data flowing into PostgreSQL with proper schema separation!

validate before posting 
curl -X PUT -H "Content-Type: application/json" \
  --data @pg-sink-customers.json \
  http://localhost:8083/connector-plugins/io.confluent.connect.jdbc.JdbcSinkConnector/config/validate
Great question 💯 — it’s really important to verify that the connector is actually running with the config you think it has. Here’s how you can be sure:

---

### 1. **Check the running connector config**

Run:

```bash
curl -s http://localhost:8083/connectors/pg-sink-customers/config | jq
```

This will return the **effective config** that Kafka Connect is using.

* Verify `pk.mode=record_key`
* Verify `delete.enabled=true`
* Verify `transforms.unwrap.drop.tombstones=false`

---

### 2. **Check connector status**

```bash
curl -s http://localhost:8083/connectors/pg-sink-customers/status | jq
```

* Look for `"state": "RUNNING"` on the tasks.
* If you see `"FAILED"`, check the logs.

---

### 3. **Check Kafka Connect logs**

Inside your Kafka Connect container:

```bash
docker logs -f kafka-connect
```

Search for `pg-sink-customers`. You’ll see the config being applied at startup.

---

### 4. **Test end-to-end**

The ultimate test is to perform a delete in MySQL and see it reflected in Postgres:

1. In MySQL:

   ```sql
   DELETE FROM customers WHERE customer_id = 1;
   ```
2. Watch Kafka topic to confirm Debezium emitted a delete:

   ```bash
   docker exec -it sql-kafka-1 kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic transactions.ecommerce.customers \
     --from-beginning \
     --max-messages 5 \
     --property print.key=true --property print.value=true
   ```

   You should see a record with `key={"customer_id":1}` and `value=null`.
3. In Postgres:

   ```bash
   docker exec -it sql-postgres-1 psql -U pgwriter -d transactions -c "SELECT * FROM customers WHERE customer_id=1;"
   ```

   → Should return **0 rows**.

---

⚡ That’s the safest way:

* Step 1 ensures config is correct.
* Step 2 ensures connector is healthy.
* Step 3/4 ensures deletes flow end-to-end.

Do you want me to also give you a **one-liner query** that checks if any rows are left in Postgres which *should* have been deleted (to quickly spot sync issues)?
Good catch 👍 — yes, Debezium will **auto-create** the history topic the first time you register the source connector **if**:

* The Kafka broker allows auto topic creation (`auto.create.topics.enable=true`, which is the default in most setups).
* You don’t manually pre-create it.

But:

* The connector **won’t set cleanup.policy=compact** by itself. It’ll just make a plain topic with `delete` policy.
* For schema history, compaction is critical — otherwise old schema records get deleted and recovery breaks.

👉 That’s why the Debezium docs always recommend **pre-creating the history topic with compaction enabled** before starting the connector.

So to answer directly:

* If you register the connector without creating the topic, it will get created automatically.
* If you want the correct retention/compaction settings, you should create it yourself first.

Do you want me to make you a small script that **checks if `dbhistory-ecommerce` exists**, and only creates it with compaction if it doesn’t?
curl -X DELETE http://localhost:8083/connectors/pg-sink-customers
Found the issue! Your UFW firewall is only allowing connections from `172.19.0.0/16`, but your Docker network is using `172.18.0.0/16`. Let's fix this:

## 1. Allow your Docker network in UFW:

```bash
# Allow connections from your actual Docker network
sudo ufw allow from 172.18.0.0/16 to any port 3306

# Check the updated rules
sudo ufw status
```

## 2. Test the connection again:

```bash
# Test from a container in your Docker network
docker run --rm --network sql_cdc-network alpine nc -zv 192.168.100.3 3306

# Or test with the gateway IP
docker run --rm --network sql_cdc-network alpine nc -zv 172.18.0.1 3306
```

## 3. Alternative: Allow all Docker networks:

```bash
# Allow common Docker network ranges
sudo ufw allow from 172.17.0.0/16 to any port 3306
sudo ufw allow from 172.18.0.0/16 to any port 3306
sudo ufw allow from 172.19.0.0/16 to any port 3306

# Or allow from any IP (less secure, but for development)
sudo ufw allow 3306
```

## 4. Verify the fix:

```bash
# Check UFW rules
sudo ufw status numbered

# Test connectivity
docker run --rm --network sql_cdc-network alpine nc -zv 192.168.100.3 3306

# If that works, test MySQL connection
docker run --rm --network sql_cdc-network --add-host host.docker.internal:host-gateway alpine sh -c "apk add mysql-client && mysql -h host.docker.internal -u debezium -p'@lgor1st' -e 'SHOW DATABASES;'"
```

## 5. Update your docker-compose with extra_hosts:

Once the connectivity is working, add this to your `docker-compose.yml`:

```yaml
debezium-connect:
  # ... existing config
  extra_hosts:
    - "host.docker.internal:host-gateway"
  # ... rest of config

confluent-connect:
  # ... existing config  
  extra_hosts:
    - "host.docker.internal:host-gateway"
  # ... rest of config
```

Then restart the services:
```bash
docker compose up -d --force-recreate debezium-connect confluent-connect
```

The firewall rule was blocking your Docker containers because they're on a different subnet (`172.18.0.0/16`) than what was allowed (`172.19.0.0/16`). Run the UFW allow command and test again!