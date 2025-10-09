Yes! This is absolutely possible and a great monitoring setup. You already have **Prometheus, Grafana, and Kafka Exporter** in your docker-compose. Let me help you add **MySQL Exporter** and **PostgreSQL Exporter** to complete the monitoring stack.

## Updated Docker Compose with All Exporters:

Add these services to your existing `docker-compose.yml`:

```yaml
  # ------------------------
  # Database Exporters
  # ------------------------
  mysql-exporter:
    image: prom/mysqld-exporter:v0.15.0
    ports:
      - "9104:9104"
    environment:
      DATA_SOURCE_NAME: "debezium:@lgor1st@tcp(172.17.0.1:3306)/"
    command:
      - "--collect.info_schema.tables"
      - "--collect.info_schema.tablestats"
      - "--collect.perf_schema.tableiowaits"
      - "--collect.perf_schema.indexiowaits"
      - "--collect.auto_increment.columns"
      - "--collect.binlog_size"
      - "--collect.global_status"
      - "--collect.global_variables"
      - "--collect.engine_innodb_status"
      - "--collect.perf_schema.eventsstatements"
    networks:
      - cdc-network

  postgres-exporter:
    image: prometheuscommunity/postgres-exporter:v0.13.2
    ports:
      - "9187:9187"
    environment:
      DATA_SOURCE_NAME: "postgresql://pgwriter:PG_PASS@172.17.0.1:5433/transactions?sslmode=disable"
    networks:
      - cdc-network
```

## Create Prometheus Configuration:

**File: `config/prometheus/prometheus.yml`**
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  # Database Exporters
  - job_name: 'mysql-exporter'
    static_configs:
      - targets: ['mysql-exporter:9104']
    scrape_interval: 10s

  - job_name: 'postgres-exporter'
    static_configs:
      - targets: ['postgres-exporter:9187']
    scrape_interval: 10s

  # Kafka Monitoring
  - job_name: 'kafka-exporter'
    static_configs:
      - targets: ['kafka-exporter:9308']
    scrape_interval: 10s

  # Kafka Connect Monitoring
  - job_name: 'debezium-connect'
    static_configs:
      - targets: ['debezium-connect:8083']
    metrics_path: /metrics
    scrape_interval: 10s

  - job_name: 'confluent-connect'
    static_configs:
      - targets: ['confluent-connect:8083']
    metrics_path: /metrics
    scrape_interval: 10s
```

## Create Grafana Data Source:

**File: `config/grafana/provisioning/datasources/prometheus.yml`**
```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: true
```

## Create Directory Structure:

```bash
mkdir -p config/prometheus config/grafana/provisioning/datasources
```

## Deploy the Monitoring Stack:

```bash
# Update and start all services
docker-compose up -d mysql-exporter postgres-exporter

# Wait for services to start
sleep 30

# Verify exporters are working
curl http://localhost:9104/metrics | head -10  # MySQL Exporter
curl http://localhost:9187/metrics | head -10  # PostgreSQL Exporter
curl http://localhost:9308/metrics | head -10  # Kafka Exporter

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets | jq '.data.activeTargets[].labels.job'
```

## Key Metrics You'll Get:

### MySQL Exporter:
- `mysql_global_status_questions` - Query rate
- `mysql_global_status_innodb_rows_inserted` - Insert operations
- `mysql_info_schema_table_size` - Table sizes
- `mysql_global_status_binlog_snapshot_file_size` - Binlog size

### PostgreSQL Exporter:
- `pg_stat_database_tup_inserted` - Row inserts
- `pg_stat_user_tables_n_tup_ins` - Table insert counts
- `pg_stat_database_blks_hit` - Cache hit ratio
- `pg_stat_activity_count` - Active connections

### Kafka Exporter:
- `kafka_topic_partitions` - Topic partitions
- `kafka_consumer_group_lag` - Consumer lag
- `kafka_broker_info` - Broker information

### Kafka Connect:
- `kafka_connect_task_status` - Connector status
- `kafka_connect_task_metrics_records_consumed_total` - Records processed

## Access Your Dashboards:

1. **Grafana**: http://localhost:3000 (admin/admin)
2. **Prometheus**: http://localhost:9090
3. **Kafka UI**: http://localhost:8080

## Complete the Setup:

1. **Add the exporters** to your docker-compose
2. **Create the config files** as shown above
3. **Restart your stack**: `docker-compose up -d`
4. **Import pre-built dashboards** in Grafana or create custom ones

This will give you **complete visibility** into your entire CDC pipeline from MySQL → Kafka → PostgreSQL!


Great question! You're absolutely right to ask about authentication. The reason we're not including usernames in the exporter configurations is because:

## 1. **MySQL Exporter** - Uses Connection String Format
The MySQL exporter uses a **connection string** format that includes the username:

```yaml
DATA_SOURCE_NAME: "debezium:@lgor1st@tcp(172.17.0.1:3306)/"
```
This is equivalent to: `"username:password@tcp(host:port)/database"`

## 2. **PostgreSQL Exporter** - Uses Connection URL Format
The PostgreSQL exporter uses a **URL format** that includes the username:

```yaml
DATA_SOURCE_NAME: "postgresql://pgwriter:PG_PASS@172.17.0.1:5433/transactions?sslmode=disable"
```
This is equivalent to: `"postgresql://username:password@host:port/database?sslmode=disable"`

## However, You Should Consider Security!

For production, you should:

### Option 1: Use Environment Variables (Recommended)
```yaml
  mysql-exporter:
    image: prom/mysqld-exporter:v0.15.0
    ports:
      - "9104:9104"
    environment:
      DATA_SOURCE_NAME: "${MYSQL_EXPORTER_DSN}"
    networks:
      - cdc-network
```

Then create a `.env` file:
```bash
# .env file
MYSQL_EXPORTER_DSN=debezium:@lgor1st@tcp(172.17.0.1:3306)/
POSTGRES_EXPORTER_DSN=postgresql://pgwriter:PG_PASS@172.17.0.1:5433/transactions?sslmode=disable
```

### Option 2: Use Docker Secrets (Most Secure)
```yaml
  mysql-exporter:
    image: prom/mysqld-exporter:v0.15.0
    ports:
      - "9104:9104"
    environment:
      DATA_SOURCE_NAME: "debezium:${MYSQL_PASSWORD}@tcp(172.17.0.1:3306)/"
    secrets:
      - mysql_password
    networks:
      - cdc-network

secrets:
  mysql_password:
    file: ./secrets/mysql_password.txt
```

### Option 3: Create Dedicated Monitoring Users
Create users with minimal permissions for monitoring:

**MySQL:**
```sql
CREATE USER 'metrics'@'%' IDENTIFIED BY 'metrics_password';
GRANT SELECT, PROCESS, REPLICATION CLIENT ON *.* TO 'metrics'@'%';
```

**PostgreSQL:**
```sql
CREATE USER metrics WITH PASSWORD 'metrics_password';
GRANT pg_monitor TO metrics;
```

Then update your exporters:
```yaml
  mysql-exporter:
    environment:
      DATA_SOURCE_NAME: "metrics:metrics_password@tcp(172.17.0.1:3306)/"

  postgres-exporter:
    environment:
      DATA_SOURCE_NAME: "postgresql://metrics:metrics_password@172.17.0.1:5433/transactions?sslmode=disable"
```

## Updated Secure Configuration:

**For production, I recommend this approach:**

```yaml
  mysql-exporter:
    image: prom/mysqld-exporter:v0.15.0
    ports:
      - "9104:9104"
    environment:
      DATA_SOURCE_NAME: "metrics:${METRICS_MYSQL_PASSWORD}@tcp(172.17.0.1:3306)/"
    networks:
      - cdc-network

  postgres-exporter:
    image: prometheuscommunity/postgres-exporter:v0.13.2
    ports:
      - "9187:9187"
    environment:
      DATA_SOURCE_NAME: "postgresql://metrics:${METRICS_PG_PASSWORD}@172.17.0.1:5433/transactions?sslmode=disable"
    networks:
      - cdc-network
```

**Create a `.env` file:**
```bash
METRICS_MYSQL_PASSWORD=your_secure_mysql_metrics_password
METRICS_PG_PASSWORD=your_secure_pg_metrics_password
```

And add `.env` to your `.gitignore` file.

You're absolutely right to be concerned about security - the current configuration exposes passwords in plain text in the docker-compose file!