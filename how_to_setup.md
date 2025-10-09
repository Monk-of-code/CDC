
## System Architecture

```
┌─────────────┐     ┌──────────┐     ┌───────────┐     ┌─────────────────┐
│ MY SQL      │     │          │     │           │     │ PostgreSQL      │
│ Server      │────▶│ Debezium │────▶│ Kafka     │────▶│ with PostGIS    │
│ (Source DB) │     │          │     │           │     │ (Target DB)     │
└─────────────┘     └──────────┘     └───────────┘     └─────────────────┘
                          │                │                    ▲
                          │                │                    │
                     ┌────▼────┐      ┌────▼────┐        ┌─────┴─────┐
                     │ Zookeeper│      │ Kafka UI │        │ JDBC Sink │
                     └─────────┘      └─────────┘        └───────────┘
                          
                     ┌───────────────────────────┐     ┌───────────┐
                     │      Exporters            │────▶│           │
                     │ (PostgreSQL/Kafka/MS SQL/ │     │  Grafana  │
                     │      Debezium)            │     │ Dashboards│
                     └───────────────────────────┘     └───────────┘
```

## Components

1. **MY SQL Server**
   - Source database containing tables with spatial data
   - CDC enabled to track data changes
   - Initial dataset with 6 relational tables
   - Periodic data generator script for testing CDC
   - Baremetal

2. **PostgreSQL/PostGIS**
   - Target database for replicating data
   - PostGIS extension for handling spatial data
   - Schema will mirror the source database structure
   - baremetal

3. **Apache Zookeeper**
   - Distributed coordination service
   - Manages Kafka broker configuration
   - Handles leader election and cluster state

4. **Confluent Kafka**
   - Distributed event streaming platform
   - Handles message queuing between source and target
   - Stores change events from Debezium

5. **Debezium**
   - CDC connector framework
   - Captures changes from MS SQL Server
   - Converts changes to Kafka events
   - Handles data type mappings between My SQL and PostgreSQL
   - Writes data from Kafka topics to PostgreSQL
   - Handles insert, update, and delete operations
   - Provides direct integration path to target database

6. **Kafka UI**
   - Web-based interface for monitoring Kafka
   - View topics, consumers, and message flow
   - Troubleshooting and administration

7. **Monitoring & Metrics**
   - PostgreSQL Exporter: Metrics for PostgreSQL
   - Kafka Exporter: Metrics for Kafka cluster
   - MS SQL Exporter: Metrics for MS SQL Server
   - Debezium Exporter: Metrics for Debezium connectors

9. **Grafana**
   - Visualization platform for monitoring
   - Customizable dashboards for all system components
   - Alerting and notification capabilities
   - Real-time infrastructure health monitoring


## Implementation Plan

### Phase 1: Infrastructure Setup
1. Set up Docker Compose environment
2. Configure MS SQL Server container with CDC enabled
3. Configure PostgreSQL container with PostGIS extension
4. Deploy Zookeeper and Kafka containers
5. Set up Kafka UI container

### Phase 2: Data Model Implementation
1. Create database schemas in MY SQL Server
2. Enable CDC on all tables
3. Create corresponding schemas in PostgreSQL

### Phase 3: CDC Configuration
1. Configure Debezium connector for MS SQL Server
2. Set up Kafka topics for each table
3. Configure PostgreSQL sink connector
4. Implement data type transformations for proper conversion
5. Configure Single Message Transformations (SMTs) for uuid,currency,unwanted rows data

### Phase 4: Monitoring Setup
1. Deploy exporter containers for all components
2. Configure Prometheus container for metrics collection
3. Deploy Grafana container
4. Create comprehensive dashboards for system monitoring
5. Configure alerts and notifications

### Phase 5: Testing
1. Insert/update/delete operations in source database
2. Verify replication to target database
3. Performance testing and optimization


# Database Setup - MySQL

## 1. Create Debezium User with Required Privileges

### Create the Debezium User
```sql
-- Create the user (if it doesn't exist already)
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'YOUR_STRONG_PASSWORD';
```

### Grant Required Privileges
```sql
-- Grant all required privileges including LOCK TABLES for snapshot
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES 
ON *.* TO 'debezium'@'%';

-- Flush privileges to apply changes
FLUSH PRIVILEGES;
```

### Verify User Privileges
```sql
-- Verify the privileges were granted correctly
SHOW GRANTS FOR 'debezium'@'%';
```

**Expected Output:**
```
+----------------------------------------------------------------------+
| Grants for debezium@%                                                |
+----------------------------------------------------------------------+
| GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE,             |
| REPLICATION CLIENT, LOCK TABLES ON *.* TO 'debezium'@'%'             |
+----------------------------------------------------------------------+
```

## 2. Create Monitoring User (Optional) ;Security measure

```sql
-- Create a separate user for monitoring/metrics
CREATE USER IF NOT EXISTS 'metrics'@'%' IDENTIFIED BY 'metrics_password';

-- Grant monitoring privileges
GRANT PROCESS, REPLICATION CLIENT ON *.* TO 'metrics'@'%';

-- Grant SELECT on performance_schema for detailed metrics
GRANT SELECT ON performance_schema.* TO 'metrics'@'%';

FLUSH PRIVILEGES;

-- Verify metrics user privileges
SHOW GRANTS FOR 'metrics'@'%';
```

## 3. Enable Change Data Capture (CDC) on MySQL

### Configure MySQL for CDC
Edit the MySQL configuration file:

```bash
sudo nano /etc/mysql/mysql.conf.d/mysqld.cnf
```

Add or update the following settings under the `[mysqld]` section:

```ini
[mysqld]
# Replication and CDC Settings
server-id         = 1                    # Unique ID for replication
log_bin           = mysql-bin            # Enable binary logging
binlog_format     = ROW                  # Required for CDC - captures row-level changes
binlog_row_image  = FULL                 # Ensures all column data is captured
binlog_expire_logs_seconds = 604800      # 7 days retention

# Network Configuration
bind-address      = 0.0.0.0              # Allow connections from any host
mysqlx-bind-address = 0.0.0.0
```

### Restart MySQL Service
```bash
sudo systemctl restart mysql
```

### Verify CDC Configuration
```sql
-- Verify binary logging is enabled and configured correctly
SHOW VARIABLES LIKE 'log_bin';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
SHOW VARIABLES LIKE 'server_id';
```

**Expected Results:**
- `log_bin`: ON
- `binlog_format`: ROW
- `binlog_row_image`: FULL
- `server_id`: 1

## 4. Verify Source Data Availability

### Check Existing Databases
```sql
-- List all databases
SHOW DATABASES;

-- Expected to see your source databases
-- ecommerce
-- hr
```

### Verify Tables in Source Databases
```sql
-- Check tables in ecommerce database
USE ecommerce;
SHOW TABLES;

-- Check tables in hr database  
USE hr;
SHOW TABLES;
```

### Sample Data Verification
```sql
-- Check sample data in key tables
USE ecommerce;
SELECT COUNT(*) as customer_count FROM customers;
SELECT COUNT(*) as order_count FROM orders;
SELECT COUNT(*) as product_count FROM products;

USE hr;
SELECT COUNT(*) as employee_count FROM employees;
SELECT COUNT(*) as department_count FROM departments;
```

## 5. Final Verification Checklist

### User Verification
```sql
-- Verify both users exist and have correct privileges
SELECT user, host FROM mysql.user WHERE user IN ('debezium', 'metrics');

-- Detailed privilege check
SHOW GRANTS FOR 'debezium'@'%';
SHOW GRANTS FOR 'metrics'@'%';
```

### CDC Configuration Verification
```sql
-- Critical CDC variables
SELECT 
    VARIABLE_NAME, 
    VARIABLE_VALUE 
FROM information_schema.GLOBAL_VARIABLES 
WHERE VARIABLE_NAME IN (
    'log_bin', 
    'binlog_format', 
    'binlog_row_image',
    'server_id'
);
```

### Data Availability Check
```sql
-- Summary of source data
SELECT 
    TABLE_SCHEMA as database_name,
    TABLE_NAME as table_name,
    TABLE_ROWS as row_count
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA IN ('ecommerce', 'hr')
ORDER BY TABLE_SCHEMA, TABLE_NAME;
```


# Database Setup -Postgres
```sql
--- Connect to your transaction database
\c transactions

--Create the two schemas (if not exists)
CREATE SCHEMA IF NOT EXISTS hr;
CREATE SCHEMA IF NOT EXISTS  ecommerce;

--Create the tables if not exists 

-- Create Ecommerce Tables
CREATE TABLE IF NOT EXISTS ecommerce.customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    address VARCHAR(255),
    city VARCHAR(50),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(30),
    date_of_birth DATE,
    profile_json JSONB,
    credit_score INTEGER,
    account_balance DECIMAL(18,2),
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS ecommerce.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    description VARCHAR(255),
    price DECIMAL(18,2),
    stock_quantity INTEGER,
    category VARCHAR(50),
    tags JSONB,
    weight DECIMAL(10,2),
    dimensions VARCHAR(50),
    is_active BOOLEAN
);

CREATE TABLE IF NOT EXISTS ecommerce.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES ecommerce.customers(customer_id),
    total_amount DECIMAL(18,2),
    status VARCHAR(20),
    shipping_address VARCHAR(255),
    payment_method VARCHAR(20),
    is_paid BOOLEAN
);

CREATE TABLE IF NOT EXISTS ecommerce.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES ecommerce.orders(order_id),
    product_id INTEGER REFERENCES ecommerce.products(product_id),
    quantity INTEGER,
    unit_price DECIMAL(18,2)
);

CREATE TABLE IF NOT EXISTS ecommerce.payments (
    payment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES ecommerce.orders(order_id),
    amount DECIMAL(18,2),
    method VARCHAR(20),
    status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS ecommerce.shipments (
    shipment_id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES ecommerce.orders(order_id),
    carrier VARCHAR(50),
    tracking_number VARCHAR(100),
    shipped_date TIMESTAMP,
    delivery_date TIMESTAMP,
    status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS ecommerce.reviews (
    review_id SERIAL PRIMARY KEY,
    void CHAR(36) NOT NULL,
    product_id INTEGER REFERENCES ecommerce.products(product_id),
    customer_id INTEGER REFERENCES ecommerce.customers(customer_id),
    rating INTEGER NOT NULL,
    title VARCHAR(255),
    comment TEXT,
    review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Create HR Tables
CREATE TABLE IF NOT EXISTS hr.departments (
    department_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    location VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS hr.employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    hire_date DATE,
    job_title VARCHAR(100),
    department_id INTEGER REFERENCES hr.departments(department_id),
    salary DECIMAL(18,2),
    manager_id INTEGER NULL
);

CREATE TABLE IF NOT EXISTS hr.attendance (
    attendance_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES hr.employees(employee_id),
    date DATE,
    status VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS hr.salaries (
    salary_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES hr.employees(employee_id),
    amount DECIMAL(18,2),
    effective_date DATE
);

CREATE TABLE IF NOT EXISTS hr.projects (
    project_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    start_date DATE,
    end_date DATE,
    budget DECIMAL(18,2),
    department_id INTEGER REFERENCES hr.departments(department_id)
);

CREATE TABLE IF NOT EXISTS hr.employee_projects (
    employee_project_id SERIAL PRIMARY KEY,
    employee_id INTEGER REFERENCES hr.employees(employee_id),
    project_id INTEGER REFERENCES hr.projects(project_id),
    role VARCHAR(50)
);

-- Create indexes for better performance
CREATE INDEX IF NOT EXISTS idx_orders_customer ON ecommerce.orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_order_items_order ON ecommerce.order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product ON ecommerce.order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_payments_order ON ecommerce.payments(order_id);
CREATE INDEX IF NOT EXISTS idx_shipments_order ON ecommerce.shipments(order_id);
CREATE INDEX IF NOT EXISTS idx_reviews_customer ON ecommerce.reviews(customer_id);

CREATE INDEX IF NOT EXISTS idx_employees_department ON hr.employees(department_id);
CREATE INDEX IF NOT EXISTS idx_employees_manager ON hr.employees(manager_id);
CREATE INDEX IF NOT EXISTS idx_attendance_employee ON hr.attendance(employee_id);
CREATE INDEX IF NOT EXISTS idx_salaries_employee ON hr.salaries(employee_id);
CREATE INDEX IF NOT EXISTS idx_projects_department ON hr.projects(department_id);
CREATE INDEX IF NOT EXISTS idx_emp_projects_employee ON hr.employee_projects(employee_id);
CREATE INDEX IF NOT EXISTS idx_emp_projects_project ON hr.employee_projects(project_id);

--create user pgwriter if not exists 
CREATE USER pgwriter WITH  PASSWORD 'YOU OWN STRONG PASSWORD'

--grant privileges for both schemas
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

--Create  publication tables for change data capture
-- Drop publications if they exist
DO $$
BEGIN
    DROP PUBLICATION hr_pub;
EXCEPTION WHEN undefined_object THEN
    RAISE NOTICE 'hr_pub publication did not exist';
END
$$;

DO $$
BEGIN
    DROP PUBLICATION ecommerce_pub;
EXCEPTION WHEN undefined_object THEN
    RAISE NOTICE 'ecommerce_pub publication did not exist';
END
$$;

-- Create publications for each table individually
CREATE PUBLICATION hr_pub FOR TABLE 
    hr.departments,
    hr.employees,
    hr.attendance,
    hr.salaries,
    hr.projects,
    hr.employee_projects;

CREATE PUBLICATION ecommerce_pub FOR TABLE 
    ecommerce.customers,
    ecommerce.products,
    ecommerce.orders,
    ecommerce.order_items,
    ecommerce.payments,
    ecommerce.shipments,
    ecommerce.reviews;

-- Verify publications
SELECT 
    p.pubname as publication_name,
    (SELECT COUNT(*) FROM pg_publication_tables WHERE pubname = p.pubname) as table_count
FROM pg_publication p;

-- Show detailed publication info
SELECT 
    pubname as publication,
    schemaname as schema,
    tablename as table
FROM pg_publication_tables 
ORDER BY pubname, schemaname, tablename;

```
Each publication exposes tables for CDC;debezium reads from these publications via replication slots

## postgres configuration (CDC Setup)

```bash
sudo nano /etc/postgresql/16/main/postgresql.conf
```
Add/change these settings
```ini
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
max_logical_replication_workers = 4
```
### 2.**Restart PostgreSql**;
```bash
sudo systemctl restart postgresql
```
### 3.**Verify wal_level**;
```sql
SELECT name, setting FROM pg_settings WHERE name = 'wal_level';
```
### 4. **Create Replication Slots** (for Debezium)
```sql
SELECT pg_create_logical_replication_slot ('hr_slot','pgoutput')
SELECT pg_create_logical_replication_slot ('ecommerce_slot','pgoutput')
```
### status verification 
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

# CONNECTOR CONFIGURATION DEBEZIUM 
## mysql source connector 

```json
{
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

    "decimal.handling.mode": "string",
    "bigint.unsigned.handling.mode": "long",

    "database.character.set": "utf8",
    "database.connectionTimeZone": "UTC",
    "snapshot.mode": "initial",
    "snapshot.delay.ms": "1000",
    "incremental.snapshot.enabled": "true",
    "include.schema.changes": "false",

    "database.allowPublicKeyRetrieval": "true",
    "database.ssl.mode": "disabled",
    "topic.prefix": "transactions",

    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true",

    "schema.history.internal.kafka.bootstrap.servers": "sql-kafka-1:9092",
    "schema.history.internal.kafka.topic": "dbhistory-ecommerce",

    "transforms": "unwrap,bit2boolean",
    
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "true",
    "transforms.unwrap.delete.handling.mode": "rewrite",

    "transforms.bit2boolean.type": "org.apache.kafka.connect.transforms.Cast$Value",
    "transforms.bit2boolean.spec": "is_active:boolean,is_paid:boolean",


    "time.precision.mode": "connect",
    "connect.timezone": "UTC"
  }
}
```
### **2. Deploy source connectors :**

```bash
curl -X POST -H "Content-Type: application/json" \
  --data @mysql_ecommerce_source.json \
  http://localhost:8083/connectors

```
Wait for 30-45 seconds for initial database screenshot 
```bash
sleep 30
```
### **3. Monitor connector status:**
Verify connector is working 
```bash
curl -s http://localhost:8083/connectors/mysql-ecommerce-source/status | jq .


* Look for `"state": "RUNNING"` on the tasks.
* If you see `"FAILED"`, check the logs.

```

## **Expected results (kafka ui):**
 **Ecommerce Topics** → PostgreSQL `ecommerce` schema:
  - `transactions.ecommerce.customers` → `customers` table
  - `transactions.ecommerce.orders` → `orders` table
  - `transactions.ecommerce.products` → `products` table
  - etc.



# SINK CONNECTOR CONFIGURATION 
Considerations;
1 . primary key and foreign key constraints
2 . transformations and kafka message structure

## 1. **customer**
```json
{
  "name": "pg-sink-customers",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:postgresql://172.17.0.1:5433/transactions",
    "connection.username": "pgwriter",
    "connection.password": "PG_PASS",
    "topics": "transactions.ecommerce.customers",
    "table.name.format": "ecommerce.customers",
    "insert.mode": "upsert",
    "primary.key.mode": "record_key",
    "primary.key.fields": "customer_id",
    "auto.create": "false",
    "auto.evolve": "false",
    "delete.enabled": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    
    "transforms": "dropFields,castBalance",
    "transforms.dropFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.dropFields.blacklist": "uuid,created_at,updated_at,__deleted",
    "transforms.castBalance.type": "org.apache.kafka.connect.transforms.Cast$Value",
    "transforms.castBalance.spec": "account_balance:float64"
  }
}
```
***Deploy the connector***
```bash

curl -X POST -H "Content-Type: application/json" \
  --data @pg-sink-customers.json \
   http://localhost:8083/connectors

```
***Monitor the connector***
Verify connector is working 
```bash
curl -s http://localhost:8083/connectors/pg-sink-customers/status | jq .


# Look for `"state": "RUNNING"` on the tasks.
# If you see `"FAILED"`, check the logs.

```
***TROUBLE SHOOTING STEPS***
### 1. **confirm running configuration**
Run:

```bash
curl -s http://localhost:8083/connectors/pg-sink-customers/config | jq
```
### 2. **with every edit ,pause,delete ,deploy connector**
Run:
```bash

# pause connector ,also can be done via kafka ui
curl -X PUT http://localhost:8083/connectors/pg-sink-customers/pause

#resume
curl -X PUT http://localhost:8083/connectors/pg-sink-customers/resume

#Delete
curl -X DELETE http://localhost:8083/connectors/pg-sink-customers

#Deploy
curl -X POST -H "Content-Type: application/json" \
  --data @pg-sink-customers.json \
  http://localhost:8083/connectors

```

## 2. **orders**
```json
{
  "name": "pg-sink-orders",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:postgresql://172.17.0.1:5433/transactions",
    "connection.username": "pgwriter",
    "connection.password": "PG_PASS",
    "topics": "transactions.ecommerce.orders",
    "table.name.format": "ecommerce.orders",
    "insert.mode": "upsert",
    "primary.key.mode": "record_key",
    "primary.key.fields": "order_id",
    "auto.create": "false",
    "auto.evolve": "false",
    "delete.enabled": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    
    "transforms": "dropFields,castTypes",
    "transforms.dropFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.dropFields.blacklist": "uuid,order_date,billing_address,shipping_cost,tax_amount,discount_amount,tracking_number,estimated_delivery,actual_delivery,order_notes,__deleted",
    "transforms.castTypes.type": "org.apache.kafka.connect.transforms.Cast$Value",
    "transforms.castTypes.spec": "total_amount:float64"
  }
}
```
***Deploy the connector***
```bash

curl -X POST -H "Content-Type: application/json" \
  --data @pg-sink-orders.json \
   http://localhost:8083/connectors

```
***Monitor the connector***
Verify connector is working 
```bash
curl -s http://localhost:8083/connectors/pg-sink-orders/status | jq .


# Look for `"state": "RUNNING"` on the tasks.
# If you see `"FAILED"`, check the logs.

```
***TROUBLE SHOOTING STEPS***
### 1. **confirm running configuration**
Run:

```bash
curl -s http://localhost:8083/connectors/pg-sink-orders/config | jq
```
### 2. **with every edit ,pause,delete ,deploy connector**
Run:
```bash

# pause connector ,also can be done via kafka ui
curl -X PUT http://localhost:8083/connectors/pg-sink-orders/pause

#resume
curl -X PUT http://localhost:8083/connectors/pg-sink-orders/resume

#Delete
curl -X DELETE http://localhost:8083/connectors/pg-sink-orders

#Deploy
curl -X POST -H "Content-Type: application/json" \
  --data @pg-sink-orders.json \
  http://localhost:8083/connectors

```

## 3. **products**
```json
{
  "name": "pg-sink-products",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:postgresql://172.17.0.1:5433/transactions",
    "connection.username": "pgwriter",
    "connection.password": "PG_PASS",
    "topics": "transactions.ecommerce.products",
    "table.name.format": "ecommerce.products",
    "insert.mode": "upsert",
    "primary.key.mode": "record_key",
    "primary.key.fields": "product_id",
    "auto.create": "false",
    "auto.evolve": "false",
    "delete.enabled": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    
    "transforms": "dropFields,renameTags,castTypes",
    "transforms.dropFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.dropFields.blacklist": "uuid,cost,min_stock_level,max_stock_level,is_available,manufacturer,sku,created_at,updated_at,rating,__deleted",
    "transforms.renameTags.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.renameTags.renames": "product_metadata:tags",
    "transforms.castTypes.type": "org.apache.kafka.connect.transforms.Cast$Value",
    "transforms.castTypes.spec": "price:float64,weight:float64"
  }
}
```
***Deploy the connector***
```bash

curl -X POST -H "Content-Type: application/json" \
  --data @pg-sink-products.json \
   http://localhost:8083/connectors

```
***Monitor the connector***
Verify connector is working 
```bash
curl -s http://localhost:8083/connectors/pg-sink-products/status | jq .


# Look for `"state": "RUNNING"` on the tasks.
# If you see `"FAILED"`, check the logs.

```
***TROUBLE SHOOTING STEPS***
### 1. **confirm running configuration**
Run:

```bash
curl -s http://localhost:8083/connectors/pg-sink-products/config | jq
```
### 2. **with every edit ,pause,delete ,deploy connector**
Run:
```bash

# pause connector ,also can be done via kafka ui
curl -X PUT http://localhost:8083/connectors/pg-sink-products/pause

#resume
curl -X PUT http://localhost:8083/connectors/pg-sink-products/resume

#Delete
curl -X DELETE http://localhost:8083/connectors/pg-sink-products

#Deploy
curl -X POST -H "Content-Type: application/json" \
  --data @pg-sink-products.json \
  http://localhost:8083/connectors

```
## 4. **order-items**
```json
{
  "name": "pg-sink-order-items",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:postgresql://172.17.0.1:5433/transactions",
    "connection.username": "pgwriter",
    "connection.password": "PG_PASS",
    "topics": "transactions.ecommerce.order_items",
    "table.name.format": "ecommerce.order_items",
    "insert.mode": "upsert",
    "primary.key.mode": "record_key",
    "primary.key.fields": "order_item_id",
    "auto.create": "false",
    "auto.evolve": "false",
    "delete.enabled": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    
    "transforms": "dropFields,castTypes",
    "transforms.dropFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.dropFields.blacklist": "total_price,discount_percentage,__deleted",
    "transforms.castTypes.type": "org.apache.kafka.connect.transforms.Cast$Value",
    "transforms.castTypes.spec": "unit_price:float64"
  }
}
```
***Deploy the connector***
```bash

curl -X POST -H "Content-Type: application/json" \
  --data @pg-sink-order-items.json \
   http://localhost:8083/connectors

```
***Monitor the connector***
Verify connector is working 
```bash
curl -s http://localhost:8083/connectors/pg-sink-order-items/status | jq .


# Look for `"state": "RUNNING"` on the tasks.
# If you see `"FAILED"`, check the logs.

```
***TROUBLE SHOOTING STEPS***
### 1. **confirm running configuration**
Run:

```bash
curl -s http://localhost:8083/connectors/pg-sink-order-items/config | jq
```
### 2. **with every edit ,pause,delete ,deploy connector**
Run:
```bash

# pause connector ,also can be done via kafka ui
curl -X PUT http://localhost:8083/connectors/pg-sink-order-items/pause

#resume
curl -X PUT http://localhost:8083/connectors/pg-sink-order-items/resume

#Delete
curl -X DELETE http://localhost:8083/connectors/pg-sink-order-items

#Deploy
curl -X POST -H "Content-Type: application/json" \
  --data @pg-sink-order-items.json \
  http://localhost:8083/connectors
```
## 5 . **payments**
```json
{
  "name": "pg-sink-payments",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:postgresql://172.17.0.1:5433/transactions",
    "connection.username": "pgwriter",
    "connection.password": "PG_PASS",
    "topics": "transactions.ecommerce.payments",
    "table.name.format": "ecommerce.payments",
    "insert.mode": "upsert",
    "primary.key.mode": "record_key",
    "primary.key.fields": "payment_id",
    "auto.create": "false",
    "auto.evolve": "false",
    "delete.enabled": "true",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    
    "transforms": "dropFields,castTypes,renameFields",
    "transforms.dropFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.dropFields.blacklist": "uuid,payment_date,transaction_id,card_last_four,card_brand,payment_gateway,currency,__deleted",
    "transforms.castTypes.type": "org.apache.kafka.connect.transforms.Cast$Value",
    "transforms.castTypes.spec": "amount:float64",
    "transforms.renameFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
    "transforms.renameFields.renames": "payment_method:method,payment_status:status"
  }
}
```
***Deploy the connector***
```bash

curl -X POST -H "Content-Type: application/json" \
  --data @pg-sink-payments.json \
   http://localhost:8083/connectors

```
***Monitor the connector***
Verify connector is working 
```bash
curl -s http://localhost:8083/connectors/pg-sink-payments/status | jq .


# Look for `"state": "RUNNING"` on the tasks.
# If you see `"FAILED"`, check the logs.

```
***TROUBLE SHOOTING STEPS***
### 1. **confirm running configuration**
Run:

```bash
curl -s http://localhost:8083/connectors/pg-sink-payments/config | jq
```
### 2. **with every edit ,pause,delete ,deploy connector**
Run:
```bash

# pause connector ,also can be done via kafka ui
curl -X PUT http://localhost:8083/connectors/pg-sink-payments/pause

#resume
curl -X PUT http://localhost:8083/connectors/pg-sink-payments/resume

#Delete
curl -X DELETE http://localhost:8083/connectors/pg-sink-payments

#Deploy
curl -X POST -H "Content-Type: application/json" \
  --data @pg-sink-payments.json \
  http://localhost:8083/connectors
```











  











