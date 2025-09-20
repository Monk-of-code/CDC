-- PostgreSQL Table Creation Script - Fixed Version
-- Run this after creating the schemas

\c transactions

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

CREATE INDEX IF NOT EXISTS idx_employees_department ON hr.employees(department_id);
CREATE INDEX IF NOT EXISTS idx_employees_manager ON hr.employees(manager_id);
CREATE INDEX IF NOT EXISTS idx_attendance_employee ON hr.attendance(employee_id);
CREATE INDEX IF NOT EXISTS idx_salaries_employee ON hr.salaries(employee_id);
CREATE INDEX IF NOT EXISTS idx_projects_department ON hr.projects(department_id);
CREATE INDEX IF NOT EXISTS idx_emp_projects_employee ON hr.employee_projects(employee_id);
CREATE INDEX IF NOT EXISTS idx_emp_projects_project ON hr.employee_projects(project_id);

-- Grant privileges to pgwriter user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ecommerce TO pgwriter;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA hr TO pgwriter;

GRANT USAGE ON ALL SEQUENCES IN SCHEMA ecommerce TO pgwriter;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA hr TO pgwriter;

-- Create publications for CDC
CREATE PUBLICATION IF NOT EXISTS hr_pub FOR ALL TABLES IN SCHEMA hr;
CREATE PUBLICATION IF NOT EXISTS ecommerce_pub FOR ALL TABLES IN SCHEMA ecommerce;

-- Verification
SELECT '✅ All tables created successfully!' as status;

-- Show table count per schema
SELECT schemaname, COUNT(*) as table_count 
FROM pg_tables 
WHERE schemaname IN ('hr', 'ecommerce')
GROUP BY schemaname;

-- Show publications
SELECT pubname, 
       (SELECT COUNT(*) FROM pg_publication_tables WHERE pubname = p.pubname) as table_count
FROM pg_publication p;