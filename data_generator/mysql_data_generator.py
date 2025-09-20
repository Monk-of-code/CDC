import pyodbc
import random
import json
from faker import Faker
from datetime import timedelta


class SQLServerDataGenerator:
    def __init__(self, server="localhost,1433", user="sa", password="YourStrongPassword123", database="transactions"):
        self.conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={user};PWD={password};"
        self.fake = Faker()
        self.connection = None
        self.cursor = None

    def connect(self):
        self.connection = pyodbc.connect(self.conn_str, autocommit=True)
        self.cursor = self.connection.cursor()
        self.cursor.fast_executemany = True
        print("✅ Connected to SQL Server")

    def create_schemas(self):
        for schema in ["ecommerce", "hr"]:
            self.cursor.execute(
                f"IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '{schema}') EXEC('CREATE SCHEMA {schema}')"
            )
            print(f"✅ Schema ready: {schema}")

    def create_tables(self):
        # --- Ecommerce ---
        self.cursor.execute("""
        IF OBJECT_ID('ecommerce.customers', 'U') IS NULL
        CREATE TABLE ecommerce.customers (
            customer_id INT IDENTITY PRIMARY KEY,
            first_name NVARCHAR(50),
            last_name NVARCHAR(50),
            email NVARCHAR(100) UNIQUE,
            phone NVARCHAR(20),
            address NVARCHAR(255),
            city NVARCHAR(50),
            state NVARCHAR(50),
            zip_code NVARCHAR(20),
            country NVARCHAR(30),
            date_of_birth DATE,
            profile_json NVARCHAR(MAX),
            credit_score INT,
            account_balance DECIMAL(18,2),
            is_active BIT
        )
        """)
        self.cursor.execute("""
        IF OBJECT_ID('ecommerce.products', 'U') IS NULL
        CREATE TABLE ecommerce.products (
            product_id INT IDENTITY PRIMARY KEY,
            name NVARCHAR(100),
            description NVARCHAR(255),
            price DECIMAL(18,2),
            stock_quantity INT,
            category NVARCHAR(50),
            tags NVARCHAR(MAX),
            weight DECIMAL(10,2),
            dimensions NVARCHAR(50),
            is_active BIT
        )
        """)
        self.cursor.execute("""
        IF OBJECT_ID('ecommerce.orders', 'U') IS NULL
        CREATE TABLE ecommerce.orders (
            order_id INT IDENTITY PRIMARY KEY,
            customer_id INT FOREIGN KEY REFERENCES ecommerce.customers(customer_id),
            total_amount DECIMAL(18,2),
            status NVARCHAR(20),
            shipping_address NVARCHAR(255),
            payment_method NVARCHAR(20),
            is_paid BIT
        )
        """)
        self.cursor.execute("""
        IF OBJECT_ID('ecommerce.order_items', 'U') IS NULL
        CREATE TABLE ecommerce.order_items (
            order_item_id INT IDENTITY PRIMARY KEY,
            order_id INT FOREIGN KEY REFERENCES ecommerce.orders(order_id),
            product_id INT FOREIGN KEY REFERENCES ecommerce.products(product_id),
            quantity INT,
            unit_price DECIMAL(18,2)
        )
        """)
        self.cursor.execute("""
        IF OBJECT_ID('ecommerce.payments', 'U') IS NULL
        CREATE TABLE ecommerce.payments (
            payment_id INT IDENTITY PRIMARY KEY,
            order_id INT FOREIGN KEY REFERENCES ecommerce.orders(order_id),
            amount DECIMAL(18,2),
            method NVARCHAR(20),
            status NVARCHAR(20)
        )
        """)
        self.cursor.execute("""
        IF OBJECT_ID('ecommerce.shipments', 'U') IS NULL
        CREATE TABLE ecommerce.shipments (
            shipment_id INT IDENTITY PRIMARY KEY,
            order_id INT FOREIGN KEY REFERENCES ecommerce.orders(order_id),
            carrier NVARCHAR(50),
            tracking_number NVARCHAR(100),
            shipped_date DATETIME2,
            delivery_date DATETIME2,
            status NVARCHAR(20)
        )
        """)
        # --- HR ---
        self.cursor.execute("""
        IF OBJECT_ID('hr.departments', 'U') IS NULL
        CREATE TABLE hr.departments (
            department_id INT IDENTITY PRIMARY KEY,
            name NVARCHAR(100),
            location NVARCHAR(100)
        )
        """)
        self.cursor.execute("""
        IF OBJECT_ID('hr.employees', 'U') IS NULL
        CREATE TABLE hr.employees (
            employee_id INT IDENTITY PRIMARY KEY,
            first_name NVARCHAR(50),
            last_name NVARCHAR(50),
            email NVARCHAR(100) UNIQUE,
            phone NVARCHAR(20),
            hire_date DATE,
            job_title NVARCHAR(100),
            department_id INT FOREIGN KEY REFERENCES hr.departments(department_id),
            salary DECIMAL(18,2),
            manager_id INT NULL
        )
        """)
        self.cursor.execute("""
        IF OBJECT_ID('hr.attendance', 'U') IS NULL
        CREATE TABLE hr.attendance (
            attendance_id INT IDENTITY PRIMARY KEY,
            employee_id INT FOREIGN KEY REFERENCES hr.employees(employee_id),
            date DATE,
            status NVARCHAR(20)
        )
        """)
        self.cursor.execute("""
        IF OBJECT_ID('hr.salaries', 'U') IS NULL
        CREATE TABLE hr.salaries (
            salary_id INT IDENTITY PRIMARY KEY,
            employee_id INT FOREIGN KEY REFERENCES hr.employees(employee_id),
            amount DECIMAL(18,2),
            effective_date DATE
        )
        """)
        self.cursor.execute("""
        IF OBJECT_ID('hr.projects', 'U') IS NULL
        CREATE TABLE hr.projects (
            project_id INT IDENTITY PRIMARY KEY,
            name NVARCHAR(100),
            start_date DATE,
            end_date DATE,
            budget DECIMAL(18,2),
            department_id INT FOREIGN KEY REFERENCES hr.departments(department_id)
        )
        """)
        self.cursor.execute("""
        IF OBJECT_ID('hr.employee_projects', 'U') IS NULL
        CREATE TABLE hr.employee_projects (
            employee_project_id INT IDENTITY PRIMARY KEY,
            employee_id INT FOREIGN KEY REFERENCES hr.employees(employee_id),
            project_id INT FOREIGN KEY REFERENCES hr.projects(project_id),
            role NVARCHAR(50)
        )
        """)
        print("✅ Tables created")

    # --- Insert methods ---
    def insert_customers(self, n=100):
        q = """INSERT INTO ecommerce.customers
        (first_name,last_name,email,phone,address,city,state,zip_code,country,
         date_of_birth,profile_json,credit_score,account_balance,is_active)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
        data = [
            (
                self.fake.first_name(),
                self.fake.last_name(),
                self.fake.unique.email(),
                self.fake.phone_number()[:15],
                self.fake.address().replace("\n", ", "),
                self.fake.city(),
                self.fake.state(),
                self.fake.zipcode(),
                self.fake.country(),
                self.fake.date_of_birth(minimum_age=18, maximum_age=80),
                json.dumps({"preferences": random.choice(["email","sms","both"])}),
                random.randint(300,850),
                round(random.uniform(-1000,20000),2),
                random.choice([0,1])
            ) for _ in range(n)
        ]
        self.cursor.executemany(q, data)
        self.connection.commit()
        print(f"✅ {n} customers inserted")

    def insert_products(self, n=50):
        q = """INSERT INTO ecommerce.products
        (name,description,price,stock_quantity,category,tags,weight,dimensions,is_active)
        VALUES (?,?,?,?,?,?,?,?,?)"""
        data = [
            (
                self.fake.word().title(),
                self.fake.sentence(),
                round(random.uniform(5,500),2),
                random.randint(1,500),
                random.choice(["Electronics","Clothing","Home","Books","Toys"]),
                json.dumps([self.fake.word() for _ in range(3)]),
                round(random.uniform(0.1,10.0),2),
                f"{random.randint(1,100)}x{random.randint(1,100)}x{random.randint(1,100)}",
                random.choice([0,1])
            ) for _ in range(n)
        ]
        self.cursor.executemany(q, data)
        self.connection.commit()
        print(f"✅ {n} products inserted")

    def insert_orders(self, n=200):
        q = """INSERT INTO ecommerce.orders
        (customer_id,total_amount,status,shipping_address,payment_method,is_paid)
        VALUES (?,?,?,?,?,?)"""
        customer_ids = [row[0] for row in self.cursor.execute("SELECT customer_id FROM ecommerce.customers")]
        data = [
            (
                random.choice(customer_ids),
                round(random.uniform(20,1000),2),
                random.choice(["pending","shipped","delivered","cancelled"]),
                self.fake.address().replace("\n", ", "),
                random.choice(["card","paypal","cod"]),
                random.choice([0,1])
            ) for _ in range(n)
        ]
        self.cursor.executemany(q, data)
        self.connection.commit()
        print(f"✅ {n} orders inserted")

    def insert_order_items(self, n=500):
        q = """INSERT INTO ecommerce.order_items
        (order_id,product_id,quantity,unit_price) VALUES (?,?,?,?)"""
        order_ids = [row[0] for row in self.cursor.execute("SELECT order_id FROM ecommerce.orders")]
        product_ids = [row[0] for row in self.cursor.execute("SELECT product_id FROM ecommerce.products")]
        data = [
            (
                random.choice(order_ids),
                random.choice(product_ids),
                random.randint(1,5),
                round(random.uniform(5,500),2)
            ) for _ in range(n)
        ]
        self.cursor.executemany(q, data)
        self.connection.commit()
        print(f"✅ {n} order_items inserted")

    def insert_payments(self, n=200):
        q = """INSERT INTO ecommerce.payments
        (order_id,amount,method,status) VALUES (?,?,?,?)"""
        order_ids = [row[0] for row in self.cursor.execute("SELECT order_id FROM ecommerce.orders")]
        data = [
            (
                random.choice(order_ids),
                round(random.uniform(20,1000),2),
                random.choice(["card","paypal","cod"]),
                random.choice(["pending","completed","failed"])
            ) for _ in range(n)
        ]
        self.cursor.executemany(q, data)
        self.connection.commit()
        print(f"✅ {n} payments inserted")

    def insert_shipments(self, n=200):
        q = """INSERT INTO ecommerce.shipments
        (order_id,carrier,tracking_number,shipped_date,delivery_date,status)
        VALUES (?,?,?,?,?,?)"""
        order_ids = [row[0] for row in self.cursor.execute("SELECT order_id FROM ecommerce.orders")]
        data = [
            (
                random.choice(order_ids),
                random.choice(["UPS","FedEx","DHL","USPS"]),
                self.fake.uuid4(),
                self.fake.date_time_this_year(),
                self.fake.date_time_between(start_date="-30d", end_date="+30d"),
                random.choice(["in_transit","delivered","delayed"])
            ) for _ in range(n)
        ]
        self.cursor.executemany(q, data)
        self.connection.commit()
        print(f"✅ {n} shipments inserted")

    def insert_departments(self, n=10):
        q = """INSERT INTO hr.departments (name,location) VALUES (?,?)"""
        data = [(self.fake.company(), self.fake.city()) for _ in range(n)]
        self.cursor.executemany(q, data)
        self.connection.commit()
        print(f"✅ {n} departments inserted")

    def insert_employees(self, n=100):
        q = """INSERT INTO hr.employees
        (first_name,last_name,email,phone,hire_date,job_title,department_id,salary,manager_id)
        VALUES (?,?,?,?,?,?,?,?,?)"""
        dept_ids = [row[0] for row in self.cursor.execute("SELECT department_id FROM hr.departments")]
        data = [
            (
                self.fake.first_name(),
                self.fake.last_name(),
                self.fake.unique.email(),
                self.fake.phone_number()[:15],
                self.fake.date_between(start_date="-10y", end_date="today"),
                self.fake.job(),
                random.choice(dept_ids),
                round(random.uniform(30000,120000),2),
                None
            ) for _ in range(n)
        ]
        self.cursor.executemany(q, data)
        self.connection.commit()
        print(f"✅ {n} employees inserted")

    def insert_attendance(self, n=300):
        q = """INSERT INTO hr.attendance (employee_id,date,status) VALUES (?,?,?)"""
        emp_ids = [row[0] for row in self.cursor.execute("SELECT employee_id FROM hr.employees")]
        data = [
            (
                random.choice(emp_ids),
                self.fake.date_between(start_date="-1y", end_date="today"),
                random.choice(["present","absent","remote"])
            ) for _ in range(n)
        ]
        self.cursor.executemany(q, data)
        self.connection.commit()
        print(f"✅ {n} attendance records inserted")

    def insert_salaries(self, n=100):
        q = """INSERT INTO hr.salaries (employee_id,amount,effective_date) VALUES (?,?,?)"""
        emp_ids = [row[0] for row in self.cursor.execute("SELECT employee_id FROM hr.employees")]
        data = [
            (
                random.choice(emp_ids),
                round(random.uniform(30000,120000),2),
                self.fake.date_between(start_date="-5y", end_date="today")
            ) for _ in range(n)
        ]
        self.cursor.executemany(q, data)
        self.connection.commit()
        print(f"✅ {n} salaries inserted")

    def insert_projects(self, n=20):
        q = """INSERT INTO hr.projects (name,start_date,end_date,budget,department_id) VALUES (?,?,?,?,?)"""
        dept_ids = [row[0] for row in self.cursor.execute("SELECT department_id FROM hr.departments")]
        data = [
            (
                self.fake.bs().title(),
                self.fake.date_between(start_date="-3y", end_date="-1y"),
                self.fake.date_between(start_date="-1y", end_date="+1y"),
                round(random.uniform(10000,500000),2),
                random.choice(dept_ids)
            ) for _ in range(n)
        ]
        self.cursor.executemany(q, data)
        self.connection.commit()
        print(f"✅ {n} projects inserted")

    def insert_employee_projects(self, n=200):
        q = """INSERT INTO hr.employee_projects (employee_id,project_id,role) VALUES (?,?,?)"""
        emp_ids = [row[0] for row in self.cursor.execute("SELECT employee_id FROM hr.employees")]
        project_ids = [row[0] for row in self.cursor.execute("SELECT project_id FROM hr.projects")]
        data = [
            (
                random.choice(emp_ids),
                random.choice(project_ids),
                random.choice(["Developer","Manager","Tester","Analyst"])
            ) for _ in range(n)
        ]
        self.cursor.executemany(q, data)
        self.connection.commit()
        print(f"✅ {n} employee_projects inserted")

    def populate_all(self):
        self.insert_customers()
        self.insert_products()
        self.insert_orders()
        self.insert_order_items()
        self.insert_payments()
        self.insert_shipments()
        self.insert_departments()
        self.insert_employees()
        self.insert_attendance()
        self.insert_salaries()
        self.insert_projects()
        self.insert_employee_projects()


if __name__ == "__main__":
    gen = SQLServerDataGenerator()
    gen.connect()
    gen.create_schemas()
    gen.create_tables()
    gen.populate_all()
