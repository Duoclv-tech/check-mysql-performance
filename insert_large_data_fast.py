import mysql.connector
from faker import Faker
import random
import uuid
from tqdm import tqdm

# Kết nối MySQL
conn = mysql.connector.connect(
    host="localhost",
    user="user",
    password="userpassword",
    database="ecommerce"
)
cursor = conn.cursor()
fake = Faker()

# Định nghĩa số lượng bản ghi
NUM_USERS = 1_000_000
NUM_CATEGORIES = 1000
NUM_PRODUCTS = 5_000_000
NUM_ORDERS = 3_000_000
NUM_ORDER_ITEMS = 10_000_000
BATCH_SIZE = 5000  # Chèn từng đợt 5000 bản ghi để tối ưu hiệu suất

# Hàm INSERT nhanh
def batch_insert(sql_prefix, data):
    """Ghép nhiều INSERT vào một query để tăng tốc"""
    if not data:
        return
    sql = sql_prefix + ", ".join(data)
    try:
        cursor.execute(sql)
        conn.commit()
    except mysql.connector.errors.IntegrityError as e:
        print(f"Lỗi khi chèn batch: {str(e)}")
        conn.rollback()

# Insert users với email độc nhất
def insert_users(n):
    sql_prefix = "INSERT INTO users (name, email, password_hash) VALUES "
    data = []

    for i in tqdm(range(n), desc="Inserting users"):
        data.append(f"('{fake.name()}', '{uuid.uuid4().hex[:8]}_{fake.user_name()}@example.com', '{fake.password()}')")
        if len(data) >= BATCH_SIZE:
            batch_insert(sql_prefix, data)
            data = []

    batch_insert(sql_prefix, data)  # Chèn batch cuối

# Insert categories
def insert_categories(n):
    sql_prefix = "INSERT INTO categories (name, parent_id) VALUES "
    data = []

    for i in tqdm(range(n), desc="Inserting categories"):
        parent_id = random.randint(1, i) if i > 1 and random.random() > 0.5 else "NULL"
        data.append(f"('{fake.word()}_{uuid.uuid4().hex[:6]}', {parent_id})")
        if len(data) >= BATCH_SIZE:
            batch_insert(sql_prefix, data)
            data = []

    batch_insert(sql_prefix, data)

# Insert products
def insert_products(n):
    sql_prefix = "INSERT INTO products (name, description, price, stock, category_id) VALUES "
    data = []

    cursor.execute("SELECT id FROM categories")
    category_ids = [row[0] for row in cursor.fetchall()]
    
    for i in tqdm(range(n), desc="Inserting products"):
        category_id = random.choice(category_ids) if category_ids else "NULL"
        data.append(f"('product_{fake.word()}_{uuid.uuid4().hex[:6]}', '{fake.text(200)}', {round(random.uniform(5, 500), 2)}, {random.randint(10, 1000)}, {category_id})")
        if len(data) >= BATCH_SIZE:
            batch_insert(sql_prefix, data)
            data = []

    batch_insert(sql_prefix, data)

# Insert orders
def insert_orders(n):
    sql_prefix = "INSERT INTO orders (user_id, total_price, status) VALUES "
    statuses = ['pending', 'shipped', 'delivered', 'canceled']
    data = []

    cursor.execute("SELECT id FROM users")
    user_ids = [row[0] for row in cursor.fetchall()]
    
    for i in tqdm(range(n), desc="Inserting orders"):
        user_id = random.choice(user_ids) if user_ids else "NULL"
        data.append(f"({user_id}, {round(random.uniform(20, 5000), 2)}, '{random.choice(statuses)}')")
        if len(data) >= BATCH_SIZE:
            batch_insert(sql_prefix, data)
            data = []

    batch_insert(sql_prefix, data)

# Insert order items
def insert_order_items(n):
    sql_prefix = "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES "
    data = []

    cursor.execute("SELECT id FROM orders LIMIT 10000")
    order_ids = [row[0] for row in cursor.fetchall()]
    
    cursor.execute("SELECT id, price FROM products LIMIT 10000")
    product_data = cursor.fetchall()
    product_cache = {id: price for id, price in product_data}
    
    product_ids = list(product_cache.keys())

    for i in tqdm(range(n), desc="Inserting order items"):
        if not order_ids or not product_ids:
            break
        order_id = random.choice(order_ids)
        product_id = random.choice(product_ids)
        quantity = random.randint(1, 5)
        price = product_cache.get(product_id, round(random.uniform(5, 500), 2))
        data.append(f"({order_id}, {product_id}, {quantity}, {price})")

        if len(data) >= BATCH_SIZE:
            batch_insert(sql_prefix, data)
            data = []

    batch_insert(sql_prefix, data)

# Insert data into tables
try:
    print("Bắt đầu chèn dữ liệu...")
    # insert_users(NUM_USERS)
    # insert_categories(NUM_CATEGORIES)
    # insert_products(NUM_PRODUCTS)
    # insert_orders(NUM_ORDERS)
    insert_order_items(NUM_ORDER_ITEMS)
    print("Hoàn thành chèn dữ liệu!")
except Exception as e:
    print(f"Lỗi không mong muốn: {str(e)}")
finally:
    cursor.close()
    conn.close()
    print("Đã đóng kết nối cơ sở dữ liệu.")
