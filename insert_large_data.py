import sys
print(sys.path)

import mysql.connector
from faker import Faker
import random
from tqdm import tqdm
import uuid

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

# Insert users với email độc nhất
def insert_users(n):
    sql = "INSERT INTO users (name, email, password_hash) VALUES (%s, %s, %s)"
    for i in tqdm(range(0, n, BATCH_SIZE), desc="Inserting users"):
        # Số lượng bản ghi trong batch cuối có thể ít hơn BATCH_SIZE
        current_batch_size = min(BATCH_SIZE, n - i)
        # Sử dụng uuid để đảm bảo email là duy nhất
        data = [(fake.name(), f"{uuid.uuid4().hex[:8]}_{fake.user_name()}@example.com", fake.password()) 
                for _ in range(current_batch_size)]
        try:
            cursor.executemany(sql, data)
            conn.commit()
        except mysql.connector.errors.IntegrityError as e:
            # Ghi log lỗi và bỏ qua batch này
            print(f"Lỗi khi chèn batch người dùng: {str(e)}")
            conn.rollback()

# Insert categories - Sửa để đảm bảo parent_id hợp lệ
def insert_categories(n):
    # Đầu tiên chèn các danh mục gốc (không có parent)
    sql = "INSERT INTO categories (name, parent_id) VALUES (%s, %s)"
    root_categories_count = min(n // 5, BATCH_SIZE)
    root_categories = [(f"root_{fake.word()}_{uuid.uuid4().hex[:6]}", None) 
                       for _ in range(root_categories_count)]
    
    try:
        cursor.executemany(sql, root_categories)
        conn.commit()
        print(f"Đã chèn {root_categories_count} danh mục gốc")
    except mysql.connector.errors.IntegrityError as e:
        print(f"Lỗi khi chèn danh mục gốc: {str(e)}")
        conn.rollback()
        return
    
    # Sau đó chèn các danh mục con với parent_id tồn tại
    remaining = n - root_categories_count
    
    for i in tqdm(range(0, remaining, BATCH_SIZE), desc="Inserting categories"):
        # Lấy danh sách ID danh mục hiện có để sử dụng làm parent_id
        cursor.execute("SELECT id FROM categories")
        existing_category_ids = [row[0] for row in cursor.fetchall()]
        
        if not existing_category_ids:
            print("Không có danh mục nào để sử dụng làm parent")
            break
            
        current_batch_size = min(BATCH_SIZE, remaining - i)
        data = [(f"sub_{fake.word()}_{uuid.uuid4().hex[:6]}", 
                random.choice(existing_category_ids) if random.random() > 0.5 else None) 
                for _ in range(current_batch_size)]
        
        try:
            cursor.executemany(sql, data)
            conn.commit()
        except mysql.connector.errors.IntegrityError as e:
            print(f"Lỗi khi chèn batch danh mục: {str(e)}")
            conn.rollback()

# Insert products - Sửa để đảm bảo category_id hợp lệ
def insert_products(n):
    sql = "INSERT INTO products (name, description, price, stock, category_id) VALUES (%s, %s, %s, %s, %s)"
    
    for i in tqdm(range(0, n, BATCH_SIZE), desc="Inserting products"):
        try:
            # Lấy danh sách ID danh mục hiện có
            cursor.execute("SELECT id FROM categories")
            category_ids = [row[0] for row in cursor.fetchall()]
            
            if not category_ids:
                print("Không có danh mục nào để liên kết với sản phẩm")
                break
                
            current_batch_size = min(BATCH_SIZE, n - i)
            data = [(f"product_{fake.word()}_{uuid.uuid4().hex[:6]}", 
                    fake.text(max_nb_chars=200), 
                    round(random.uniform(5, 500), 2), 
                    random.randint(10, 1000), 
                    random.choice(category_ids)) for _ in range(current_batch_size)]
                    
            cursor.executemany(sql, data)
            conn.commit()
        except mysql.connector.errors.IntegrityError as e:
            print(f"Lỗi khi chèn batch sản phẩm: {str(e)}")
            conn.rollback()

# Insert orders - Sửa để đảm bảo user_id hợp lệ
def insert_orders(n):
    sql = "INSERT INTO orders (user_id, total_price, status) VALUES (%s, %s, %s)"
    statuses = ['pending', 'shipped', 'delivered', 'canceled']
    
    for i in tqdm(range(0, n, BATCH_SIZE), desc="Inserting orders"):
        try:
            # Lấy danh sách ID người dùng hiện có
            cursor.execute("SELECT id FROM users")
            user_ids = [row[0] for row in cursor.fetchall()]
            
            if not user_ids:
                print("Không có người dùng nào để liên kết với đơn hàng")
                break
                
            current_batch_size = min(BATCH_SIZE, n - i)
            data = [(random.choice(user_ids), 
                    round(random.uniform(20, 5000), 2), 
                    random.choice(statuses)) for _ in range(current_batch_size)]
                    
            cursor.executemany(sql, data)
            conn.commit()
        except mysql.connector.errors.IntegrityError as e:
            print(f"Lỗi khi chèn batch đơn hàng: {str(e)}")
            conn.rollback()

# Insert order items - Sửa để đảm bảo order_id và product_id hợp lệ
def insert_order_items(n):
    sql = "INSERT INTO order_items (order_id, product_id, quantity, price) VALUES (%s, %s, %s, %s)"
    
    # Cache products để cải thiện hiệu suất
    cursor.execute("SELECT id, price FROM products LIMIT 10000")
    product_data = cursor.fetchall()
    product_cache = {id: price for id, price in product_data}
    
    for i in tqdm(range(0, n, BATCH_SIZE), desc="Inserting order items"):
        try:
            # Lấy danh sách ID đơn hàng hiện có
            cursor.execute("SELECT id FROM orders LIMIT 10000")
            order_ids = [row[0] for row in cursor.fetchall()]
            
            # Nếu cache sản phẩm trống hoặc ít, cập nhật lại
            if len(product_cache) < 100:
                cursor.execute("SELECT id, price FROM products LIMIT 10000")
                product_data = cursor.fetchall()
                product_cache = {id: price for id, price in product_data}
            
            product_ids = list(product_cache.keys())
            
            if not order_ids or not product_ids:
                print("Không có đơn hàng hoặc sản phẩm để tạo chi tiết đơn hàng")
                break
                
            current_batch_size = min(BATCH_SIZE, n - i)
            data = []
            
            for _ in range(current_batch_size):
                product_id = random.choice(product_ids)
                quantity = random.randint(1, 5)
                # Sử dụng giá sản phẩm từ cache hoặc tạo giá ngẫu nhiên
                price = product_cache.get(product_id, round(random.uniform(5, 500), 2))
                
                data.append((
                    random.choice(order_ids),
                    product_id,
                    quantity,
                    price
                ))
                    
            cursor.executemany(sql, data)
            conn.commit()
        except mysql.connector.errors.IntegrityError as e:
            print(f"Lỗi khi chèn batch chi tiết đơn hàng: {str(e)}")
            conn.rollback()

# Insert data into tables
try:
    print("Bắt đầu chèn dữ liệu...")
    insert_users(NUM_USERS)
    insert_categories(NUM_CATEGORIES)
    insert_products(NUM_PRODUCTS)
    insert_orders(NUM_ORDERS)
    insert_order_items(NUM_ORDER_ITEMS)
    print("Hoàn thành chèn dữ liệu!")
except Exception as e:
    print(f"Lỗi không mong muốn: {str(e)}")
finally:
    # Đóng kết nối
    cursor.close()
    conn.close()
    print("Đã đóng kết nối cơ sở dữ liệu.")