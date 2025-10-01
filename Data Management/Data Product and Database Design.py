
import pandas as pd
import numpy as np
import random
from faker import Faker
import uuid
from itertools import cycle
from datetime import datetime, timedelta
import seaborn as sns
import matplotlib.pyplot as plt
import sqlite3
import time

fake = Faker("en_GB")
Faker.seed(42)
random.seed(42)

def connect_to_db(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
    conn.commit()
    return conn, cursor

conn, cursor = connect_to_db("business_data.db")

table_creation_queries = [
    """CREATE TABLE IF NOT EXISTS Clients (
        Client_ID VARCHAR(36) PRIMARY KEY,
        First_Name VARCHAR(50),
        Last_Name VARCHAR(50),
        Email VARCHAR(100),
        Phone_Number VARCHAR(14),
        Street_Address VARCHAR(150),
        City VARCHAR(50),
        Postal_Code VARCHAR(4),
        County VARCHAR(50)
    )""",
    """CREATE TABLE IF NOT EXISTS Products (
        Product_ID VARCHAR(36) PRIMARY KEY,
        Product_Name VARCHAR(20),
        Format_Category VARCHAR(20),
        SKU VARCHAR(13),
        Price REAL,
        Launch_Date DATE
    )""",
    """CREATE TABLE IF NOT EXISTS Sales (
        Transaction_ID VARCHAR(36) PRIMARY KEY,
        Client_ID VARCHAR(36),
        Product_ID VARCHAR(36),
        Order_Date DATE,
        Quantity INTEGER,
        Price_Per_Item REAL,
        FOREIGN KEY (Client_ID) REFERENCES Clients(Client_ID),
        FOREIGN KEY (Product_ID) REFERENCES Products(Product_ID)
    )""",
    """CREATE TABLE IF NOT EXISTS Financial_Transactions (
        Transaction_ID VARCHAR(36) PRIMARY KEY,
        Payment_Date DATE,
        Payment_Amount REAL,
        Is_Refunded VARCHAR(3),
        Refund_Date DATE,
        Refund_Amount REAL,
        Net_Amount REAL,
        Net_Amount_Without_Shipping REAL,
        Payment_Method VARCHAR(20),
        Transaction_Status VARCHAR(20),
        Shipping_Cost REAL,
        FOREIGN KEY (Transaction_ID) REFERENCES Sales(Transaction_ID)
    )""",
    """CREATE TABLE IF NOT EXISTS Client_Deliveries (
        Client_Delivery_ID VARCHAR(36) PRIMARY KEY,
        Transaction_ID VARCHAR(36),
        Client_ID VARCHAR(36),
        Product_ID VARCHAR(36),
        Delivery_Status VARCHAR(15),
        Carrier VARCHAR(20),
        Estimated_Delivery_Date DATE,
        Actual_Delivery_Date DATE,
        FOREIGN KEY (Transaction_ID) REFERENCES Sales(Transaction_ID),
        FOREIGN KEY (Client_ID) REFERENCES Clients(Client_ID),
        FOREIGN KEY (Product_ID) REFERENCES Products(Product_ID)
    )""",
    """CREATE TABLE IF NOT EXISTS Client_Reviews (
        Review_ID VARCHAR(36) PRIMARY KEY,
        Client_ID VARCHAR(36),
        Transaction_ID VARCHAR(36),
        Product_ID VARCHAR(36),
        Review_Date DATE,
        Star_Rating INTEGER,
        FOREIGN KEY (Client_ID) REFERENCES Clients(Client_ID),
        FOREIGN KEY (Transaction_ID) REFERENCES Sales(Transaction_ID),
        FOREIGN KEY (Product_ID) REFERENCES Products(Product_ID)
    )""",
    """CREATE TABLE IF NOT EXISTS Product_Damage (
        Damage_Report_ID VARCHAR(36) PRIMARY KEY,
        Transaction_ID VARCHAR(36),
        Client_ID VARCHAR(36),
        Product_ID VARCHAR(36),
        Review_ID VARCHAR(36),
        Client_Delivery_ID VARCHAR(36),
        Damage_Type VARCHAR(20),
        Resolution_Status VARCHAR(15),
        Resolution_Date DATE,
        FOREIGN KEY (Transaction_ID) REFERENCES Sales(Transaction_ID),
        FOREIGN KEY (Client_ID) REFERENCES Clients(Client_ID),
        FOREIGN KEY (Product_ID) REFERENCES Products(Product_ID),
        FOREIGN KEY (Review_ID) REFERENCES Client_Reviews(Review_ID),
        FOREIGN KEY (Client_Delivery_ID) REFERENCES Client_Deliveries(Client_Delivery_ID)
    )""",
    """CREATE TABLE IF NOT EXISTS Distributors (
        Distributor_ID VARCHAR(8) PRIMARY KEY,
        Distributor_Name VARCHAR(50),
        Email VARCHAR(100),
        Phone_Number VARCHAR(14),
        Street_Address VARCHAR(150),
        City VARCHAR(50),
        County VARCHAR(50),
        Postcode VARCHAR(10),
        Country VARCHAR(60)
    )""",
    """CREATE TABLE IF NOT EXISTS Distributor_Supply (
        Batch_ID VARCHAR(6) PRIMARY KEY,
        Distributor_ID VARCHAR(8),
        Product_ID VARCHAR(36),
        Batch_Quantity INTEGER,
        Delivery_Status VARCHAR(15),
        Shipping_Method VARCHAR(10),
        Estimated_Arrival_Date DATE,
        Actual_Arrival_Date DATE,
        FOREIGN KEY (Distributor_ID) REFERENCES Distributors(Distributor_ID),
        FOREIGN KEY (Product_ID) REFERENCES Products(Product_ID)
    )"""
]

for query in table_creation_queries:
    cursor.execute(query)
conn.commit()

def insert_data_to_table(df, table_name):
    try:
        cursor.execute(f"DELETE FROM {table_name};")
        conn.commit()
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['?' for _ in df.columns])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        for row in df.itertuples(index=False):
            cursor.execute(query, tuple(row))
        conn.commit()
    except Exception as e:
        print(f"Error inserting into {table_name}: {e}")

RANDOM_SEED = 42
random.seed(RANDOM_SEED)
fake = Faker()
Faker.seed(RANDOM_SEED)

uk_locations = {
    "London": {"Postal_Code": "EC1A", "County": "Greater London"},
    "Manchester": {"Postal_Code": "M1", "County": "Greater Manchester"},
    "Birmingham": {"Postal_Code": "B1", "County": "West Midlands"},
    "Leeds": {"Postal_Code": "LS1", "County": "West Yorkshire"},
    "Sheffield": {"Postal_Code": "S1", "County": "South Yorkshire"},
    "Bristol": {"Postal_Code": "BS1", "County": "Bristol"},
    "Newcastle upon Tyne": {"Postal_Code": "NE1", "County": "Tyne and Wear"},
    "Nottingham": {"Postal_Code": "NG1", "County": "Nottinghamshire"},
    "Leicester": {"Postal_Code": "LE1", "County": "Leicestershire"},
    "Glasgow": {"Postal_Code": "G1", "County": "Glasgow City"},
    "Edinburgh": {"Postal_Code": "EH1", "County": "City of Edinburgh"},
    "Cardiff": {"Postal_Code": "CF10", "County": "Cardiff"},
    "Belfast": {"Postal_Code": "BT1", "County": "County Antrim"},
    "Southampton": {"Postal_Code": "SO14", "County": "Hampshire"}
}

def generate_email(first_name, last_name):
    domain = random.choice(["gmail.com", "icloud.com"])
    return f"{first_name.lower()}.{last_name.lower()}@{domain}"

def generate_uk_phone():
    return f"+44 7{random.randint(0, 9)}{random.randint(10000000, 99999999)}"

num_records = 1000
clients = []
clients_ids_list = []
for _ in range(num_records):
    first_name = fake.first_name()
    last_name = fake.last_name()
    client_id = str(uuid.uuid4())
    clients_ids_list.append(client_id)
    city, details = random.choice(list(uk_locations.items()))
    postcode = details["Postal_Code"]
    county = details["County"]
    clients.append([
        client_id,
        first_name,
        last_name,
        generate_email(first_name, last_name),
        generate_uk_phone(),
        fake.street_address(),
        city,
        postcode,
        county
    ])

df_clients = pd.DataFrame(
    clients,
    columns=["Client_ID", "First_Name", "Last_Name", "Email", "Phone_Number", "Street_Address", "City", "Postal_Code", "County"]
)

insert_data_to_table(df_clients, "Clients")

product_combinations = [
    ("Arabica", "Powder"), ("Arabica", "Whole Bean"), ("Arabica", "Pods"),
    ("Robusta", "Powder"), ("Robusta", "Whole Bean"), ("Robusta", "Pods"),
    ("Excelsia", "Powder"), ("Excelsia", "Whole Bean"), ("Excelsia", "Pods"),
    ("Liberica", "Powder"), ("Liberica", "Whole Bean"), ("Liberica", "Pods")
]

price_range = {
    ("Arabica", "Powder"): (5, 10),
    ("Arabica", "Whole Bean"): (12, 20),
    ("Arabica", "Pods"): (22, 30),
    ("Robusta", "Powder"): (4, 9),
    ("Robusta", "Whole Bean"): (10, 18),
    ("Robusta", "Pods"): (20, 28),
    ("Excelsia", "Powder"): (6, 12),
    ("Excelsia", "Whole Bean"): (14, 22),
    ("Excelsia", "Pods"): (25, 32),
    ("Liberica", "Powder"): (7, 13),
    ("Liberica", "Whole Bean"): (16, 25),
    ("Liberica", "Pods"): (28, 35)
}

dates = [
    "April 5, 2015", "October 22, 2017", "June 14, 2019", "December 8, 2021",
    "March 30, 2016", "July 11, 2014", "January 3, 2018", "September 25, 2020",
    "November 17, 2022", "August 9, 2014", "May 4, 2023", "February 19, 2024"
]

products = []
product_id_list = []
for (product_name, format_category), date in zip(product_combinations, dates):
    product_id = str(uuid.uuid4())
    product_id_list.append(product_id)
    sku = f"{product_name[:3].upper()}-{format_category[:3].upper()}-{random.randint(10000, 99999)}"
    price = round(random.uniform(*price_range[(product_name, format_category)]), 2)
    products.append([
        product_id,
        product_name,
        format_category,
        sku,
        price,
        date
    ])

df_products = pd.DataFrame(
    products,
    columns=["Product_ID", "Product_Name", "Format_Category", "SKU", "Price", "Launch_date"]
)

df_products.to_csv("products_catalog.csv", index=False)
insert_data_to_table(df_products, "Products")

transactions = []
transaction_id_list = []
clients_list = clients_ids_list.copy()

for client_id in clients_list:
    num_orders_for_client = random.randint(1, 10)
    for _ in range(num_orders_for_client):
        transaction_id = str(uuid.uuid4())
        transaction_id_list.append(transaction_id)
        product_id = random.choice(df_products['Product_ID'])
        order_date = fake.date_this_decade()
        quantity = random.randint(1, 10)
        price_per_item = df_products[df_products['Product_ID'] == product_id]['Price'].values[0]
        transactions.append([
            transaction_id,
            client_id,
            product_id,
            order_date,
            quantity,
            price_per_item
        ])

remaining_transactions = 100 - len(transactions)
for _ in range(max(0, remaining_transactions)):
    transaction_id = str(uuid.uuid4())
    if transaction_id not in transaction_id_list:
        transaction_id_list.append(transaction_id)
    client_id = random.choice(clients_list)
    product_id = random.choice(df_products['Product_ID'])
    order_date = fake.date_this_decade()
    quantity = random.randint(1, 10)
    price_per_item = df_products[df_products['Product_ID'] == product_id]['Price'].values[0]
    transactions.append([
        transaction_id,
        client_id,
        product_id,
        order_date,
        quantity,
        price_per_item
    ])

df_transactions = pd.DataFrame(
    transactions,
    columns=["Transaction_ID", "Client_ID", "Product_ID", "Order_Date", "Quantity", "Price_Per_Item"]
)

df_transactions.to_csv("sales_transaction.csv", index=False)
insert_data_to_table(df_transactions, "Sales")

payment_methods = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Cash"]

shipping_cost_by_city = {
    "London": 5.0, "Manchester": 6.0, "Birmingham": 4.5, "Leeds": 5.5, "Sheffield": 4.0,
    "Bristol": 5.0, "Newcastle upon Tyne": 6.5, "Nottingham": 4.0, "Leicester": 5.5,
    "Glasgow": 7.0, "Edinburgh": 6.0, "Cardiff": 5.0, "Belfast": 8.0, "Southampton": 4.5
}

transaction_status_distribution = [
    ("Completed", 0.6),
    ("Pending", 0.2),
    ("Partial Refund", 0.15),
    ("Full Refund", 0.05)
]

financial_transactions = []
for index, row in df_transactions.iterrows():
    transaction_id = row["Transaction_ID"]
    payment_date = row["Order_Date"]
    payment_amount = round(row["Quantity"] * row["Price_Per_Item"], 2)
    client_city = df_clients[df_clients["Client_ID"] == row["Client_ID"]]["City"].values[0]
    shipping_cost = shipping_cost_by_city.get(client_city, 5.0)
    transaction_status = random.choices(
        [status[0] for status in transaction_status_distribution],
        [status[1] for status in transaction_status_distribution]
    )[0]
    if transaction_status in ["Partial Refund", "Full Refund"]:
        is_refunded = "Yes"
        refund_quantity = row["Quantity"] if transaction_status == "Full Refund" else random.randint(1, row["Quantity"])
        refund_amount = round(refund_quantity * row["Price_Per_Item"], 2) - shipping_cost
        refund_date = payment_date + pd.Timedelta(days=10)
    else:
        is_refunded = "No"
        refund_amount = 0
        refund_date = None
    net_amount = round(payment_amount - refund_amount, 2)
    net_amount_with_shipping = round(net_amount - (0 if is_refunded == "Yes" else shipping_cost), 2)
    payment_method = random.choice(payment_methods)
    financial_transactions.append([
        transaction_id,
        payment_date,
        payment_amount,
        is_refunded,
        refund_date,
        refund_amount,
        net_amount,
        net_amount_with_shipping,
        payment_method,
        transaction_status,
        shipping_cost
    ])

df_financial_transactions = pd.DataFrame(
    financial_transactions,
    columns=["Transaction_ID", "Payment_Date", "Payment_Amount", "Is_Refunded", "Refund_Date", "Refund_Amount", "Net_Amount", "Net_Amount_Without_Shipping", "Payment_Method", "Transaction_Status", "Shipping_Cost"]
)

df_financial_transactions.to_csv("financial_transactions.csv", index=False)
insert_data_to_table(df_financial_transactions, "Financial_Transactions")

transactions_ids_copy = transaction_id_list.copy()
client_delivery_ids_list = []
client_deliveries = []

for index, row in df_financial_transactions.iterrows():
    transaction_id = row["Transaction_ID"]
    client_info = df_transactions[df_transactions["Transaction_ID"] == transaction_id]
    if not client_info.empty:
        client_id = client_info.iloc[0]["Client_ID"]
        product_id = client_info.iloc[0]["Product_ID"]
    else:
        continue
    client_delivery_id = str(uuid.uuid4())
    client_delivery_ids_list.append(client_delivery_id)
    if row["Transaction_Status"] == "Pending":
        delivery_status = "Pending"
        carrier = ""
        estimated_delivery_date = None
        actual_delivery_date = None
    else:
        delivery_status = random.choices(
            ["Shipped", "Delivered", "Delayed", "Cancelled"],
            weights=[0.29, 0.59, 0.1, 0.01]
        )[0]
        carrier = random.choice(["DHL", "Royal Mail", "DPD"])
        order_date = client_info.iloc[0]["Order_Date"]
        estimated_delivery_date = order_date + timedelta(days=random.randint(1, 10)) if delivery_status in ("Shipped", "Delivered", "Delayed") else None
        actual_delivery_date = None
        if delivery_status == "Delivered":
            if random.random() < 0.95:
                actual_delivery_date = estimated_delivery_date - timedelta(days=random.randint(0, 10))
                if actual_delivery_date < order_date:
                    actual_delivery_date = order_date
            else:
                actual_delivery_date = estimated_delivery_date + timedelta(days=random.randint(1, 10))
        elif delivery_status == "Delayed":
            actual_delivery_date = estimated_delivery_date + timedelta(days=random.uniform(7, 30))
    client_deliveries.append([
        transaction_id,
        client_id,
        product_id,
        client_delivery_id,
        delivery_status,
        carrier,
        estimated_delivery_date,
        actual_delivery_date
    ])

df_client_deliveries = pd.DataFrame(
    client_deliveries,
    columns=["Transaction_ID", "Client_ID", "Product_ID", "Client_Delivery_ID", "Delivery_Status", "Carrier", "Estimated_Delivery_Date", "Actual_Delivery_Date"]
)

df_client_deliveries.to_csv("client_deliveries.csv", index=False)
insert_data_to_table(df_client_deliveries, "Client_Deliveries")

delivered_transactions = df_client_deliveries[df_client_deliveries['Delivery_Status'] == 'Delivered']
num_reviews = len(delivered_transactions)
review_ids = [str(uuid.uuid4()) for _ in range(num_reviews)]
client_ids = delivered_transactions["Client_ID"].values
transaction_ids = delivered_transactions["Transaction_ID"].values
product_ids_for_reviews = delivered_transactions["Product_ID"].values
actual_delivery_dates = delivered_transactions["Actual_Delivery_Date"].values
estimated_delivery_dates = delivered_transactions["Estimated_Delivery_Date"].values

delivered_with_refund = delivered_transactions.merge(
    df_financial_transactions[["Transaction_ID", "Is_Refunded"]],
    on="Transaction_ID",
    how="left"
).fillna({"Is_Refunded": "No"})

refund_statuses = delivered_with_refund["Is_Refunded"].values
review_dates = [actual_delivery_dates[i] + timedelta(days=random.randint(2, 3)) for i in range(num_reviews)]

def assign_review_rating(refund_status, actual_date, estimated_date):
    if refund_status == "Yes":
        return random.choice([1, 2])
    elif actual_date > estimated_date:
        return 4
    else:
        return random.choice([4, 5])

star_ratings = [
    assign_review_rating(refund_statuses[i], actual_delivery_dates[i], estimated_delivery_dates[i])
    for i in range(num_reviews)
]

df_reviews = pd.DataFrame({
    "Review_ID": review_ids,
    "Client_ID": client_ids,
    "Transaction_ID": transaction_ids,
    "Product_ID": product_ids_for_reviews,
    "Review_Date": review_dates,
    "Star_Rating": star_ratings
})

df_reviews.to_csv("generated_reviews.csv", index=False)
insert_data_to_table(df_reviews, "Client_Reviews")

def generate_product_damage_data(conn, num_records=1000):
    existing_transaction_ids = pd.read_sql_query("SELECT Transaction_ID FROM Sales;", conn)['Transaction_ID'].tolist()
    existing_client_ids = pd.read_sql_query("SELECT Client_ID FROM Clients;", conn)['Client_ID'].tolist()
    existing_product_ids = pd.read_sql_query("SELECT Product_ID FROM Products;", conn)['Product_ID'].tolist()
    existing_review_ids = pd.read_sql_query("SELECT Review_ID FROM Client_Reviews;", conn)['Review_ID'].tolist()
    existing_client_delivery_ids = pd.read_sql_query("SELECT Client_Delivery_ID FROM Client_Deliveries;", conn)['Client_Delivery_ID'].tolist()
    if not (existing_transaction_ids and existing_client_ids and existing_product_ids and existing_review_ids and existing_client_delivery_ids):
        return
    damage_reports = []
    for _ in range(num_records):
        damage_report_id = str(uuid.uuid4())
        try:
            transaction_id = random.choice(existing_transaction_ids)
            client_id = random.choice(existing_client_ids)
            product_id = random.choice(existing_product_ids)
            review_id = random.choice(existing_review_ids)
            client_delivery_id = random.choice(existing_client_delivery_ids)
        except IndexError:
            return
        damage_type = random.choice([
            "Product Defect", "Shipping Damage", "Wrong Item",
            "Missing Item", "Packaging Issue"
        ])
        resolution_status = random.choice(["Pending", "In Progress", "Resolved"])
        resolution_date = fake.date_this_decade() if resolution_status == "Resolved" else None
        damage_reports.append([
            damage_report_id, transaction_id, client_id, product_id, review_id, client_delivery_id, damage_type, resolution_status, resolution_date
        ])
    df_damage_reports = pd.DataFrame(
        damage_reports,
        columns=[
            "Damage_Report_ID", "Transaction_ID", "Client_ID", "Product_ID", "Review_ID",
            "Client_Delivery_ID", "Damage_Type", "Resolution_Status", "Resolution_Date"
        ]
    )
    df_damage_reports.to_csv("damage_reports.csv", index=False)
    insert_data_to_table(df_damage_reports, "Product_Damage")

generate_product_damage_data(conn, num_records=1000)

def create_connection():
    try:
        conn2 = sqlite3.connect("business_data.db", timeout=30)
        conn2.execute("PRAGMA foreign_keys = ON;")
        conn2.execute("PRAGMA synchronous = OFF;")
        conn2.execute("PRAGMA journal_mode = DELETE;")
        return conn2
    except Exception:
        return None

def insert_data_to_table_retry(df, table_name, max_retries=5):
    retries = 0
    while retries < max_retries:
        try:
            conn2 = create_connection()
            if conn2 is None:
                raise Exception("Failed to establish a connection.")
            cursor2 = conn2.cursor()
            cursor2.execute(f"DELETE FROM {table_name};")
            conn2.commit()
            conn2.execute("BEGIN TRANSACTION;")
            columns = ', '.join(df.columns)
            placeholders = ', '.join(['?' for _ in df.columns])
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor2.executemany(query, df.values.tolist())
            conn2.commit()
            cursor2.execute("VACUUM;")
            conn2.commit()
            conn2.close()
            break
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                retries += 1
                time.sleep(5)
            else:
                break
        except Exception:
            break
        finally:
            try:
                conn2.close()
            except Exception:
                pass

def generate_distributors_data():
    countries = {
        "Brazil": {"prefix": "B", "phone_code": "+55", "postcode_format": "#####-###"},
        "Colombia": {"prefix": "C", "phone_code": "+57", "postcode_format": "######"},
        "Vietnam": {"prefix": "V", "phone_code": "+84", "postcode_format": "######"},
        "India": {"prefix": "I", "phone_code": "+91", "postcode_format": "######"},
        "Mexico": {"prefix": "M", "phone_code": "+52", "postcode_format": "#####"
        }
    }
    data = []
    for country, details in countries.items():
        distributor_id = details["prefix"] + str(random.randint(1000000, 9999999))
        name = fake.company()
        email = name.replace(" ", "").replace(",", "").replace(".", "").lower() + "@gmail.com"
        phone_number = f"{details['phone_code']} {random.randint(1000000000, 9999999999)}"
        street_address = fake.street_address()
        city = fake.city()
        county = fake.state()
        postcode = fake.postcode()
        data.append([
            distributor_id, name, email, phone_number, street_address, city, county, postcode, country
        ])
    df_distributor_info_local = pd.DataFrame(data, columns=[
        "Distributor_ID", "Distributor_Name", "Email", "Phone_Number", "Street_Address", "City", "County", "Postcode", "Country"
    ])
    return df_distributor_info_local

df_distributor_info = generate_distributors_data()
insert_data_to_table_retry(df_distributor_info, "Distributors")

distributor_df = df_distributor_info.copy()
distributor_ids = distributor_df["Distributor_ID"].tolist()
product_ids_dummy = [str(uuid.uuid4()) for _ in range(10)]
batch_counter = 0

def generate_supply_data(distributor_ids, product_ids):
    global batch_counter
    statuses = ["Delivered"] * 50 + ["Pending"] * 20 + ["Shipped"] * 15 + ["Delayed"] * 10 + ["Cancelled"] * 5
    shipping_methods = ["Air"] * 50 + ["Road"] * 30 + ["Water"] * 20
    supply_data = []
    for distributor_id in distributor_ids:
        for product_id in product_ids:
            batch_id = f"B{batch_counter:05d}"
            batch_counter += 1
            batch_quantity = random.randint(200, 400)
            delivery_status = random.choice(statuses)
            shipping_method = random.choice(shipping_methods)
            base_date = datetime(2024, random.randint(1, 12), random.randint(1, 28))
            estimated_arrival = base_date + timedelta(days=random.randint(1, 30))
            if delivery_status in ["Pending", "Cancelled"]:
                actual_arrival = None
            elif shipping_method == "Air":
                actual_arrival = estimated_arrival + timedelta(days=random.choice([-3, -2, -1, 0]))
            elif shipping_method == "Road":
                actual_arrival = estimated_arrival + timedelta(days=random.choice([-1, 0, 1, 2]))
            elif shipping_method == "Water":
                actual_arrival = estimated_arrival + timedelta(days=random.choice([1, 2, 3, 4, 5]))
            supply_data.append({
                "Batch_ID": batch_id,
                "Distributor_ID": distributor_id,
                "Product_ID": product_id,
                "Batch_Quantity": batch_quantity,
                "Delivery_Status": delivery_status,
                "Shipping_Method": shipping_method,
                "Estimated_Arrival_Date": estimated_arrival.strftime('%Y-%m-%d'),
                "Actual_Arrival_Date": actual_arrival.strftime('%Y-%m-%d') if actual_arrival else None
            })
    return pd.DataFrame(supply_data)

supply_df = generate_supply_data(distributor_ids, product_ids_dummy)
supply_df.to_csv("distributor_supply_data.csv", index=False)
insert_data_to_table_retry(supply_df, "Distributor_Supply")

conn_norm = sqlite3.connect("business_data.db")
tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn_norm)
table_names = tables['name'].tolist()
results = []
for table_name in table_names:
    df_structure = pd.read_sql_query(f"PRAGMA table_info({table_name});", conn_norm)
    df_foreign_keys = pd.read_sql_query(f"PRAGMA foreign_key_list({table_name});", conn_norm)
    num_columns = len(df_structure)
    primary_keys = df_structure[df_structure['pk'] > 0]['name'].tolist()
    foreign_keys = df_foreign_keys['from'].tolist()
    non_key_columns = [col for col in df_structure['name'] if col not in primary_keys]
    first_nf = all(df_structure['type'].apply(lambda x: x not in ['BLOB', 'LIST', 'ARRAY']))
    if len(primary_keys) == 1:
        second_nf = True
    else:
        second_nf = all(col in primary_keys or col in foreign_keys for col in non_key_columns)
    third_nf = True
    if len(primary_keys) == 1:
        for column in non_key_columns:
            if column in foreign_keys:
                continue
            if column.endswith("_ID"):
                third_nf = False
                break
    else:
        for column in non_key_columns:
            if column not in primary_keys and column not in foreign_keys:
                third_nf = False
                break
    results.append({
        "Table": table_name,
        "Primary Keys": str(primary_keys),
        "Foreign Keys": str(foreign_keys),
        "1NF (Atomicity)": "0" if first_nf else "1",
        "2NF (Full Dependency)": "0" if second_nf else "1",
        "3NF (No Transitive Dependency)": "0" if third_nf else "1"
    })
conn_norm.close()
df_results = pd.DataFrame(results)

conn_kpi = sqlite3.connect("business_data.db")
df_kpi1 = pd.read_sql_query("""
SELECT DISTINCT
    COUNT(ci.Client_ID) AS CLIENT_ID_COUNT,
    ci.County,
    cd.Delivery_Status,
    cd.Carrier,
    cr.Star_Rating,
    pc.Product_Name,
    pc.Format_Category
FROM Clients ci
LEFT JOIN Sales ss on ss.Client_ID = ci.Client_ID
LEFT JOIN Client_Deliveries cd ON ci.Client_ID = cd.Client_ID
LEFT JOIN Client_Reviews cr ON ci.Client_ID = cr.Client_ID
LEFT JOIN Financial_Transactions ft ON ft.Transaction_ID = ss.Transaction_ID
LEFT JOIN Products pc ON ss.Product_ID = pc.Product_ID
WHERE cd.Delivery_Status IN ('Delivered','Delayed')
AND cr.Star_Rating != 0
GROUP BY ci.County, cd.Delivery_Status, cd.Carrier, cr.Star_Rating, pc.Product_Name, pc.Format_Category
""", conn_kpi)
df_kpi1.to_csv("kpi1.csv", index=False)

df_kpi2 = pd.read_sql_query("""
SELECT
    ci.Client_ID,
    ss.Transaction_ID,
    ci.County,
    cd.Carrier,
    cd.Estimated_Delivery_Date,
    cd.Actual_Delivery_Date,
    julianday(Estimated_Delivery_Date) - julianday(Actual_Delivery_Date) AS DELAY_DAYS,
    cd.Delivery_Status,
    ft.Is_Refunded
FROM Clients ci
LEFT JOIN Sales ss ON ss.Client_ID = ci.Client_ID
LEFT JOIN Client_Deliveries cd ON cd.Transaction_ID = ss.Transaction_ID
LEFT JOIN Client_Reviews cr ON cr.Client_ID = ci.Client_ID AND cr.Product_ID = ss.Product_ID
LEFT JOIN Financial_Transactions ft ON ft.Transaction_ID = ss.Transaction_ID
WHERE cd.Delivery_Status IN ('Delivered','Delayed')
""", conn_kpi)
df_kpi2.to_csv("kpi2.csv", index=False)

df_kpi3 = pd.read_sql_query("""
WITH Refund_By_Product AS (
    SELECT pc.Product_Name, pc.Format_Category, SUM(ft.Refund_Amount) AS Total_Refund_Amount
    FROM Sales ss
    LEFT JOIN Products pc ON ss.Product_ID = pc.Product_ID
    LEFT JOIN Financial_Transactions ft ON ss.Transaction_ID = ft.Transaction_ID
    WHERE ft.Refund_Amount IS NOT NULL AND ft.Is_Refunded = 'Yes'
    GROUP BY pc.Product_Name, pc.Format_Category
),
Revenue_By_Product AS (
    SELECT pc.Product_Name, pc.Format_Category, SUM(ft.Payment_Amount) AS Total_Revenue
    FROM Sales ss
    LEFT JOIN Products pc ON ss.Product_ID = pc.Product_ID
    LEFT JOIN Financial_Transactions ft ON ss.Transaction_ID = ft.Transaction_ID
    WHERE ft.Payment_Amount IS NOT NULL
    GROUP BY pc.Product_Name, pc.Format_Category
)
SELECT
    rbp.Product_Name,
    rbp.Format_Category,
    rbp.Total_Refund_Amount,
    rb.Total_Revenue,
    ROUND((rbp.Total_Refund_Amount * 100.0 / rb.Total_Revenue), 2) AS Refund_Percentage
FROM Refund_By_Product rbp
LEFT JOIN Revenue_By_Product rb
ON rbp.Product_Name = rb.Product_Name AND rbp.Format_Category = rb.Format_Category;
""", conn_kpi)
df_kpi3.to_csv("kpi3.csv", index=False)

df_delivery_status = pd.read_sql_query("""
SELECT
    cd.Carrier,
    cd.Delivery_Status,
    COUNT(*) AS Status_Count,
    (SELECT COUNT(*) FROM Client_Deliveries WHERE Carrier = cd.Carrier) AS Total_Deliveries,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM Client_Deliveries WHERE Carrier = cd.Carrier), 2) AS Status_Percentage
FROM Client_Deliveries cd
WHERE cd.Carrier IS NOT NULL AND cd.Carrier <> ''
GROUP BY cd.Carrier, cd.Delivery_Status
""", conn_kpi)

df_refund_damage = pd.read_sql_query("""
SELECT
    pd.Damage_Type AS Damage_Type,
    'Refund Related' AS Category,
    COUNT(*) AS Status_Count
FROM Product_Damage pd
WHERE pd.Damage_Type IS NOT NULL
GROUP BY pd.Damage_Type
""", conn_kpi)

conn_kpi.close()
df_delivery_status.to_csv("Delivery_Status.csv", index=False)
df_refund_damage.to_csv("Refund_Damage.csv", index=False)
