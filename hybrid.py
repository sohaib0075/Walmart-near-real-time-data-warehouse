import csv
import threading
import time
import mysql.connector
from datetime import datetime
from collections import deque, defaultdict

CUSTOMER_CSV = r"C://Users//Shaban Hassan//OneDrive//Desktop//shaban//customer_master_data.csv"
PRODUCT_CSV  = r"C://Users//Shaban Hassan//OneDrive//Desktop//shaban//product_master_data.csv"
TRANS_CSV    = r"C://Users//Shaban Hassan//OneDrive//Desktop//shaban//transactional_data.csv"

HS = 10000     # Hash table slots
vP = 500       # Disk partition size
BATCH_SIZE = 1000   # Batch size for inserts

# Global buffers and counters
stream_buffer = deque()
hash_table = defaultdict(list) #multi map hash table, multiple stream entriess allowed for same procut id
queue = deque()
tuple_id_counter = 0
stream_finished = False

# Master data in memory
product_partitions = []
product_partition_index = {}
customer_lookup = {}

# Load master data

def load_product_master():
    global product_partitions, product_partition_index
    rows = []
    with open(PRODUCT_CSV, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    product_partitions = [rows[i:i+vP] for i in range(0, len(rows), vP)]
    for p_idx, part in enumerate(product_partitions):
        for row in part:
            product_partition_index[row["Product_ID"]] = p_idx
    print(f"[INIT] Product master loaded → {len(rows)} rows, {len(product_partitions)} partitions")

def load_customer_master():
    global customer_lookup
    with open(CUSTOMER_CSV, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            customer_lookup[r["Customer_ID"]] = r
    print(f"[INIT] Customer master loaded → {len(customer_lookup)} customers")


# Database connection
def get_connection():
    host = input("Host (default localhost): ") or "localhost"
    port = int(input("Port (default 3306): ") or 3306)
    dbname = input("Database name: ")
    user = input("DB user: ")
    password = input("DB password: ")
    conn = mysql.connector.connect(host=host, port=port, user=user, passwd=password, database=dbname, autocommit=False)
    print("[DB] Connected successfully.")
    return conn


# Batch upserts
def upsert_customers_batch(conn, customer_rows):
    cursor = conn.cursor()
    result = {}
    unique_customers = {r["Customer_ID"]: r for r in customer_rows}
    customer_ids = tuple(unique_customers.keys())
    if customer_ids:
        cursor.execute(f"SELECT customer_id, customer_sk FROM customer_dim_v2 WHERE customer_id IN ({','.join(['%s']*len(customer_ids))})", customer_ids)
        for cid, sk in cursor.fetchall():
            result[cid] = sk
    to_insert = [(cid, r["Gender"], r["Age"], r["Occupation"], r["City_Category"], r["Stay_In_Current_City_Years"], int(r["Marital_Status"]))
                 for cid, r in unique_customers.items() if cid not in result]
    if to_insert:
        cursor.executemany("""INSERT INTO customer_dim_v2 (customer_id, gender, age_group, occupation, city_category, stay_in_current_city_years, marital_status) VALUES (%s,%s,%s,%s,%s,%s,%s)""", to_insert)
        conn.commit()
        cursor.execute(f"SELECT customer_id, customer_sk FROM customer_dim_v2 WHERE customer_id IN ({','.join(['%s']*len(to_insert))})", [r[0] for r in to_insert])
        for cid, sk in cursor.fetchall():
            result[cid] = sk
    return result

def upsert_store_batch(conn, store_rows):
    cursor = conn.cursor()
    result = {}
    unique_stores = {r["store_id"]: r["store_name"] for r in store_rows}
    store_ids = tuple(unique_stores.keys())
    if store_ids:
        cursor.execute(f"SELECT store_id, store_sk FROM store_dim_v2 WHERE store_id IN ({','.join(['%s']*len(store_ids))})", store_ids)
        for sid, sk in cursor.fetchall():
            result[sid] = sk
    to_insert = [(sid, sname) for sid, sname in unique_stores.items() if sid not in result]
    if to_insert:
        cursor.executemany("INSERT IGNORE INTO store_dim_v2 (store_id, store_name) VALUES (%s,%s)", to_insert)
        conn.commit()
        cursor.execute(f"SELECT store_id, store_sk FROM store_dim_v2 WHERE store_id IN ({','.join(['%s']*len(to_insert))})", [r[0] for r in to_insert])
        for sid, sk in cursor.fetchall():
            result[sid] = sk
    return result

def upsert_supplier_batch(conn, supplier_rows):
    cursor = conn.cursor()
    result = {}
    unique_suppliers = {r["supplier_id"]: r["supplier_name"] for r in supplier_rows}
    supplier_ids = tuple(unique_suppliers.keys())
    if supplier_ids:
        cursor.execute(f"SELECT supplier_id, supplier_sk FROM supplier_dim_v2 WHERE supplier_id IN ({','.join(['%s']*len(supplier_ids))})", supplier_ids)
        for sid, sk in cursor.fetchall():
            result[sid] = sk
    to_insert = [(sid, sname) for sid, sname in unique_suppliers.items() if sid not in result]
    if to_insert:
        cursor.executemany("INSERT IGNORE INTO supplier_dim_v2 (supplier_id, supplier_name) VALUES (%s,%s)", to_insert)
        conn.commit()
        cursor.execute(f"SELECT supplier_id, supplier_sk FROM supplier_dim_v2 WHERE supplier_id IN ({','.join(['%s']*len(to_insert))})", [r[0] for r in to_insert])
        for sid, sk in cursor.fetchall():
            result[sid] = sk
    return result

def upsert_product_batch(conn, product_rows):
    cursor = conn.cursor()
    result = {}
    unique_products = {r["Product_ID"]: r for r in product_rows}
    product_ids = tuple(unique_products.keys())
    if product_ids:
        cursor.execute(f"SELECT product_id, product_sk FROM product_dim_v2 WHERE product_id IN ({','.join(['%s']*len(product_ids))})", product_ids)
        for pid, sk in cursor.fetchall():
            result[pid] = sk
    # Upsert stores first
    store_rows = [{"store_id": str(r["storeID"]), "store_name": r["storeName"]} for r in unique_products.values()]
    upsert_store_batch(conn, store_rows)
    # Upsert suppliers
    supplier_rows = [{"supplier_id": str(r["supplierID"]), "supplier_name": r["supplierName"]} for r in unique_products.values()]
    upsert_supplier_batch(conn, supplier_rows)
    to_insert = [(pid, r["Product_Category"], float(r["price$"]), str(r["storeID"]), str(r["supplierID"]), r["storeName"], r["supplierName"]) for pid, r in unique_products.items() if pid not in result]
    if to_insert:
        cursor.executemany("""INSERT IGNORE INTO product_dim_v2 (product_id, product_category, price, store_id, supplier_id, store_name, supplier_name) VALUES (%s,%s,%s,%s,%s,%s,%s)""", to_insert)
        conn.commit()
        cursor.execute(f"SELECT product_id, product_sk FROM product_dim_v2 WHERE product_id IN ({','.join(['%s']*len(to_insert))})", [r[0] for r in to_insert])
        for pid, sk in cursor.fetchall():
            result[pid] = sk
    return result

def ensure_date_batch(conn, date_list):
    cursor = conn.cursor()
    result = {}
    unique_dates = list(set(date_list))
    if not unique_dates:
        return result
    cursor.execute(f"SELECT date, date_sk FROM date_dim_v2 WHERE date IN ({','.join(['%s']*len(unique_dates))})", unique_dates)
    for d, sk in cursor.fetchall():
        result[str(d)] = sk
    to_insert = [(datetime.strptime(d, '%Y-%m-%d').date(), datetime.strptime(d, '%Y-%m-%d').year, datetime.strptime(d, '%Y-%m-%d').month, datetime.strptime(d, '%Y-%m-%d').day) for d in unique_dates if str(d) not in result]
    if to_insert:
        cursor.executemany("INSERT IGNORE INTO date_dim_v2 (date, year, month, day) VALUES (%s,%s,%s,%s)", to_insert)
        conn.commit()
        cursor.execute(f"SELECT date, date_sk FROM date_dim_v2 WHERE date IN ({','.join(['%s']*len(to_insert))})", [d[0] for d in to_insert])
        for d, sk in cursor.fetchall():
            result[str(d)] = sk
    return result

# Stream reader
def stream_reader():
    global tuple_id_counter, stream_finished
    with open(TRANS_CSV, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            tuple_id_counter += 1
            stream_buffer.append((tuple_id_counter, row))
            if tuple_id_counter % 1000 == 0:
                time.sleep(0.01)
    stream_finished = True
    print("[STREAM] Completed reading transactional CSV.")


# Hybrid join worker
def hybridjoin(conn):
    print("[HYBRIDJOIN] Started.")
    w = HS
    while True:
        moved = 0
        batch_rows = []

        while w > 0 and stream_buffer:
            tid, row = stream_buffer.popleft()
            key = row["Product_ID"]
            hash_table[key].append((tid, row)) # storing duplicates initailly as seperate entries
            queue.append(key)
            moved += 1
            w -= 1

        if moved:
            print(f"[HYBRIDJOIN] Loaded {moved} new stream records → w={w}")

        if stream_finished and not stream_buffer and all(len(v)==0 for v in hash_table.values()):
            break

        if not queue:
            time.sleep(0.05)
            continue

        oldest_key = queue[0]
        pidx = product_partition_index.get(oldest_key)
        if pidx is None:
            queue.rotate(-1)
            continue
        partition = product_partitions[pidx]
        matched = []

        for tid, srow in list(hash_table[oldest_key]):
            matched_prow = next((p for p in partition if p["Product_ID"]==oldest_key), None)
            if not matched_prow:
                continue
            batch_rows.append((srow, matched_prow))
            matched.append(tid)

            if len(batch_rows) >= BATCH_SIZE:
                process_batch(conn, batch_rows)
                batch_rows = []

        if batch_rows:
            process_batch(conn, batch_rows)

        for tid in matched: # each matched tuple is removed individually using its unique tuple id tid
            hash_table[oldest_key] = [t for t in hash_table[oldest_key] if t[0]!=tid]

        if matched:
            w += len(matched)
            print(f"[HYBRIDJOIN] Matched {len(matched)} rows for key {oldest_key} → w={w}")

        if len(hash_table.get(oldest_key, [])) == 0 and queue and queue[0]==oldest_key:
            queue.popleft()
        else:
            queue.rotate(-1)

    print("[HYBRIDJOIN] Completed.")


# Process batch
def process_batch(conn, batch_rows):
    customer_rows = [customer_lookup[r[0]["Customer_ID"]] for r in batch_rows]
    product_rows = [r[1] for r in batch_rows]
    date_list = [r[0]["date"] for r in batch_rows]

    cust_map = upsert_customers_batch(conn, customer_rows)
    prod_map = upsert_product_batch(conn, product_rows)
    date_map = ensure_date_batch(conn, date_list)

    store_ids = set(str(p["storeID"]) for _, p in batch_rows)
    supplier_ids = set(str(p["supplierID"]) for _, p in batch_rows)

    store_map = upsert_store_batch(conn, [{"store_id": s, "store_name": next((p["storeName"] for _, p in batch_rows if str(p["storeID"])==s), '')} for s in store_ids])
    supplier_map = upsert_supplier_batch(conn, [{"supplier_id": s, "supplier_name": next((p["supplierName"] for _, p in batch_rows if str(p["supplierID"])==s), '')} for s in supplier_ids])

    fact_values = []
    for srow, prow in batch_rows:
        cust_sk = cust_map[srow["Customer_ID"]]
        prod_sk = prod_map[prow["Product_ID"]]
        date_sk = date_map[srow["date"]]
        store_sk = store_map.get(str(prow["storeID"]))
        supplier_sk = supplier_map.get(str(prow["supplierID"]))
        qty = int(srow["quantity"])
        price = float(prow["price$"])
        total_price = qty * price
        fact_values.append((srow["orderID"], cust_sk, prod_sk, date_sk, qty, total_price, store_sk, supplier_sk))

    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO sales_fact_v2
        (order_id, customer_sk, product_sk, date_sk, quantity, total_price, store_sk, supplier_sk)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, fact_values)
    conn.commit()


# Main
def main():
    conn = get_connection()
    load_product_master()
    load_customer_master()

    t1 = threading.Thread(target=stream_reader)
    t2 = threading.Thread(target=hybridjoin, args=(conn,))

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    print("[DONE] All data loaded into DW.")

if __name__ == "__main__":
    main()
