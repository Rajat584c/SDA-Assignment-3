"""
Assignment 3 - Kafka consumer
Reads the four e-commerce topics filled by producer.py (Assignment 2), cleans each
record and writes it into MySQL, which Grafana reads for the live dashboard.

    clickstream-topic -> clickstream_events
    orders-topic      -> orders            (CDC envelope, latest state per order_id)
    payments-topic    -> payments
    inventory-topic   -> inventory + products (product dimension)
    every message     -> stream_log        (topic, partition, offset, time consumed)

Run:  pip install kafka-python mysql-connector-python
      python consumer.py
"""

import json
import os
from datetime import datetime

import mysql.connector
from kafka import KafkaConsumer

# ---------------- config ----------------
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPICS = ["clickstream-topic", "orders-topic", "payments-topic", "inventory-topic"]
GROUP_ID = "ecom-dashboard-consumer"

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "root")
DB_NAME = "ecommerce_analytics"

SCHEMA = [
    """CREATE TABLE IF NOT EXISTS clickstream_events (
        event_id VARCHAR(20) PRIMARY KEY,
        session_id VARCHAR(20),
        user_id VARCHAR(20),
        is_guest BOOLEAN,
        event_type VARCHAR(30),
        product_id VARCHAR(10),
        search_term VARCHAR(50),
        device_type VARCHAR(20),
        os VARCHAR(20),
        browser VARCHAR(20),
        referrer VARCHAR(30),
        event_ts DATETIME,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS orders (
        order_id VARCHAR(20) PRIMARY KEY,
        user_id VARCHAR(20),
        product_id VARCHAR(10),
        quantity INT,
        order_amount DECIMAL(12,2),
        discount DECIMAL(12,2),
        net_amount DECIMAL(12,2),
        order_status VARCHAR(20),
        payment_method VARCHAR(20),
        shipping_city VARCHAR(30),
        cdc_op CHAR(1),
        created_at DATETIME,
        updated_at DATETIME,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS payments (
        transaction_id VARCHAR(20) PRIMARY KEY,
        order_id VARCHAR(20),
        gateway VARCHAR(20),
        payment_method VARCHAR(20),
        amount DECIMAL(12,2),
        currency CHAR(3),
        status VARCHAR(20),
        failure_reason VARCHAR(40),
        processed_at DATETIME,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS inventory (
        record_id VARCHAR(20) PRIMARY KEY,
        product_id VARCHAR(10),
        sku VARCHAR(20),
        warehouse_id VARCHAR(20),
        city VARCHAR(30),
        stock_on_hand INT,
        reserved_qty INT,
        available_qty INT,
        reorder_level INT,
        supplier_id VARCHAR(10),
        last_updated DATETIME,
        ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""",
    """CREATE TABLE IF NOT EXISTS products (
        product_id VARCHAR(10) PRIMARY KEY,
        sku VARCHAR(20),
        product_name VARCHAR(60),
        category VARCHAR(30),
        unit_price DECIMAL(12,2))""",
    """CREATE TABLE IF NOT EXISTS stream_log (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        topic VARCHAR(30),
        kafka_partition INT,
        kafka_offset BIGINT,
        consumed_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))""",
    # latest stock row for every product x warehouse, used by the low-stock panel
    """CREATE OR REPLACE VIEW inventory_latest AS
       SELECT i.*, p.product_name, p.category FROM (
           SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, warehouse_id
                                        ORDER BY last_updated DESC) AS rn
           FROM inventory) i
       JOIN products p USING (product_id)
       WHERE i.rn = 1""",
]


# ---------------- cleaning ----------------
def to_dt(value):
    """ISO-8601 string with a Z suffix -> naive UTC datetime (None stays None)."""
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


def upper(value):
    return value.strip().upper() if value else None


def clean_clickstream(r):
    row = {
        "event_id": r["event_id"],
        "session_id": r["session_id"],
        "user_id": r["user_id"] or "GUEST",        # ~15% of events are not logged in
        "is_guest": r["user_id"] is None,
        "event_type": r["event_type"].strip().lower(),
        "product_id": r["product_id"],
        "search_term": r["search_term"].strip().lower() if r["search_term"] else None,
        "device_type": r["device_type"],
        "os": r["os"],
        "browser": r["browser"],
        "referrer": r["referrer"],
        "event_ts": to_dt(r["event_timestamp"]),   # ip_address is dropped (PII)
    }
    return [("clickstream_events", row)]


def clean_order(r):
    """Debezium envelope: keep the 'after' image; a delete only has 'before'."""
    image = r["after"] or r["before"]
    row = {
        "order_id": image["order_id"],
        "user_id": image["user_id"],
        "product_id": image["product_id"],
        "quantity": int(image["quantity"]),
        "order_amount": float(image["order_amount"]),
        "discount": float(image["discount"] or 0),
        "net_amount": float(image["net_amount"]),
        "order_status": "DELETED" if r["op"] == "d" else upper(image["order_status"]),
        "payment_method": upper(image["payment_method"]),
        "shipping_city": image["shipping_city"],
        "cdc_op": r["op"],
        "created_at": to_dt(image["created_at"]),
        "updated_at": to_dt(image["updated_at"]),
    }
    return [("orders", row)]


def clean_payment(r):
    row = {
        "transaction_id": r["transaction_id"],
        "order_id": r["order_id"],
        "gateway": r["gateway"],
        "payment_method": upper(r["payment_method"]),
        "amount": float(r["amount"]),
        "currency": upper(r["currency"]) or "INR",
        "status": upper(r["status"]),
        "failure_reason": r["failure_reason"],
        "processed_at": to_dt(r["processed_at"]),  # card_last4 / vpa are dropped (PII)
    }
    return [("payments", row)]


def clean_inventory(r):
    stock = {
        "record_id": r["record_id"],
        "product_id": r["product_id"],
        "sku": r["sku"],
        "warehouse_id": r["warehouse_id"],
        "city": r["city"],
        "stock_on_hand": max(int(r["stock_on_hand"]), 0),
        "reserved_qty": max(int(r["reserved_qty"]), 0),
        "available_qty": max(int(r["available_qty"]), 0),
        "reorder_level": int(r["reorder_level"]),
        "supplier_id": r["supplier_id"],
        "last_updated": to_dt(r["last_updated"]),
    }
    product = {
        "product_id": r["product_id"],
        "sku": r["sku"],
        "product_name": r["product_name"],
        "category": r["category"],
        "unit_price": float(r["unit_price"]),
    }
    return [("inventory", stock), ("products", product)]


CLEANERS = {
    "clickstream-topic": clean_clickstream,
    "orders-topic": clean_order,
    "payments-topic": clean_payment,
    "inventory-topic": clean_inventory,
}


def upsert(table, row):
    """INSERT ... ON DUPLICATE KEY UPDATE, so a replayed message never duplicates a row."""
    cols = ", ".join(row)
    marks = ", ".join(["%s"] * len(row))
    updates = ", ".join(f"{c} = VALUES({c})" for c in row)
    sql = f"INSERT INTO {table} ({cols}) VALUES ({marks}) ON DUPLICATE KEY UPDATE {updates}"
    return sql, tuple(row.values())


def summary(topic, row):
    if topic == "orders-topic":
        return f"{row['order_id']}  {row['order_status']:<10} INR {row['net_amount']:>10,.2f}"
    if topic == "payments-topic":
        return f"{row['order_id']}  {row['gateway']:<9} {row['status']:<9} INR {row['amount']:>10,.2f}"
    if topic == "inventory-topic":
        return f"{row['sku']}  {row['warehouse_id']}  available={row['available_qty']}"
    return f"{row['user_id']:<6} {row['event_type']:<16} {row['product_id'] or '-'}"


# ---------------- main ----------------
def main():
    db = mysql.connector.connect(host=MYSQL_HOST, port=MYSQL_PORT,
                                 user=MYSQL_USER, password=MYSQL_PASSWORD)
    cur = db.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
    cur.execute(f"USE {DB_NAME}")
    cur.execute("SET time_zone = '+00:00'")
    for ddl in SCHEMA:
        cur.execute(ddl)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print(f"Consumer started  Kafka {KAFKA_BROKER} -> MySQL {MYSQL_HOST}:{MYSQL_PORT}/{DB_NAME}")
    print(f"Subscribed to: {', '.join(TOPICS)}")
    print("-" * 90)

    counts = {t: 0 for t in TOPICS}
    rejected = 0
    try:
        for msg in consumer:
            try:
                rows = CLEANERS[msg.topic](msg.value)
                for table, row in rows:
                    cur.execute(*upsert(table, row))
                cur.execute("INSERT INTO stream_log (topic, kafka_partition, kafka_offset) "
                            "VALUES (%s, %s, %s)", (msg.topic, msg.partition, msg.offset))
                db.commit()
                counts[msg.topic] += 1
                n = sum(counts.values())
                print(f"[{n:04d}] {msg.topic:<18} p{msg.partition} off={msg.offset:<4} "
                      f"{summary(msg.topic, rows[0][1])}", flush=True)
            except (KeyError, TypeError, ValueError) as err:
                db.rollback()
                rejected += 1
                print(f"[skip] {msg.topic} offset {msg.offset}: bad record ({err})", flush=True)
            consumer.commit()   # offset moves only after MySQL has the row
    except KeyboardInterrupt:
        print("\nStopped.")
    finally:
        print("consumed:", ", ".join(f"{t}={n}" for t, n in counts.items()), f"| rejected={rejected}")
        consumer.close()
        cur.close()
        db.close()


if __name__ == "__main__":
    main()
