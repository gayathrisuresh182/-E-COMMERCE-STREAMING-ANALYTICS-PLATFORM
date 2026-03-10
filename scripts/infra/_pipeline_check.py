import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault('GOOGLE_CLOUD_PROJECT', 'ecommerce-analytics-prod')
from dotenv import load_dotenv
load_dotenv()
from google.cloud import bigquery
c = bigquery.Client()

# Check staging tables that could be source for dimension tables
for table in ['stg_customers', 'stg_products', 'stg_sellers', 'stg_orders', 'stg_order_items']:
    try:
        rows = list(c.query(f"""
            SELECT column_name, data_type 
            FROM staging.INFORMATION_SCHEMA.COLUMNS 
            WHERE table_name='{table}' 
            ORDER BY ordinal_position
        """).result())
        if rows:
            print(f'\n=== staging.{table} ===')
            for row in rows:
                d = dict(row)
                print(f'  {d["column_name"]:35s} {d["data_type"]}')
            count = list(c.query(f'SELECT COUNT(*) as cnt FROM staging.{table}').result())
            print(f'  [ROW COUNT: {dict(count[0])["cnt"]}]')
    except Exception as e:
        print(f'\n=== staging.{table} === ERROR: {e}')

# Check $0 orders
print('\n=== $0 ORDERS CHECK ===')
rows = list(c.query("""
    SELECT 
        COUNT(*) as total,
        COUNTIF(order_total = 0) as zero_total,
        COUNTIF(order_total IS NULL) as null_total,
        COUNTIF(order_total > 0) as positive_total,
        MIN(DATE(order_purchase_timestamp)) as min_date,
        MAX(DATE(order_purchase_timestamp)) as max_date
    FROM marts.fct_orders
""").result())
for r in rows:
    print(f'  {dict(r)}')

# Check recent orders with $0
print('\n=== RECENT $0 ORDERS SAMPLE ===')
rows = list(c.query("""
    SELECT order_id, order_status, order_total, DATE(order_purchase_timestamp) as order_date
    FROM marts.fct_orders
    ORDER BY order_purchase_timestamp DESC
    LIMIT 20
""").result())
for r in rows:
    print(f'  {dict(r)}')

# Check if fct_orders has order_total from stg_order_items or stg_payments
print('\n=== ORDER TOTAL SOURCE CHECK ===')
rows = list(c.query("""
    SELECT o.order_id, o.order_total, 
           COALESCE(SUM(oi.price + oi.freight_value), 0) as items_total
    FROM marts.fct_orders o
    LEFT JOIN staging.stg_order_items oi ON o.order_id = oi.order_id
    WHERE o.order_total = 0
    GROUP BY o.order_id, o.order_total
    LIMIT 10
""").result())
for r in rows:
    print(f'  {dict(r)}')
