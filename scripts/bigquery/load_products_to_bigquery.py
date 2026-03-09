"""Load staging tables to BigQuery for dashboard analysis."""
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except ImportError:
    pass

import pandas as pd
from google.cloud import bigquery

RAW = ROOT / "raw" / "olist"
PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "")


def main():
    if not PROJECT or not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("Set GOOGLE_CLOUD_PROJECT and GOOGLE_APPLICATION_CREDENTIALS")
        sys.exit(1)

    client = bigquery.Client(project=PROJECT)
    cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")

    # Load products with category translation
    products = pd.read_csv(RAW / "olist_products_dataset.csv")
    translation = pd.read_csv(RAW / "product_category_name_translation.csv")
    products = products.merge(translation, on="product_category_name", how="left")
    products = products[["product_id", "product_category_name", "product_category_name_english",
                         "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]].copy()
    ref = f"{PROJECT}.staging.stg_products"
    client.load_table_from_dataframe(products, ref, job_config=cfg).result()
    print(f"Loaded {len(products)} products to {ref}")

    # Load order items
    items = pd.read_csv(RAW / "olist_order_items_dataset.csv")
    items = items[["order_id", "order_item_id", "product_id", "seller_id", "price", "freight_value"]].copy()
    ref2 = f"{PROJECT}.staging.stg_order_items"
    client.load_table_from_dataframe(items, ref2, job_config=cfg).result()
    print(f"Loaded {len(items)} order items to {ref2}")

    # Load reviews
    reviews = pd.read_csv(RAW / "olist_order_reviews_dataset.csv")
    reviews = reviews[["review_id", "order_id", "review_score"]].drop_duplicates(subset=["review_id"])
    ref3 = f"{PROJECT}.staging.stg_reviews"
    client.load_table_from_dataframe(reviews, ref3, job_config=cfg).result()
    print(f"Loaded {len(reviews)} reviews to {ref3}")

    # Load customers (for repeat purchase analysis)
    customers = pd.read_csv(RAW / "olist_customers_dataset.csv")
    customers = customers[["customer_id", "customer_unique_id", "customer_zip_code_prefix",
                           "customer_city", "customer_state"]].copy()
    ref4 = f"{PROJECT}.staging.stg_customers"
    client.load_table_from_dataframe(customers, ref4, job_config=cfg).result()
    print(f"Loaded {len(customers)} customers to {ref4}")

    print("Done.")


if __name__ == "__main__":
    main()
