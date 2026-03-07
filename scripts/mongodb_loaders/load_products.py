"""
Phase 2E LOAD 1: Products collection.
Input: olist_products_dataset.csv
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pymongo
from pymongo import ReplaceOne

from .config import BATCH_SIZE, RAW_OLIST

# Portuguese category -> English (expand as needed)
CATEGORY_TRANSLATION = {
    "beleza_saude": "beauty_health",
    "informatica_acessorios": "computers_accessories",
    "perfumaria": "perfumery",
    "casa_conforto": "home_comfort",
    "esporte_lazer": "sports_leisure",
    "moveis_decoracao": "furniture_decoration",
    "utilidades_domesticas": "household_utilities",
    "brinquedos": "toys",
    "eletrodomesticos": "appliances",
    "automotivo": "automotive",
    "artes": "arts",
    "bebes": "baby",
    "instrumentos_musicais": "musical_instruments",
    "cool_stuff": "cool_stuff",
    "cama_mesa_banho": "bed_bath_table",
    "construcao_ferramentas_seguranca": "construction_tools",
    "malas_acessorios": "luggage_accessories",
    "ferramentas_jardim": "garden_tools",
    "moveis_escritorio": "office_furniture",
    "eletronicos": "electronics",
    "fashion_calcados": "fashion_shoes",
    "telefonia": "telephony",
    "papelaria": "stationery",
    "fashion_bolsas_e_acessorios": "fashion_bags",
    "pcs": "computers",
    "casa_construcao": "home_construction",
    "relogios_presentes": "watches_gifts",
    "pet_shop": "pet_shop",
    "eletroportateis": "portable_appliances",
    "agro_industria_e_comercio": "agro_industry",
}


def _translate_category(cat_pt: str | None) -> str | None:
    if cat_pt is None or (isinstance(cat_pt, float) and pd.isna(cat_pt)):
        return None
    s = str(cat_pt).strip()
    return CATEGORY_TRANSLATION.get(s, s.replace("_", " "))


def _safe_int(v, default=None):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    try:
        return int(float(v))
    except (ValueError, TypeError):
        return default


def _safe_float(v, default=None):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    try:
        return float(v)
    except (ValueError, TypeError):
        return default


def transform_row(row: pd.Series) -> dict:
    """Transform CSV row to Phase 2E product document."""
    product_id = str(row["product_id"]).strip()
    cat_pt = row.get("product_category_name")
    cat_en = _translate_category(cat_pt) if pd.notna(cat_pt) else None
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    doc = {
        "_id": product_id,
        "product_id": product_id,
        "category": cat_en or str(cat_pt) if pd.notna(cat_pt) else None,
        "attributes": {
            "weight_g": _safe_int(row.get("product_weight_g")),
            "dimensions": {
                "length_cm": _safe_float(row.get("product_length_cm")),
                "height_cm": _safe_float(row.get("product_height_cm")),
                "width_cm": _safe_float(row.get("product_width_cm")),
            },
            "photos_qty": _safe_int(row.get("product_photos_qty")),
            "name_length": _safe_int(row.get("product_name_lenght")) or _safe_int(row.get("product_name_length")),
            "description_length": _safe_int(row.get("product_description_lenght")) or _safe_int(row.get("product_description_length")),
        },
        "metadata": {
            "created_at": now,
            "last_updated": now,
        },
    }
    return doc


def load_products(db: pymongo.database.Database, drop: bool = False) -> int:
    """Load products into MongoDB. Returns count loaded."""
    products_path = RAW_OLIST / "olist_products_dataset.csv"
    if not products_path.exists():
        raise FileNotFoundError(f"Missing {products_path}")

    coll = db["products"]
    if drop:
        coll.drop()

    df = pd.read_csv(products_path).drop_duplicates(subset=["product_id"], keep="first")
    docs = [transform_row(row) for _, row in df.iterrows()]

    for i in range(0, len(docs), BATCH_SIZE):
        batch = docs[i : i + BATCH_SIZE]
        ops = [ReplaceOne({"_id": d["_id"]}, d, upsert=True) for d in batch]
        coll.bulk_write(ops, ordered=False)
    return len(docs)
