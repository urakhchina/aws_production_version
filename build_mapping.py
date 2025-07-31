# build_mapping.py
"""
Rebuilds the `product_mapping` dimension table.

Workflow:
1. Extract distinct (item_code, description) from `transactions`.
2. Clean descriptions -> canonical `name_clean`.
3. Group SKUs per product; optional fuzzy merge.
4. Write to temp table with sku_list as text[]; add PK.
5. Atomically swap into place, keeping the old table as `product_mapping_old`.
6. Optionally back‑fill `product_key` in `transactions`.

Run locally or from a post‑deploy hook:
    $ python build_mapping.py  # uses env vars for DB creds
"""

from __future__ import annotations

import logging
import os
import re
from typing import List

import pandas as pd
from rapidfuzz import fuzz, process
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import ARRAY, TEXT
from sqlalchemy.engine import Engine

# ---------------------------------------------------------------------------
# Config --------------------------------------------------------------------
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(format="%(asctime)s %(levelname)s | %(message)s", level=LOG_LEVEL)
logger = logging.getLogger(__name__)

PG_USER = os.getenv("PG_USER", "shababy")
PG_PASS = os.getenv("PG_PASS", "changeme")
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_DB   = os.getenv("PG_DB",   "eb-app-local-db")

# SQLAlchemy engine ----------------------------------------------------------
CONN_STR = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}/{PG_DB}"
engine: Engine = create_engine(CONN_STR)

# ---------------------------------------------------------------------------
# Cleaning helpers -----------------------------------------------------------
# ---------------------------------------------------------------------------
PACK_UNITS = r"(CT|CAPS?|SOFTGELS?|SG(?:ELS?)?)"

PREFIX_RE  = re.compile(r"^LIT (DISPENSER|CARD)\s+", re.I)
TRMARK_RE  = re.compile(r"[™®]")
PUNCT_RE   = re.compile(r"[^\w\s]")
PACK_RE    = re.compile(rf"\b(\d+)\s*{PACK_UNITS}\b", re.I)
MULTISPACE = re.compile(r"\s+")


def clean(txt: str) -> str:
    """Return canonical product name."""
    txt = txt.upper()
    txt = TRMARK_RE.sub("", txt)
    txt = PUNCT_RE.sub(" ", txt)
    txt = PREFIX_RE.sub("", txt)
    txt = PACK_RE.sub("", txt)
    txt = MULTISPACE.sub(" ", txt)
    return txt.strip()


# ---------------------------------------------------------------------------
# Build mapping --------------------------------------------------------------
# ---------------------------------------------------------------------------

def build_mapping(engine: Engine, enable_fuzzy: bool = False) -> int:
    """Returns number of rows written to product_mapping."""
    logger.info("Extracting distinct SKUs …")
    df_raw = pd.read_sql(
        """
        SELECT DISTINCT item_code, description
        FROM   transactions
        """,
        engine,
    )

    logger.info("Cleaning descriptions …")
    df_raw["name_clean"] = df_raw["description"].map(clean)

    logger.info("Grouping exact duplicates …")
    groups = (
        df_raw.groupby("name_clean")
        .agg(
            display_name=("description", "first"),
            sku_list=("item_code", lambda s: sorted(set(s))),
        )
        .reset_index()
    )
    logger.info("Exact groups: %d", len(groups))

    if enable_fuzzy:
        logger.info("Running fuzzy merge …")
        canon: List[str] = groups["name_clean"].tolist()
        unmapped = df_raw[~df_raw["name_clean"].isin(canon)]
        for _, row in unmapped.iterrows():
            peg, score, _ = process.extractOne(row["name_clean"], canon, scorer=fuzz.token_set_ratio)
            if score >= 90:
                idx = groups.index[groups["name_clean"] == peg][0]
                groups.at[idx, "sku_list"].append(row["item_code"])
            else:
                groups = pd.concat(
                    [
                        groups,
                        pd.DataFrame(
                            {
                                "name_clean": [row["name_clean"]],
                                "display_name": [row["description"]],
                                "sku_list": [[row["item_code"]]],
                            }
                        ),
                    ],
                    ignore_index=True,
                )
        logger.info("After fuzzy merge: %d", len(groups))

    # Final tidy
    groups["sku_list"] = groups["sku_list"].map(lambda lst: sorted(set(lst)))
    groups = groups.sort_values("display_name").reset_index(drop=True)

    logger.info("Writing temp table …")
    groups.to_sql(
        "product_mapping_tmp",
        engine,
        if_exists="replace",
        index=False,
        dtype={"sku_list": ARRAY(TEXT)},
        method="multi",
    )

    with engine.begin() as cx:
        # Add PK
        cx.exec_driver_sql("ALTER TABLE product_mapping_tmp ADD PRIMARY KEY (name_clean);")
        # Validate duplicates
        dup = cx.exec_driver_sql(
            "SELECT COUNT(*) - COUNT(DISTINCT name_clean) FROM product_mapping_tmp;"
        ).scalar_one()
        if dup:
            raise RuntimeError(f"Duplicate keys detected ({dup}) — aborting swap")
        logger.info("No duplicate keys, proceeding to swap …")

        # Atomic swap
        cx.exec_driver_sql(
            """
            DROP TABLE IF EXISTS product_mapping_old;
            ALTER TABLE IF EXISTS product_mapping RENAME TO product_mapping_old;
            ALTER TABLE product_mapping_tmp RENAME TO product_mapping;
            """
        )

    logger.info("Swap complete; live rows: %d", len(groups))
    return len(groups)


# ---------------------------------------------------------------------------
# Entry‑point ---------------------------------------------------------------
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        rows = build_mapping(engine, enable_fuzzy=False)
        logger.info("✅ build_mapping finished — %d products", rows)
    except Exception as exc:
        logger.exception("❌ build_mapping failed: %s", exc)
        raise
