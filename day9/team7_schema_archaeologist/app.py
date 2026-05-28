import json
import os
import sys
from datetime import datetime

import duckdb
import pandas as pd
import streamlit as st
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "shared"))

from bedrock_helper import call_nova_lite, call_nova_pro
from sample_data import MIGRATION_V1_TO_V2, MIGRATION_V2_TO_V3, SCHEMA_V1, SCHEMA_V2, SCHEMA_V3


APP_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(APP_DIR, "..", "shared", "sigma_platform.duckdb")
VERDICT_PATH = os.path.join(APP_DIR, "verdict.json")


st.set_page_config(page_title="Schema Archaeologist", layout="wide")


def safe_ai_call(fn, fallback: str, system: str, user: str, max_tokens: int = 1200) -> str:
    try:
        return fn(system, user, max_tokens=max_tokens)
    except (NoCredentialsError, BotoCoreError, ClientError, Exception) as exc:
        return f"{fallback}\n\n_Local fallback used because Bedrock was unavailable: {type(exc).__name__}._"


@st.cache_resource
def get_connection():
    return duckdb.connect(DB_PATH, read_only=True)


@st.cache_data
def table_df(query: str) -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        return con.execute(query).fetchdf()
    finally:
        con.close()


def schema_profile(table_name: str) -> pd.DataFrame:
    return table_df(f"DESCRIBE {table_name}")


def row_count(table_name: str) -> int:
    return int(table_df(f"SELECT COUNT(*) AS n FROM {table_name}")["n"].iloc[0])


def column_names(table_name: str) -> list[str]:
    return schema_profile(table_name)["column_name"].tolist()


def schema_diff(left: str, right: str) -> pd.DataFrame:
    left_cols = set(column_names(left))
    right_cols = set(column_names(right))
    rows = []
    for col in sorted(left_cols | right_cols):
        if col in left_cols and col in right_cols:
            status = "kept"
        elif col in right_cols:
            status = "added"
        else:
            status = "removed"
        rows.append({"column": col, "change": status, left: col in left_cols, right: col in right_cols})
    return pd.DataFrame(rows)


def historian_prompt() -> str:
    return f"""Compare these three transaction schemas and reconstruct the likely business story.

Schema v1:
{SCHEMA_V1}

Schema v2:
{SCHEMA_V2}

Schema v3:
{SCHEMA_V3}

Write concise bullets for v1, v1 to v2, and v2 to v3. Include business motivation and operational risk."""


def auditor_prompt() -> str:
    return f"""Review these migration steps and assign risk LOW, MEDIUM, HIGH, or CRITICAL.
Give specific reasons and what data/report could break.

Migration v1 to v2:
{MIGRATION_V1_TO_V2}

Migration v2 to v3:
{MIGRATION_V2_TO_V3}"""


HISTORIAN_FALLBACK = """- v1: Early transaction ledger focused on payment facts: transaction id, amount, status, merchant, and date.
- v1 to v2: Customer analytics and payment-channel analysis arrived, so customer_id and payment_method were added.
- v2 to v3: Identity language moved from customer_id to user_id, probably for product/platform consistency.
- Hidden risk: v3 removes payment_method, which erases the ability to segment UPI, card, and debit-card behavior."""


AUDITOR_FALLBACK = """- v1 to v2: LOW risk. It adds nullable columns and preserves existing transaction facts, though backfill quality should be checked.
- v2 to v3: MEDIUM risk on paper because customer_id is renamed to user_id and payment_method is dropped.
- Audit concern: Dropping payment_method can break channel-based revenue, failure-rate, and UPI adoption reports.
- Corrected judgment after evidence: the drop is CRITICAL because downstream reports can still run with wrong zero/null results."""


def build_verdict() -> dict:
    upi_v2 = table_df(
        """
        SELECT
            payment_method,
            COUNT(*) AS transactions,
            ROUND(SUM(amount), 2) AS revenue
        FROM txn_v2
        WHERE payment_method = 'UPI'
        GROUP BY payment_method
        """
    )
    silent_break = table_df(
        """
        SELECT
            COUNT(*) AS transactions,
            ROUND(SUM(amount), 2) AS revenue
        FROM (
            SELECT *, CAST(NULL AS VARCHAR) AS payment_method
            FROM txn_v3
        )
        WHERE payment_method = 'UPI'
        """
    )
    return {
        "team": "team7_schema_archaeologist",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "verdict": "DO NOT MIGRATE AS WRITTEN",
        "dangerous_step": "v2_to_v3 drops payment_method",
        "risk_rating": "CRITICAL",
        "why_it_is_silent": "A compatibility view can add payment_method as NULL, allowing downstream UPI filters to run and return zero rows instead of failing.",
        "proof": {
            "v2_upi_transactions": int(upi_v2["transactions"].sum()) if not upi_v2.empty else 0,
            "v2_upi_revenue": float(upi_v2["revenue"].sum()) if not upi_v2.empty else 0.0,
            "v3_compat_upi_transactions": int(silent_break["transactions"].iloc[0]),
            "v3_compat_upi_revenue": 0.0 if pd.isna(silent_break["revenue"].iloc[0]) else float(silent_break["revenue"].iloc[0]),
        },
        "downstream_query_that_breaks": "SELECT COUNT(*), SUM(amount) FROM txn_current WHERE payment_method = 'UPI';",
        "safer_migration": [
            "Keep payment_method in v3 until every downstream report is migrated.",
            "If a rename is required, create a compatibility view that maps customer_id to user_id but preserves payment_method.",
            "Add CI checks comparing channel-level counts and revenue between v2 and v3 before cutover.",
        ],
        "what_ai_got_wrong": "The AI auditor may label the dropped column as medium schema cleanup, but business evidence shows it destroys payment-channel reporting.",
    }


def save_verdict(verdict: dict) -> None:
    with open(VERDICT_PATH, "w", encoding="utf-8") as f:
        json.dump(verdict, f, indent=2)


st.title("Schema Archaeologist")
st.caption("Sigma DataTech AI Ops Platform - Day 9 - Team 7")

if not os.path.exists(DB_PATH):
    st.error("Shared DuckDB database not found. Run `python day9/shared/setup_duckdb.py` first.")
    st.stop()

conn = get_connection()
verdict = build_verdict()
save_verdict(verdict)

top = st.columns([1, 1, 1, 1])
top[0].metric("Schema Versions", "3")
top[1].metric("Rows Per Version", row_count("txn_v2"))
top[2].metric("UPI Revenue at Risk", f"{verdict['proof']['v2_upi_revenue']:,.2f}")
top[3].metric("Verdict", "Do Not Migrate")

st.divider()

st.subheader("Schema Timeline")
schema_tabs = st.tabs(["v1", "v2", "v3", "Diffs"])
with schema_tabs[0]:
    st.code(SCHEMA_V1, language="sql")
    st.dataframe(schema_profile("txn_v1"), use_container_width=True, hide_index=True)
with schema_tabs[1]:
    st.code(SCHEMA_V2, language="sql")
    st.dataframe(schema_profile("txn_v2"), use_container_width=True, hide_index=True)
with schema_tabs[2]:
    st.code(SCHEMA_V3, language="sql")
    st.dataframe(schema_profile("txn_v3"), use_container_width=True, hide_index=True)
with schema_tabs[3]:
    c1, c2 = st.columns(2)
    c1.markdown("**v1 to v2**")
    c1.dataframe(schema_diff("txn_v1", "txn_v2"), use_container_width=True, hide_index=True)
    c2.markdown("**v2 to v3**")
    c2.dataframe(schema_diff("txn_v2", "txn_v3"), use_container_width=True, hide_index=True)

st.subheader("Round 1 - AI Historian")
historian_text = safe_ai_call(
    call_nova_pro,
    HISTORIAN_FALLBACK,
    "You are a senior data platform historian explaining schema evolution to business stakeholders.",
    historian_prompt(),
)
st.markdown(historian_text)

st.subheader("Round 2 - AI Risk Auditor")
m1, m2 = st.columns(2)
with m1:
    st.markdown("**Migration v1 to v2**")
    st.code(MIGRATION_V1_TO_V2, language="sql")
with m2:
    st.markdown("**Migration v2 to v3**")
    st.code(MIGRATION_V2_TO_V3, language="sql")

auditor_text = safe_ai_call(
    call_nova_lite,
    AUDITOR_FALLBACK,
    "You are a cautious data migration risk auditor.",
    auditor_prompt(),
)
st.markdown(auditor_text)

st.subheader("Round 3 - Archaeological Finding")
st.warning("The dangerous step is v2 to v3: `payment_method` is dropped. This is critical because UPI reports can return wrong zero results without crashing.")

q1 = """
SELECT
    payment_method,
    COUNT(*) AS transactions,
    ROUND(SUM(amount), 2) AS revenue
FROM txn_v2
WHERE payment_method = 'UPI'
GROUP BY payment_method;
"""
q2 = """
SELECT
    COUNT(*) AS transactions,
    ROUND(SUM(amount), 2) AS revenue
FROM (
    SELECT *, CAST(NULL AS VARCHAR) AS payment_method
    FROM txn_v3
)
WHERE payment_method = 'UPI';
"""

proof_left, proof_right = st.columns(2)
with proof_left:
    st.markdown("**Before migration: v2 UPI report**")
    st.code(q1, language="sql")
    st.dataframe(table_df(q1), use_container_width=True, hide_index=True)
with proof_right:
    st.markdown("**After unsafe compatibility migration: silently wrong**")
    st.code(q2, language="sql")
    st.dataframe(table_df(q2), use_container_width=True, hide_index=True)

st.subheader("Safer Migration")
safe_sql = """
CREATE VIEW txn_v3_compat AS
SELECT
    transaction_id,
    amount,
    status,
    merchant_id,
    customer_id AS user_id,
    transaction_date,
    payment_method
FROM txn_v2;

-- Cutover gate: must match before reports move to v3.
SELECT payment_method, COUNT(*) AS txns, SUM(amount) AS revenue
FROM txn_v3_compat
GROUP BY payment_method;
"""
st.code(safe_sql, language="sql")
st.markdown(
    "- Preserve `payment_method` until all downstream reports are migrated.\n"
    "- Ship a compatibility view first, then deprecate the old table after report owners sign off.\n"
    "- Add CI checks for payment-channel counts and revenue before every migration."
)

st.subheader("What AI Got Wrong")
st.info(verdict["what_ai_got_wrong"])

with st.expander("Saved verdict.json", expanded=False):
    st.json(verdict)

st.success(f"Verdict saved to {VERDICT_PATH}")
