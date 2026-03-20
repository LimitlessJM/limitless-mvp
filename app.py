import calendar as pycal
import io
import json
import sqlite3
from datetime import date, datetime as _datetime, timezone as _tz, timedelta as _td
from pathlib import Path

def _today_aest():
    """Return today's date in AEST (UTC+10)."""
    return (_datetime.now(_tz.utc) + _td(hours=10)).date()

def _fmt_date(d):
    """Format a date string or date object as Australian format: 18 May 2026"""
    if not d: return ""
    try:
        if hasattr(d, "strftime"):
            return d.strftime("%-d %b %Y")
        # Try parsing common formats
        for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"]:
            try:
                from datetime import datetime as _dtp
                return _dtp.strptime(str(d).strip(), fmt).strftime("%-d %b %Y")
            except: pass
        return str(d)
    except: return str(d)

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

# ─────────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────────
# Use /data for Railway persistent volume, fallback to app directory
import os as _os
_data_dir = "/data" if _os.path.exists("/data") else str(Path(__file__).parent)
DB_PATH = Path(_data_dir) / "limitless.db"
CATALOGUE_PATH = Path(__file__).with_name("limitless_catalogue_clean_rebuilt.xlsx")
CALENDAR_PATH  = Path(__file__).with_name("calendar.html")

st.set_page_config(page_title="Limitless", layout="wide", page_icon="⬛")

# ── Database config ────────────────────────────────────────────────────────
import os as _os

# Railway PostgreSQL or SQLite fallback
DATABASE_URL = _os.environ.get("DATABASE_URL", "")
try:
    SUPABASE_URL = st.secrets.get("SUPABASE_URL", "")
    SUPABASE_KEY = st.secrets.get("SUPABASE_KEY", "")
    if not DATABASE_URL:
        DATABASE_URL = st.secrets.get("DATABASE_URL", "")
except:
    SUPABASE_URL = ""
    SUPABASE_KEY = ""

USE_POSTGRES = bool(DATABASE_URL)
USE_SUPABASE = False
_supa_client = None

# Supabase for mobile sync only
if SUPABASE_URL and SUPABASE_KEY:
    try:
        from supabase import create_client
        _supa_client = create_client(SUPABASE_URL, SUPABASE_KEY)
        USE_SUPABASE = True
    except:
        pass

if USE_POSTGRES:
    try:
        import psycopg2
        import psycopg2.extras
    except ImportError:
        USE_POSTGRES = False

def adapt_query(query):
    if USE_POSTGRES:
        return query.replace("?", "%s")
    return query

# ─── Global dark theme ────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

/* ── BASE — big readable text for tradies ── */
html, body, [class*="css"] {
    font-family: 'Inter', sans-serif !important;
    background-color: #0d1526 !important;
    color: #e2e8f0 !important;
    font-size: 18px !important;
    line-height: 1.7 !important;
}

/* ── Main content ── */
.main { background: #0d1526 !important; }
.main .block-container {
    background: #0d1526 !important;
    padding-top: 2rem !important;
    padding-bottom: 4rem !important;
    max-width: 1400px !important;
    padding-left: 2rem !important;
    padding-right: 2rem !important;
}

/* ── Sidebar ── */
section[data-testid="stSidebar"] {
    background: #080f1e !important;
    border-right: 2px solid #1e2d3d !important;
    min-width: 220px !important;
}
section[data-testid="stSidebar"] * { color: #94a3b8 !important; }
section[data-testid="stSidebar"] .stButton > button {
    font-size: 15px !important;
    font-weight: 500 !important;
    padding: 10px 16px !important;
    margin-bottom: 2px !important;
    border-radius: 8px !important;
    border: none !important;
    background: transparent !important;
    color: #94a3b8 !important;
    text-align: left !important;
    width: 100% !important;
}
section[data-testid="stSidebar"] .stButton > button:hover {
    background: #1e2d3d !important;
    color: #e2e8f0 !important;
    border: none !important;
}

/* ── Page titles ── */
h1 {
    font-weight: 900 !important;
    font-size: 2.4rem !important;
    color: #f1f5f9 !important;
    letter-spacing: -0.03em !important;
    margin-bottom: 0.5rem !important;
}
h2 {
    font-weight: 800 !important;
    font-size: 1.6rem !important;
    color: #e2e8f0 !important;
    margin-top: 1.2rem !important;
}
h3 {
    font-weight: 700 !important;
    font-size: 1.3rem !important;
    color: #cbd5e1 !important;
}
p { color: #cbd5e1 !important; font-size: 17px !important; }
li { color: #cbd5e1 !important; font-size: 17px !important; }
label {
    color: #94a3b8 !important;
    font-size: 15px !important;
    font-weight: 600 !important;
}
strong, b { color: #f1f5f9 !important; }

/* ── Tabs ── */
[data-testid="stTabs"] [data-baseweb="tab"] {
    font-size: 16px !important;
    font-weight: 700 !important;
    padding: 12px 20px !important;
    color: #64748b !important;
    letter-spacing: 0.01em !important;
}
[data-testid="stTabs"] [aria-selected="true"] {
    color: #2dd4bf !important;
    border-bottom: 3px solid #2dd4bf !important;
}

/* ── Metric cards ── */
[data-testid="metric-container"] {
    background: #162032 !important;
    border: 1px solid #2a3d4f !important;
    border-top: 4px solid #2dd4bf !important;
    border-radius: 12px !important;
    padding: 1.2rem 1.5rem !important;
}
[data-testid="metric-container"] label {
    font-size: 13px !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
    color: #64748b !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 2.2rem !important;
    font-weight: 900 !important;
    color: #2dd4bf !important;
    line-height: 1.2 !important;
}
[data-testid="metric-container"] label {
    font-size: 13px !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.12em !important;
    color: #64748b !important;
}

/* ── Buttons ── */
.stButton > button {
    border-radius: 10px !important;
    font-weight: 800 !important;
    font-size: 16px !important;
    border: 2px solid #2dd4bf !important;
    background: #2dd4bf !important;
    color: #0d1526 !important;
    padding: 0.7rem 1.6rem !important;
    transition: all 0.15s ease !important;
    letter-spacing: 0.02em !important;
}
.stButton > button:hover {
    background: #14b8a6 !important;
    border-color: #14b8a6 !important;
    transform: translateY(-1px) !important;
}
.stButton > button[kind="secondary"] {
    background: #162032 !important;
    color: #94a3b8 !important;
    border: 2px solid #2a3d4f !important;
}
.stButton > button[kind="secondary"]:hover {
    background: #2a3d4f !important;
    color: #e2e8f0 !important;
}

/* ── Inputs — BIG and clear ── */
input, textarea, select {
    background: #162032 !important;
    border: 2px solid #2a3d4f !important;
    border-radius: 10px !important;
    color: #f1f5f9 !important;
    font-size: 17px !important;
    padding: 12px 16px !important;
    line-height: 1.6 !important;
}
input:focus, textarea:focus {
    border-color: #2dd4bf !important;
    box-shadow: 0 0 0 3px rgba(45,212,191,0.2) !important;
    outline: none !important;
}
[data-baseweb="input"] { background: #162032 !important; }
[data-baseweb="input"] input { font-size: 16px !important; }
[data-baseweb="select"] > div {
    background: #162032 !important;
    border: 2px solid #2a3d4f !important;
    border-radius: 10px !important;
    color: #f1f5f9 !important;
    font-size: 17px !important;
    min-height: 48px !important;
}
[data-baseweb="select"] * { color: #f1f5f9 !important; }
[data-baseweb="popover"] {
    background: #162032 !important;
    border: 2px solid #2a3d4f !important;
    border-radius: 10px !important;
}
[role="option"] {
    background: #162032 !important;
    color: #e2e8f0 !important;
    font-size: 15px !important;
    padding: 10px 14px !important;
}
[role="option"]:hover { background: #2a3d4f !important; color: #2dd4bf !important; }

/* ── Number inputs ── */
[data-testid="stNumberInput"] input {
    background: #162032 !important;
    color: #f1f5f9 !important;
    font-size: 16px !important;
    font-weight: 600 !important;
}
[data-testid="stNumberInput"] button {
    background: #2a3d4f !important;
    border-color: #2a3d4f !important;
    color: #94a3b8 !important;
    font-size: 18px !important;
}

/* ── Date inputs ── */
[data-testid="stDateInput"] input {
    background: #162032 !important;
    color: #f1f5f9 !important;
    font-size: 16px !important;
}

/* ── Checkboxes ── */
[data-testid="stCheckbox"] label {
    color: #cbd5e1 !important;
    font-size: 15px !important;
}

/* ── Dividers ── */
hr { border-color: #1e2d3d !important; margin: 2rem 0 !important; }

/* ── Dataframes ── */
[data-testid="stDataFrame"] {
    border-radius: 12px !important;
    overflow: hidden !important;
    border: 1px solid #2a3d4f !important;
    font-size: 16px !important;
}

/* ── Alerts ── */
[data-testid="stAlert"] {
    border-radius: 10px !important;
    font-size: 15px !important;
    padding: 14px 18px !important;
}

/* ── Expanders ── */
details {
    background: #162032 !important;
    border: 1px solid #2a3d4f !important;
    border-radius: 12px !important;
}
details summary {
    color: #cbd5e1 !important;
    font-size: 15px !important;
    font-weight: 600 !important;
    padding: 12px 16px !important;
}

/* ── Forms ── */
[data-testid="stForm"] {
    background: #162032 !important;
    border: 1px solid #2a3d4f !important;
    border-radius: 14px !important;
    padding: 1.5rem !important;
}

/* ── Captions ── */
[data-testid="stCaptionContainer"] {
    color: #64748b !important;
    font-size: 14px !important;
}

/* ── Success / warning / error ── */
.stSuccess, [data-testid="stAlert"][data-baseweb="notification"] {
    font-size: 15px !important;
}

/* ── Scan sheet qty inputs — big and tabbable ── */
.scan-qty input {
    font-size: 20px !important;
    font-weight: 800 !important;
    text-align: center !important;
    color: #2dd4bf !important;
    background: #0d2233 !important;
    border: 2px solid #2dd4bf !important;
}
/* Make number inputs easier to tab through */
[data-testid="stNumberInput"] input:focus {
    border-color: #2dd4bf !important;
    box-shadow: 0 0 0 4px rgba(45,212,191,0.25) !important;
    font-size: 18px !important;
    font-weight: 800 !important;
    color: #2dd4bf !important;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 8px; height: 8px; }
::-webkit-scrollbar-track { background: #0d1526; }
::-webkit-scrollbar-thumb { background: #2a3d4f; border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: #2dd4bf; }

/* ── Slider ── */
[data-testid="stSlider"] { padding: 10px 0 !important; }

/* ── Tab content spacing ── */
[data-testid="stTabContent"] { padding-top: 1.5rem !important; }

/* ── Selectbox label ── */
[data-testid="stSelectbox"] label { font-size: 15px !important; color: #94a3b8 !important; font-weight: 600 !important; }
[data-testid="stTextInput"] label { font-size: 15px !important; color: #94a3b8 !important; font-weight: 600 !important; }
[data-testid="stNumberInput"] label { font-size: 15px !important; color: #94a3b8 !important; font-weight: 600 !important; }
[data-testid="stTextArea"] label { font-size: 15px !important; color: #94a3b8 !important; font-weight: 600 !important; }
[data-testid="stNumberInput"] input { font-size: 17px !important; font-weight: 700 !important; }
[data-testid="stCaptionContainer"] { font-size: 15px !important; color: #64748b !important; }

</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  DATABASE
# ─────────────────────────────────────────────
@st.cache_resource
def _get_pool():
    from psycopg2 import pool as _pool
    return _pool.ThreadedConnectionPool(2, 10, DATABASE_URL, connect_timeout=10)

def get_conn():
    if USE_POSTGRES:
        try:
            return _get_pool().getconn()
        except:
            return psycopg2.connect(DATABASE_URL, connect_timeout=10)
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def release_conn(conn):
    if USE_POSTGRES:
        try:
            _get_pool().putconn(conn)
            return
        except:
            pass
    try:
        conn.close()
    except:
        pass


def init_db():
    conn = get_conn()
    cur  = conn.cursor()

    # ── Rebuild jobs table with correct PRIMARY KEY if needed ─────────────
    # Older DB versions were created without PRIMARY KEY on job_id which
    # breaks ON CONFLICT clauses. Detect and rebuild transparently.
    existing_tables = {r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "jobs" in existing_tables:
        pk_cols = [r[1] for r in cur.execute("PRAGMA table_info(jobs)").fetchall() if r[5] == 1]
        if "job_id" not in pk_cols:
            cur.executescript("""
                ALTER TABLE jobs RENAME TO _jobs_old;
                CREATE TABLE jobs (
                    job_id                 TEXT PRIMARY KEY,
                    client                 TEXT DEFAULT '',
                    address                TEXT DEFAULT '',
                    estimator              TEXT DEFAULT '',
                    stage                  TEXT DEFAULT 'Lead',
                    sell_price             REAL DEFAULT 0,
                    running_cost_pct       REAL DEFAULT 0.11,
                    tender_material_budget REAL DEFAULT 0,
                    tender_labour_budget   REAL DEFAULT 0,
                    tender_profit_pct      REAL DEFAULT 0,
                    archived               INTEGER DEFAULT 0
                );
                INSERT INTO jobs
                    SELECT
                        job_id,
                        COALESCE(client,''),
                        COALESCE(address,''),
                        COALESCE(estimator,''),
                        COALESCE(stage,'Lead'),
                        COALESCE(sell_price,0),
                        COALESCE(running_cost_pct,0.11),
                        COALESCE(tender_material_budget,0),
                        COALESCE(tender_labour_budget,0),
                        COALESCE(tender_profit_pct,0),
                        COALESCE(archived,0)
                    FROM _jobs_old;
                DROP TABLE _jobs_old;
            """)
            conn.commit()

    # ── Rebuild day_assignments if schema is stale ─────────────────────
    if "day_assignments" in existing_tables:
        da_cols = {r[1] for r in cur.execute("PRAGMA table_info(day_assignments)").fetchall()}
        if "client" not in da_cols or "employee" not in da_cols or "date" not in da_cols:
            cur.execute("DROP TABLE day_assignments")
            conn.commit()

    # ── Create all tables ─────────────────────────────────────────────────
    cur.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id                 TEXT PRIMARY KEY,
            client                 TEXT DEFAULT '',
            address                TEXT DEFAULT '',
            estimator              TEXT DEFAULT '',
            stage                  TEXT DEFAULT 'Lead',
            sell_price             REAL DEFAULT 0,
            running_cost_pct       REAL DEFAULT 0.11,
            tender_material_budget REAL DEFAULT 0,
            tender_labour_budget   REAL DEFAULT 0,
            tender_profit_pct      REAL DEFAULT 0,
            archived               INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS employees (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT UNIQUE NOT NULL,
            role        TEXT DEFAULT 'Roofer',
            hourly_rate REAL DEFAULT 0,
            phone       TEXT DEFAULT '',
            active      INTEGER DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS day_assignments (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id   TEXT DEFAULT '',
            client   TEXT DEFAULT '',
            employee TEXT DEFAULT '__unassigned__',
            date     TEXT DEFAULT '',
            note     TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS labour_logs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            work_date   TEXT DEFAULT '',
            job_id      TEXT DEFAULT '',
            employee    TEXT DEFAULT '',
            hours       REAL DEFAULT 0,
            hourly_rate REAL DEFAULT 0,
            note        TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS material_invoices (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_date   TEXT DEFAULT '',
            job_id         TEXT DEFAULT '',
            supplier       TEXT DEFAULT '',
            invoice_number TEXT DEFAULT '',
            amount         REAL DEFAULT 0,
            status         TEXT DEFAULT 'Entered',
            note           TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS estimate_lines (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id        TEXT DEFAULT '',
            section       TEXT DEFAULT '',
            item          TEXT DEFAULT '',
            uom           TEXT DEFAULT '',
            qty           REAL DEFAULT 0,
            material_rate REAL DEFAULT 0,
            labour_rate   REAL DEFAULT 0,
            material_cost REAL DEFAULT 0,
            labour_cost   REAL DEFAULT 0,
            total_cost    REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS pipeline (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id          TEXT DEFAULT '',
            client          TEXT DEFAULT '',
            value           REAL DEFAULT 0,
            probability_pct REAL DEFAULT 50,
            target_month    TEXT DEFAULT '',
            notes           TEXT DEFAULT '',
            archived        INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS monthly_targets (
            month       TEXT PRIMARY KEY,
            target      REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS job_files (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id      TEXT DEFAULT '',
            filename    TEXT DEFAULT '',
            filetype    TEXT DEFAULT '',
            filedata    BLOB,
            uploaded_at TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS recipes (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT DEFAULT '',
            unit_measure TEXT DEFAULT 'm2',
            notes        TEXT DEFAULT '',
            sort_order   INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS recipe_items (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_id     INTEGER DEFAULT 0,
            description   TEXT DEFAULT '',
            item_type     TEXT DEFAULT 'Material',
            unit_qty      REAL DEFAULT 1.0,
            uom           TEXT DEFAULT 'lm',
            material_rate REAL DEFAULT 0,
            labour_rate   REAL DEFAULT 0,
            supplier      TEXT DEFAULT '',
            sort_order    INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS variations (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id          TEXT DEFAULT '',
            var_number      TEXT DEFAULT '',
            description     TEXT DEFAULT '',
            value           REAL DEFAULT 0,
            status          TEXT DEFAULT 'Pending',
            date_raised     TEXT DEFAULT '',
            approved_by     TEXT DEFAULT '',
            notes           TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS payment_schedule (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id          TEXT DEFAULT '',
            milestone       TEXT DEFAULT '',
            pct             REAL DEFAULT 0,
            amount          REAL DEFAULT 0,
            due_date        TEXT DEFAULT '',
            status          TEXT DEFAULT 'Unpaid',
            paid_date       TEXT DEFAULT '',
            notes           TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS job_retention (
            job_id          TEXT PRIMARY KEY,
            retention_pct   REAL DEFAULT 0,
            retention_amt   REAL DEFAULT 0,
            release_date    TEXT DEFAULT '',
            released        INTEGER DEFAULT 0,
            notes           TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS clients (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            name            TEXT DEFAULT '',
            company         TEXT DEFAULT '',
            phone           TEXT DEFAULT '',
            email           TEXT DEFAULT '',
            address         TEXT DEFAULT '',
            client_type     TEXT DEFAULT 'Builder',
            notes           TEXT DEFAULT '',
            created_date    TEXT DEFAULT '',
            billing_name    TEXT DEFAULT '',
            billing_email   TEXT DEFAULT '',
            billing_phone   TEXT DEFAULT '',
            ca_name         TEXT DEFAULT '',
            ca_email        TEXT DEFAULT '',
            ca_phone        TEXT DEFAULT '',
            pm_name         TEXT DEFAULT '',
            pm_email        TEXT DEFAULT '',
            pm_phone        TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS client_interactions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id       INTEGER DEFAULT 0,
            interaction_date TEXT DEFAULT '',
            type            TEXT DEFAULT 'Call',
            notes           TEXT DEFAULT '',
            follow_up_date  TEXT DEFAULT '',
            job_id          TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS client_invoices (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_number  TEXT DEFAULT '',
            job_id          TEXT DEFAULT '',
            milestone_id    INTEGER DEFAULT 0,
            issue_date      TEXT DEFAULT '',
            due_date        TEXT DEFAULT '',
            amount          REAL DEFAULT 0,
            gst             REAL DEFAULT 0,
            total_inc_gst   REAL DEFAULT 0,
            status          TEXT DEFAULT 'Issued',
            paid_date       TEXT DEFAULT '',
            notes           TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS invoice_counter (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            last_number     INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS job_counter (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            prefix      TEXT DEFAULT 'LES',
            last_number INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS material_finishes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            finish_name     TEXT DEFAULT '',
            sort_order      INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS catalogue_finishes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            catalogue_item  TEXT DEFAULT '',
            catalogue_section TEXT DEFAULT '',
            finish_id       INTEGER DEFAULT 0,
            material_rate   REAL DEFAULT 0,
            labour_rate     REAL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS stackct_mapping (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            stackct_name    TEXT DEFAULT '',
            catalogue_item  TEXT DEFAULT '',
            catalogue_section TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS payroll_rules (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id         INTEGER DEFAULT 0,
            award_name          TEXT DEFAULT 'Building & Construction General On-site Award',
            standard_start      TEXT DEFAULT '07:00',
            standard_end        TEXT DEFAULT '15:30',
            break_mins          INTEGER DEFAULT 30,
            ordinary_hours      REAL DEFAULT 8.0,
            overtime_rate       REAL DEFAULT 1.5,
            saturday_rate       REAL DEFAULT 2.0,
            sunday_rate         REAL DEFAULT 2.0,
            public_holiday_rate REAL DEFAULT 2.0,
            travel_allowance    REAL DEFAULT 0,
            tool_allowance      REAL DEFAULT 0,
            meal_allowance      REAL DEFAULT 0,
            workcover_pct       REAL DEFAULT 2.0,
            leave_loading_pct   REAL DEFAULT 17.5
        );

        CREATE TABLE IF NOT EXISTS timesheet_entries (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            employee_id     INTEGER DEFAULT 0,
            job_id          TEXT DEFAULT '',
            work_date       TEXT DEFAULT '',
            start_time      TEXT DEFAULT '',
            end_time        TEXT DEFAULT '',
            break_mins      INTEGER DEFAULT 30,
            ordinary_hours  REAL DEFAULT 0,
            overtime_hours  REAL DEFAULT 0,
            saturday_hours  REAL DEFAULT 0,
            sunday_hours    REAL DEFAULT 0,
            ph_hours        REAL DEFAULT 0,
            travel_allow    REAL DEFAULT 0,
            tool_allow      REAL DEFAULT 0,
            meal_allow      REAL DEFAULT 0,
            gross_pay       REAL DEFAULT 0,
            notes           TEXT DEFAULT '',
            approved        INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS public_holidays (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            holiday_date    TEXT DEFAULT '',
            name            TEXT DEFAULT '',
            state           TEXT DEFAULT 'NSW'
        );

        CREATE TABLE IF NOT EXISTS users (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            username        TEXT UNIQUE NOT NULL,
            password_hash   TEXT NOT NULL,
            full_name       TEXT DEFAULT '',
            role            TEXT DEFAULT 'Ops',
            active          INTEGER DEFAULT 1,
            created_date    TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS company_settings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            company_name    TEXT DEFAULT 'Limitless Estimation Services',
            abn             TEXT DEFAULT '',
            address         TEXT DEFAULT '',
            phone           TEXT DEFAULT '',
            email           TEXT DEFAULT '',
            bank_name       TEXT DEFAULT '',
            bsb             TEXT DEFAULT '',
            account_number  TEXT DEFAULT '',
            account_name    TEXT DEFAULT '',
            payment_terms   INTEGER DEFAULT 14,
            logo_text       TEXT DEFAULT 'LIMITLESS',
            overhead_pct    REAL DEFAULT 11.0,
            markup_default  REAL DEFAULT 30.0
        );

        CREATE TABLE IF NOT EXISTS site_diary (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id          TEXT DEFAULT '',
            diary_date      TEXT DEFAULT '',
            weather         TEXT DEFAULT '',
            temp            TEXT DEFAULT '',
            workers_on_site TEXT DEFAULT '',
            hours_worked    REAL DEFAULT 0,
            progress_notes  TEXT DEFAULT '',
            delays          TEXT DEFAULT '',
            visitors        TEXT DEFAULT '',
            created_by      TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS job_photos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id          TEXT DEFAULT '',
            photo_date      TEXT DEFAULT '',
            caption         TEXT DEFAULT '',
            category        TEXT DEFAULT 'Progress',
            filedata        BLOB,
            filename        TEXT DEFAULT '',
            uploaded_at     TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS safety_docs (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id          TEXT DEFAULT '',
            doc_type        TEXT DEFAULT 'SWMS',
            title           TEXT DEFAULT '',
            filename        TEXT DEFAULT '',
            filetype        TEXT DEFAULT '',
            filedata        BLOB,
            reviewed        INTEGER DEFAULT 0,
            reviewed_by     TEXT DEFAULT '',
            review_date     TEXT DEFAULT '',
            uploaded_at     TEXT DEFAULT ''
        );
    """)
    conn.commit()

    # ── Safe column additions (idempotent) ────────────────────────────────
    for table, col, defn in [
        ("jobs",              "archived",        "INTEGER DEFAULT 0"),
        ("jobs",              "job_type",        "TEXT DEFAULT 'Residential'"),
        ("jobs",              "job_finish",      "TEXT DEFAULT 'Steel'"),
        ("jobs",              "parent_job",      "TEXT DEFAULT ''"),
        ("jobs",              "is_variation",    "INTEGER DEFAULT 0"),
        ("jobs",              "variation_title", "TEXT DEFAULT ''"),
        ("day_assignments",   "client",          "TEXT DEFAULT ''"),
        ("day_assignments",   "note",            "TEXT DEFAULT ''"),
        ("company_settings",  "overhead_pct",      "REAL DEFAULT 11.0"),
        ("company_settings",  "markup_default",   "REAL DEFAULT 30.0"),
        ("company_settings",  "logo_data",        "BLOB DEFAULT NULL"),
        ("company_settings",  "logo_filename",    "TEXT DEFAULT ''"),
        ("company_settings",  "terms_conditions", "TEXT DEFAULT ''"),
        ("company_settings",  "website",          "TEXT DEFAULT ''"),
        ("client_invoices",   "amount_ex_gst",    "REAL DEFAULT 0"),
        ("client_invoices",   "gst",               "REAL DEFAULT 0"),
        ("client_invoices",   "total_inc_gst",    "REAL DEFAULT 0"),
        ("client_invoices",   "milestone",        "TEXT DEFAULT ''"),
        ("pipeline",          "follow_up_date",  "TEXT DEFAULT ''"),
        ("pipeline", "status_notes",    "TEXT DEFAULT ''"),
        ("pipeline", "secured",         "INTEGER DEFAULT 0"),
        ("pipeline", "contact_name",    "TEXT DEFAULT ''"),
        ("pipeline", "contact_phone",   "TEXT DEFAULT ''"),
        ("pipeline", "contact_email",   "TEXT DEFAULT ''"),
    ]:
        try:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")
            conn.commit()
        except Exception:
            pass

    # ── Rebuild labour_logs if it has old crew_name column ────────────────
    ll_cols = {r[1] for r in cur.execute("PRAGMA table_info(labour_logs)").fetchall()}
    if ll_cols and "employee" not in ll_cols:
        cur.executescript("""
            ALTER TABLE labour_logs RENAME TO _labour_logs_old;
            CREATE TABLE labour_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                work_date   TEXT DEFAULT '',
                job_id      TEXT DEFAULT '',
                employee    TEXT DEFAULT '',
                hours       REAL DEFAULT 0,
                hourly_rate REAL DEFAULT 0,
                note        TEXT DEFAULT ''
            );
            INSERT INTO labour_logs
                SELECT id, work_date, job_id,
                       COALESCE(crew_name, ''),
                       COALESCE(hours, 0),
                       COALESCE(hourly_rate, 0),
                       COALESCE(note, '')
                FROM _labour_logs_old;
            DROP TABLE _labour_logs_old;
        """)
        conn.commit()

    # ── Seed material finishes ────────────────────────────────────────────
    if cur.execute("SELECT COUNT(*) FROM material_finishes").fetchone()[0] == 0:
        for i,f in enumerate(["Steel","MATT","ULTRA","Aluminium","VM Zinc","Copper","Zincalume"]):
            cur.execute("INSERT INTO material_finishes (finish_name,sort_order) VALUES (?,?)",(f,i))
        conn.commit()

    # ── Add created_date to jobs if missing ──────────────────────────────
    try:
        cur.execute("ALTER TABLE jobs ADD COLUMN created_date TEXT DEFAULT ''")
    except: pass

    # ── Assemblies table (component breakdown per catalogue item) ────────
    cur.execute("""CREATE TABLE IF NOT EXISTS assemblies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        catalogue_id INTEGER DEFAULT 0,
        catalogue_description TEXT DEFAULT '',
        component_description TEXT DEFAULT '',
        qty REAL DEFAULT 1,
        uom TEXT DEFAULT '',
        unit_cost REAL DEFAULT 0,
        component_type TEXT DEFAULT 'Material',
        sort_order INTEGER DEFAULT 0
    )""")
    conn.commit()

    # ── Catalogue items table (imported from Buildxact/CSV) ──────────────
    cur.execute("""CREATE TABLE IF NOT EXISTS catalogue_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT DEFAULT '',
        description TEXT NOT NULL,
        uom TEXT DEFAULT 'lm',
        unit_cost REAL DEFAULT 0,
        labour_cost REAL DEFAULT 0,
        source TEXT DEFAULT 'manual',
        created_at TEXT DEFAULT ''
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS catalogue_components (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        item_id INTEGER NOT NULL,
        description TEXT NOT NULL,
        item_type TEXT DEFAULT 'Material',
        qty REAL DEFAULT 1.0,
        uom TEXT DEFAULT 'Ea',
        unit_cost REAL DEFAULT 0,
        sort_order INTEGER DEFAULT 0
    )""")
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_cat_items_desc ON catalogue_items(description, category)")
    except: pass
    conn.commit()

    # ── Catalogue overrides table (edits to base catalogue) ──────────────
    cur.execute("""CREATE TABLE IF NOT EXISTS catalogue_overrides (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        description TEXT NOT NULL,
        category TEXT DEFAULT '',
        uom TEXT DEFAULT '',
        material_cost REAL DEFAULT 0,
        labour_cost REAL DEFAULT 0,
        sell_unit_rate REAL DEFAULT 0,
        updated_at TEXT DEFAULT ''
    )""")
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_cat_ovr_desc ON catalogue_overrides(description)")
    except: pass
    conn.commit()

    # ── Custom catalogue table ─────────────────────────────────────────────
    cur.execute("""CREATE TABLE IF NOT EXISTS custom_catalogue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier_code TEXT DEFAULT '',
        supplier_name TEXT DEFAULT '',
        category TEXT DEFAULT '', description TEXT NOT NULL,
        uom TEXT DEFAULT 'lm', material_cost REAL DEFAULT 0,
        labour_cost REAL DEFAULT 0, sell_unit_rate REAL DEFAULT 0,
        created_by TEXT DEFAULT '', created_at TEXT DEFAULT '')""")
    try:
        cur.execute("ALTER TABLE custom_catalogue ADD COLUMN supplier_code TEXT DEFAULT ''")
    except: pass
    try:
        cur.execute("ALTER TABLE custom_catalogue ADD COLUMN supplier_name TEXT DEFAULT ''")
    except: pass

    # ── Add handover columns if missing ──────────────────────────────────
    for _col, _def in [
        ("handover_crew", "TEXT DEFAULT ''"),
        ("handover_notes", "TEXT DEFAULT ''"),
        ("handover_risks", "TEXT DEFAULT ''"),
        ("handover_access", "TEXT DEFAULT ''"),
    ]:
        try:
            cur.execute(f"ALTER TABLE jobs ADD COLUMN {_col} {_def}")
        except: pass

    # ── Expenses table ──────────────────────────────────────────────────────
    cur.execute("""CREATE TABLE IF NOT EXISTS expenses (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        expense_date  TEXT DEFAULT '',
        category      TEXT DEFAULT '',
        description   TEXT DEFAULT '',
        amount        REAL DEFAULT 0,
        gst           REAL DEFAULT 0,
        job_id        TEXT DEFAULT '',
        submitted_by  TEXT DEFAULT '',
        status        TEXT DEFAULT 'Pending',
        receipt_filename TEXT DEFAULT '',
        notes         TEXT DEFAULT '',
        created_at    TEXT DEFAULT ''
    )""")
    conn.commit()

    # ── Add client contact columns if missing ─────────────────────────────
    for _ccol in ["billing_name","billing_email","billing_phone",
                  "ca_name","ca_email","ca_phone",
                  "pm_name","pm_email","pm_phone",
                  "abn"]:
        try:
            cur.execute(f"ALTER TABLE clients ADD COLUMN {_ccol} TEXT DEFAULT ''")
        except: pass

    # ── Add day_assignments time columns if missing ───────────────────────
    for _tcol in ["start_time", "end_time"]:
        try:
            cur.execute(f"ALTER TABLE day_assignments ADD COLUMN {_tcol} TEXT DEFAULT ''")
        except: pass

    # ── No demo data — clean slate for real use ──────────────────────────

    conn.commit()
    conn.close()


def fetch_df(query, params=()):
    conn = get_conn()
    q = adapt_query(query)
    try:
        if USE_POSTGRES:
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(q, list(params) if params else None)
            rows = cur.fetchall()
            return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()
        else:
            return pd.read_sql_query(q, conn, params=list(params) if params else [])
    finally:
        release_conn(conn)


def execute(query, params=()):
    conn = get_conn()
    q = adapt_query(query)
    try:
        cur = conn.cursor()
        cur.execute(q, list(params) if params else None)
        conn.commit()
    finally:
        release_conn(conn)


# ── Supabase sync helpers ──────────────────────────────────────────────────
def supa_push(table, data):
    """Push a record to Supabase."""
    if not USE_SUPABASE or not _supa_client:
        return
    try:
        _supa_client.table(table).upsert(data).execute()
    except Exception as _e:
        pass  # Fail silently — local DB is source of truth


def supa_pull(table):
    """Pull all records from Supabase table as list of dicts."""
    if not USE_SUPABASE or not _supa_client:
        return []
    try:
        result = _supa_client.table(table).select("*").execute()
        return result.data or []
    except:
        return []


def sync_to_mobile():
    """Push employees, jobs, day_assignments to Supabase for mobile to read."""
    if not USE_SUPABASE:
        return
    try:
        # Sync employees
        emps = fetch_df("SELECT id, name, role, hourly_rate, active, pin FROM employees WHERE active=1")
        for _, r in emps.iterrows():
            supa_push("employees", {
                "id": int(r["id"]), "name": str(r["name"]),
                "role": str(r.get("role","")), "hourly_rate": float(r.get("hourly_rate",0)),
                "active": int(r.get("active",1)), "pin": str(r.get("pin",""))
            })
        # Sync jobs
        jobs = fetch_df("SELECT job_id, client, address, stage FROM jobs WHERE archived=0 AND COALESCE(is_variation,0)=0")
        for _, r in jobs.iterrows():
            supa_push("jobs", {
                "job_id": str(r["job_id"]), "client": str(r.get("client","")),
                "address": str(r.get("address","")), "stage": str(r.get("stage",""))
            })
        # Sync day assignments — last 7 days and next 30 days
        assigns = fetch_df("SELECT id, job_id, client, employee, date, note FROM day_assignments WHERE date >= date('now', '-7 days')")
        for _, r in assigns.iterrows():
            supa_push("day_assignments", {
                "id": int(r["id"]), "job_id": str(r.get("job_id","")),
                "client": str(r.get("client","")), "employee": str(r.get("employee","")),
                "date": str(r.get("date","")), "note": str(r.get("note",""))
            })
    except Exception as _se:
        pass


def sync_from_mobile():
    """Pull clock events and variations from Supabase into local DB."""
    if not USE_SUPABASE:
        return
    try:
        # Pull clock events
        events = supa_pull("clock_events")
        for e in events:
            existing = fetch_df("SELECT id FROM clock_events WHERE id=?", (e["id"],))
            if existing.empty:
                execute("""INSERT INTO clock_events
                    (id, employee, job_id, event_type, event_time, event_date, note)
                    VALUES (?,?,?,?,?,?,?)""",
                    (e["id"], e.get("employee",""), e.get("job_id",""),
                     e.get("event_type",""), e.get("event_time",""),
                     e.get("event_date",""), e.get("note","")))
        # Pull mobile variations
        vars_data = supa_pull("mobile_variations")
        for v in vars_data:
            existing = fetch_df("SELECT id FROM mobile_variations WHERE id=?", (v["id"],))
            if existing.empty:
                execute("""INSERT INTO mobile_variations
                    (id, employee, job_id, description, submitted_at, status)
                    VALUES (?,?,?,?,?,?)""",
                    (v["id"], v.get("employee",""), v.get("job_id",""),
                     v.get("description",""), v.get("submitted_at",""),
                     v.get("status","Pending")))
    except Exception as _se:
        pass


def generate_quote_pdf(job, estimate_df, quote_opts=None):
    """Generate a professional quote PDF using reportlab.
    quote_opts: dict with keys show_cat_totals, show_ref_nums, show_qty_uom, show_line_amounts, show_terms
    """
    if quote_opts is None:
        quote_opts = {}
    opt_cat_totals   = quote_opts.get("show_cat_totals", True)
    opt_ref_nums     = quote_opts.get("show_ref_nums", True)
    opt_qty_uom      = quote_opts.get("show_qty_uom", True)
    opt_line_amounts = quote_opts.get("show_line_amounts", False)
    opt_terms        = quote_opts.get("show_terms", True)
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
    )

    settings = get_company_settings()

    buf    = io.BytesIO()
    W, H   = A4
    margin = 20 * mm

    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=margin, rightMargin=margin,
        topMargin=margin, bottomMargin=margin,
    )

    # Colours
    DARK    = colors.HexColor("#0f172a")
    TEAL    = colors.HexColor("#2dd4bf")
    SLATE   = colors.HexColor("#64748b")
    LIGHT   = colors.HexColor("#e2e8f0")
    MID     = colors.HexColor("#1e2d3d")
    WHITE   = colors.white

    styles  = getSampleStyleSheet()
    story   = []

    # Header — company + job info
    _logo_img = get_logo_image()
    _co_name  = str(settings.get("logo_text","LIMITLESS")) if settings else "LIMITLESS"
    _co_sub   = str(settings.get("company_name","Estimation Services")) if settings else "Estimation Services"
    if _logo_img:
        left_cell = _logo_img
    else:
        left_cell = Paragraph("<font size=22><b>" + _co_name + "</b></font><br/><font size=9 color='#64748b'>" + _co_sub + "</font>", styles["Normal"])
    header_data = [[
        left_cell,
        Paragraph(
            f"<font size=16><b>QUOTE</b></font><br/>"
            f"<font size=9 color='#64748b'>{job.get('job_id','')}</font>", styles["Normal"]),
    ]]
    header_tbl = Table(header_data, colWidths=[W - 2*margin - 65*mm, 65*mm],
                       rowHeights=[26*mm])
    header_tbl.setStyle(TableStyle([
        ("BACKGROUND",   (0,0),(-1,-1), DARK),
        ("TEXTCOLOR",    (0,0),(-1,-1), WHITE),
        ("ALIGN",        (1,0),(1,0),   "RIGHT"),
        ("VALIGN",       (0,0),(-1,-1), "MIDDLE"),
        ("TOPPADDING",   (0,0),(-1,-1), 8),
        ("BOTTOMPADDING",(0,0),(-1,-1), 8),
        ("LEFTPADDING",  (0,0),(0,0),   14),
        ("RIGHTPADDING", (1,0),(1,0),   14),
        ("ROUNDEDCORNERS", [6,6,6,6]),
    ]))
    story.append(header_tbl)
    story.append(Spacer(1, 6*mm))

    # Job details
    detail_data = [
        ["Client",    job.get("client","—"),    "Date",      date.today().strftime("%-d %B %Y")],
        ["Address",   job.get("address","—"),   "Estimator", job.get("estimator","—")],
        ["Job ID",    job.get("job_id","—"),    "Type",      job.get("job_type","—")],
    ]
    detail_tbl = Table(detail_data, colWidths=[25*mm, 75*mm, 25*mm, 45*mm])
    detail_tbl.setStyle(TableStyle([
        ("FONTSIZE",     (0,0),(-1,-1), 9),
        ("TEXTCOLOR",    (0,0),(0,-1),  SLATE),
        ("TEXTCOLOR",    (2,0),(2,-1),  SLATE),
        ("TEXTCOLOR",    (1,0),(1,-1),  DARK),
        ("TEXTCOLOR",    (3,0),(3,-1),  DARK),
        ("FONTNAME",     (0,0),(0,-1),  "Helvetica"),
        ("FONTNAME",     (2,0),(2,-1),  "Helvetica"),
        ("FONTNAME",     (1,0),(1,-1),  "Helvetica-Bold"),
        ("FONTNAME",     (3,0),(3,-1),  "Helvetica-Bold"),
        ("TOPPADDING",   (0,0),(-1,-1), 4),
        ("BOTTOMPADDING",(0,0),(-1,-1), 4),
    ]))
    story.append(detail_tbl)
    story.append(HRFlowable(width="100%", thickness=1, color=TEAL, spaceAfter=6*mm))

    # ── Line items — format controlled by quote_opts ──────────────────────
    sell_price = float(job.get("sell_price") or 0)
    cost_total_raw = sum(
        float(r["Material Cost"]) + float(r["Labour Cost"])
        for _, r in estimate_df.iterrows()
    )
    ratio = (sell_price / cost_total_raw) if cost_total_raw else 1.0
    if not sell_price:
        sell_price = cost_total_raw

    # Group by section
    sections_data = {}
    for _, row in estimate_df.iterrows():
        sec = str(row["Section"])
        if sec not in sections_data:
            sections_data[sec] = []
        line_cost  = float(row["Material Cost"]) + float(row["Labour Cost"])
        line_price = line_cost * ratio
        qty = row["Qty"]
        sections_data[sec].append({
            "item":  str(row["Item"]),
            "uom":   str(row["UOM"]),
            "qty":   qty,
            "price": line_price,
        })

    sec_totals = {sec: sum(r["price"] for r in rows) for sec, rows in sections_data.items()}

    # Build columns based on options
    col_defs = []
    if opt_ref_nums:
        col_defs.append(("Ref", 14*mm))
    col_defs.append(("Description", None))
    if opt_qty_uom:
        col_defs.append(("Qty", 18*mm))
        col_defs.append(("UOM", 18*mm))
    if opt_line_amounts:
        col_defs.append(("Amount", 32*mm))

    fixed_w  = sum(w for _, w in col_defs if w is not None)
    desc_w   = (170*mm) - fixed_w
    col_widths = [desc_w if w is None else w for _, w in col_defs]
    hdr_labels = [n for n, _ in col_defs]
    desc_idx   = next(i for i,(n,_) in enumerate(col_defs) if n=="Description")

    tbl_header      = [hdr_labels]
    tbl_data        = []
    sec_header_rows = []

    sec_num = 0
    for sec, rows in sections_data.items():
        sec_num += 1
        sec_header_rows.append(len(tbl_data))
        # Section header — always put text in col 0 since SPAN starts there
        sec_label = sec
        if opt_cat_totals:
            sec_label = sec + "     $" + f"{sec_totals[sec]:,.2f}"
        sec_row = [""] * len(col_defs)
        sec_row[0] = Paragraph("<b>" + sec_label + "</b>",
            ParagraphStyle("sec", parent=styles["Normal"],
                textColor=colors.HexColor("#2dd4bf"),
                fontSize=10, fontName="Helvetica-Bold"))
        tbl_data.append(sec_row)

        for idx, item in enumerate(rows, 1):
            data_row = []
            qty = item["qty"]
            qty_str = str(int(qty)) if qty == int(qty) else f"{qty:.1f}"
            for col_name, _ in col_defs:
                if col_name == "Ref":
                    data_row.append(f"{sec_num}.{idx}")
                elif col_name == "Description":
                    data_row.append(Paragraph(item["item"], styles["Normal"]))
                elif col_name == "Qty":
                    data_row.append(qty_str)
                elif col_name == "UOM":
                    data_row.append(item["uom"])
                elif col_name == "Amount":
                    data_row.append(f"${item['price']:,.2f}")
            tbl_data.append(data_row)

    all_rows = tbl_header + tbl_data
    line_tbl = Table(all_rows, colWidths=col_widths, repeatRows=1)

    style_cmds = [
        ("BACKGROUND",    (0,0),(-1,0),   MID),
        ("TEXTCOLOR",     (0,0),(-1,0),   TEAL),
        ("FONTNAME",      (0,0),(-1,0),   "Helvetica-Bold"),
        ("FONTSIZE",      (0,0),(-1,0),   10),
        ("FONTSIZE",      (0,1),(-1,-1),  9),
        ("TOPPADDING",    (0,0),(-1,-1),  6),
        ("BOTTOMPADDING", (0,0),(-1,-1),  6),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),  [colors.HexColor("#f8fafc"), WHITE]),
        ("GRID",          (0,0),(-1,-1),  0.25, colors.HexColor("#e2e8f0")),
        ("LINEBELOW",     (0,0),(-1,0),   1.5, TEAL),
    ]
    for i in sec_header_rows:
        ri = i + 1
        style_cmds += [
            ("BACKGROUND",    (0,ri),(-1,ri), DARK),
            ("TEXTCOLOR",     (0,ri),(-1,ri), TEAL),
            ("SPAN",          (0,ri),(-1,ri)),
            ("FONTNAME",      (0,ri),(0,ri),  "Helvetica-Bold"),
            ("FONTSIZE",      (0,ri),(0,ri),  10),
            ("TOPPADDING",    (0,ri),(-1,ri), 9),
            ("BOTTOMPADDING", (0,ri),(-1,ri), 9),
        ]
    for ci, (col_name, _) in enumerate(col_defs):
        if col_name in ("Qty","Amount"):
            style_cmds.append(("ALIGN", (ci,0),(ci,-1), "RIGHT"))
        elif col_name == "Ref":
            style_cmds.append(("ALIGN", (ci,0),(ci,-1), "CENTER"))
            style_cmds.append(("TEXTCOLOR", (ci,1),(ci,-1), SLATE))

    line_tbl.setStyle(TableStyle(style_cmds))
    story.append(line_tbl)
    story.append(Spacer(1, 6*mm))

    # Totals — only show the final quote total, nothing else
    totals_data = [
        ["", "TOTAL (excl. GST)", f"${sell_price:,.2f}"],
        ["", "GST (10%)",         f"${sell_price * 0.1:,.2f}"],
        ["", "TOTAL (incl. GST)", f"${sell_price * 1.1:,.2f}"],
    ]
    tot_tbl = Table(totals_data, colWidths=[80*mm, 65*mm, 25*mm])
    tot_tbl.setStyle(TableStyle([
        ("ALIGN",         (1,0),(-1,-1),  "RIGHT"),
        ("FONTSIZE",      (0,0),(-1,-1),  10),
        ("FONTNAME",      (1,0),(1,-1),   "Helvetica"),
        ("FONTNAME",      (2,0),(2,-1),   "Helvetica-Bold"),
        ("TEXTCOLOR",     (1,0),(1,-2),   SLATE),
        ("TEXTCOLOR",     (2,0),(2,-2),   DARK),
        ("TOPPADDING",    (0,0),(-1,-1),  7),
        ("BOTTOMPADDING", (0,0),(-1,-1),  7),
        ("LINEABOVE",     (0,-1),(-1,-1), 1.5, TEAL),
        ("BACKGROUND",    (0,-1),(-1,-1), DARK),
        ("TEXTCOLOR",     (1,-1),(-1,-1), TEAL),
        ("FONTNAME",      (1,-1),(-1,-1), "Helvetica-Bold"),
        ("FONTSIZE",      (1,-1),(-1,-1), 13),
        ("TOPPADDING",    (0,-1),(-1,-1), 10),
        ("BOTTOMPADDING", (0,-1),(-1,-1), 10),
    ]))
    story.append(tot_tbl)
    story.append(Spacer(1, 10*mm))

    # Footer
    story.append(HRFlowable(width="100%", thickness=0.5, color=SLATE))
    story.append(Spacer(1, 3*mm))
    story.append(Paragraph(
        "<font size=8 color='#64748b'>This quote is valid for 30 days from the date of issue. "
        "Prices exclude GST unless otherwise stated. " +
        str(settings.get("company_name","Limitless Estimation Services")) + "</font>",
        styles["Normal"]
    ))

    # Terms & Conditions page
    if opt_terms:
        from reportlab.platypus import PageBreak
        story.append(PageBreak())
        story.append(Paragraph("<b>Terms and Conditions</b>", styles["Normal"]))
        story.append(Spacer(1,4*mm))
        tc_text = str(settings.get("terms_conditions","") or "")
        if not tc_text:
            co = str(settings.get("company_name","Limitless Estimation Services"))
            co = str(settings.get("company_name","Limitless Estimation Services"))
            tc_parts = [
                "1. QUOTATION VALIDITY",
                "This quote is valid for 30 days. After this period prices may be subject to change.",
                "2. PAYMENT TERMS",
                "Payment terms are as specified on the invoice. Late payments may incur interest charges.",
                "3. VARIATIONS",
                "Any variations must be agreed in writing prior to commencement. Verbal instructions will not be accepted.",
                "4. MATERIALS",
                "All materials are subject to availability. Substitutions of equivalent specification may be made.",
                "5. SITE ACCESS",
                "The client is responsible for ensuring safe and unobstructed access to the work site.",
                "6. INSURANCE",
                co + " holds current public liability insurance. Certificates of currency available on request.",
                "7. WARRANTY",
                "All workmanship is warranted for 12 months from practical completion subject to normal use.",
                "8. DISPUTE RESOLUTION",
                "Any disputes shall be resolved in accordance with the laws of New South Wales, Australia.",
                "9. ACCEPTANCE",
                "Acceptance of this quote constitutes agreement to these terms and conditions.",
            ]
            tc_text = "\n\n".join(tc_parts)
        for para in tc_text.split("\n\n"):

            if para.strip():
                story.append(Paragraph(para.strip(), styles["Normal"]))
                story.append(Spacer(1,3*mm))

    doc.build(story)
    buf.seek(0)
    return buf


def generate_supplier_po_pdf(job, estimate_df):
    """Supplier purchase order — items and quantities only, no pricing."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable

    buf    = io.BytesIO()
    W, H   = A4
    margin = 20 * mm
    doc    = SimpleDocTemplate(buf, pagesize=A4,
                leftMargin=margin, rightMargin=margin,
                topMargin=margin, bottomMargin=margin)

    DARK  = colors.HexColor("#0f172a")
    TEAL  = colors.HexColor("#2dd4bf")
    SLATE = colors.HexColor("#64748b")
    MID   = colors.HexColor("#1e2d3d")
    WHITE = colors.white
    LIGHT = colors.HexColor("#f8fafc")
    styles = getSampleStyleSheet()
    story  = []

    # Header
    _po_logo = get_logo_image()
    _po_co   = get_company_settings()
    _po_left = _po_logo if _po_logo else Paragraph("<font size=22><b>" + str(_po_co.get("logo_text","LIMITLESS")) + "</b></font><br/><font size=9 color='#64748b'>" + str(_po_co.get("company_name","Estimation Services")) + "</font>", styles["Normal"])
    hdr = Table([[
        _po_left,
        Paragraph("<font size=16><b>PURCHASE ORDER</b></font><br/><font size=9 color='#64748b'>" + str(job.get('job_id','')) + "</font>", styles["Normal"]),
    ]], colWidths=[W-2*margin-70*mm, 70*mm])
    hdr.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,-1),DARK),("TEXTCOLOR",(0,0),(-1,-1),WHITE),
        ("ALIGN",(1,0),(1,0),"RIGHT"),("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("TOPPADDING",(0,0),(-1,-1),14),("BOTTOMPADDING",(0,0),(-1,-1),14),
        ("LEFTPADDING",(0,0),(0,0),14),("RIGHTPADDING",(1,0),(1,0),14),
    ]))
    story += [hdr, Spacer(1,5*mm)]

    # Job info
    det = Table([
        ["Job", job.get("job_id","—"), "Date", date.today().strftime("%-d %B %Y")],
        ["Client", job.get("client","—"), "Address", job.get("address","—")],
    ], colWidths=[20*mm,80*mm,20*mm,50*mm])
    det.setStyle(TableStyle([
        ("FONTSIZE",(0,0),(-1,-1),9),
        ("TEXTCOLOR",(0,0),(0,-1),SLATE),("TEXTCOLOR",(2,0),(2,-1),SLATE),
        ("FONTNAME",(1,0),(1,-1),"Helvetica-Bold"),("FONTNAME",(3,0),(3,-1),"Helvetica-Bold"),
        ("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),
    ]))
    story += [det, HRFlowable(width="100%",thickness=1,color=TEAL,spaceAfter=4*mm)]
    story.append(Paragraph(
        "<font size=9 color='#64748b'>Please supply the following items for the above job. Confirm availability and delivery date by return.</font>",
        styles["Normal"]))
    story.append(Spacer(1,5*mm))

    # Load component breakdown from catalogue Excel
    comp_map = {}  # ParentDescription -> [{desc, uom, unit_qty}]
    try:
        import pandas as _pdpo
        # Raw_Original has the real unit quantities per parent UOM
        raw_df     = _pdpo.read_excel(CATALOGUE_PATH, sheet_name="Raw_Original")
        raw_lookup = {str(r["DisplayedOrder"]).strip(): float(r["Units"] or 1)
                      for _, r in raw_df.iterrows()}
        # Component_Breakdown links parent descriptions to components
        comp_df = _pdpo.read_excel(CATALOGUE_PATH, sheet_name="Component_Breakdown", header=3)
        for _, cr in comp_df.iterrows():
            parent = str(cr.get("ParentDescription","") or "").strip()
            ctype  = str(cr.get("ComponentType","") or "").strip()
            if ctype == "Labour":
                continue
            cdesc  = str(cr.get("ComponentDescription","") or "").strip()
            cuom   = str(cr.get("UOM","") or "").strip()
            cord   = str(cr.get("ComponentDisplayedOrder","") or "").strip()
            # Get real unit qty from Raw_Original
            unit_qty = raw_lookup.get(cord, 1.0)
            if parent and cdesc:
                if parent not in comp_map:
                    comp_map[parent] = []
                comp_map[parent].append({
                    "desc":     cdesc,
                    "uom":      cuom,
                    "unit_qty": unit_qty,
                })
    except Exception as _e:
        comp_map = {}

    # Build expanded PO lines grouped by section
    tbl_header = [["Ref","Description","UOM","Qty"]]
    tbl_data   = []
    sec_header_rows = []
    mat_count  = 0
    sec_num    = 0
    current_section = None

    # Aggregate components across all estimate lines
    # {section -> {component_desc -> {uom, total_qty, unit_cost}}}
    sections_comps = {}

    for _, row in estimate_df.iterrows():
        sec      = str(row.get("Section",""))
        item     = str(row["Item"]).strip()
        job_qty  = float(row["Qty"])

        if sec not in sections_comps:
            sections_comps[sec] = {}

        # Get components for this item
        components = comp_map.get(item, [])

        if components:
            for comp in components:
                key = comp["desc"] + "||" + comp["uom"]
                if key not in sections_comps[sec]:
                    sections_comps[sec][key] = {
                        "desc":      comp["desc"],
                        "uom":       comp["uom"],
                        "total_qty": 0.0,
                    }
                # Multiply component unit_qty per parent UOM by job qty
                unit_qty = float(comp.get("unit_qty", 1.0) or 1.0)
                sections_comps[sec][key]["total_qty"] += job_qty * unit_qty
        else:
            # No components found — show parent item directly
            key = item + "||" + str(row["UOM"])
            if key not in sections_comps[sec]:
                sections_comps[sec][key] = {
                    "desc":      item,
                    "uom":       str(row["UOM"]),
                    "total_qty": 0.0,
                    "unit_cost": 0.0,
                }
            sections_comps[sec][key]["total_qty"] += job_qty

    # Build table rows
    for sec, comps in sections_comps.items():
        sec_num += 1
        sec_header_rows.append(len(tbl_data))
        sec_row = [Paragraph("<b>" + sec + "</b>",
            ParagraphStyle("poh", parent=styles["Normal"],
                textColor=colors.HexColor("#2dd4bf"),
                fontSize=9, fontName="Helvetica-Bold")),
            "", "", ""]
        tbl_data.append(sec_row)

        for idx, (key, comp) in enumerate(comps.items(), 1):
            qty     = comp["total_qty"]
            qty_str = str(int(qty)) if qty == int(qty) else f"{qty:.2f}"
            tbl_data.append([
                f"{sec_num}.{idx}",
                Paragraph(comp["desc"], styles["Normal"]),
                comp["uom"],
                qty_str,
            ])
            mat_count += 1

    all_rows = tbl_header + tbl_data
    ltbl = Table(all_rows, colWidths=[14*mm, 106*mm, 20*mm, 30*mm], repeatRows=1)
    sc = [
        ("BACKGROUND",    (0,0),(-1,0),  MID),
        ("TEXTCOLOR",     (0,0),(-1,0),  TEAL),
        ("FONTNAME",      (0,0),(-1,0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0,0),(-1,0),  10),
        ("FONTSIZE",      (0,1),(-1,-1), 9),
        ("ALIGN",         (0,0),(0,-1),  "CENTER"),
        ("ALIGN",         (2,0),(-1,-1), "CENTER"),
        ("TOPPADDING",    (0,0),(-1,-1), 6),
        ("BOTTOMPADDING", (0,0),(-1,-1), 6),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [LIGHT, WHITE]),
        ("GRID",          (0,0),(-1,-1), 0.25, colors.HexColor("#e2e8f0")),
        ("LINEBELOW",     (0,0),(-1,0),  1.5, TEAL),
        ("TEXTCOLOR",     (0,1),(-1,-1), colors.HexColor("#475569")),
    ]
    for i in sec_header_rows:
        ri = i + 1
        sc += [
            ("BACKGROUND", (0,ri),(-1,ri), DARK),
            ("TEXTCOLOR",  (0,ri),(-1,ri), TEAL),
            ("SPAN",       (0,ri),(-1,ri)),
            ("FONTNAME",   (0,ri),(0,ri),  "Helvetica-Bold"),
            ("FONTSIZE",   (0,ri),(0,ri),  10),
            ("TOPPADDING", (0,ri),(-1,ri), 8),
            ("BOTTOMPADDING",(0,ri),(-1,ri),8),
        ]
    ltbl.setStyle(TableStyle(sc))
    story += [ltbl, Spacer(1,10*mm)]

    # Summary note
    comp_note = "Material components expanded from catalogue" if comp_map else "Components not found — showing parent items"
    story += [
        HRFlowable(width="100%", thickness=0.5, color=SLATE),
        Spacer(1,3*mm),
        Paragraph(
            "<font size=8 color='#64748b'>PO generated " +
            date.today().strftime("%-d %B %Y") + " — " +
            str(_po_co.get("company_name","Limitless Estimation Services")) +
            " — " + str(mat_count) + " material lines — " + comp_note + "</font>",
            styles["Normal"])
    ]
    doc.build(story)
    buf.seek(0)
    return buf


def get_next_invoice_number():
    """Get and increment invoice counter."""
    conn = get_conn()
    cur  = conn.cursor()
    row  = cur.execute("SELECT id, last_number FROM invoice_counter LIMIT 1").fetchone()
    if row:
        new_num = row[1] + 1
        cur.execute("UPDATE invoice_counter SET last_number=? WHERE id=?", (new_num, row[0]))
    else:
        new_num = 1
        cur.execute("INSERT INTO invoice_counter (last_number) VALUES (?)", (new_num,))
    conn.commit()
    conn.close()
    return f"INV-{new_num:04d}"


def get_company_settings():
    df = fetch_df("SELECT * FROM company_settings LIMIT 1")
    if df.empty:
        execute("""INSERT INTO company_settings
            (company_name,abn,address,phone,email,bank_name,bsb,account_number,account_name,payment_terms,logo_text,overhead_pct,markup_default)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            ("Limitless Estimation Services","","","","","","","","Limitless Estimation Services",14,"LIMITLESS",11.0,30.0))
        df = fetch_df("SELECT * FROM company_settings LIMIT 1")
    return df.iloc[0].to_dict()


def get_logo_image():
    """Returns logo as ReportLab Image or None if no logo uploaded."""
    try:
        from reportlab.platypus import Image as RLImage
        import io as _io3
        logo_df = fetch_df("SELECT logo_data FROM company_settings LIMIT 1")
        if logo_df.empty or logo_df.iloc[0]["logo_data"] is None:
            return None
        logo_bytes = bytes(logo_df.iloc[0]["logo_data"])
        if not logo_bytes:
            return None
        buf = _io3.BytesIO(logo_bytes)
        img = RLImage(buf, width=40*__import__("reportlab.lib.units", fromlist=["mm"]).mm, height=15*__import__("reportlab.lib.units", fromlist=["mm"]).mm)
        img.hAlign = "LEFT"
        return img
    except:
        return None


def generate_invoice_pdf(job, invoice, settings):
    """Generate a branded client invoice PDF."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable

    buf    = io.BytesIO()
    W, H   = A4
    margin = 20 * mm
    doc    = SimpleDocTemplate(buf, pagesize=A4,
                leftMargin=margin, rightMargin=margin,
                topMargin=margin, bottomMargin=margin)

    DARK  = colors.HexColor("#0f172a")
    TEAL  = colors.HexColor("#2dd4bf")
    SLATE = colors.HexColor("#64748b")
    MID   = colors.HexColor("#1e2d3d")
    WHITE = colors.white
    LIGHT = colors.HexColor("#f8fafc")
    RED   = colors.HexColor("#f43f5e")
    styles = getSampleStyleSheet()
    story  = []

    # ── Header ────────────────────────────────────────────────────────────
    co_name   = str(settings.get("logo_text","LIMITLESS"))
    _inv_logo = get_logo_image()
    _inv_left = _inv_logo if _inv_logo else Paragraph("<font size=24><b>" + co_name + "</b></font><br/><font size=8 color='#64748b'>" + str(settings.get('company_name','')) + "</font>", styles["Normal"])
    hdr = Table([[
        _inv_left,
        Paragraph(f"<font size=18><b>TAX INVOICE</b></font><br/>"
                  f"<font size=10 color='#2dd4bf'><b>{invoice.get('invoice_number','')}</b></font>",
                  styles["Normal"]),
    ]], colWidths=[W-2*margin-65*mm, 65*mm])
    hdr.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,-1),DARK),
        ("TEXTCOLOR",(0,0),(-1,-1),WHITE),
        ("ALIGN",(1,0),(1,0),"RIGHT"),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("TOPPADDING",(0,0),(-1,-1),16),
        ("BOTTOMPADDING",(0,0),(-1,-1),16),
        ("LEFTPADDING",(0,0),(0,0),16),
        ("RIGHTPADDING",(1,0),(1,0),16),
    ]))
    story += [hdr, Spacer(1,6*mm)]

    # ── From / To / Invoice details ───────────────────────────────────────
    issue_date = invoice.get("issue_date","")
    due_date   = invoice.get("due_date","")
    from_info  = (
        f"<b>{settings.get('company_name','')}</b><br/>"
        f"{settings.get('address','')}<br/>"
        f"ABN: {settings.get('abn','')}<br/>"
        f"{settings.get('phone','')} | {settings.get('email','')}"
    )
    to_info = (
        f"<b>{job.get('client','')}</b><br/>"
        f"{job.get('address','')}<br/>"
        f"Job: {job.get('job_id','')} | {job.get('job_type','')}"
    )
    inv_info = (
        f"Invoice: <b>{invoice.get('invoice_number','')}</b><br/>"
        f"Issue date: <b>{issue_date}</b><br/>"
        f"Due date: <b>{due_date}</b><br/>"
        f"Terms: <b>{settings.get('payment_terms',14)} days</b>"
    )
    addr_tbl = Table([[
        Paragraph(from_info, styles["Normal"]),
        Paragraph(to_info, styles["Normal"]),
        Paragraph(inv_info, styles["Normal"]),
    ]], colWidths=[(W-2*margin)/3]*3)
    addr_tbl.setStyle(TableStyle([
        ("FONTSIZE",(0,0),(-1,-1),8),
        ("VALIGN",(0,0),(-1,-1),"TOP"),
        ("TOPPADDING",(0,0),(-1,-1),0),
        ("BOTTOMPADDING",(0,0),(-1,-1),6),
        ("LINEAFTER",(0,0),(1,-1),0.5,colors.HexColor("#e2e8f0")),
        ("LEFTPADDING",(1,0),(2,-1),10),
    ]))
    story += [addr_tbl, HRFlowable(width="100%",thickness=1.5,color=TEAL,spaceAfter=6*mm)]

    # ── Invoice line ──────────────────────────────────────────────────────
    milestone = invoice.get("milestone","")
    amount    = float(invoice.get("amount_ex_gst") or invoice.get("amount") or 0)
    gst       = float(invoice.get("gst",0))
    total     = float(invoice.get("total_inc_gst",0))
    # Recalculate if missing
    if amount == 0 and total > 0:
        amount = round(total / 1.1, 2)
        gst    = round(total - amount, 2)

    line_tbl = Table([
        ["Description", "Amount"],
        [Paragraph(f"<b>{milestone}</b><br/><font size=8 color='#64748b'>"
                   f"Works at {job.get('address','')} — {job.get('job_id','')}</font>",
                   styles["Normal"]),
         f"${amount:,.2f}"],
    ], colWidths=[W-2*margin-40*mm, 40*mm])
    line_tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),MID),
        ("TEXTCOLOR",(0,0),(-1,0),TEAL),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1),9),
        ("ALIGN",(1,0),(1,-1),"RIGHT"),
        ("TOPPADDING",(0,0),(-1,-1),6),
        ("BOTTOMPADDING",(0,0),(-1,-1),6),
        ("LINEBELOW",(0,0),(-1,0),1,TEAL),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT,WHITE]),
        ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#e2e8f0")),
    ]))
    story += [line_tbl, Spacer(1,4*mm)]

    # ── Totals ────────────────────────────────────────────────────────────
    tot_tbl = Table([
        ["","Subtotal (excl. GST)", f"${amount:,.2f}"],
        ["","GST (10%)",            f"${gst:,.2f}"],
        ["","TOTAL DUE",            f"${total:,.2f}"],
    ], colWidths=[W-2*margin-80*mm, 50*mm, 30*mm])
    tot_tbl.setStyle(TableStyle([
        ("ALIGN",(1,0),(-1,-1),"RIGHT"),
        ("FONTSIZE",(0,0),(-1,-1),9),
        ("FONTNAME",(1,0),(1,-1),"Helvetica"),
        ("FONTNAME",(2,0),(2,-1),"Helvetica-Bold"),
        ("TEXTCOLOR",(1,0),(1,-2),SLATE),
        ("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),
        ("LINEABOVE",(0,-1),(-1,-1),1.5,DARK),
        ("BACKGROUND",(0,-1),(-1,-1),DARK),
        ("TEXTCOLOR",(1,-1),(-1,-1),TEAL),
        ("FONTNAME",(1,-1),(-1,-1),"Helvetica-Bold"),
        ("FONTSIZE",(1,-1),(-1,-1),12),
        ("TOPPADDING",(0,-1),(-1,-1),8),("BOTTOMPADDING",(0,-1),(-1,-1),8),
    ]))
    story += [tot_tbl, Spacer(1,8*mm)]

    # ── Payment details ───────────────────────────────────────────────────
    story.append(HRFlowable(width="100%",thickness=0.5,color=SLATE))
    story.append(Spacer(1,4*mm))
    story.append(Paragraph("<b>Payment Details</b>", styles["Normal"]))
    story.append(Spacer(1,3*mm))

    bank_data = [
        ["Bank:",           settings.get("bank_name",""),   "BSB:",    settings.get("bsb","")],
        ["Account Name:",   settings.get("account_name",""),"Account:",settings.get("account_number","")],
        ["Reference:",      invoice.get("invoice_number",""),"Due:",   due_date],
    ]
    bank_tbl = Table(bank_data, colWidths=[25*mm,65*mm,20*mm,60*mm])
    bank_tbl.setStyle(TableStyle([
        ("FONTSIZE",(0,0),(-1,-1),9),
        ("TEXTCOLOR",(0,0),(0,-1),SLATE),("TEXTCOLOR",(2,0),(2,-1),SLATE),
        ("FONTNAME",(1,0),(1,-1),"Helvetica-Bold"),("FONTNAME",(3,0),(3,-1),"Helvetica-Bold"),
        ("TOPPADDING",(0,0),(-1,-1),3),("BOTTOMPADDING",(0,0),(-1,-1),3),
    ]))
    story += [bank_tbl, Spacer(1,6*mm)]

    # ── Footer ────────────────────────────────────────────────────────────
    story += [HRFlowable(width="100%",thickness=0.5,color=SLATE), Spacer(1,3*mm)]
    story.append(Paragraph(
        f"<font size=8 color='#64748b'>Payment due within {settings.get('payment_terms',14)} days of issue date. "
        f"Late payments may incur interest. Thank you for your business. "
        f"{settings.get('company_name','')} — ABN {settings.get('abn','')}</font>",
        styles["Normal"]))

    doc.build(story)
    buf.seek(0)
    return buf


def generate_tender_analysis_pdf(job, analysis):
    """Generate a professional tender stage commercial analysis PDF."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable

    buf    = io.BytesIO()
    W, H   = A4
    margin = 20 * mm
    doc    = SimpleDocTemplate(buf, pagesize=A4,
                leftMargin=margin, rightMargin=margin,
                topMargin=margin, bottomMargin=margin)

    DARK   = colors.HexColor("#0f172a")
    TEAL   = colors.HexColor("#2dd4bf")
    SLATE  = colors.HexColor("#64748b")
    MID    = colors.HexColor("#1e2d3d")
    WHITE  = colors.white
    LIGHT  = colors.HexColor("#f8fafc")
    AMBER  = colors.HexColor("#f59e0b")
    RED    = colors.HexColor("#f43f5e")
    GREEN  = colors.HexColor("#2dd4bf")

    styles = getSampleStyleSheet()
    story  = []

    settings = get_company_settings()

    # ── Header ──────────────────────────────────────────────────────────
    hdr = Table([[
        Paragraph("<font size=22><b>" + str(settings.get("logo_text","LIMITLESS")) + "</b></font><br/>"
                  "<font size=8 color='#64748b'>" + str(settings.get("company_name","")) + "</font>",
                  styles["Normal"]),
        Paragraph("<font size=14><b>TENDER STAGE COMMERCIAL ANALYSIS</b></font><br/>"
                  "<font size=9 color='#2dd4bf'>" + str(job.get("job_id","")) + "</font>",
                  styles["Normal"]),
    ]], colWidths=[W-2*margin-80*mm, 80*mm])
    hdr.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,-1),DARK),
        ("TEXTCOLOR",(0,0),(-1,-1),WHITE),
        ("ALIGN",(1,0),(1,0),"RIGHT"),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("TOPPADDING",(0,0),(-1,-1),16),
        ("BOTTOMPADDING",(0,0),(-1,-1),16),
        ("LEFTPADDING",(0,0),(0,0),16),
        ("RIGHTPADDING",(1,0),(1,0),16),
    ]))
    story += [hdr, Spacer(1,4*mm)]

    # Job meta
    meta = Table([
        ["Project", job.get("address","—"), "Report Type", "Tender Stage Analysis"],
        ["Client",  job.get("client","—"),  "Date",        date.today().strftime("%-d %B %Y")],
        ["Estimator", job.get("estimator","—"), "Job Type", job.get("job_type","—")],
    ], colWidths=[25*mm,75*mm,25*mm,45*mm])
    meta.setStyle(TableStyle([
        ("FONTSIZE",(0,0),(-1,-1),9),
        ("TEXTCOLOR",(0,0),(0,-1),SLATE),("TEXTCOLOR",(2,0),(2,-1),SLATE),
        ("FONTNAME",(1,0),(1,-1),"Helvetica-Bold"),("FONTNAME",(3,0),(3,-1),"Helvetica-Bold"),
        ("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),
    ]))
    story += [meta, HRFlowable(width="100%",thickness=1.5,color=TEAL,spaceAfter=5*mm)]

    # ── 1. Job Snapshot ──────────────────────────────────────────────────
    story.append(Paragraph("<b>1. Job Snapshot</b>", styles["Normal"]))
    story.append(Spacer(1,3*mm))

    snap_data = [
        ["Metric", "Value"],
        ["Quoted Price (EX GST)",    f"${analysis['sell']:,.2f}"],
        ["Direct Job Cost",          f"${analysis['direct_cost']:,.2f}"],
        ["Running Costs (" + f"{analysis['overhead_pct']:.0f}%)", f"${analysis['running_cost']:,.2f}"],
        ["Total Cost to Business",   f"${analysis['total_cost']:,.2f}"],
        ["Profit Before Tax",        f"${analysis['profit_before_tax']:,.2f}"],
        ["Company Tax (25%)",        f"${analysis['tax']:,.2f}"],
        ["TRUE PROFIT After Tax",    f"${analysis['true_profit']:,.2f}"],
        ["True Profit Margin",       f"{analysis['true_margin']:.2f}%"],
    ]
    snap_tbl = Table(snap_data, colWidths=[120*mm, 50*mm])
    snap_style = [
        ("BACKGROUND",(0,0),(-1,0),MID),("TEXTCOLOR",(0,0),(-1,0),TEAL),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1),9),
        ("ALIGN",(1,0),(1,-1),"RIGHT"),
        ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
        ("ROWBACKGROUNDS",(0,1),(-1,-2),[LIGHT,WHITE]),
        ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#e2e8f0")),
        ("LINEBELOW",(0,0),(-1,0),1,TEAL),
        ("BACKGROUND",(0,-1),(-1,-1),DARK),
        ("TEXTCOLOR",(0,-1),(-1,-1),TEAL),
        ("FONTNAME",(0,-1),(-1,-1),"Helvetica-Bold"),
        ("FONTSIZE",(0,-1),(-1,-1),10),
    ]
    snap_tbl.setStyle(TableStyle(snap_style))
    story += [snap_tbl, Spacer(1,5*mm)]

    # ── 2. Cost Structure ────────────────────────────────────────────────
    story.append(Paragraph("<b>2. Cost Structure</b>", styles["Normal"]))
    story.append(Spacer(1,3*mm))

    cost_data = [
        ["Cost Area", "Value", "% of Direct Cost"],
        ["Material",   f"${analysis['mat']:,.2f}",   f"{analysis['mat_pct']:.0f}%"],
        ["Labour",     f"${analysis['lab']:,.2f}",   f"{analysis['lab_pct']:.0f}%"],
        ["Access / Crane / Other", f"${analysis['other']:,.2f}", f"{analysis['other_pct']:.0f}%"],
    ]
    cost_tbl = Table(cost_data, colWidths=[90*mm, 50*mm, 30*mm])
    cost_tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),MID),("TEXTCOLOR",(0,0),(-1,0),TEAL),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1),9),
        ("ALIGN",(1,0),(-1,-1),"RIGHT"),
        ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT,WHITE]),
        ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#e2e8f0")),
        ("LINEBELOW",(0,0),(-1,0),1,TEAL),
    ]))
    story += [cost_tbl, Spacer(1,5*mm)]

    # ── 3. Commercial Margin Analysis ────────────────────────────────────
    story.append(Paragraph("<b>3. Commercial Margin Analysis</b>", styles["Normal"]))
    story.append(Spacer(1,3*mm))

    margin_cat = analysis["margin_category"]
    cat_color  = {"Aggressive":RED,"Competitive":AMBER,"Safe":GREEN,"High Margin":GREEN}.get(margin_cat, TEAL)

    margin_data = [
        ["Metric", "Value"],
        ["Quoted Price",          f"${analysis['sell']:,.2f}"],
        ["Direct Cost",           f"${analysis['direct_cost']:,.2f}"],
        ["Gross Margin",          f"${analysis['gross_margin']:,.2f}"],
        ["True Profit After Tax", f"${analysis['true_profit']:,.2f}"],
        ["True Margin",           f"{analysis['true_margin']:.2f}%"],
        ["Category",              margin_cat],
    ]
    mar_tbl = Table(margin_data, colWidths=[120*mm, 50*mm])
    mar_style = [
        ("BACKGROUND",(0,0),(-1,0),MID),("TEXTCOLOR",(0,0),(-1,0),TEAL),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1),9),
        ("ALIGN",(1,0),(1,-1),"RIGHT"),
        ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
        ("ROWBACKGROUNDS",(0,1),(-1,-2),[LIGHT,WHITE]),
        ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#e2e8f0")),
        ("LINEBELOW",(0,0),(-1,0),1,TEAL),
        ("BACKGROUND",(0,-1),(-1,-1),cat_color),
        ("TEXTCOLOR",(0,-1),(-1,-1),DARK),
        ("FONTNAME",(0,-1),(-1,-1),"Helvetica-Bold"),
    ]
    mar_tbl.setStyle(TableStyle(mar_style))
    story += [mar_tbl, Spacer(1,4*mm)]

    # Margin scale reference
    scale_data = [["Margin Range", "Category"],
                  ["10–12%","Aggressive"],["12–15%","Competitive"],
                  ["15–18%","Safe"],["20%+","High Margin"]]
    scale_tbl = Table(scale_data, colWidths=[85*mm, 85*mm])
    scale_tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0),MID),("TEXTCOLOR",(0,0),(-1,0),TEAL),
        ("FONTNAME",(0,0),(-1,0),"Helvetica-Bold"),
        ("FONTSIZE",(0,0),(-1,-1),8),
        ("ALIGN",(0,0),(-1,-1),"CENTER"),
        ("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),
        ("ROWBACKGROUNDS",(0,1),(-1,-1),[LIGHT,WHITE]),
        ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#e2e8f0")),
    ]))
    story += [scale_tbl, Spacer(1,5*mm)]

    # ── 4. Risk Indicators ───────────────────────────────────────────────
    story.append(Paragraph("<b>4. Risk Indicators</b>", styles["Normal"]))
    story.append(Spacer(1,3*mm))

    for risk in analysis["risks"]:
        rc = RED if risk["level"]=="HIGH" else AMBER if risk["level"]=="MEDIUM" else GREEN
        risk_row = Table([[
            Paragraph("<font color='" + ("red" if risk["level"]=="HIGH" else "orange" if risk["level"]=="MEDIUM" else "green") + "'><b>" + risk["level"] + "</b></font>",
                      styles["Normal"]),
            Paragraph("<b>" + risk["title"] + "</b><br/><font size=8>" + risk["detail"] + "</font>",
                      styles["Normal"]),
        ]], colWidths=[20*mm, 150*mm])
        risk_row.setStyle(TableStyle([
            ("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),
            ("LEFTPADDING",(0,0),(0,-1),8),
            ("LINEAFTER",(0,0),(0,-1),2,rc),
            ("ROWBACKGROUNDS",(0,0),(-1,-1),[LIGHT]),
            ("GRID",(0,0),(-1,-1),0.25,colors.HexColor("#e2e8f0")),
        ]))
        story += [risk_row, Spacer(1,2*mm)]

    story.append(Spacer(1,5*mm))

    # ── 5. Estimator Commentary ──────────────────────────────────────────
    story.append(Paragraph("<b>5. Estimator Commentary</b>", styles["Normal"]))
    story.append(Spacer(1,3*mm))
    story.append(Paragraph(analysis["commentary"], styles["Normal"]))
    story.append(Spacer(1,6*mm))

    # Footer
    story += [HRFlowable(width="100%",thickness=0.5,color=SLATE), Spacer(1,3*mm)]
    story.append(Paragraph(
        "<font size=8 color='#64748b'>Tender Stage Analysis — " +
        str(job.get("job_id","")) + " — Generated " + date.today().strftime("%-d %B %Y") +
        " — " + str(settings.get("company_name","")) + " — CONFIDENTIAL</font>",
        styles["Normal"]))

    doc.build(story)
    buf.seek(0)
    return buf


def generate_variation_pdf(job, variation, approved_total):
    """Generate a variation order PDF."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable

    buf    = io.BytesIO()
    W, H   = A4
    margin = 20 * mm
    doc    = SimpleDocTemplate(buf, pagesize=A4,
                leftMargin=margin, rightMargin=margin,
                topMargin=margin, bottomMargin=margin)

    DARK  = colors.HexColor("#0f172a")
    TEAL  = colors.HexColor("#2dd4bf")
    SLATE = colors.HexColor("#64748b")
    MID   = colors.HexColor("#1e2d3d")
    WHITE = colors.white
    AMBER = colors.HexColor("#f59e0b")
    styles = getSampleStyleSheet()
    story  = []

    # Header
    hdr = Table([[
        Paragraph("<font size=22><b>LIMITLESS</b></font><br/><font size=9 color='#64748b'>Estimation Services</font>", styles["Normal"]),
        Paragraph(f"<font size=16><b>VARIATION ORDER</b></font><br/><font size=9 color='#64748b'>{variation.get('var_number','')}</font>", styles["Normal"]),
    ]], colWidths=[W-2*margin-70*mm, 70*mm])
    hdr.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,-1),DARK),("TEXTCOLOR",(0,0),(-1,-1),WHITE),
        ("ALIGN",(1,0),(1,0),"RIGHT"),("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("TOPPADDING",(0,0),(-1,-1),14),("BOTTOMPADDING",(0,0),(-1,-1),14),
        ("LEFTPADDING",(0,0),(0,0),14),("RIGHTPADDING",(1,0),(1,0),14),
    ]))
    story += [hdr, Spacer(1,5*mm)]

    # Job details
    det = Table([
        ["Job",      job.get("job_id","—"),   "Date",     date.today().strftime("%-d %B %Y")],
        ["Client",   job.get("client","—"),    "Address",  job.get("address","—")],
        ["Estimator",job.get("estimator","—"), "Status",   variation.get("status","Pending")],
    ], colWidths=[25*mm,75*mm,20*mm,50*mm])
    det.setStyle(TableStyle([
        ("FONTSIZE",(0,0),(-1,-1),9),
        ("TEXTCOLOR",(0,0),(0,-1),SLATE),("TEXTCOLOR",(2,0),(2,-1),SLATE),
        ("FONTNAME",(1,0),(1,-1),"Helvetica-Bold"),("FONTNAME",(3,0),(3,-1),"Helvetica-Bold"),
        ("TOPPADDING",(0,0),(-1,-1),4),("BOTTOMPADDING",(0,0),(-1,-1),4),
    ]))
    story += [det, HRFlowable(width="100%",thickness=1,color=TEAL,spaceAfter=5*mm)]

    # Variation details
    story.append(Paragraph("<b>Variation Description</b>", styles["Normal"]))
    story.append(Spacer(1,3*mm))
    story.append(Paragraph(variation.get("description","—"), styles["Normal"]))
    story.append(Spacer(1,6*mm))

    # Value table
    orig_val = float(job.get("sell_price") or 0)
    var_val  = float(variation.get("value") or 0)
    new_val  = orig_val + approved_total

    val_tbl = Table([
        ["Original Contract Value",  f"${orig_val:,.2f}"],
        ["This Variation",           f"${var_val:+,.2f}"],
        ["Total Approved Variations",f"${approved_total:,.2f}"],
        ["Revised Contract Value",   f"${new_val:,.2f}"],
    ], colWidths=[120*mm, 50*mm])
    val_tbl.setStyle(TableStyle([
        ("FONTSIZE",(0,0),(-1,-1),10),
        ("ALIGN",(1,0),(1,-1),"RIGHT"),
        ("FONTNAME",(0,0),(0,-1),"Helvetica"),
        ("FONTNAME",(1,0),(1,-1),"Helvetica-Bold"),
        ("TEXTCOLOR",(0,0),(0,-2),SLATE),
        ("TOPPADDING",(0,0),(-1,-1),5),("BOTTOMPADDING",(0,0),(-1,-1),5),
        ("LINEABOVE",(0,-1),(-1,-1),1.5,TEAL),
        ("BACKGROUND",(0,-1),(-1,-1),DARK),
        ("TEXTCOLOR",(0,-1),(-1,-1),TEAL),
        ("FONTNAME",(0,-1),(-1,-1),"Helvetica-Bold"),
        ("FONTSIZE",(0,-1),(-1,-1),11),
        ("TOPPADDING",(0,-1),(-1,-1),8),("BOTTOMPADDING",(0,-1),(-1,-1),8),
    ]))
    story += [val_tbl, Spacer(1,8*mm)]

    # Approval section
    story.append(HRFlowable(width="100%",thickness=0.5,color=SLATE))
    story.append(Spacer(1,4*mm))
    story.append(Paragraph("<b>Approval</b>", styles["Normal"]))
    story.append(Spacer(1,3*mm))

    approval_tbl = Table([
        ["Approved by:", variation.get("approved_by","_______________________"), "Date:", "_______________"],
        ["Signature:",   "_______________________",                              "",      ""],
    ], colWidths=[25*mm,80*mm,15*mm,50*mm])
    approval_tbl.setStyle(TableStyle([
        ("FONTSIZE",(0,0),(-1,-1),9),
        ("TEXTCOLOR",(0,0),(0,-1),SLATE),("TEXTCOLOR",(2,0),(2,-1),SLATE),
        ("TOPPADDING",(0,0),(-1,-1),6),("BOTTOMPADDING",(0,0),(-1,-1),6),
    ]))
    story += [approval_tbl, Spacer(1,5*mm)]
    story += [HRFlowable(width="100%",thickness=0.5,color=SLATE), Spacer(1,3*mm)]
    story.append(Paragraph(
        f"<font size=8 color='#64748b'>Variation {variation.get('var_number','')} — "
        f"Generated {date.today().strftime('%d %B %Y')} — Limitless Estimation Services</font>",
        styles["Normal"]))

    doc.build(story)
    buf.seek(0)
    return buf


# ─────────────────────────────────────────────
#  AUTH HELPERS
# ─────────────────────────────────────────────
import hashlib as _hashlib
import math as _math

def html(content):
    """Safely render HTML — avoids Python 3.14 f-string issues."""
    import streamlit as _st
    _st.markdown(content, unsafe_allow_html=True)


def safe_int(val, default=0):
    """Safely convert a value to int, handling NaN and None."""
    if val is None: return default
    if isinstance(val, float) and _math.isnan(val): return default
    try: return int(val)
    except: return default

def safe_float(val, default=0.0):
    """Safely convert a value to float, handling NaN and None."""
    if val is None: return default
    if isinstance(val, float) and _math.isnan(val): return default
    try: return float(val)
    except: return default

def safe_str(val, default=""):
    """Safely convert a value to str, handling NaN and None."""
    if val is None: return default
    if isinstance(val, float) and _math.isnan(val): return default
    return str(val)

def hash_password(password):
    return _hashlib.sha256(password.encode()).hexdigest()

def verify_password(password, hashed):
    return _hashlib.sha256(password.encode()).hexdigest() == hashed

def get_user(username):
    df = fetch_df("SELECT * FROM users WHERE username=? AND active=1", (username,))
    return None if df.empty else df.iloc[0].to_dict()

def seed_admin():
    """Create default admin user if no users exist."""
    count = fetch_df("SELECT COUNT(*) AS n FROM users").iloc[0]["n"]
    if count == 0:
        execute("""INSERT INTO users (username, password_hash, full_name, role, active, created_date)
                   VALUES (?,?,?,?,?,?)""",
                ("admin", hash_password("LeviNashMikaela181889!"),
                 "Administrator", "Admin", 1, date.today().isoformat()))

ROLE_PAGES = {
    "Admin": [
        "Dashboard","Jobs","Schedule Calendar","Clients","Employees","Timesheets",
        "Payroll Rules","Catalogue","Assemblies","StackCT Import","Pipeline","Budget Planner","Company P&L",
        "Financial Health","Job Costing Report","Expenses","Notifications",
        "Company Settings","User Management",
    ],
    "Estimator": [
        "Dashboard","Jobs","Clients","Catalogue","Assemblies","StackCT Import","Pipeline",
        "Budget Planner","Company P&L","Financial Health","Notifications",
    ],
    "Ops": [
        "Dashboard","Jobs","Schedule Calendar","Employees","Timesheets",
        "Payroll Rules","Notifications",
    ],
}


def get_next_job_id(prefix="LES"):
    """Auto-generate next job ID e.g. LES-001, LES-002..."""
    conn = get_conn()
    cur  = conn.cursor()
    row  = cur.execute("SELECT id, last_number FROM job_counter WHERE prefix=? LIMIT 1", (prefix,)).fetchone()
    if row:
        new_num = row[1] + 1
        cur.execute("UPDATE job_counter SET last_number=? WHERE prefix=?", (new_num, prefix))
    else:
        new_num = 1
        cur.execute("INSERT INTO job_counter (prefix, last_number) VALUES (?,?)", (prefix, new_num))
    conn.commit()
    conn.close()
    return f"{prefix}-{new_num:03d}"


# ── Global constants ──────────────────────────────────────────────────────
JOB_TYPES = ["Residential","Commercial","Industrial","Maintenance","Insurance Repair","Renovation","Variation"]
TYPE_COLORS = {
    "Residential":     "#7dd3fc",
    "Commercial":      "#a78bfa",
    "Industrial":      "#fb923c",
    "Maintenance":     "#4ade80",
    "Insurance Repair":"#f59e0b",
    "Renovation":      "#f472b6",
    "Variation":       "#2dd4bf",
}
FINISH_COLORS = {
    "Steel":"#94a3b8","MATT":"#2dd4bf","ULTRA":"#a78bfa",
    "Aluminium":"#7dd3fc","VM Zinc":"#f59e0b","Copper":"#fb923c","Zincalume":"#4ade80"
}


def upsert_job(job_id, client, address, estimator, stage):
    """Insert or update a job row — works regardless of DB version."""
    existing = fetch_df("SELECT job_id FROM jobs WHERE job_id=?", (job_id,))
    if existing.empty:
        execute(
            "INSERT INTO jobs (job_id, client, address, estimator, stage, archived) VALUES (?,?,?,?,?,0)",
            (job_id, client, address, estimator, stage),
        )
    else:
        execute(
            "UPDATE jobs SET client=?, address=?, estimator=?, stage=? WHERE job_id=?",
            (client, address, estimator, stage, job_id),
        )


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────
def get_job(job_id):
    df = fetch_df("SELECT * FROM jobs WHERE job_id=?", (job_id,))
    return None if df.empty else df.iloc[0].to_dict()


def get_estimate(job_id):
    df = fetch_df("""
        SELECT section AS "Section", item AS "Item", uom AS "UOM",
               qty AS "Qty", material_rate AS "Material Rate",
               labour_rate AS "Labour Rate", material_cost AS "Material Cost",
               labour_cost AS "Labour Cost", total_cost AS "Total Cost"
        FROM estimate_lines WHERE job_id=? ORDER BY id
    """, (job_id,))
    for col in ["Qty", "Material Rate", "Labour Rate", "Material Cost", "Labour Cost", "Total Cost"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
    return df


def load_catalogue():
    # First try catalogue_items table (imported from Buildxact/manual)
    try:
        cat_items = fetch_df("""
            SELECT category AS Category, description AS Description,
                   uom AS UOM, unit_cost AS MaterialCost,
                   labour_cost AS LabourCost, 0 AS SellUnitRate
            FROM catalogue_items ORDER BY category, description
        """)
        if not cat_items.empty:
            for col in ["MaterialCost","LabourCost","SellUnitRate"]:
                cat_items[col] = pd.to_numeric(cat_items[col], errors="coerce").fillna(0.0)
            # Also merge custom_catalogue on top
            try:
                custom_extra = fetch_df("""
                    SELECT category AS Category, description AS Description,
                           uom AS UOM, material_cost AS MaterialCost,
                           labour_cost AS LabourCost, COALESCE(sell_unit_rate,0) AS SellUnitRate
                    FROM custom_catalogue ORDER BY category, description
                """)
                if not custom_extra.empty:
                    for col in ["MaterialCost","LabourCost","SellUnitRate"]:
                        custom_extra[col] = pd.to_numeric(custom_extra[col], errors="coerce").fillna(0.0)
                    cat_items = pd.concat([cat_items, custom_extra], ignore_index=True)
            except: pass
            return cat_items
    except: pass

    # If no catalogue_items, fall back to Excel
    if not CATALOGUE_PATH.exists():
        try:
            custom_only = fetch_df("""
                SELECT category AS Category, description AS Description,
                       uom AS UOM, material_cost AS MaterialCost,
                       labour_cost AS LabourCost,
                       COALESCE(sell_unit_rate,0) AS SellUnitRate
                FROM custom_catalogue ORDER BY category, description
            """)
            if not custom_only.empty:
                for col in ["MaterialCost","LabourCost","SellUnitRate"]:
                    custom_only[col] = pd.to_numeric(custom_only[col], errors="coerce").fillna(0.0)
                return custom_only
        except: pass
        raise FileNotFoundError(f"Catalogue file not found: {CATALOGUE_PATH}")
    try:
        import openpyxl  # noqa — ensure it's available
    except ImportError:
        raise ImportError("openpyxl not installed — add to requirements.txt")
    # Read with header=2 then promote row 0 to column names
    df = pd.read_excel(CATALOGUE_PATH, sheet_name="Catalogue_Clean", header=2)
    df.columns = df.iloc[0].tolist()
    df = df.iloc[1:].reset_index(drop=True)
    # Keep only rows with valid Category and Description
    df = df[df["Category"].notna() & df["Description"].notna()]
    df = df[df["Category"].astype(str).str.strip() != ""]
    df = df[df["Description"].astype(str).str.strip() != ""]
    # Ensure all columns are proper types — no bytes, no mixed types
    for col in df.columns:
        if col in ["MaterialCost","LabourCost","SellUnitRate","BaseUnitCost",
                   "MarkupAmount","MarkupPercent","TaxAmount","TaxPercent"]:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            # Convert any bytes or mixed types to string
            df[col] = df[col].apply(lambda x: str(x) if isinstance(x, bytes) else x)
            df[col] = df[col].astype(str).where(df[col].notna(), "")
    # Clean column names — remove any NaN column names
    df.columns = [str(c) if not (isinstance(c, float)) else f"col_{i}"
                  for i, c in enumerate(df.columns)]
    df["Category"]     = df["Category"].fillna("").astype(str)
    df["Description"]  = df["Description"].fillna("").astype(str)
    df["UOM"]          = df["UOM"].fillna("").astype(str)
    df["MaterialCost"] = pd.to_numeric(df["MaterialCost"], errors="coerce").fillna(0.0)
    df["LabourCost"]   = pd.to_numeric(df["LabourCost"],   errors="coerce").fillna(0.0)
    # Apply any in-app overrides to base catalogue
    try:
        overrides = fetch_df("""
            SELECT description, category, uom, material_cost, labour_cost, sell_unit_rate
            FROM catalogue_overrides
        """)
        if not overrides.empty:
            for _, ovr in overrides.iterrows():
                mask = df["Description"] == ovr["description"]
                if mask.any():
                    df.loc[mask, "MaterialCost"]  = float(ovr["material_cost"] or 0)
                    df.loc[mask, "LabourCost"]    = float(ovr["labour_cost"] or 0)
                    df.loc[mask, "SellUnitRate"]  = float(ovr["sell_unit_rate"] or 0)
    except: pass

    # Merge custom catalogue items from database
    try:
        custom = fetch_df("""
            SELECT category AS Category, description AS Description,
                   uom AS UOM, material_cost AS MaterialCost,
                   labour_cost AS LabourCost,
                   COALESCE(sell_unit_rate, 0) AS SellUnitRate
            FROM custom_catalogue ORDER BY category, description
        """)
        if not custom.empty:
            for col in ["MaterialCost","LabourCost","SellUnitRate"]:
                custom[col] = pd.to_numeric(custom[col], errors="coerce").fillna(0.0)
            df = pd.concat([df, custom], ignore_index=True)
    except Exception as _cce:
        pass
    return df


def import_buildxact_items(filepath):
    """Import Buildxact estimate items CSV/Excel into custom_catalogue + assemblies."""
    import pandas as _pd_imp
    df = _pd_imp.read_excel(filepath, sheet_name="Data")

    current_parent_id = None
    current_parent_desc = None
    current_category = None
    sort_order = 0
    parents = 0
    components = 0

    for _, row in df.iterrows():
        order = str(row.get("DisplayedOrder","")).strip()
        desc = str(row.get("Description","") or "").strip()
        cat  = str(row.get("CategoryDescription","") or "").strip()
        uom  = str(row.get("UOM","") or "").strip()
        unit_cost = float(row.get("UnitCost",0) or 0)
        qty  = float(row.get("Units",1) or 1)
        cat_code = str(row.get("CatalogueCode","") or "").strip()

        if not desc or desc == "nan": continue
        if cat and cat != "nan": current_category = cat

        dots = order.count(".")

        if dots == 1 and cat_code:
            # Check if already exists
            existing = fetch_df("SELECT id FROM custom_catalogue WHERE description=?", (desc,))
            if existing.empty:
                execute("""INSERT INTO custom_catalogue
                    (supplier_code,category,description,uom,material_cost,labour_cost,sell_unit_rate,created_by,created_at)
                    VALUES (?,?,?,?,?,?,?,?,?)""",
                    (cat_code, current_category or "", desc, uom,
                     unit_cost, 0, unit_cost, "Buildxact Import", _today_aest().isoformat()))
                new_id = fetch_df("SELECT id FROM custom_catalogue WHERE description=?", (desc,))
                current_parent_id = int(new_id.iloc[0]["id"]) if not new_id.empty else None
            else:
                current_parent_id = int(existing.iloc[0]["id"])
            current_parent_desc = desc
            sort_order = 0
            parents += 1

        elif dots == 2 and current_parent_id:
            comp_type = "Labour" if desc.lower() in ["labour","labor"] else "Material"
            # Remove existing components first on first component
            if sort_order == 0:
                execute("DELETE FROM assemblies WHERE catalogue_id=?", (current_parent_id,))
            execute("""INSERT INTO assemblies
                (catalogue_id,catalogue_description,component_description,qty,uom,unit_cost,component_type,sort_order)
                VALUES (?,?,?,?,?,?,?,?)""",
                (current_parent_id, current_parent_desc, desc, qty, uom, unit_cost, comp_type, sort_order))
            sort_order += 1
            components += 1

    return parents, components


def badge(text):
    colors = {
        "UNDER BUDGET":    "#15803d",
        "GETTING CLOSE":   "#b45309",
        "LABOUR WARNING":  "#c2410c",
        "LABOUR OVER":     "#b91c1c",
        "MATERIAL WARNING":"#c2410c",
        "MATERIAL OVER":   "#b91c1c",
        "A": "#15803d", "B": "#b45309", "C": "#b91c1c",
    }
    st.markdown(
        f"<span style='padding:5px 14px;border-radius:999px;background:{colors.get(text,'#334155')};"
        f"color:#fff;font-weight:700;font-size:13px'>{text}</span>",
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────
#  METRICS
# ─────────────────────────────────────────────
def labour_metrics(job_id):
    job = get_job(job_id)
    if not job:
        return {}

    tender = float(job.get("tender_labour_budget") or 0)

    assign_df = fetch_df("""
        SELECT da.employee, COALESCE(e.hourly_rate, 0) AS hourly_rate
        FROM day_assignments da
        LEFT JOIN employees e ON e.name = da.employee
        WHERE da.job_id=? AND da.employee != '__unassigned__'
    """, (job_id,))

    sched_days = len(assign_df)
    sched_cost = float((assign_df["hourly_rate"] * 8).sum()) if not assign_df.empty else 0.0

    actual_df    = fetch_df("SELECT COALESCE(SUM(hours*hourly_rate),0) AS v FROM labour_logs WHERE job_id=?", (job_id,))
    actual       = float(actual_df.iloc[0]["v"])
    projected    = max(sched_cost, actual)
    variance     = projected - tender
    variance_pct = (variance / tender) if tender else 0

    if variance_pct <= 0:      health = "UNDER BUDGET"
    elif variance_pct < 0.05:  health = "GETTING CLOSE"
    elif variance_pct < 0.10:  health = "LABOUR WARNING"
    else:                      health = "LABOUR OVER"

    return {
        "tender_labour":     tender,
        "scheduled_days":    sched_days,
        "scheduled_cost":    sched_cost,
        "actual_labour":     actual,
        "projected_labour":  projected,
        "labour_variance":   variance,
        "labour_health":     health,
    }


def material_metrics(job_id):
    job = get_job(job_id)
    if not job:
        return {}

    tender   = float(job.get("tender_material_budget") or 0)
    df       = fetch_df("SELECT COALESCE(SUM(amount),0) AS s, COUNT(*) AS c FROM material_invoices WHERE job_id=? AND status='Entered'", (job_id,))
    actual   = float(df.iloc[0]["s"])
    count    = int(df.iloc[0]["c"])
    ratio    = (actual / tender) if tender else 0

    if ratio < 0.90:    health = "UNDER BUDGET"
    elif ratio <= 1.00: health = "GETTING CLOSE"
    elif ratio <= 1.10: health = "MATERIAL WARNING"
    else:               health = "MATERIAL OVER"

    return {
        "tender_material":   tender,
        "actual_material":   actual,
        "material_variance": actual - tender,
        "material_health":   health,
        "invoice_count":     count,
    }


def profit_metrics(job_id):
    job = get_job(job_id)
    if not job:
        return {}

    lab        = labour_metrics(job_id)
    mat        = material_metrics(job_id)
    sell       = float(job.get("sell_price") or 0)
    rc         = sell * float(job.get("running_cost_pct") or 0)
    cost       = lab.get("projected_labour", 0) + mat.get("actual_material", 0) + rc
    profit     = sell - cost
    profit_pct = (profit / sell) if sell else 0
    tender_pct = float(job.get("tender_profit_pct") or 0)
    drift      = profit_pct - tender_pct

    lh, mh = lab.get("labour_health", ""), mat.get("material_health", "")
    if lh == "UNDER BUDGET" and mh == "UNDER BUDGET" and profit_pct >= tender_pct:
        grade = "A"
    elif profit_pct >= tender_pct - 0.03 and lh != "LABOUR OVER" and mh != "MATERIAL OVER":
        grade = "B"
    else:
        grade = "C"

    return {
        "sell_price":        sell,
        "running_cost":      rc,
        "live_cost":         cost,
        "live_profit":       profit,
        "live_profit_pct":   profit_pct,
        "tender_profit_pct": tender_pct,
        "profit_drift":      drift,
        "grade":             grade,
    }


# ─────────────────────────────────────────────
#  INIT + SIDEBAR
# ─────────────────────────────────────────────
if USE_POSTGRES:
    try:
        _pgc = get_conn()
        _pgcur = _pgc.cursor()
        # Create all tables
        for _sql in [
            "CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, username TEXT UNIQUE, password_hash TEXT, role TEXT DEFAULT 'Estimator', full_name TEXT DEFAULT '', active INTEGER DEFAULT 1, created_at TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS jobs (job_id TEXT PRIMARY KEY, client TEXT DEFAULT '', address TEXT DEFAULT '', estimator TEXT DEFAULT '', stage TEXT DEFAULT 'Lead', sell_price REAL DEFAULT 0, archived INTEGER DEFAULT 0, job_type TEXT DEFAULT 'Residential', job_finish TEXT DEFAULT 'Steel', parent_job TEXT DEFAULT '', is_variation INTEGER DEFAULT 0, variation_title TEXT DEFAULT '', running_cost_pct REAL DEFAULT 0.11, tender_material_budget REAL DEFAULT 0, tender_labour_budget REAL DEFAULT 0, tender_profit_pct REAL DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS clients (id SERIAL PRIMARY KEY, company TEXT DEFAULT '', name TEXT DEFAULT '', email TEXT DEFAULT '', phone TEXT DEFAULT '', address TEXT DEFAULT '', client_type TEXT DEFAULT 'Builder', notes TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS employees (id SERIAL PRIMARY KEY, name TEXT UNIQUE, role TEXT DEFAULT 'Roofer', hourly_rate REAL DEFAULT 0, phone TEXT DEFAULT '', active INTEGER DEFAULT 1, pin TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS estimate_lines (id SERIAL PRIMARY KEY, job_id TEXT, section TEXT DEFAULT '', item TEXT DEFAULT '', uom TEXT DEFAULT '', qty REAL DEFAULT 0, material_cost REAL DEFAULT 0, labour_cost REAL DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS labour_logs (id SERIAL PRIMARY KEY, work_date TEXT, job_id TEXT, employee TEXT, hours REAL DEFAULT 8, hourly_rate REAL DEFAULT 0, note TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS material_invoices (id SERIAL PRIMARY KEY, invoice_date TEXT, job_id TEXT, supplier TEXT DEFAULT '', invoice_number TEXT DEFAULT '', amount REAL DEFAULT 0, status TEXT DEFAULT 'Entered', note TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS client_invoices (id SERIAL PRIMARY KEY, invoice_number TEXT, job_id TEXT, milestone_id INTEGER DEFAULT 0, issue_date TEXT, due_date TEXT DEFAULT '', amount_ex_gst REAL DEFAULT 0, gst REAL DEFAULT 0, total_inc_gst REAL DEFAULT 0, status TEXT DEFAULT 'Issued', milestone TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS payment_schedule (id SERIAL PRIMARY KEY, job_id TEXT, milestone TEXT DEFAULT '', pct REAL DEFAULT 0, amount REAL DEFAULT 0, status TEXT DEFAULT 'Unpaid', due_date TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS variations (id SERIAL PRIMARY KEY, job_id TEXT, var_number TEXT DEFAULT '', description TEXT DEFAULT '', value REAL DEFAULT 0, status TEXT DEFAULT 'Pending', date_raised TEXT DEFAULT '', approved_by TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS pipeline (id SERIAL PRIMARY KEY, job_id TEXT UNIQUE, client TEXT DEFAULT '', sell_price REAL DEFAULT 0, probability INTEGER DEFAULT 50, probability_pct REAL DEFAULT 50, value REAL DEFAULT 0, stage TEXT DEFAULT 'Tender', target_month TEXT DEFAULT '', expected_date TEXT DEFAULT '', follow_up_date TEXT DEFAULT '', last_contact TEXT DEFAULT '', contact_name TEXT DEFAULT '', notes TEXT DEFAULT '', secured INTEGER DEFAULT 0, archived INTEGER DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS day_assignments (id SERIAL PRIMARY KEY, job_id TEXT DEFAULT '', client TEXT DEFAULT '', employee TEXT DEFAULT '__unassigned__', date TEXT DEFAULT '', note TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS company_settings (id SERIAL PRIMARY KEY, company_name TEXT DEFAULT 'Limitless Estimation Services', abn TEXT DEFAULT '', address TEXT DEFAULT '', phone TEXT DEFAULT '', email TEXT DEFAULT '', bank_name TEXT DEFAULT '', bsb TEXT DEFAULT '', account_number TEXT DEFAULT '', account_name TEXT DEFAULT '', payment_terms INTEGER DEFAULT 14, logo_text TEXT DEFAULT 'LIMITLESS', overhead_pct REAL DEFAULT 11.0, markup_default REAL DEFAULT 30.0, logo_filename TEXT DEFAULT '', terms_conditions TEXT DEFAULT '', website TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS invoice_counter (id SERIAL PRIMARY KEY, last_number INTEGER DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS job_counter (id SERIAL PRIMARY KEY, prefix TEXT UNIQUE, last_number INTEGER DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS site_diary (id SERIAL PRIMARY KEY, job_id TEXT, diary_date TEXT, weather TEXT DEFAULT '', temp TEXT DEFAULT '', workers_on_site TEXT DEFAULT '', hours_worked REAL DEFAULT 0, notes TEXT DEFAULT '', created_by TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS job_photos (id SERIAL PRIMARY KEY, job_id TEXT, photo_date TEXT, caption TEXT DEFAULT '', uploaded_by TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS job_files (id SERIAL PRIMARY KEY, job_id TEXT, filename TEXT, filetype TEXT DEFAULT '', uploaded_at TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS custom_catalogue (id SERIAL PRIMARY KEY, category TEXT DEFAULT '', description TEXT NOT NULL, uom TEXT DEFAULT 'lm', material_cost REAL DEFAULT 0, labour_cost REAL DEFAULT 0, sell_unit_rate REAL DEFAULT 0, created_by TEXT DEFAULT '', created_at TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS clock_events (id SERIAL PRIMARY KEY, employee TEXT NOT NULL, job_id TEXT DEFAULT '', event_type TEXT NOT NULL, event_time TEXT NOT NULL, event_date TEXT NOT NULL, note TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS mobile_variations (id SERIAL PRIMARY KEY, employee TEXT NOT NULL, job_id TEXT NOT NULL, description TEXT NOT NULL, submitted_at TEXT NOT NULL, status TEXT DEFAULT 'Pending')",
            "CREATE TABLE IF NOT EXISTS monthly_targets (id SERIAL PRIMARY KEY, month TEXT UNIQUE, target REAL DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS payroll_rules (id SERIAL PRIMARY KEY, employee TEXT, rule_type TEXT DEFAULT '', value REAL DEFAULT 0, note TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS timesheet_entries (id SERIAL PRIMARY KEY, employee TEXT, week_start TEXT, ordinary_hours REAL DEFAULT 0, overtime_hours REAL DEFAULT 0, allowances REAL DEFAULT 0, note TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS public_holidays (id SERIAL PRIMARY KEY, holiday_date TEXT, name TEXT DEFAULT '', state TEXT DEFAULT 'NSW')",
            "CREATE TABLE IF NOT EXISTS safety_docs (id SERIAL PRIMARY KEY, job_id TEXT, doc_type TEXT DEFAULT '', filename TEXT DEFAULT '', status TEXT DEFAULT 'Pending', reviewed_by TEXT DEFAULT '', reviewed_at TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS job_retention (id SERIAL PRIMARY KEY, job_id TEXT UNIQUE, retention_pct REAL DEFAULT 0, retention_amount REAL DEFAULT 0, released INTEGER DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS stackct_mapping (id SERIAL PRIMARY KEY, stackct_name TEXT, catalogue_item TEXT, finish TEXT DEFAULT '', uom TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS material_finishes (id SERIAL PRIMARY KEY, name TEXT UNIQUE, markup_pct REAL DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS catalogue_finishes (id SERIAL PRIMARY KEY, finish_id INTEGER, catalogue_item TEXT, adjusted_mat_cost REAL DEFAULT 0, adjusted_lab_cost REAL DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS client_interactions (id SERIAL PRIMARY KEY, client_id INTEGER, interaction_date TEXT, type TEXT DEFAULT '', notes TEXT DEFAULT '', follow_up TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS recipes (id SERIAL PRIMARY KEY, name TEXT, section TEXT DEFAULT '', uom TEXT DEFAULT '', description TEXT DEFAULT '')",
            "CREATE TABLE IF NOT EXISTS recipe_items (id SERIAL PRIMARY KEY, recipe_id INTEGER, description TEXT, item_type TEXT DEFAULT 'Material', unit_qty REAL DEFAULT 1, uom TEXT DEFAULT '', material_rate REAL DEFAULT 0, labour_rate REAL DEFAULT 0)",
            "CREATE TABLE IF NOT EXISTS variations (id SERIAL PRIMARY KEY, job_id TEXT, var_number TEXT DEFAULT '', description TEXT DEFAULT '', value REAL DEFAULT 0, status TEXT DEFAULT 'Pending', date_raised TEXT DEFAULT '', approved_by TEXT DEFAULT '')",
        ]:
            try: _pgcur.execute(_sql)
            except: pass
        _pgc.commit()
        # Add any missing columns (safe to run multiple times)
        for _alter in [
            "ALTER TABLE company_settings ADD COLUMN IF NOT EXISTS logo_data BYTEA",
            "ALTER TABLE company_settings ADD COLUMN IF NOT EXISTS logo_filename TEXT DEFAULT ''",
            "ALTER TABLE company_settings ADD COLUMN IF NOT EXISTS terms_conditions TEXT DEFAULT ''",
            "ALTER TABLE company_settings ADD COLUMN IF NOT EXISTS website TEXT DEFAULT ''",
            "ALTER TABLE employees ADD COLUMN IF NOT EXISTS hourly_rate REAL DEFAULT 0",
            "ALTER TABLE employees ADD COLUMN IF NOT EXISTS phone TEXT DEFAULT ''",
            "ALTER TABLE employees ADD COLUMN IF NOT EXISTS pin TEXT DEFAULT ''",
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS sell_price REAL DEFAULT 0",
            "ALTER TABLE jobs ADD COLUMN IF NOT EXISTS archived INTEGER DEFAULT 0",
            "ALTER TABLE pipeline ADD COLUMN IF NOT EXISTS value REAL DEFAULT 0",
            "ALTER TABLE pipeline ADD COLUMN IF NOT EXISTS probability_pct REAL DEFAULT 50",
            "ALTER TABLE pipeline ADD COLUMN IF NOT EXISTS target_month TEXT DEFAULT ''",
            "ALTER TABLE pipeline ADD COLUMN IF NOT EXISTS follow_up_date TEXT DEFAULT ''",
            "ALTER TABLE pipeline ADD COLUMN IF NOT EXISTS last_contact TEXT DEFAULT ''",
            "ALTER TABLE pipeline ADD COLUMN IF NOT EXISTS contact_name TEXT DEFAULT ''",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS full_name TEXT DEFAULT ''",
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS active INTEGER DEFAULT 1",
            "ALTER TABLE client_invoices ADD COLUMN IF NOT EXISTS amount_ex_gst REAL DEFAULT 0",
            "ALTER TABLE client_invoices ADD COLUMN IF NOT EXISTS milestone TEXT DEFAULT ''",
            "ALTER TABLE variations ADD COLUMN IF NOT EXISTS date_raised TEXT DEFAULT ''",
            "ALTER TABLE variations ADD COLUMN IF NOT EXISTS approved_by TEXT DEFAULT ''",
            "ALTER TABLE labour_logs ADD COLUMN IF NOT EXISTS note TEXT DEFAULT ''",
            "ALTER TABLE material_invoices ADD COLUMN IF NOT EXISTS note TEXT DEFAULT ''",
            "ALTER TABLE day_assignments ADD COLUMN IF NOT EXISTS note TEXT DEFAULT ''",
            "ALTER TABLE site_diary ADD COLUMN IF NOT EXISTS created_by TEXT DEFAULT ''",
            "ALTER TABLE job_photos ADD COLUMN IF NOT EXISTS photo_data BYTEA",
            "ALTER TABLE job_files ADD COLUMN IF NOT EXISTS filedata BYTEA",
            "ALTER TABLE job_files ADD COLUMN IF NOT EXISTS filetype TEXT DEFAULT ''",
            "ALTER TABLE job_files ADD COLUMN IF NOT EXISTS uploaded_at TEXT DEFAULT ''",
        ]:
            try: _pgcur.execute(_alter)
            except: pass
        _pgc.commit()
        # Seed defaults
        _pgcur.execute("SELECT COUNT(*) FROM company_settings")
        if _pgcur.fetchone()[0] == 0:
            _pgcur.execute("INSERT INTO company_settings (id) VALUES (1)")
        _pgcur.execute("SELECT COUNT(*) FROM invoice_counter")
        if _pgcur.fetchone()[0] == 0:
            _pgcur.execute("INSERT INTO invoice_counter (last_number) VALUES (0)")
        _pgcur.execute("SELECT COUNT(*) FROM users")
        if _pgcur.fetchone()[0] == 0:
            import hashlib as _hl
            _h = _hl.sha256("limitless2024".encode()).hexdigest()
            _pgcur.execute("INSERT INTO users (username, password_hash, role, active) VALUES (%s,%s,%s,%s)", ("admin", _h, "Admin", 1))
        _pgc.commit()
        release_conn(_pgc)
    except Exception as _pge:
        st.error(f"DB init error: {_pge}")
else:
    init_db()
    seed_admin()

#  LOGIN GATE
# ─────────────────────────────────────────────
if "authenticated_user" not in st.session_state:
    st.session_state["authenticated_user"] = None
if "show_login" not in st.session_state:
    st.session_state["show_login"] = False

if not st.session_state["authenticated_user"]:
    st.markdown("""
    <style>
    section[data-testid="stSidebar"] { display: none !important; }
    .main .block-container { max-width: 1200px !important; margin: 0 auto; padding-top: 0 !important; }
    </style>
    """, unsafe_allow_html=True)

    # ── Show landing page or login ─────────────────────────────────────────
    if not st.session_state["show_login"]:
        # ══════════════════════════════════════════════════════════════════
        # LANDING PAGE
        # ══════════════════════════════════════════════════════════════════
        st.markdown("""
        <style>
        .hero { 
            background: linear-gradient(135deg, #080f1e 0%, #0d1a2e 50%, #0a1f1f 100%);
            border-bottom: 2px solid #1e2d3d;
            padding: 80px 40px 60px;
            text-align: center;
        }
        .hero-tag {
            display: inline-block;
            background: #2dd4bf22;
            color: #2dd4bf;
            border: 1px solid #2dd4bf44;
            border-radius: 999px;
            padding: 6px 20px;
            font-size: 13px;
            font-weight: 700;
            letter-spacing: .15em;
            text-transform: uppercase;
            margin-bottom: 24px;
        }
        .hero-title {
            font-size: 72px;
            font-weight: 900;
            color: #ffffff;
            letter-spacing: -.04em;
            line-height: 1;
            margin-bottom: 12px;
        }
        .hero-sub {
            font-size: 24px;
            color: #2dd4bf;
            font-weight: 700;
            margin-bottom: 16px;
        }
        .hero-desc {
            font-size: 18px;
            color: #94a3b8;
            max-width: 600px;
            margin: 0 auto 40px;
            line-height: 1.7;
        }
        .cta-btn {
            display: inline-block;
            background: #2dd4bf;
            color: #080f1e;
            font-weight: 900;
            font-size: 18px;
            padding: 16px 40px;
            border-radius: 10px;
            text-decoration: none;
            margin-right: 12px;
            cursor: pointer;
        }
        .cta-btn-outline {
            display: inline-block;
            background: transparent;
            color: #e2e8f0;
            font-weight: 700;
            font-size: 18px;
            padding: 16px 40px;
            border-radius: 10px;
            border: 2px solid #2a3d4f;
            cursor: pointer;
        }
        .feature-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 20px;
            padding: 20px 0;
        }
        .feature-card {
            background: #111c27;
            border: 1px solid #1e2d3d;
            border-top: 3px solid #2dd4bf;
            border-radius: 12px;
            padding: 28px 24px;
        }
        .feature-icon { font-size: 32px; margin-bottom: 12px; }
        .feature-title { font-size: 18px; font-weight: 800; color: #f1f5f9; margin-bottom: 8px; }
        .feature-desc { font-size: 15px; color: #64748b; line-height: 1.6; }
        .stat-grid {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 16px;
            background: #0d1526;
            border-top: 1px solid #1e2d3d;
            border-bottom: 1px solid #1e2d3d;
            padding: 40px;
            text-align: center;
        }
        .stat-num { font-size: 42px; font-weight: 900; color: #2dd4bf; }
        .stat-label { font-size: 14px; color: #475569; font-weight: 600; text-transform: uppercase; letter-spacing: .1em; }
        </style>

        <!-- HERO -->
        <div class="hero">
            <div class="hero-tag">⚒️ Built for tradies. By a tradie.</div>
            <img src="data:image/png;base64,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" style="width:180px;margin:0 auto 16px;display:block">
            <div class="hero-sub">Job Management for Trade Businesses</div>
            <div class="hero-desc">
                Quote faster. Win more jobs. Know your numbers.<br>
                The all-in-one platform built for roofers, landscapers, plumbers and every trade in between.
            </div>
        </div>
        """, unsafe_allow_html=True)

        # CTA buttons
        btn1, btn2, btn3 = st.columns([2,1,2])
        with btn2:
            if st.button("🚀 Get Started", type="primary", use_container_width=True):
                st.session_state["show_login"] = True
                st.rerun()

        st.markdown("<div style='text-align:center;margin-top:8px'>", unsafe_allow_html=True)
        st.markdown("""
        <div style='text-align:center;padding:12px 0'>
            <a href="mailto:pete@limitlesstakeoffs.com?subject=Book a Demo — Limitless Job Management" 
               style='color:#64748b;font-size:15px;text-decoration:none'>
               📧 Book a demo → pete@limitlesstakeoffs.com
            </a>
        </div>
        """, unsafe_allow_html=True)

        # Stats bar
        st.markdown("""
        <div class="stat-grid">
            <div><div class="stat-num">5min</div><div class="stat-label">To build a quote</div></div>
            <div><div class="stat-num">100%</div><div class="stat-label">Invoice accuracy</div></div>
            <div><div class="stat-num">Live</div><div class="stat-label">Job tracking</div></div>
            <div><div class="stat-num">📱</div><div class="stat-label">Mobile app included</div></div>
        </div>
        """, unsafe_allow_html=True)

        # Features
        st.markdown("<div style='padding: 60px 40px'>", unsafe_allow_html=True)
        st.markdown("<div style='text-align:center;font-size:36px;font-weight:900;color:#f1f5f9;margin-bottom:8px'>Everything your trade business needs</div>", unsafe_allow_html=True)
        st.markdown("<div style='text-align:center;color:#64748b;font-size:18px;margin-bottom:40px'>One platform. No spreadsheets. No paper.</div>", unsafe_allow_html=True)

        st.markdown("""
        <div class="feature-grid">
            <div class="feature-card">
                <div class="feature-icon">📋</div>
                <div class="feature-title">Professional Quoting</div>
                <div class="feature-desc">Build quotes from your catalogue in minutes. Generate PDF quotes that win jobs.</div>
            </div>
            <div class="feature-card" style="border-top-color:#f59e0b">
                <div class="feature-icon">📅</div>
                <div class="feature-title">Job Scheduling</div>
                <div class="feature-desc">See your whole team on a calendar. Assign jobs, track who's where, plan ahead.</div>
            </div>
            <div class="feature-card" style="border-top-color:#a78bfa">
                <div class="feature-icon">💰</div>
                <div class="feature-title">Invoice & Get Paid</div>
                <div class="feature-desc">Issue invoices straight from the job. Track what's paid, what's outstanding.</div>
            </div>
            <div class="feature-card" style="border-top-color:#4ade80">
                <div class="feature-icon">📱</div>
                <div class="feature-title">Mobile App for the Lads</div>
                <div class="feature-desc">Clock in/out, log variations, upload site photos — all from their phone.</div>
            </div>
            <div class="feature-card" style="border-top-color:#f43f5e">
                <div class="feature-icon">📊</div>
                <div class="feature-title">Know Your Numbers</div>
                <div class="feature-desc">P&L, BAS, super — all calculated automatically. No surprises at tax time.</div>
            </div>
            <div class="feature-card" style="border-top-color:#60a5fa">
                <div class="feature-icon">🔧</div>
                <div class="feature-title">Works for Any Trade</div>
                <div class="feature-desc">Roofing, landscaping, plumbing, electrical — build your own catalogue and rates.</div>
            </div>
        </div>
        </div>
        """, unsafe_allow_html=True)

        # Footer CTA
        st.markdown("""
        <div style='background:#0d1526;border-top:1px solid #1e2d3d;padding:60px 40px;text-align:center'>
            <div style='font-size:36px;font-weight:900;color:#ffffff;margin-bottom:12px'>Ready to run a tighter business?</div>
            <div style='color:#64748b;font-size:18px;margin-bottom:32px'>Start your free 14-day trial. No credit card required.</div>
        </div>
        """, unsafe_allow_html=True)

        fc1, fc2, fc3 = st.columns([2,1,2])
        with fc2:
            if st.button("🚀 Start Free Trial", type="primary", use_container_width=True):
                st.session_state["show_login"] = True
                st.rerun()

        st.markdown("""
        <div style='text-align:center;padding:20px 0 40px;color:#475569;font-size:14px'>
            Already have an account? 
        </div>
        """, unsafe_allow_html=True)

        lc1, lc2, lc3 = st.columns([2,1,2])
        with lc2:
            if st.button("Log In →", use_container_width=True):
                st.session_state["show_login"] = True
                st.rerun()

        st.stop()

    # ── Login page ────────────────────────────────────────────────────────
    st.markdown("""
    <style>
    section[data-testid="stSidebar"] { display: none !important; }
    .main .block-container { max-width: 480px !important; margin: 0 auto; padding-top: 8vh; }
    </style>
    """, unsafe_allow_html=True)

    st.markdown(f"""
    <div style="text-align:center;margin-bottom:2rem">
        <img src="data:image/png;base64,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" style="width:180px;margin-bottom:4px">
    </div>
    """, unsafe_allow_html=True)

    with st.form("login_form"):
        st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)
        username = st.text_input("Username", placeholder="Enter your username")
        password = st.text_input("Password", type="password", placeholder="Enter your password")
        st.markdown("<div style='height:.25rem'></div>", unsafe_allow_html=True)
        login_btn = st.form_submit_button("Sign In", type="primary", width="stretch")

        if login_btn:
            if username.strip() and password.strip():
                user = get_user(username.strip())
                if user and verify_password(password.strip(), user["password_hash"]):
                    st.session_state["authenticated_user"] = user
                    st.success(f"Welcome back, {user['full_name'] or user['username']}!")
                    # Sync from mobile on login
                    try: sync_from_mobile()
                    except: pass
                    # Sync to mobile
                    try: sync_to_mobile()
                    except: pass
                    st.rerun()
                else:
                    st.error("Invalid username or password.")
            else:
                st.warning("Please enter your username and password.")

    st.markdown("""
    <div style="text-align:center;margin-top:2rem;font-size:13px;color:#475569">
        Default login: <strong style="color:#64748b">admin</strong> /
        <strong style="color:#64748b">contact admin for access</strong><br>
        Change your password in User Management after first login.
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ── Authenticated ─────────────────────────────────────────────────────────
current_user = st.session_state["authenticated_user"]
user_role    = current_user.get("role", "Ops")
user_pages   = ROLE_PAGES.get(user_role, ROLE_PAGES["Ops"])

_uname = str(current_user.get('full_name') or current_user.get('username',''))
_urole = str(user_role)
st.sidebar.markdown(
    "<div style='padding:1.2rem 0 1rem;text-align:center'>"
    "<img src='data:image/png;base64,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' style='width:120px;margin-bottom:4px'><br>"
    "<div style='font-size:11px;font-weight:600;letter-spacing:0.15em;color:#2dd4bf;text-transform:uppercase;margin-top:2px'>Limitless Job Management</div>"
    "</div>"
    "<hr style='border-color:#2a2a2a;margin:0 0 .5rem'>"
    "<div style='padding:0 0 .75rem;text-align:center'>"
    "<div style='font-size:14px;font-weight:600;color:#e2e8f0'>" + _uname + "</div>"
    "<div style='font-size:13px;color:#2dd4bf;font-weight:600;letter-spacing:.06em;text-transform:uppercase'>" + _urole + "</div>"
    "</div>"
    "<hr style='border-color:#2a2a2a;margin:0 0 1rem'>",
    unsafe_allow_html=True)

if st.sidebar.button("Sign Out", key="signout"):
    st.session_state["authenticated_user"] = None
    st.rerun()



# DB status
import os as _os2

# Supabase sync buttons
if USE_SUPABASE:
    st.sidebar.divider()
    if st.sidebar.button("🔄 Sync to mobile", use_container_width=True):
        try:
            if not _supa_client:
                st.sidebar.error("Supabase client not initialized")
            else:
                # Add pin column if missing
                try:
                    execute("ALTER TABLE employees ADD COLUMN pin TEXT DEFAULT ''")
                except: pass
                emps = fetch_df("SELECT id, name, role, hourly_rate, active, COALESCE(pin,'') as pin FROM employees WHERE active=1")
                st.sidebar.write(f"Found {len(emps)} employees to sync")
                for _, r in emps.iterrows():
                    result = _supa_client.table("employees").upsert({
                        "id": int(r["id"]), "name": str(r["name"]),
                        "role": str(r.get("role","")), "hourly_rate": float(r.get("hourly_rate",0)),
                        "active": int(r.get("active",1)), "pin": str(r.get("pin",""))
                    }).execute()
                    st.sidebar.write(f"Synced: {r['name']}")
                st.sidebar.success("✅ Done!")
        except Exception as _e:
            st.sidebar.error(f"Error: {_e}")
    if st.sidebar.button("📥 Pull from mobile", use_container_width=True):
        try:
            sync_from_mobile()
            st.sidebar.success("✅ Pulled clock-ins!")
        except Exception as _e:
            st.sidebar.error(str(_e))


st.sidebar.markdown("<div style='font-size:13px;font-weight:600;letter-spacing:0.1em;color:#666;text-transform:uppercase;margin-bottom:4px'>Navigation</div>", unsafe_allow_html=True)
# Group pages with dividers
SIDEBAR_GROUPS = {
    "📋 JOBS":       ["Dashboard","Jobs","Schedule Calendar"],
    "👥 PEOPLE":     ["Clients","Employees","Timesheets","Payroll Rules"],
    "📐 ESTIMATING": ["Catalogue","Assemblies","StackCT Import"],
    "💰 FINANCIALS": ["Pipeline","Budget Planner","Company P&L","Financial Health","Job Costing Report","Expenses"],
    "⚙️ SETTINGS":   ["Notifications","Company Settings","User Management"],
}

# Build filtered page list respecting user role
visible_pages = [p for p in user_pages]

# Render grouped sidebar
selected_page = None
for group_label, group_pages in SIDEBAR_GROUPS.items():
    pages_in_group = [p for p in group_pages if p in visible_pages]
    if not pages_in_group:
        continue
    st.sidebar.markdown(
        f"<div style='font-size:13px;font-weight:700;letter-spacing:.12em;"
        f"color:#2dd4bf;text-transform:uppercase;padding:8px 0 4px;margin-top:6px'>"
        f"{group_label}</div>",
        unsafe_allow_html=True
    )
    for p in pages_in_group:
        is_active = st.session_state.get("current_page","Dashboard") == p
        btn_style = "background:#1a3a3a;color:#2dd4bf;" if is_active else "background:transparent;color:#94a3b8;"
        if st.sidebar.button(
            p,
            key=f"nav_{p}",
            width="stretch",
        ):
            st.session_state["current_page"] = p
            st.rerun()

page = st.session_state.get("current_page","Dashboard")
# Ensure page is accessible to this role
if page not in visible_pages:
    page = visible_pages[0] if visible_pages else "Dashboard"
    st.session_state["current_page"] = page

# Jobs are opened via the Jobs board — no global selected_job needed
selected_job = None


# ─────────────────────────────────────────────
#  PAGE: DASHBOARD
# ─────────────────────────────────────────────
if page == "Dashboard":
    from datetime import datetime as _dt
    import json as _json

    # ── Pull all-jobs data ────────────────────────────────────────────────
    all_active_jobs = fetch_df("SELECT job_id, client, stage, job_type FROM jobs WHERE archived=0 ORDER BY job_id")
    today_str       = _today_aest().isoformat()
    total_active    = len(all_active_jobs)

    on_site_today = fetch_df("""
        SELECT DISTINCT da.employee FROM day_assignments da
        WHERE da.date=? AND da.employee != '__unassigned__'
    """, (today_str,))
    people_today = len(on_site_today)

    # Build emp_color_map for dashboard
    EMP_COLORS_DASH = ["#2dd4bf","#f59e0b","#a78bfa","#f43f5e","#60a5fa","#4ade80","#fb923c","#e879f9"]
    _emp_list_dash = fetch_df("SELECT name FROM employees WHERE active=1 ORDER BY name")
    emp_color_map = {}
    if not _emp_list_dash.empty:
        for _i, _n in enumerate(_emp_list_dash["name"].tolist()):
            emp_color_map[_n] = EMP_COLORS_DASH[_i % len(EMP_COLORS_DASH)]

    today_cost_df = fetch_df("""
        SELECT COALESCE(SUM(e.hourly_rate*8),0) AS cost
        FROM day_assignments da LEFT JOIN employees e ON e.name=da.employee
        WHERE da.date=? AND da.employee != '__unassigned__'
    """, (today_str,))
    est_daily_cost = float(today_cost_df.iloc[0]["cost"]) if not today_cost_df.empty else 0

    pipe_wtd = fetch_df("SELECT COALESCE(SUM(value*probability_pct/100.0),0) AS wtd FROM pipeline WHERE archived=0")
    wtd_pipeline = float(pipe_wtd.iloc[0]["wtd"]) if not pipe_wtd.empty else 0

    # ── Header ────────────────────────────────────────────────────────────
    now_aest    = _datetime.now(_tz.utc) + _td(hours=10)
    today_label = now_aest.strftime("%A, %d %B %Y")
    hour        = now_aest.hour
    greeting    = "Good morning" if hour < 12 else "Good afternoon" if hour < 17 else "Good evening"
    time_str    = now_aest.strftime("%I:%M %p").lstrip("0")

    # ── Clock + greeting header ───────────────────────────────────────────
    hdr_left, hdr_right = st.columns([3,2])
    with hdr_left:
        st.markdown(
            "<div style='background:linear-gradient(135deg,#1a2332 0%,#1e3040 40%,#1a3a3a 100%);"
            "border-radius:16px;padding:2rem 2.5rem;height:100%'>"
            "<div style='font-size:13px;font-weight:600;letter-spacing:.15em;"
            "color:#2dd4bf;text-transform:uppercase;margin-bottom:6px'>Operations Centre</div>"
            "<div style='font-size:28px;font-weight:800;color:#fff;margin-bottom:4px'>" + greeting + ", " + str(current_user.get("full_name","") or current_user.get("username","")).split()[0] + ".</div>"
            "<div style='font-size:60px;font-weight:900;color:#2dd4bf;letter-spacing:-.02em;line-height:1'>" + time_str + "</div>"
            "<div style='font-size:14px;color:#94a3b8;margin-top:6px'>" + today_label + "</div>"
            "</div>",
            unsafe_allow_html=True)

    # ── 7-day weather forecast ────────────────────────────────────────────
    with hdr_right:
        try:
            import urllib.request as _ur
            import json as _wjson
            # Get company address for location — default to Sydney if not set
            _co = get_company_settings()
            _weather_url = "https://api.open-meteo.com/v1/forecast?latitude=-33.87&longitude=151.21&daily=weathercode,temperature_2m_max,temperature_2m_min,precipitation_sum&timezone=Australia%2FSydney&forecast_days=7"
            _wreq = _ur.urlopen(_weather_url, timeout=3)
            _wdata = _wjson.loads(_wreq.read())
            _daily = _wdata.get("daily", {})
            _dates  = _daily.get("time", [])
            _codes  = _daily.get("weathercode", [])
            _maxts  = _daily.get("temperature_2m_max", [])
            _mints  = _daily.get("temperature_2m_min", [])
            _precs  = _daily.get("precipitation_sum", [])

            def _wcode_icon(code):
                if code == 0: return "☀️"
                elif code <= 3: return "⛅"
                elif code <= 49: return "🌫️"
                elif code <= 67: return "🌧️"
                elif code <= 77: return "❄️"
                elif code <= 82: return "🌦️"
                elif code <= 99: return "⛈️"
                return "🌡️"

            def _wcode_label(code):
                if code == 0: return "Sunny"
                elif code <= 3: return "Cloudy"
                elif code <= 49: return "Foggy"
                elif code <= 67: return "Rain"
                elif code <= 77: return "Snow"
                elif code <= 82: return "Showers"
                elif code <= 99: return "Thunder"
                return "Mixed"

            weather_html = "<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:16px;padding:16px'>"
            weather_html += "<div style='font-size:13px;font-weight:700;color:#2dd4bf;letter-spacing:.1em;text-transform:uppercase;margin-bottom:12px'>7-Day Forecast · Sydney</div>"
            weather_html += "<div style='display:flex;gap:6px;overflow-x:auto'>"

            for i in range(min(7, len(_dates))):
                try:
                    _d = _datetime.strptime(_dates[i], "%Y-%m-%d")
                    _day_name = "Today" if i == 0 else _d.strftime("%a")
                    _icon = _wcode_icon(int(_codes[i] if i < len(_codes) else 0))
                    _maxt = f"{_maxts[i]:.0f}°" if i < len(_maxts) else "—"
                    _mint = f"{_mints[i]:.0f}°" if i < len(_mints) else "—"
                    _prec = f"{_precs[i]:.0f}mm" if i < len(_precs) and _precs[i] > 0 else ""
                    _label = _wcode_label(int(_codes[i] if i < len(_codes) else 0))
                    _is_today = i == 0
                    _bg = "#0d2233" if _is_today else "#111c27"
                    _border = "1px solid #2dd4bf" if _is_today else "1px solid #1e2d3d"

                    weather_html += (
                        f"<div style='background:{_bg};border:{_border};border-radius:10px;"
                        f"padding:8px 6px;text-align:center;min-width:52px;flex:1'>"
                        f"<div style='font-size:13px;font-weight:700;color:{'#2dd4bf' if _is_today else '#94a3b8'}'>{_day_name}</div>"
                        f"<div style='font-size:22px;margin:4px 0'>{_icon}</div>"
                        f"<div style='font-size:13px;font-weight:700;color:#e2e8f0'>{_maxt}</div>"
                        f"<div style='font-size:13px;color:#64748b'>{_mint}</div>"
                        + (f"<div style='font-size:13px;color:#7dd3fc;margin-top:2px'>{_prec}</div>" if _prec else "") +
                        f"</div>"
                    )
                except:
                    pass

            weather_html += "</div></div>"
            st.markdown(weather_html, unsafe_allow_html=True)

        except Exception as _we:
            st.markdown(
                "<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:16px;"
                "padding:16px;text-align:center'>"
                "<div style='color:#475569;font-size:13px'>Weather unavailable</div>"
                "</div>",
                unsafe_allow_html=True)

    # ── Stat cards ────────────────────────────────────────────────────────
    stats = [
        ("Active Jobs",    str(total_active),         "#2dd4bf"),
        ("On Site Today",  str(people_today),          "#7dd3fc"),
        ("Est. Daily Cost",f"${est_daily_cost:,.0f}",  "#fbbf24"),
        ("Wtd. Pipeline",  f"${wtd_pipeline:,.0f}",    "#a78bfa"),
    ]
    sc = st.columns(4)
    for col,(label,val,color) in zip(sc,stats):
        col.markdown(f"""
        <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-top:3px solid {color};
            border-radius:10px;padding:1rem 1.2rem;text-align:center;margin-bottom:8px">
            <div style="font-size:13px;font-weight:700;letter-spacing:.12em;
                text-transform:uppercase;color:#64748b;margin-bottom:6px">{label}</div>
            <div style="font-size:26px;font-weight:800;color:{color};letter-spacing:-.02em">{val}</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    # ── Charts row ────────────────────────────────────────────────────────
    ch1, ch2, ch3 = st.columns(3)

    # Chart 1: Jobs by stage (bar)
    with ch1:
        st.markdown("<div style='font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#2dd4bf;margin-bottom:8px'>Jobs by stage</div>", unsafe_allow_html=True)
        if not all_active_jobs.empty:
            stage_counts = all_active_jobs["stage"].value_counts()
            stage_order  = ["Lead","Take-off","Tender Review","Pre-Live Handover","Live Job","Completed"]
            stage_colors_map = {"Lead":"#7dd3fc","Take-off":"#2dd4bf","Tender Review":"#f59e0b",
                                "Pre-Live Handover":"#a78bfa","Live Job":"#4ade80","Completed":"#64748b"}
            bars_html = ""
            max_count = max(stage_counts.values) if len(stage_counts) else 1
            for stage in stage_order:
                count = stage_counts.get(stage, 0)
                if count == 0: continue
                pct  = count/max_count*100
                color= stage_colors_map.get(stage,"#64748b")
                bars_html += f"""
                <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
                    <div style="font-size:13px;color:#94a3b8;width:100px;flex-shrink:0;
                        white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{stage}</div>
                    <div style="flex:1;background:#0f172a;border-radius:4px;height:16px;position:relative">
                        <div style="background:{color};width:{pct:.0f}%;height:16px;border-radius:4px"></div>
                    </div>
                    <div style="font-size:13px;font-weight:700;color:{color};width:20px;text-align:right">{count}</div>
                </div>"""
            st.markdown(f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:14px'>{bars_html}</div>", unsafe_allow_html=True)
        else:
            st.info("No jobs yet.")

    # Chart 2: Jobs by type (donut-style)
    with ch2:
        st.markdown("<div style='font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#2dd4bf;margin-bottom:8px'>Jobs by type</div>", unsafe_allow_html=True)
        if not all_active_jobs.empty and "job_type" in all_active_jobs.columns:
            type_counts = all_active_jobs["job_type"].fillna("Unknown").value_counts()
            type_colors_map = {"Residential":"#7dd3fc","Commercial":"#a78bfa","Industrial":"#fb923c",
                               "Maintenance":"#4ade80","Insurance Repair":"#f59e0b",
                               "Renovation":"#f472b6","Variation":"#2dd4bf"}
            total_t = type_counts.sum()
            rows_html = ""
            for tname, tcount in type_counts.items():
                pct   = tcount/total_t*100
                color = type_colors_map.get(str(tname),"#64748b")
                rows_html += f"""
                <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
                    <div style="width:10px;height:10px;border-radius:50%;background:{color};flex-shrink:0"></div>
                    <div style="font-size:13px;color:#94a3b8;flex:1">{tname}</div>
                    <div style="font-size:13px;font-weight:700;color:{color}">{tcount} <span style="color:#475569;font-weight:400">({pct:.0f}%)</span></div>
                </div>"""
            st.markdown(f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:14px'>{rows_html}</div>", unsafe_allow_html=True)
        else:
            st.info("No data.")

    # Chart 3: Revenue pipeline by month
    with ch3:
        st.markdown("<div style='font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#2dd4bf;margin-bottom:8px'>Pipeline by month</div>", unsafe_allow_html=True)
        pipe_by_month = fetch_df("""
            SELECT target_month, SUM(value) as total, SUM(value*probability_pct/100) as wtd
            FROM pipeline WHERE archived=0 GROUP BY target_month ORDER BY target_month LIMIT 6
        """)
        if not pipe_by_month.empty:
            max_val = max(pipe_by_month["total"].max(), 1)
            bars_html2 = ""
            for _, pr in pipe_by_month.iterrows():
                mo_label = pr["target_month"][5:7] + "/" + pr["target_month"][2:4]
                t_pct    = float(pr["total"])/max_val*100
                w_pct    = float(pr["wtd"])/max_val*100
                bars_html2 += f"""
                <div style="margin-bottom:8px">
                    <div style="font-size:13px;color:#64748b;margin-bottom:2px">{mo_label}</div>
                    <div style="background:#0f172a;border-radius:4px;height:10px;margin-bottom:2px">
                        <div style="background:#2a3d4f;width:{t_pct:.0f}%;height:10px;border-radius:4px;position:relative">
                            <div style="background:#2dd4bf;width:{w_pct/t_pct*100 if t_pct else 0:.0f}%;height:10px;border-radius:4px"></div>
                        </div>
                    </div>
                    <div style="font-size:13px;color:#475569">${float(pr['wtd']):,.0f} wtd / ${float(pr['total']):,.0f} total</div>
                </div>"""
            st.markdown(f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:14px'>{bars_html2}</div>", unsafe_allow_html=True)
        else:
            st.info("No pipeline data.")

    st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    # ── Main content ──────────────────────────────────────────────────────
    left_col, right_col = st.columns([3,2])

    with left_col:
        st.markdown("<div style='font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#2dd4bf;margin-bottom:10px'>Live job health</div>", unsafe_allow_html=True)
        dot_colors = {"green":"#2dd4bf","amber":"#f59e0b","red":"#f43f5e"}
        grade_colors= {"A":"#2dd4bf","B":"#f59e0b","C":"#f43f5e"}

        live_jobs_health = all_active_jobs[all_active_jobs["stage"] == "Live Job"]
        if not live_jobs_health.empty:
            for _, jrow in live_jobs_health.iterrows():
                jid  = jrow["job_id"]
                jlab = labour_metrics(jid)
                jmat = material_metrics(jid)
                jlp  = profit_metrics(jid)
                lh   = jlab.get("labour_health","—")
                mh   = jmat.get("material_health","—")
                gr   = jlp.get("grade","—")
                if "OVER" in lh or "OVER" in mh:        dot = "red"
                elif "WARNING" in lh or "WARNING" in mh: dot = "amber"
                elif "CLOSE" in lh or "CLOSE" in mh:    dot = "amber"
                else:                                    dot = "green"
                dc  = dot_colors[dot]
                gc  = grade_colors.get(gr,"#64748b")
                lhc = dot_colors["red" if "OVER" in lh else "amber" if "WARNING" in lh or "CLOSE" in lh else "green"]
                mhc = dot_colors["red" if "OVER" in mh else "amber" if "WARNING" in mh or "CLOSE" in mh else "green"]
                sell= jlp.get("sell_price",0)
                sell_span = ("<span style='color:#e2e8f0;font-weight:600;margin-left:auto'>$" + f"{sell:,.0f}" + "</span>") if sell > 0 else ""
                health_html = (
                    "<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;"
                    "padding:12px 16px;margin-bottom:8px;display:flex;align-items:center;gap:14px'>"
                    "<div style='width:10px;height:10px;border-radius:50%;background:" + dc + ";"
                    "flex-shrink:0;box-shadow:0 0 6px " + dc + "55'></div>"
                    "<div style='flex:1;min-width:0'>"
                    "<div style='display:flex;align-items:center;gap:10px;margin-bottom:3px'>"
                    "<span style='font-weight:700;font-size:16px;color:#e2e8f0'>" + str(jid) + "</span>"
                    "<span style='font-size:14px;color:#64748b'>" + str(jrow.get('client','') or '') + "</span>"
                    "<span style='font-size:13px;background:#2a3d4f;color:#94a3b8;"
                    "padding:2px 10px;border-radius:999px;margin-left:auto'>" + str(jrow.get('stage','')) + "</span>"
                    "</div>"
                    "<div style='display:flex;gap:16px;font-size:14px'>"
                    "<span style='color:" + lhc + "'>&#9679; " + str(lh) + "</span>"
                    "<span style='color:" + mhc + "'>&#9679; " + str(mh) + "</span>"
                    + sell_span +
                    "</div></div>"
                    "<div style='background:" + gc + ";color:#0f172a;font-weight:800;font-size:15px;"
                    "width:32px;height:32px;border-radius:6px;display:flex;align-items:center;"
                    "justify-content:center;flex-shrink:0'>" + str(gr) + "</div>"
                    "</div>"
                )
                st.markdown(health_html, unsafe_allow_html=True)
        else:
            st.markdown("<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:1.5rem;text-align:center;color:#64748b;font-size:13px'>No active jobs</div>", unsafe_allow_html=True)

        # Today on site
        today_sched = fetch_df("""
            SELECT da.job_id, da.employee, da.client, da.note,
                   COALESCE(j.address,'') AS address,
                   COALESCE(da.start_time,'') AS start_time,
                   COALESCE(da.end_time,'') AS end_time
            FROM day_assignments da
            LEFT JOIN jobs j ON j.job_id = da.job_id
            WHERE da.date=? AND da.employee != '__unassigned__'
            ORDER BY da.employee
        """, (today_str,))
        st.markdown("<div style='font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#2dd4bf;margin:1.2rem 0 10px'>On site today</div>", unsafe_allow_html=True)
        if not today_sched.empty:
            for _, ts in today_sched.iterrows():
                emp_init = "".join([w[0].upper() for w in str(ts["employee"]).split()])[:2]
                _addr = str(ts.get('address','') or '')
                _st_t = str(ts.get('start_time','') or '')
                _en_t = str(ts.get('end_time','') or '')
                _time = f"{_st_t[:5]}–{_en_t[:5]}" if _st_t and _en_t else ""
                emp_color = emp_color_map.get(str(ts['employee']), "#2dd4bf")
                _addr_html = f"<div style='font-size:13px;color:#64748b'>📍 {_addr}</div>" if _addr else ""
                _time_html = f"<div style='font-size:12px;color:#f59e0b'>🕐 {_time}</div>" if _time else ""
                _note_html = f"<div style='font-size:13px;color:#475569'>{ts.get('note','') or ''}</div>" if ts.get('note') else ""
                onsite_html = (
                    f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:9px;"
                    f"padding:12px 16px;margin-bottom:8px;display:flex;align-items:center;gap:12px'>"
                    f"<div style='width:36px;height:36px;border-radius:50%;background:#1a3a3a;"
                    f"border:2px solid {emp_color};display:flex;align-items:center;justify-content:center;"
                    f"font-size:13px;font-weight:700;color:{emp_color};flex-shrink:0'>{emp_init}</div>"
                    f"<div style='flex:1'>"
                    f"<div style='font-size:15px;font-weight:700;color:#e2e8f0'>{ts['employee']}</div>"
                    f"<div style='font-size:13px;color:#2dd4bf'>{ts['job_id']} · {ts.get('client','') or ''}</div>"
                    f"{_addr_html}{_time_html}"
                    f"</div>"
                    f"{_note_html}"
                    f"</div>"
                )
                st.markdown(onsite_html, unsafe_allow_html=True)
        else:
            st.markdown("<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:1rem;text-align:center;color:#64748b;font-size:13px'>Nobody scheduled today</div>", unsafe_allow_html=True)

    with right_col:
        # Pipeline snapshot
        st.markdown("<div style='font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#2dd4bf;margin-bottom:10px'>Pipeline snapshot</div>", unsafe_allow_html=True)
        pipe_snap = fetch_df("SELECT job_id,client,value,probability_pct,target_month FROM pipeline WHERE archived=0 ORDER BY target_month LIMIT 7")
        if not pipe_snap.empty:
            for _, pr in pipe_snap.iterrows():
                prob = int(pr["probability_pct"])
                pc   = "#2dd4bf" if prob>=75 else "#f59e0b" if prob>=40 else "#f43f5e"
                snap_html = (
                    "<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:9px;"
                    "padding:12px 16px;margin-bottom:8px'>"
                    "<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:8px'>"
                    "<div><span style='font-weight:700;font-size:16px;color:#e2e8f0'>" + str(pr['job_id']) + "</span>"
                    "<span style='font-size:14px;color:#64748b;margin-left:8px'>" + str(pr.get('client','') or '') + "</span></div>"
                    "<div><span style='font-size:15px;font-weight:700;color:#e2e8f0'>$" + f"{float(pr['value']):,.0f}" + "</span>"
                    "<span style='font-size:13px;color:" + pc + ";margin-left:6px'>" + str(prob) + "%</span></div>"
                    "</div>"
                    "<div style='background:#0f172a;border-radius:999px;height:6px'>"
                    "<div style='background:" + pc + ";width:" + str(prob) + "%;height:6px;border-radius:999px'></div>"
                    "</div>"
                    "<div style='font-size:13px;color:#475569;margin-top:6px'>" + str(pr.get('target_month','')) + "</div>"
                    "</div>"
                )
                st.markdown(snap_html, unsafe_allow_html=True)
        else:
            st.markdown("<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:1rem;text-align:center;color:#64748b;font-size:13px'>No pipeline entries</div>", unsafe_allow_html=True)

        # Recent activity
        st.markdown("<div style='font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#2dd4bf;margin:1.2rem 0 10px'>Recent activity</div>", unsafe_allow_html=True)
        recent = fetch_df("""
            SELECT ll.work_date, ll.job_id, ll.employee,
                   ll.hours, ROUND(ll.hours*ll.hourly_rate,2)AS cost
            FROM labour_logs ll ORDER BY ll.work_date DESC, ll.id DESC LIMIT 6
        """)
        if not recent.empty:
            for _, rl in recent.iterrows():
                emp_init = "".join([w[0].upper() for w in str(rl["employee"]).split()])[:2]
                recent_html = (
                    "<div style='display:flex;align-items:center;gap:10px;padding:10px 0;"
                    "border-bottom:1px solid #1e2d3d'>"
                    "<div style='width:32px;height:32px;border-radius:50%;background:#2a3d4f;"
                    "display:flex;align-items:center;justify-content:center;font-size:13px;"
                    "font-weight:700;color:#7dd3fc;flex-shrink:0'>" + str(emp_init) + "</div>"
                    "<div style='flex:1;min-width:0'>"
                    "<div style='font-size:15px;color:#e2e8f0;font-weight:500'>" + str(rl['employee']) + " — " + str(rl['job_id']) + "</div>"
                    "<div style='font-size:13px;color:#64748b'>" + str(rl['work_date']) + " · " + str(rl['hours']) + "h</div>"
                    "</div>"
                    "<div style='font-size:14px;font-weight:700;color:#2dd4bf;flex-shrink:0'>$" + f"{float(rl['cost']):,.0f}" + "</div>"
                    "</div>"
                )
                st.markdown(recent_html, unsafe_allow_html=True)
        else:
            st.markdown("<div style='color:#64748b;font-size:13px;padding:.5rem 0'>No labour logged yet</div>", unsafe_allow_html=True)

        # Overdue alerts
        overdue_fu = fetch_df("""
            SELECT COUNT(*) AS cnt FROM pipeline
            WHERE archived=0 AND follow_up_date != '' AND follow_up_date < ?
        """, (today_str,))
        overdue_pay = fetch_df("""
            SELECT COUNT(*) AS cnt FROM payment_schedule
            WHERE status != 'Paid' AND due_date != '' AND due_date < ?
        """, (today_str,))
        cnt_fu  = int(overdue_fu.iloc[0]["cnt"]) if not overdue_fu.empty else 0
        cnt_pay = int(overdue_pay.iloc[0]["cnt"]) if not overdue_pay.empty else 0
        if cnt_fu > 0 or cnt_pay > 0:
            st.markdown("<div style='height:.8rem'></div>", unsafe_allow_html=True)
            if cnt_fu > 0:
                st.warning(f"⚠️  {cnt_fu} pipeline follow-up{'s' if cnt_fu>1 else ''} overdue — check Notifications")
            if cnt_pay > 0:
                st.warning(f"⚠️  {cnt_pay} payment milestone{'s' if cnt_pay>1 else ''} overdue — check Notifications")


elif page == "Quote Builder":
    st.title("Estimate Builder")

    if not selected_job:
        st.info("No jobs yet — go to Jobs to add one.")
        st.stop()

    try:
        catalogue = load_catalogue()
    except FileNotFoundError:
        st.error(f"Catalogue file not found: {CATALOGUE_PATH.name}")
        st.stop()

    if st.session_state.get("estimate_job") != selected_job:
        st.session_state["estimate_lines"] = estimate.to_dict("records")
        st.session_state["estimate_job"]   = selected_job
    if "estimate_lines" not in st.session_state:
        st.session_state["estimate_lines"] = estimate.to_dict("records")

    left, right = st.columns([1, 2])

    with left:
        st.subheader("Add line")
        sections = sorted([s for s in catalogue["Category"].unique() if s.strip()])
        section  = st.selectbox("Section", sections)
        items    = catalogue[catalogue["Category"] == section]
        desc     = st.selectbox("Item", items["Description"].tolist())
        row      = items[items["Description"] == desc].iloc[0]
        st.caption(f"UOM: {row['UOM']} | Mat: ${row['MaterialCost']:,.2f} | Labour: ${row['LabourCost']:,.2f}")
        qty = st.number_input("Quantity", min_value=0.0, value=0.0, step=1.0)

        ca, cb = st.columns(2)
        with ca:
            if st.button("Add"):
                mc = qty * float(row["MaterialCost"])
                lc = qty * float(row["LabourCost"])
                st.session_state["estimate_lines"].append({
                    "Section": section, "Item": desc, "UOM": row["UOM"], "Qty": qty,
                    "Material Rate": float(row["MaterialCost"]), "Labour Rate": float(row["LabourCost"]),
                    "Material Cost": mc, "Labour Cost": lc, "Total Cost": mc + lc,
                })
                st.success("Added.")
        with cb:
            if st.button("Clear"):
                st.session_state["estimate_lines"] = []
                st.rerun()

    with right:
        st.subheader("Current estimate")
        lines = st.session_state["estimate_lines"]
        if lines:
            est_df     = pd.DataFrame(lines)
            st.dataframe(est_df, width="stretch")

            mat_total  = est_df["Material Cost"].sum()
            lab_total  = est_df["Labour Cost"].sum()
            cost_total = est_df["Total Cost"].sum()
            markup     = st.number_input("Markup %", min_value=0.0, value=20.0, step=1.0)
            sell       = cost_total * (1 + markup / 100)
            gp         = sell - cost_total
            margin_pct = (gp / sell * 100) if sell else 0
            tp         = (gp / sell) if sell else 0

            c1, c2, c3, c4, c5 = st.columns(5)
            c1.metric("Material", f"${mat_total:,.2f}")
            c2.metric("Labour",   f"${lab_total:,.2f}")
            c3.metric("Cost",     f"${cost_total:,.2f}")
            c4.metric("Sell",     f"${sell:,.2f}")
            c5.metric("Margin",   f"{margin_pct:.1f}%")

            if st.button("Save Estimate to Job", type="primary"):
                execute("""
                    UPDATE jobs SET
                        sell_price=?, tender_material_budget=?,
                        tender_labour_budget=?, tender_profit_pct=?
                    WHERE job_id=?
                """, (float(sell), float(mat_total), float(lab_total), float(tp), selected_job))
                execute("DELETE FROM estimate_lines WHERE job_id=?", (selected_job,))
                for r in lines:
                    execute("""
                        INSERT INTO estimate_lines
                            (job_id, section, item, uom, qty, material_rate, labour_rate,
                             material_cost, labour_cost, total_cost)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (
                        selected_job, r["Section"], r["Item"], r["UOM"],
                        float(r["Qty"]), float(r["Material Rate"]), float(r["Labour Rate"]),
                        float(r["Material Cost"]), float(r["Labour Cost"]), float(r["Total Cost"]),
                    ))
                st.success(f"Estimate saved to {selected_job}.")
                st.rerun()

            st.divider()
            st.subheader("Labour plan")
            labour_lines = est_df[est_df["Labour Cost"] > 0]
            if not labour_lines.empty:
                total_lc = labour_lines["Labour Cost"].sum()
                st.metric("Estimated labour from quote builder", f"${total_lc:,.2f}")
                emp_df = fetch_df(
                    "SELECT name AS emp_name, hourly_rate*8 AS daily_cost FROM employees WHERE active=1 ORDER BY name"
                )
                if not emp_df.empty:
                    sel_emp  = st.selectbox("Employee for plan", emp_df["emp_name"].tolist(), key="emp_plan")
                    daily    = float(emp_df.loc[emp_df["emp_name"] == sel_emp, "daily_cost"].iloc[0])
                    sug_days = total_lc / daily if daily else 0
                    rd       = max(1, int(round(sug_days)))

                    a1, a2, a3 = st.columns(3)
                    a1.metric("Daily cost",     f"${daily:,.2f}")
                    a2.metric("Labour budget",  f"${total_lc:,.2f}")
                    a3.metric("Suggested days", f"{sug_days:.1f}")

                    sc1, sc2 = st.columns(2)
                    with sc1:
                        sched_start = st.date_input("Start date", value=date.today(), key="sched_start")
                    with sc2:
                        block_name = st.text_input("Block label", value="Estimated Install", key="sched_block")

                    if st.button("Generate schedule block"):
                        for i in range(rd):
                            d = pd.bdate_range(sched_start, periods=i + 1)[-1].date()
                            execute(
                                "INSERT INTO day_assignments (job_id, client, employee, date, note) VALUES (?,?,?,?,?)",
                                (selected_job, job["client"] if job else "", sel_emp, d.isoformat(), block_name),
                            )
                        st.success(f"{rd} day assignment(s) created for {sel_emp}.")
        else:
            st.info("Add lines from the left panel.")


# ─────────────────────────────────────────────
#  PAGE: CATALOGUE
# ─────────────────────────────────────────────
elif page == "Catalogue":
    st.title("Catalogue")
    st.caption("Your fully editable catalogue — import from Buildxact, add items, edit recipes and components.")

    UOM_OPTIONS = ["lm","m2","Ea","each","m3","hr","item","allow","t","kg","L","box","tube","roll"]

    cat_tabs = st.tabs(["📋 My Catalogue", "📥 Import from Buildxact", "➕ Add Item", "⭐ Custom Items"])

    # ── TAB 1: MY CATALOGUE with recipe editor ─────────────────────────
    with cat_tabs[0]:
        ci_df = fetch_df("SELECT * FROM catalogue_items ORDER BY category, description")

        bc1, bc2, bc3, bc4 = st.columns([2,2,1,1])
        with bc1:
            ci_search = st.text_input("🔍 Search", placeholder="Item name...", key="ci_srch")
        with bc2:
            ci_cats = ["All"] + sorted(ci_df["category"].dropna().unique().tolist()) if not ci_df.empty else ["All"]
            ci_cat_f = st.selectbox("Category", ci_cats, key="ci_catf")
        with bc3:
            st.metric("Items", len(ci_df))
        with bc4:
            if not ci_df.empty:
                csv_out = ci_df[["category","description","uom","unit_cost","labour_cost"]].to_csv(index=False)
                st.download_button("📊 Export", data=csv_out,
                    file_name=f"catalogue_{_today_aest().isoformat()}.csv", mime="text/csv")

        if ci_df.empty:
            st.info("No items yet — import from Buildxact or add manually.")
        else:
            disp = ci_df.copy()
            if ci_search:
                disp = disp[disp["description"].str.contains(ci_search, case=False, na=False)]
            if ci_cat_f != "All":
                disp = disp[disp["category"] == ci_cat_f]

            # Group by category
            for cat_name, cat_grp in disp.groupby("category"):
                st.markdown(f"<div style='background:#111c27;border-left:3px solid #2dd4bf;padding:8px 14px;margin:12px 0 6px;font-weight:700;color:#2dd4bf;font-size:14px;text-transform:uppercase;letter-spacing:.1em'>{cat_name}</div>", unsafe_allow_html=True)

                for _, item in cat_grp.iterrows():
                    iid = int(item["id"])
                    is_open = st.session_state.get("cat_open_item") == iid

                    # Item row
                    total_cost = float(item.get("unit_cost",0) or 0) + float(item.get("labour_cost",0) or 0)
                    ir1,ir2,ir3,ir4,ir5 = st.columns([4,1,1.5,1.5,1.5])
                    ir1.markdown(f"<div style='font-size:14px;color:#e2e8f0;padding:6px 0'>{item['description']}</div>", unsafe_allow_html=True)
                    ir2.markdown(f"<div style='font-size:13px;color:#94a3b8;padding:6px 0'>{item.get('uom','')}</div>", unsafe_allow_html=True)
                    ir3.markdown(f"<div style='font-size:13px;color:#64748b;padding:6px 0'>Mat: ${float(item.get('unit_cost',0)):,.2f}</div>", unsafe_allow_html=True)
                    ir4.markdown(f"<div style='font-size:13px;color:#64748b;padding:6px 0'>Lab: ${float(item.get('labour_cost',0)):,.2f}</div>", unsafe_allow_html=True)
                    with ir5:
                        ic1,ic2,ic3 = st.columns(3)
                        with ic1:
                            if st.button("📋" if not is_open else "▲", key=f"ci_open_{iid}", help="View/edit recipe"):
                                if is_open:
                                    st.session_state.pop("cat_open_item", None)
                                else:
                                    st.session_state["cat_open_item"] = iid
                                st.rerun()
                        with ic2:
                            if st.button("✏️", key=f"ci_ed_{iid}"):
                                st.session_state["ci_editing"] = iid; st.rerun()
                        with ic3:
                            if st.button("🗑", key=f"ci_dl_{iid}"):
                                execute("DELETE FROM catalogue_components WHERE item_id=?", (iid,))
                                execute("DELETE FROM catalogue_items WHERE id=?", (iid,))
                                st.rerun()

                    # Edit item form
                    if st.session_state.get("ci_editing") == iid:
                        with st.form(f"ci_edit_{iid}"):
                            ef1,ef2,ef3,ef4,ef5 = st.columns([3,1,1.5,1.5,1])
                            with ef1: e_desc = st.text_input("", value=str(item.get("description","") or ""), label_visibility="collapsed")
                            with ef2: e_uom  = st.selectbox("", UOM_OPTIONS,
                                index=UOM_OPTIONS.index(str(item.get("uom","lm"))) if str(item.get("uom","lm")) in UOM_OPTIONS else 0,
                                label_visibility="collapsed")
                            with ef3: e_mat  = st.number_input("Mat $", value=float(item.get("unit_cost",0) or 0), step=0.5)
                            with ef4: e_lab  = st.number_input("Lab $", value=float(item.get("labour_cost",0) or 0), step=0.5)
                            with ef5:
                                if st.form_submit_button("💾"):
                                    execute("UPDATE catalogue_items SET description=?,uom=?,unit_cost=?,labour_cost=? WHERE id=?",
                                        (e_desc.strip(), e_uom, e_mat, e_lab, iid))
                                    st.session_state.pop("ci_editing", None); st.rerun()

                    # Recipe / component view
                    if is_open:
                        comps = fetch_df("""
                            SELECT * FROM catalogue_components
                            WHERE item_id=? ORDER BY sort_order, id
                        """, (iid,))

                        st.markdown(f"""
                        <div style='background:#0d1f2d;border:1px solid #2a3d4f;border-radius:10px;
                            padding:12px 16px;margin:4px 0 8px 16px'>
                            <div style='font-size:12px;font-weight:700;color:#2dd4bf;text-transform:uppercase;
                                letter-spacing:.1em;margin-bottom:10px'>
                                Recipe Components — {item['description']}
                            </div>
                        """, unsafe_allow_html=True)

                        if comps.empty:
                            st.markdown("<div style='color:#475569;font-size:13px;padding:4px 0'>No components yet — add below</div>", unsafe_allow_html=True)
                        else:
                            # Header
                            rh1,rh2,rh3,rh4,rh5,rh6 = st.columns([3,1.5,1,1.5,1.5,1])
                            for col, lbl in zip([rh1,rh2,rh3,rh4,rh5,rh6],
                                ["Component","Type","Qty","UOM","Unit Cost","Total"]):
                                col.markdown(f"<div style='color:#475569;font-size:12px;font-weight:700'>{lbl}</div>", unsafe_allow_html=True)

                            mat_total = 0
                            lab_total = 0
                            for _, comp in comps.iterrows():
                                cmpid = int(comp["id"])
                                qty = float(comp.get("qty",1) or 1)
                                uc  = float(comp.get("unit_cost",0) or 0)
                                total_c = round(qty * uc, 2)
                                ctype = str(comp.get("item_type","Material"))
                                if ctype == "Labour": lab_total += total_c
                                else: mat_total += total_c

                                is_comp_edit = st.session_state.get("comp_editing") == cmpid
                                if is_comp_edit:
                                    with st.form(f"comp_edit_{cmpid}"):
                                        ce1,ce2,ce3,ce4,ce5,ce6 = st.columns([3,1.5,1,1.5,1.5,1])
                                        with ce1: c_desc = st.text_input("", value=str(comp.get("description","") or ""), label_visibility="collapsed")
                                        with ce2: c_type = st.selectbox("", ["Material","Labour","Other"], index=["Material","Labour","Other"].index(ctype) if ctype in ["Material","Labour","Other"] else 0, label_visibility="collapsed")
                                        with ce3: c_qty  = st.number_input("", value=qty, step=0.1, label_visibility="collapsed")
                                        with ce4: c_uom  = st.selectbox("", UOM_OPTIONS, index=UOM_OPTIONS.index(str(comp.get("uom","Ea"))) if str(comp.get("uom","Ea")) in UOM_OPTIONS else 0, label_visibility="collapsed")
                                        with ce5: c_uc   = st.number_input("", value=uc, step=0.1, label_visibility="collapsed")
                                        with ce6:
                                            if st.form_submit_button("💾"):
                                                execute("UPDATE catalogue_components SET description=?,item_type=?,qty=?,uom=?,unit_cost=? WHERE id=?",
                                                    (c_desc.strip(), c_type, c_qty, c_uom, c_uc, cmpid))
                                                # Recalculate parent totals
                                                all_comps = fetch_df("SELECT item_type, qty, unit_cost FROM catalogue_components WHERE item_id=?", (iid,))
                                                new_mat = sum(float(r["qty"] or 1)*float(r["unit_cost"] or 0) for _,r in all_comps.iterrows() if r["item_type"] != "Labour")
                                                new_lab = sum(float(r["qty"] or 1)*float(r["unit_cost"] or 0) for _,r in all_comps.iterrows() if r["item_type"] == "Labour")
                                                execute("UPDATE catalogue_items SET unit_cost=?, labour_cost=? WHERE id=?", (round(new_mat,2), round(new_lab,2), iid))
                                                st.session_state.pop("comp_editing", None); st.rerun()
                                else:
                                    rc1,rc2,rc3,rc4,rc5,rc6 = st.columns([3,1.5,1,1.5,1.5,1])
                                    col_c = "#64748b" if ctype == "Labour" else "#e2e8f0"
                                    rc1.markdown(f"<div style='font-size:13px;color:{col_c};padding:4px 0'>{comp.get('description','')}</div>", unsafe_allow_html=True)
                                    rc2.markdown(f"<div style='font-size:12px;color:{'#a78bfa' if ctype=='Labour' else '#7dd3fc'};padding:4px 0'>{ctype}</div>", unsafe_allow_html=True)
                                    rc3.markdown(f"<div style='font-size:13px;color:#94a3b8;padding:4px 0'>{qty:g}</div>", unsafe_allow_html=True)
                                    rc4.markdown(f"<div style='font-size:13px;color:#94a3b8;padding:4px 0'>{comp.get('uom','')}</div>", unsafe_allow_html=True)
                                    rc5.markdown(f"<div style='font-size:13px;color:#94a3b8;padding:4px 0'>${uc:,.2f}</div>", unsafe_allow_html=True)
                                    rc6.markdown(f"<div style='font-size:13px;color:#e2e8f0;font-weight:700;padding:4px 0'>${total_c:,.2f}</div>", unsafe_allow_html=True)
                                    with rc6:
                                        cc1,cc2 = st.columns(2)
                                        with cc1:
                                            if st.button("✏️", key=f"ced_{cmpid}"):
                                                st.session_state["comp_editing"] = cmpid; st.rerun()
                                        with cc2:
                                            if st.button("🗑", key=f"cdl_{cmpid}"):
                                                execute("DELETE FROM catalogue_components WHERE id=?", (cmpid,))
                                                # Recalc parent
                                                all_comps2 = fetch_df("SELECT item_type, qty, unit_cost FROM catalogue_components WHERE item_id=?", (iid,))
                                                nm = sum(float(r["qty"] or 1)*float(r["unit_cost"] or 0) for _,r in all_comps2.iterrows() if r["item_type"] != "Labour") if not all_comps2.empty else 0
                                                nl = sum(float(r["qty"] or 1)*float(r["unit_cost"] or 0) for _,r in all_comps2.iterrows() if r["item_type"] == "Labour") if not all_comps2.empty else 0
                                                execute("UPDATE catalogue_items SET unit_cost=?, labour_cost=? WHERE id=?", (round(nm,2), round(nl,2), iid))
                                                st.rerun()

                            # Totals row
                            st.markdown(f"""
                            <div style='display:flex;gap:20px;padding:8px 0;border-top:1px solid #2a3d4f;margin-top:4px'>
                                <span style='color:#7dd3fc;font-size:13px'>Material: <b>${mat_total:,.2f}</b></span>
                                <span style='color:#a78bfa;font-size:13px'>Labour: <b>${lab_total:,.2f}</b></span>
                                <span style='color:#2dd4bf;font-size:14px;font-weight:800;margin-left:auto'>Total: ${mat_total+lab_total:,.2f} per {item.get('uom','unit')}</span>
                            </div>""", unsafe_allow_html=True)

                        # Add component form
                        with st.expander("+ Add component", expanded=False):
                            with st.form(f"add_comp_{iid}"):
                                ac1,ac2,ac3,ac4,ac5 = st.columns([3,1.5,1,1.5,1.5])
                                with ac1: ac_desc = st.text_input("Description *", placeholder="e.g. Rafter Bracket")
                                with ac2: ac_type = st.selectbox("Type", ["Material","Labour","Other"])
                                with ac3: ac_qty  = st.number_input("Qty", value=1.0, step=0.1, min_value=0.0)
                                with ac4: ac_uom  = st.selectbox("UOM", UOM_OPTIONS)
                                with ac5: ac_uc   = st.number_input("Unit cost $", value=0.0, step=0.5, min_value=0.0)
                                if st.form_submit_button("Add component", type="primary"):
                                    if ac_desc.strip():
                                        execute("""INSERT INTO catalogue_components
                                            (item_id, description, item_type, qty, uom, unit_cost, sort_order)
                                            VALUES (?,?,?,?,?,?,?)""",
                                            (iid, ac_desc.strip(), ac_type, ac_qty, ac_uom, ac_uc,
                                             len(comps) if not comps.empty else 0))
                                        # Recalc parent totals
                                        all_comps3 = fetch_df("SELECT item_type, qty, unit_cost FROM catalogue_components WHERE item_id=?", (iid,))
                                        nm3 = sum(float(r["qty"] or 1)*float(r["unit_cost"] or 0) for _,r in all_comps3.iterrows() if r["item_type"] != "Labour")
                                        nl3 = sum(float(r["qty"] or 1)*float(r["unit_cost"] or 0) for _,r in all_comps3.iterrows() if r["item_type"] == "Labour")
                                        execute("UPDATE catalogue_items SET unit_cost=?, labour_cost=? WHERE id=?", (round(nm3,2), round(nl3,2), iid))
                                        st.rerun()

                        st.markdown("</div>", unsafe_allow_html=True)

    # ── TAB 2: IMPORT FROM BUILDXACT ──────────────────────────────────
    with cat_tabs[1]:
        st.markdown("### Import from Buildxact Export")
        st.markdown("""
        <div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:16px;margin-bottom:16px'>
            <div style='color:#2dd4bf;font-weight:700;margin-bottom:8px'>How to export from Buildxact:</div>
            <div style='color:#94a3b8;font-size:14px;line-height:1.8'>
                1. Go to Buildxact → Estimate → Export estimate items (.xlsx)<br>
                2. Upload that file here<br>
                3. All parent items and their components import automatically<br>
                4. Edit any prices or quantities in My Catalogue
            </div>
        </div>
        """, unsafe_allow_html=True)

        bx_file = st.file_uploader("Upload Buildxact export (.xlsx)", type=["xlsx","xls","csv"], key="bx_upload")

        if bx_file:
            try:
                import io as _io3
                if bx_file.name.endswith(".csv"):
                    bx_df = pd.read_csv(bx_file)
                else:
                    bx_df = pd.read_excel(bx_file)

                st.success(f"Found {len(bx_df)} rows")
                st.write("Preview:")
                st.dataframe(bx_df.head(5), hide_index=True)

                # Auto-detect Buildxact format
                has_displayed_order = "DisplayedOrder" in bx_df.columns
                has_description = "Description" in bx_df.columns

                if has_displayed_order and has_description:
                    st.success("✅ Buildxact format detected — ready to import!")

                    imp_mode = st.radio("Import mode",
                        ["Add new items only", "Replace all existing items"],
                        horizontal=True, key="bx_mode")

                    if st.button("📥 Import to My Catalogue", type="primary", key="bx_import"):
                        if imp_mode == "Replace all existing items":
                            execute("DELETE FROM catalogue_components")
                            execute("DELETE FROM catalogue_items")

                        # Parse hierarchy
                        bx_df["_level"] = bx_df["DisplayedOrder"].apply(
                            lambda x: len(str(x).split(".")) if pd.notna(x) else 0)
                        bx_df["_parent"] = bx_df["DisplayedOrder"].apply(
                            lambda x: ".".join(str(x).split(".")[:2]) if len(str(x).split(".")) > 2 else None)

                        parents_bx = bx_df[bx_df["_level"] == 2].copy()
                        comps_bx   = bx_df[bx_df["_level"] == 3].copy()

                        imported = 0
                        skipped  = 0
                        current_cat = ""

                        for _, prow in parents_bx.iterrows():
                            desc = str(prow.get("Description","") or "").strip()
                            if not desc: continue

                            # Get category from nearest category row above
                            cat = str(prow.get("CategoryDescription","") or current_cat or "General").strip()
                            if cat: current_cat = cat
                            uom = str(prow.get("UOM","lm") or "lm").strip()
                            unit_cost = float(pd.to_numeric(prow.get("UnitCost",0), errors="coerce") or 0)
                            order = str(prow.get("DisplayedOrder",""))

                            # Check exists
                            existing = fetch_df("SELECT id FROM catalogue_items WHERE description=? AND category=?", (desc, cat))
                            if not existing.empty and imp_mode == "Add new items only":
                                skipped += 1
                                continue

                            if not existing.empty:
                                item_id = int(existing.iloc[0]["id"])
                                execute("DELETE FROM catalogue_components WHERE item_id=?", (item_id,))
                            else:
                                execute("""INSERT INTO catalogue_items
                                    (category, description, uom, unit_cost, labour_cost, source, created_at)
                                    VALUES (?,?,?,?,?,?,?)""",
                                    (cat, desc, uom, 0, 0, "Buildxact", _today_aest().isoformat()))
                                item_id = fetch_df("SELECT id FROM catalogue_items WHERE description=? AND category=?", (desc, cat)).iloc[0]["id"]

                            # Import components
                            item_comps = comps_bx[comps_bx["_parent"] == order]
                            mat_total = 0
                            lab_total = 0
                            for sort_i, (_, crow) in enumerate(item_comps.iterrows()):
                                c_desc = str(crow.get("Description","") or "").strip()
                                if not c_desc: continue
                                c_qty  = float(pd.to_numeric(crow.get("Units",1), errors="coerce") or 1)
                                c_uom  = str(crow.get("UOM","Ea") or "Ea").strip()
                                c_uc   = float(pd.to_numeric(crow.get("UnitCost",0), errors="coerce") or 0)
                                c_type = "Labour" if "labour" in c_desc.lower() else "Material"
                                execute("""INSERT INTO catalogue_components
                                    (item_id, description, item_type, qty, uom, unit_cost, sort_order)
                                    VALUES (?,?,?,?,?,?,?)""",
                                    (item_id, c_desc, c_type, c_qty, c_uom, c_uc, sort_i))
                                if c_type == "Labour": lab_total += c_qty * c_uc
                                else: mat_total += c_qty * c_uc

                            # Update parent with calculated totals
                            execute("UPDATE catalogue_items SET unit_cost=?, labour_cost=? WHERE id=?",
                                (round(mat_total,2), round(lab_total,2), item_id))
                            imported += 1

                        st.success(f"✅ Imported {imported} items ({skipped} skipped). Go to My Catalogue to view and edit!")
                        st.rerun()
                else:
                    st.warning("Doesn't look like a Buildxact export. Make sure it has DisplayedOrder and Description columns.")

            except Exception as _bxe:
                st.error(f"Import error: {_bxe}")

    # ── TAB 3: ADD ITEM ────────────────────────────────────────────────
    with cat_tabs[2]:
        st.markdown("### Add New Item")
        try:
            add_cats = sorted(fetch_df("SELECT DISTINCT category FROM catalogue_items WHERE category != ''")["category"].tolist()) + ["+ New category"]
        except:
            add_cats = ["+ New category"]

        with st.form("add_cat_item"):
            ai1, ai2 = st.columns(2)
            with ai1:
                ai_cat_pick = st.selectbox("Category", add_cats)
                ai_cat_new  = st.text_input("New category name", placeholder="e.g. Plumbing")
                ai_desc = st.text_input("Description *", placeholder="e.g. Supply and Install 100mm Downpipe")
                ai_uom  = st.selectbox("UOM", UOM_OPTIONS)
            with ai2:
                ai_mat  = st.number_input("Material cost ($/UOM)", min_value=0.0, value=0.0, step=1.0)
                ai_lab  = st.number_input("Labour cost ($/UOM)",   min_value=0.0, value=0.0, step=1.0)
                st.caption("Add components via the recipe editor after creating the item.")
            if st.form_submit_button("Create Item", type="primary"):
                if ai_desc.strip():
                    ai_cat = ai_cat_new.strip() if ai_cat_pick == "+ New category" else ai_cat_pick
                    execute("""INSERT OR IGNORE INTO catalogue_items
                        (category,description,uom,unit_cost,labour_cost,source,created_at)
                        VALUES (?,?,?,?,?,?,?)""",
                        (ai_cat, ai_desc.strip(), ai_uom, ai_mat, ai_lab, "manual", _today_aest().isoformat()))
                    st.success(f"✅ Added! Open the item in My Catalogue to add components.")
                    st.rerun()
                else:
                    st.error("Description required.")

    # ── TAB 4: CUSTOM ITEMS (legacy) ──────────────────────────────────
    with cat_tabs[3]:
        st.caption("These are your original custom items. Use My Catalogue for new items.")
        custom_df = fetch_df("SELECT * FROM custom_catalogue ORDER BY category, description")
        if custom_df.empty:
            st.info("No custom items.")
        else:
            st.dataframe(custom_df[["category","description","uom","material_cost","labour_cost","sell_unit_rate"]],
                hide_index=True, width="stretch")


# ─────────────────────────────────────────────
#  PAGE: JOBS
# ─────────────────────────────────────────────
elif page == "Jobs":

    STAGES = ["Lead", "Quoted", "Handover", "Live Job", "Completed"]
    STAGE_COLORS = {
        "Lead":        ("#1e3a5f", "#7dd3fc"),
        "Quoted":      ("#2d1f0d", "#f59e0b"),
        "Handover":    ("#2a1a2e", "#a78bfa"),
        "Live Job":    ("#0d2a1f", "#2dd4bf"),
        "Completed":   ("#1a2d1a", "#4ade80"),
        "Performance": ("#1f1a2d", "#c084fc"),
        "Quote Builder": ("#1f1a0d", "#fbbf24"),
    }

    # ── Session state: which job is open ────────────────────────────────
    open_job = st.session_state.get("open_job")

    # ── If a job is open → show workspace ───────────────────────────────
    if open_job:
        wjob = fetch_df("SELECT * FROM jobs WHERE job_id=?", (open_job,))
        if wjob.empty:
            st.session_state.pop("open_job", None); st.rerun()
        wjob = wjob.iloc[0].to_dict()

        # Back button + parent job link for variations
        back_col, parent_col = st.columns([1,4])
        with back_col:
            if st.button("← All Jobs"):
                st.session_state.pop("open_job", None); st.rerun()
        with parent_col:
            if int(wjob.get("is_variation",0) or 0) == 1:
                parent = str(wjob.get("parent_job","") or "")
                var_title = str(wjob.get("variation_title","") or "")
                if parent:
                    st.markdown(
                        "<div style='background:#2a1f0d;border:1px solid #f59e0b;border-radius:8px;"
                        "padding:8px 14px;font-size:14px;color:#f59e0b'>"
                        "⚠️ Variation of <strong>" + parent + "</strong>"
                        + (" — " + var_title if var_title else "") +
                        " &nbsp;<span style='cursor:pointer'></span></div>",
                        unsafe_allow_html=True)
                    if st.button("← Back to " + parent, key="back_to_parent"):
                        st.session_state["open_job"] = parent
                        st.rerun()

        # Job header — using columns for Python 3.14 compatibility
        sc, tc  = STAGE_COLORS.get(wjob.get("stage",""), ("#1e2d3d","#94a3b8"))
        jt      = str(wjob.get("job_type","") or "Residential")
        jt_col  = TYPE_COLORS.get(jt, "#64748b")

        hdr_html = (
            "<div style='background:" + sc + ";border:1px solid #2a3d4f;border-radius:12px;"
            "padding:16px 20px;margin-bottom:1.2rem'>"
            "<div style='display:flex;align-items:center;gap:12px;flex-wrap:wrap'>"
            "<div style='flex:1;min-width:200px'>"
            "<div style='font-size:22px;font-weight:800;color:#f1f5f9'>"
            + str(wjob.get('job_id','')) +
            "<span style='font-size:13px;font-weight:500;color:#94a3b8;margin-left:10px'>"
            + str(wjob.get('client','') or '') + "</span></div>"
            "<div style='font-size:13px;color:#64748b;margin-top:3px'>"
            + str(wjob.get('address','') or '') + " &nbsp;·&nbsp; Estimator: "
            + str(wjob.get('estimator','') or '—') + "</div></div>"
            "<span style='background:" + jt_col + "22;color:" + jt_col + ";padding:4px 12px;"
            "border-radius:999px;font-size:13px;font-weight:700'>" + jt + "</span>"
            "<span style='background:" + tc + "22;color:" + tc + ";padding:5px 14px;"
            "border-radius:999px;font-size:13px;font-weight:700;text-transform:uppercase'>"
            + str(wjob.get('stage','') or '') + "</span>"
            "</div></div>"
        )
        st.markdown(hdr_html, unsafe_allow_html=True)

        # Workspace tabs
        wtab1, wtab2, wtab3, wtab4, wtab5, wtab6, wtab7, wtab8, wtab9, wtab10, wtab11, wtab12 = st.tabs([
            "Overview", "Quote Builder", "Tender Review", "Handover", "Labour", "Materials",
            "Performance", "Documents", "Financials", "Site Diary", "Photos", "Safety"
        ])

        # ── TAB 1: Overview ──────────────────────────────────────────────
        with wtab1:
            wlab = labour_metrics(open_job)
            wmat = material_metrics(open_job)
            wlp  = profit_metrics(open_job)

            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Sell Price",       f"${wlp.get('sell_price',0):,.0f}")
            c2.metric("Projected Labour", f"${wlab.get('projected_labour',0):,.0f}")
            c3.metric("Material Spend",   f"${wmat.get('actual_material',0):,.0f}")
            c4.metric("Live Profit %",    f"{wlp.get('live_profit_pct',0)*100:.1f}%")

            st.divider()
            oc1, oc2 = st.columns(2)
            with oc1:
                st.subheader("Edit job details")
                with st.form("job_edit_form"):
                    f_cl   = st.text_input("Client",    value=wjob.get("client",""))
                    f_addr = st.text_input("Address",   value=wjob.get("address",""))
                    f_est  = st.text_input("Estimator", value=wjob.get("estimator",""))
                    f_stg  = st.selectbox("Stage", STAGES,
                                index=STAGES.index(wjob["stage"]) if wjob.get("stage") in STAGES else 0)
                    cur_jt = str(wjob.get("job_type","") or "Residential")
                    f_type = st.selectbox("Job type", JOB_TYPES,
                                index=JOB_TYPES.index(cur_jt) if cur_jt in JOB_TYPES else 0)
                    if st.form_submit_button("Save", type="primary"):
                        upsert_job(open_job, f_cl, f_addr, f_est, f_stg)
                        execute("UPDATE jobs SET job_type=? WHERE job_id=?", (f_type, open_job))
                        st.success("Saved."); st.rerun()
            with oc2:
                st.subheader("Danger zone")
                st.caption("Archiving hides the job from all lists. All data is preserved.")
                if st.button("Archive this job"):
                    st.session_state["confirm_arc"] = open_job
                if st.session_state.get("confirm_arc") == open_job:
                    st.warning(f"Archive {open_job}? This can be undone from the Jobs page.")
                    b1,b2 = st.columns(2)
                    with b1:
                        if st.button("Yes, archive", type="primary", key="arch_confirm"):
                            execute("UPDATE jobs SET archived=1 WHERE job_id=?", (open_job,))
                            st.session_state.pop("confirm_arc",None)
                            st.session_state.pop("open_job",None)
                            st.success("Archived."); st.rerun()
                    with b2:
                        if st.button("Cancel", key="arch_cancel"):
                            st.session_state.pop("confirm_arc",None); st.rerun()

        # ── TAB 2: Quote Builder scan sheet ───────────────────────────────────
        with wtab2:
            st.subheader("Quote Builder — Scan Sheet")
            st.caption("Every catalogue item is preloaded at 0. Enter a qty to include it in the quote. Edit material and labour rates per line as needed.")

            # ── Job Finish selector ───────────────────────────────────────
            finishes_df  = fetch_df("SELECT finish_name FROM material_finishes ORDER BY sort_order")
            finish_opts  = finishes_df["finish_name"].tolist() if not finishes_df.empty else ["Steel","MATT","ULTRA","Aluminium","VM Zinc"]
            cur_finish   = str(wjob.get("job_finish","") or "Steel")
            fin1, fin2   = st.columns([2,4])
            with fin1:
                sel_finish = st.selectbox(
                    "🎨 Job Finish",
                    finish_opts,
                    index=finish_opts.index(cur_finish) if cur_finish in finish_opts else 0,
                    key=f"job_finish_{open_job}",
                    help="Sets the default finish for all items. Override per line if needed."
                )
                if sel_finish != cur_finish:
                    execute("UPDATE jobs SET job_finish=? WHERE job_id=?", (sel_finish, open_job))
                    st.session_state.pop(f"scan_{open_job}", None)
                    st.success(f"Finish set to {sel_finish}"); st.rerun()
            with fin2:
                fc_map = {"Steel":"#94a3b8","MATT":"#2dd4bf","ULTRA":"#a78bfa",
                          "Aluminium":"#7dd3fc","VM Zinc":"#f59e0b","Copper":"#fb923c","Zincalume":"#4ade80"}
                fc = fc_map.get(sel_finish,"#64748b")
                st.markdown(
                    f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;"
                    f"border-left:4px solid {fc};border-radius:8px;padding:8px 14px;"
                    f"font-size:13px;color:#94a3b8;margin-top:4px'>"
                    f"All items will use <strong style='color:{fc}'>{sel_finish}</strong> rates. "
                    f"Override individual lines below if needed.</div>",
                    unsafe_allow_html=True)
            st.divider()

            try:
                cat = load_catalogue()
            except FileNotFoundError:
                st.error(f"Catalogue not found: {CATALOGUE_PATH.name}"); st.stop()
            except ImportError:
                st.error("Missing dependency — run: pip install openpyxl"); st.stop()
            except Exception as _cat_err:
                st.error(f"Catalogue error: {_cat_err}"); st.stop()

            # Load saved estimate lines for this job
            saved_est = get_estimate(open_job)
            saved_map = {}
            if not saved_est.empty:
                for _, sr in saved_est.iterrows():
                    saved_map[str(sr["Item"])] = {
                        "qty": float(sr["Qty"]),
                        "mat": float(sr["Material Rate"]),
                        "lab": float(sr["Labour Rate"]),
                    }

            sections = sorted([s for s in cat["Category"].unique() if s.strip()])

            # Session state for scan sheet
            ss_key = f"scan_{open_job}"
            if ss_key not in st.session_state:
                # Initialise from saved estimate or defaults
                scan_data = {}
                for _, row in cat.iterrows():
                    item = str(row["Description"])
                    sv   = saved_map.get(item, {})
                    scan_data[item] = {
                        "section": str(row["Category"]),
                        "uom":     str(row["UOM"]),
                        "qty":     sv.get("qty", 0.0),
                        "mat":     sv.get("mat", float(row["MaterialCost"])),
                        "lab":     sv.get("lab", float(row["LabourCost"])),
                    }
                st.session_state[ss_key] = scan_data
            scan = st.session_state[ss_key]

            # Running totals — read live from session state keys so numbers update without rerun
            def _get_live(item, field, default):
                k = f"{field}_{open_job}_{item}"
                return float(st.session_state[k]) if k in st.session_state else float(scan[item][field.replace("qty","qty").replace("mat","mat").replace("lab","lab")])

            live_lines = {}
            for item, v in scan.items():
                _raw_q = st.session_state.get(f"qty_{open_job}_{item}", v["qty"])
                try: q = float(_raw_q) if str(_raw_q).strip() else 0.0
                except: q = 0.0
                m = float(st.session_state.get(f"mat_{open_job}_{item}", v["mat"]))
                l = float(st.session_state.get(f"lab_{open_job}_{item}", v["lab"]))
                scan[item]["qty"] = q
                scan[item]["mat"] = m
                scan[item]["lab"] = l
                if q > 0:
                    live_lines[item] = {"section":v["section"],"uom":v["uom"],"qty":q,"mat":m,"lab":l}

            active_lines = live_lines
            mat_run = sum(v["qty"]*v["mat"] for v in active_lines.values())
            lab_run = sum(v["qty"]*v["lab"] for v in active_lines.values())
            tot_run = mat_run + lab_run

            # Totals banner
            t1,t2,t3,t4 = st.columns(4)
            t1.metric("Active lines",   len(active_lines))
            t2.metric("Material",       f"${mat_run:,.2f}")
            t3.metric("Labour",         f"${lab_run:,.2f}")
            t4.metric("Cost total",     f"${tot_run:,.2f}")

            st.divider()

            # Markup + save row — read from session state so it persists across reruns
            markup_ss_key = f"markup_{open_job}"
            if markup_ss_key not in st.session_state:
                # Pre-fill from saved job margin if available
                saved_tp = float(wjob.get("tender_profit_pct") or 0)
                st.session_state[markup_ss_key] = round(saved_tp * 100 / (1 - saved_tp), 1) if saved_tp and saved_tp < 1 else 20.0

            mc1, mc2, mc3 = st.columns([2,2,2])
            with mc1:
                markup = st.number_input("Markup %", min_value=0.0, step=1.0, key=markup_ss_key)
            sell_calc = tot_run * (1 + markup/100)
            gp        = sell_calc - tot_run
            margin    = (gp/sell_calc*100) if sell_calc else 0
            with mc2:
                st.metric("Sell price", f"${sell_calc:,.2f}")
            with mc3:
                st.metric("Gross margin", f"{margin:.1f}%")

            # Apply Recipe button
            if st.button("Apply Recipe", key=f"apply_recipe_{open_job}"):
                st.session_state[f"show_recipe_modal_{open_job}"] = True

            if st.session_state.get(f"show_recipe_modal_{open_job}"):
                recipes_df = fetch_df("SELECT id, name, unit_measure FROM recipes ORDER BY name")
                if recipes_df.empty:
                    st.warning("No recipes yet — go to the Recipes page to build your first one.")
                    st.session_state.pop(f"show_recipe_modal_{open_job}", None)
                else:
                    with st.form(f"recipe_apply_{open_job}"):
                        st.subheader("Apply Recipe")
                        r_names = recipes_df["name"].tolist()
                        r_pick  = st.selectbox("Select recipe", r_names)
                        r_row   = recipes_df[recipes_df["name"]==r_pick].iloc[0]
                        r_id    = int(r_row["id"])
                        r_uom   = r_row["unit_measure"]
                        r_meas  = st.number_input(
                            f"Enter measurement ({r_uom})",
                            min_value=0.0, value=0.0, step=1.0
                        )
                        ra1, ra2 = st.columns(2)
                        with ra1: apply = st.form_submit_button("Apply to scan sheet", type="primary")
                        with ra2: cancel_r = st.form_submit_button("Cancel")

                        if apply and r_meas > 0:
                            r_items = fetch_df(
                                "SELECT * FROM recipe_items WHERE recipe_id=? ORDER BY sort_order, id",
                                (r_id,)
                            )
                            ss_key = f"scan_{open_job}"
                            if ss_key not in st.session_state:
                                st.session_state[ss_key] = {}
                            for _, ri in r_items.iterrows():
                                calc_qty = float(ri["unit_qty"]) * float(r_meas)
                                item_key = str(ri["description"])
                                # If item already in scan sheet, update qty; else add it
                                if item_key in st.session_state[ss_key]:
                                    st.session_state[ss_key][item_key]["qty"] += calc_qty
                                else:
                                    st.session_state[ss_key][item_key] = {
                                        "section":       r_pick,
                                        "uom":           str(ri["uom"]),
                                        "qty":           calc_qty,
                                        "mat":           float(ri["material_rate"]),
                                        "lab":           float(ri["labour_rate"]),
                                    }
                            st.session_state.pop(f"show_recipe_modal_{open_job}", None)
                            st.success(f"Recipe '{r_pick}' applied — {len(r_items)} lines added at {r_meas} {r_uom}.")
                            st.rerun()
                        if cancel_r:
                            st.session_state.pop(f"show_recipe_modal_{open_job}", None)
                            st.rerun()

            st.markdown("</div>", unsafe_allow_html=True)
            if st.button("Save estimate to job", type="primary", key=f"save_est_{open_job}"):
                execute("DELETE FROM estimate_lines WHERE job_id=?", (open_job,))
                tp = (gp/sell_calc) if sell_calc else 0
                for item, v in active_lines.items():
                    execute("""
                        INSERT INTO estimate_lines
                            (job_id,section,item,uom,qty,material_rate,labour_rate,
                             material_cost,labour_cost,total_cost)
                        VALUES (?,?,?,?,?,?,?,?,?,?)
                    """, (
                        open_job, v["section"], item, v["uom"],
                        v["qty"], v["mat"], v["lab"],
                        v["qty"]*v["mat"], v["qty"]*v["lab"],
                        v["qty"]*(v["mat"]+v["lab"]),
                    ))
                execute("""
                    UPDATE jobs SET sell_price=?,
                        tender_material_budget=?,
                        tender_labour_budget=?,
                        tender_profit_pct=?
                    WHERE job_id=?
                """, (sell_calc, mat_run, lab_run, tp, open_job))
                st.success(f"Estimate saved — {len(active_lines)} lines, ${sell_calc:,.2f} sell price.")
                st.session_state.pop(ss_key, None)
                st.rerun()

            st.divider()

            # Rate edit toggle — once above scan sheet
            edit_rates = st.session_state.get(f"edit_rates_{open_job}", False)
            rc1, rc2 = st.columns([1,5])
            with rc1:
                if st.button("🔓 Lock rates" if edit_rates else "🔒 Edit rates",
                             key=f"toggle_rates_{open_job}"):
                    st.session_state[f"edit_rates_{open_job}"] = not edit_rates
                    st.rerun()
            with rc2:
                if not edit_rates:
                    st.markdown("<div style='font-size:14px;color:#475569;padding:8px 0'>Rates locked — tab through quantities only. Click Edit rates to change material/labour rates.</div>", unsafe_allow_html=True)
                else:
                    st.markdown("<div style='font-size:14px;color:#f59e0b;padding:8px 0'>⚠️ Rate editing ON — rates are now editable.</div>", unsafe_allow_html=True)

            # CSS + JS for row highlight on focus
            st.markdown("""
            <style>
            /* Make qty inputs bigger and more visible */
            div[data-testid="stNumberInput"] input {
                font-size: 16px !important;
                font-weight: 700 !important;
                text-align: center !important;
                background: #111c27 !important;
                border: 1px solid #2a3d4f !important;
                border-radius: 6px !important;
                color: #e2e8f0 !important;
                height: 42px !important;
            }
            div[data-testid="stNumberInput"] input:focus {
                border: 2px solid #2dd4bf !important;
                background: #0d2233 !important;
                color: #2dd4bf !important;
            }
            /* Force text selection on focus */
            .qb-section div[data-testid="stNumberInput"] input {
                user-select: all !important;
                -webkit-user-select: all !important;
            }
            /* Hover highlight — target stHorizontalBlock rows */
            .qb-section .stHorizontalBlock {
                border-radius: 6px !important;
                border: 1px solid transparent !important;
                border-left: 3px solid transparent !important;
                padding: 2px 4px !important;
                margin-bottom: 3px !important;
                transition: all 0.08s !important;
                cursor: default !important;
            }
            .qb-section .stHorizontalBlock:hover {
                background: #0d2233 !important;
                border-color: #2dd4bf !important;
                border-left: 3px solid #2dd4bf !important;
                box-shadow: 0 0 0 1px #2dd4bf44 !important;
            }
            .qb-section .stHorizontalBlock:hover p,
            .qb-section .stHorizontalBlock:hover div {
                color: #ffffff !important;
            }
            .qb-section .stHorizontalBlock:focus-within {
                background: #0d2233 !important;
                border-color: #2dd4bf !important;
                border-left: 3px solid #2dd4bf !important;
                box-shadow: 0 0 0 2px #2dd4bf66 !important;
            }
            .qb-section .stHorizontalBlock:focus-within p,
            .qb-section .stHorizontalBlock:focus-within div {
                color: #ffffff !important;
            }
            /* Row highlight on focus */
            .qb-row-focused {
                background: #0d2233 !important;
                border-color: #2dd4bf !important;
                border-left-color: #2dd4bf !important;
            }
            </style>
            <script>
            // Highlight parent row div when input is focused
            function setupRowHighlight() {
                document.querySelectorAll('[data-testid="stNumberInput"] input').forEach(input => {
                    input.addEventListener('focus', function() {
                        let el = this;
                        for (let i = 0; i < 15; i++) {
                            el = el.parentElement;
                            if (!el) break;
                            if (el.id && el.id.startsWith('qb_row_')) {
                                el.style.background = '#0d2233';
                                el.style.borderColor = '#2dd4bf';
                                el.style.borderLeftColor = '#2dd4bf';
                                el.style.borderLeftWidth = '3px';
                                el.style.boxShadow = '0 0 0 1px #2dd4bf44';
                                // Highlight description text
                                el.querySelectorAll('div').forEach(d => {
                                    if (d.children.length === 0 && d.textContent.trim().length > 3) {
                                        d.style.color = '#ffffff';
                                        d.style.fontWeight = '800';
                                        d.dataset.wasHighlighted = 'true';
                                    }
                                });
                                break;
                            }
                        }
                    });
                    input.addEventListener('blur', function() {
                        let el = this;
                        for (let i = 0; i < 15; i++) {
                            el = el.parentElement;
                            if (!el) break;
                            if (el.id && el.id.startsWith('qb_row_')) {
                                const inputEl = el.querySelector('input');
                                const val = inputEl ? parseFloat(inputEl.value) : 0;
                                el.style.boxShadow = '';
                                if (val > 0) {
                                    el.style.background = '#162a3a';
                                    el.style.borderColor = '#2dd4bf';
                                    el.querySelectorAll('div').forEach(d => {
                                        if (d.dataset.wasHighlighted) {
                                            d.style.color = '#2dd4bf';
                                            d.style.fontWeight = '700';
                                        }
                                    });
                                } else {
                                    el.style.background = '#0f1923';
                                    el.style.borderColor = '#1e2d3d';
                                    el.style.borderLeftColor = '#1e2d3d';
                                    el.querySelectorAll('div').forEach(d => {
                                        if (d.dataset.wasHighlighted) {
                                            d.style.color = '#94a3b8';
                                            d.style.fontWeight = '400';
                                            delete d.dataset.wasHighlighted;
                                        }
                                    });
                                }
                                break;
                            }
                        }
                    });
                });
            }
            // Run on load and after Streamlit rerenders
            setupRowHighlight();
            setTimeout(setupRowHighlight, 500);
            setTimeout(setupRowHighlight, 1500);
            // Also observe DOM changes (Streamlit rerenders)
            new MutationObserver(() => setTimeout(setupRowHighlight, 200))
                .observe(document.body, {childList: true, subtree: true});

            // Select all text on click/focus so typing replaces value
            function setupSelectAll() {
                document.querySelectorAll('.qb-section input[type="number"]').forEach(input => {
                    if (!input.dataset.selectAllSetup) {
                        input.dataset.selectAllSetup = 'true';
                        input.addEventListener('focus', function() {
                            this.select();
                        });
                        input.addEventListener('click', function() {
                            this.select();
                        });
                    }
                });
            }
            setupSelectAll();
            setTimeout(setupSelectAll, 500);
            setTimeout(setupSelectAll, 1500);
            new MutationObserver(() => setTimeout(setupSelectAll, 200))
                .observe(document.body, {childList: true, subtree: true});
            </script>
            <style>
            /* When row is focused — make description text pop */
            .qb-row-active div[data-testid="stMarkdownContainer"] div {
                color: #ffffff !important;
                font-weight: 800 !important;
            }
            </style>
            """, unsafe_allow_html=True)

            st.divider()

            # Section groups
            st.markdown("<div class='qb-section'>", unsafe_allow_html=True)
            for section in sections:
                sec_items = {k:v for k,v in scan.items() if v["section"]==section}
                sec_active = sum(1 for v in sec_items.values() if v["qty"]>0)
                sec_total  = sum(v["qty"]*(v["mat"]+v["lab"]) for v in sec_items.values())

                with st.expander(
                    f"**{section}** — {sec_active} active lines  |  ${sec_total:,.2f}",
                    expanded=sec_active > 0
                ):
                    # Column headers
                    # Column headers — read edit_rates from session state
                    edit_rates = st.session_state.get(f"edit_rates_{open_job}", False)
                    hc_cols = [4, 1, 1.5, 1.2, 1.2] if edit_rates else [5, 1, 1.5]
                    hc = st.columns(hc_cols)
                    hc[0].markdown("<div style='color:#64748b;font-size:14px;font-weight:700;padding:4px 0'>Description</div>", unsafe_allow_html=True)
                    hc[1].markdown("<div style='color:#64748b;font-size:14px;font-weight:700;text-align:center'>UOM</div>", unsafe_allow_html=True)
                    hc[2].markdown("<div style='color:#64748b;font-size:14px;font-weight:700;text-align:center'>Qty</div>", unsafe_allow_html=True)
                    if edit_rates:
                        hc[3].markdown("<div style='color:#f59e0b;font-size:14px;font-weight:700;text-align:center'>Mat $</div>", unsafe_allow_html=True)
                        hc[4].markdown("<div style='color:#f59e0b;font-size:14px;font-weight:700;text-align:center'>Lab $</div>", unsafe_allow_html=True)

                    for _row_idx, (item, v) in enumerate(sec_items.items()):
                        is_active = v["qty"] > 0
                        _row_id = f"qb_row_{open_job}_{section}_{_row_idx}".replace(" ","_")
                        row_bg = "#162a3a" if is_active else "#0f1923"
                        row_border = "#2dd4bf" if is_active else "#1e2d3d"
                        st.markdown(
                            f"<div id='{_row_id}' style='background:{row_bg};"
                            f"border:1px solid {row_border};"
                            f"border-left:3px solid {'#2dd4bf' if is_active else '#1e2d3d'};"
                            f"border-radius:6px;padding:4px 8px;margin-bottom:3px;"
                            f"transition:all 0.1s'>",
                            unsafe_allow_html=True)

                        cols = st.columns(hc_cols)
                        with cols[0]:
                            label_color = "#2dd4bf" if is_active else "#94a3b8"
                            weight = "700" if is_active else "400"
                            st.markdown(
                                "<div style='font-size:16px;color:" + label_color + ";padding:8px 0;font-weight:" + weight + ";line-height:1.3'>" + str(item) + "</div>",
                                unsafe_allow_html=True)
                        with cols[1]:
                            st.markdown(
                                "<div style='font-size:15px;color:#64748b;padding:10px 0;text-align:center'>" + str(v['uom']) + "</div>",
                                unsafe_allow_html=True)
                        # Qty — always editable, tabable
                        with cols[2]:
                            _qty_val = float(v["qty"])
                            _qty_display = "" if _qty_val == 0 else str(int(_qty_val)) if _qty_val == int(_qty_val) else str(_qty_val)
                            _qty_input = st.text_input("Qty",
                                value=_qty_display,
                                key=f"qty_{open_job}_{item}",
                                label_visibility="collapsed",
                                placeholder="0",
                                help="Type qty and tab")
                            try:
                                new_qty = float(_qty_input) if _qty_input.strip() else 0.0
                            except:
                                new_qty = _qty_val
                            scan[item]["qty"] = new_qty

                        # Rates — only editable in edit mode
                        if edit_rates:
                            with cols[3]:
                                new_mat = st.number_input("Mat $",
                                    min_value=0.0,
                                    value=float(v["mat"]),
                                    step=0.5,
                                    key=f"mat_{open_job}_{item}",
                                    label_visibility="collapsed")
                                scan[item]["mat"] = new_mat
                            with cols[4]:
                                new_lab = st.number_input("Lab $",
                                    min_value=0.0,
                                    value=float(v["lab"]),
                                    step=0.5,
                                    key=f"lab_{open_job}_{item}",
                                    label_visibility="collapsed")
                                scan[item]["lab"] = new_lab
                        else:
                            # Show rates as read-only text
                            total_rate = float(v["mat"]) + float(v["lab"])
                            pass  # rates shown in section summary only

                        st.markdown("</div>", unsafe_allow_html=True)

        # ── TAB 3: Tender Review — Commercial Analysis ──────────────────
        with wtab3:
            wlab2 = labour_metrics(open_job)
            wmat2 = material_metrics(open_job)
            wlp2  = profit_metrics(open_job)
            sell2  = float(wlp2.get("sell_price",0) or 0)
            mat2   = float(wmat2.get("tender_material",0) or 0)
            lab2   = float(wlab2.get("tender_labour",0) or 0)

            # Get overhead from company settings
            _cs2        = get_company_settings()
            _ovhd_pct2  = float(_cs2.get("overhead_pct",11.0) or 11.0)
            _markup_def = float(_cs2.get("markup_default",30.0) or 30.0)

            # Other costs (crane, access etc) — anything not mat or lab
            w_est2      = get_estimate(open_job)
            other2 = 0.0
            if not w_est2.empty:
                for _, er in w_est2.iterrows():
                    sec = str(er.get("Section","")).lower()
                    if any(x in sec for x in ["crane","access","hire","scaffold","other"]):
                        other2 += float(er.get("Material Cost",0) or 0) + float(er.get("Labour Cost",0) or 0)

            direct_cost     = mat2 + lab2 + other2
            running_cost    = sell2 * (_ovhd_pct2/100)
            total_cost      = direct_cost + running_cost
            profit_before   = sell2 - total_cost
            tax             = max(profit_before * 0.25, 0)
            true_profit     = profit_before - tax
            true_margin     = (true_profit / sell2 * 100) if sell2 else 0
            gross_margin    = sell2 - direct_cost

            mat_pct   = (mat2/direct_cost*100)   if direct_cost else 0
            lab_pct   = (lab2/direct_cost*100)   if direct_cost else 0
            other_pct = (other2/direct_cost*100) if direct_cost else 0

            # Margin category
            if true_margin >= 20:   margin_cat = "High Margin"
            elif true_margin >= 15: margin_cat = "Safe"
            elif true_margin >= 12: margin_cat = "Competitive"
            else:                   margin_cat = "Aggressive"

            cat_colors = {
                "Aggressive": "#f43f5e",
                "Competitive": "#f59e0b",
                "Safe": "#2dd4bf",
                "High Margin": "#4ade80"
            }
            cat_c = cat_colors.get(margin_cat, "#64748b")

            # Auto risk detection
            risks = []
            if sell2 == 0:
                risks.append({"level":"HIGH","title":"No estimate saved",
                    "detail":"Run Quote Builder and save estimate before tender review."})
            if true_margin < 10:
                risks.append({"level":"HIGH","title":"Margin below 10%",
                    "detail":f"True margin of {true_margin:.1f}% is below minimum acceptable threshold."})
            elif true_margin < 12:
                risks.append({"level":"MEDIUM","title":"Aggressive margin",
                    "detail":f"True margin of {true_margin:.1f}% leaves little room for cost overruns."})
            if lab_pct > 55:
                risks.append({"level":"MEDIUM","title":"Labour-heavy job",
                    "detail":f"Labour is {lab_pct:.0f}% of direct cost. Labour productivity is critical."})
            if mat_pct > 60:
                risks.append({"level":"MEDIUM","title":"Material-heavy job",
                    "detail":f"Materials are {mat_pct:.0f}% of direct cost. Confirm supplier pricing is current."})
            if other2 > 0:
                risks.append({"level":"MEDIUM","title":"Access/crane costs present",
                    "detail":f"${other2:,.0f} in access/crane/other costs. Confirm quotes are current."})
            if not risks:
                risks.append({"level":"LOW","title":"No significant risks detected",
                    "detail":"Job is within acceptable parameters. Standard project management applies."})

            # Auto commentary
            commentary = (
                f"This tender represents a {wjob.get('job_type','').lower()} project "
                f"at {wjob.get('address','the project site')}. "
                f"The quoted price of ${sell2:,.2f} (excl. GST) returns a true after-tax margin of {true_margin:.2f}%, "
                f"placing this tender in the {margin_cat.lower()} range. "
            )
            if lab_pct > mat_pct:
                commentary += f"The cost structure is labour-dominant at {lab_pct:.0f}% of direct costs, indicating a technically involved installation. "
            else:
                commentary += f"The cost structure is material-dominant at {mat_pct:.0f}% of direct costs. "
            if true_margin < 15:
                commentary += "Close management of labour productivity will be important to protect the margin achieved at tender stage."
            else:
                commentary += "The margin provides reasonable contingency for normal project variations."

            analysis = {
                "sell": sell2, "mat": mat2, "lab": lab2, "other": other2,
                "direct_cost": direct_cost, "running_cost": running_cost,
                "total_cost": total_cost, "profit_before_tax": profit_before,
                "tax": tax, "true_profit": true_profit, "true_margin": true_margin,
                "gross_margin": gross_margin, "mat_pct": mat_pct, "lab_pct": lab_pct,
                "other_pct": other_pct, "margin_category": margin_cat,
                "overhead_pct": _ovhd_pct2, "risks": risks, "commentary": commentary,
            }

            if sell2 == 0:
                st.warning("⚠️ No estimate saved yet — run Quote Builder and save estimate first.")

            # ── Job Snapshot ──────────────────────────────────────────────
            st.markdown("<div style='font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#2dd4bf;margin-bottom:10px'>1. Job Snapshot</div>", unsafe_allow_html=True)

            sc1,sc2,sc3,sc4 = st.columns(4)
            sc1.metric("Quoted Price (ex GST)", f"${sell2:,.2f}")
            sc2.metric("Direct Job Cost",        f"${direct_cost:,.2f}")
            sc3.metric("TRUE Profit After Tax",  f"${true_profit:,.2f}")
            sc4.metric("True Margin",            f"{true_margin:.2f}%")

            snap_rows = [
                ("Quoted Price (EX GST)",  f"${sell2:,.2f}",       "#e2e8f0"),
                ("Direct Job Cost",        f"${direct_cost:,.2f}",  "#e2e8f0"),
                (f"Running Costs ({_ovhd_pct2:.0f}%)", f"${running_cost:,.2f}", "#94a3b8"),
                ("Total Cost to Business", f"${total_cost:,.2f}",   "#e2e8f0"),
                ("Profit Before Tax",      f"${profit_before:,.2f}","#f59e0b"),
                ("Company Tax (25%)",      f"${tax:,.2f}",          "#94a3b8"),
                ("TRUE PROFIT After Tax",  f"${true_profit:,.2f}",  "#2dd4bf"),
                ("True Profit Margin",     f"{true_margin:.2f}%",   cat_c),
            ]
            for label, val, color in snap_rows:
                is_last = label == "True Profit Margin"
                bg = "#0d2233" if is_last else "#1e2d3d"
                st.markdown(
                    "<div style='background:" + bg + ";border:1px solid #2a3d4f;"
                    "display:flex;justify-content:space-between;align-items:center;"
                    "padding:10px 16px;margin-bottom:3px;border-radius:8px'>"
                    "<span style='font-size:15px;color:#94a3b8'>" + label + "</span>"
                    "<span style='font-size:16px;font-weight:700;color:" + color + "'>" + val + "</span>"
                    "</div>",
                    unsafe_allow_html=True)

            st.divider()

            # ── Cost Structure ─────────────────────────────────────────────
            st.markdown("<div style='font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#2dd4bf;margin-bottom:10px'>2. Cost Structure</div>", unsafe_allow_html=True)
            cc1,cc2,cc3 = st.columns(3)
            for col, label, val, pct, color in [
                (cc1, "Material",  mat2,   mat_pct,   "#f59e0b"),
                (cc2, "Labour",    lab2,   lab_pct,   "#7dd3fc"),
                (cc3, "Other",     other2, other_pct, "#a78bfa"),
            ]:
                col.markdown(
                    "<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:14px;text-align:center'>"
                    "<div style='font-size:13px;color:#64748b;text-transform:uppercase;letter-spacing:.1em;margin-bottom:6px'>" + label + "</div>"
                    "<div style='font-size:22px;font-weight:800;color:" + color + "'>$" + f"{val:,.0f}" + "</div>"
                    "<div style='font-size:13px;color:#475569;margin-top:4px'>" + f"{pct:.0f}%" + " of direct cost</div>"
                    "<div style='background:#0f172a;border-radius:4px;height:6px;margin-top:8px'>"
                    "<div style='background:" + color + ";width:" + f"{min(pct,100):.0f}" + "%;height:6px;border-radius:4px'></div>"
                    "</div></div>",
                    unsafe_allow_html=True)

            st.divider()

            # ── Margin Category ────────────────────────────────────────────
            st.markdown("<div style='font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#2dd4bf;margin-bottom:10px'>3. Commercial Margin Analysis</div>", unsafe_allow_html=True)

            st.markdown(
                "<div style='background:" + cat_c + "22;border:2px solid " + cat_c + ";border-radius:12px;"
                "padding:16px 20px;text-align:center;margin-bottom:12px'>"
                "<div style='font-size:13px;color:" + cat_c + ";text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px'>Margin Category</div>"
                "<div style='font-size:36px;font-weight:900;color:" + cat_c + "'>" + margin_cat + "</div>"
                "<div style='font-size:14px;color:#64748b;margin-top:4px'>" + f"{true_margin:.2f}% true after-tax margin" + "</div>"
                "</div>",
                unsafe_allow_html=True)

            # Scale reference
            scale_cols = st.columns(4)
            for col, cat, rng, c in [
                (scale_cols[0], "Aggressive",  "10–12%", "#f43f5e"),
                (scale_cols[1], "Competitive", "12–15%", "#f59e0b"),
                (scale_cols[2], "Safe",        "15–18%", "#2dd4bf"),
                (scale_cols[3], "High Margin", "20%+",   "#4ade80"),
            ]:
                is_active = cat == margin_cat
                col.markdown(
                    "<div style='background:" + (c+"22" if is_active else "#1e2d3d") + ";border:" +
                    ("2px solid " + c if is_active else "1px solid #2a3d4f") + ";border-radius:8px;"
                    "padding:10px;text-align:center'>"
                    "<div style='font-size:13px;font-weight:700;color:" + c + "'>" + rng + "</div>"
                    "<div style='font-size:13px;color:#e2e8f0;font-weight:600'>" + cat + "</div>"
                    "</div>",
                    unsafe_allow_html=True)

            st.divider()

            # ── Risk Indicators ────────────────────────────────────────────
            st.markdown("<div style='font-size:13px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:#2dd4bf;margin-bottom:10px'>4. Risk Indicators</div>", unsafe_allow_html=True)

            for risk in risks:
                rc = {"HIGH":"#f43f5e","MEDIUM":"#f59e0b","LOW":"#2dd4bf"}.get(risk["level"],"#64748b")
                st.markdown(
                    "<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-left:4px solid " + rc + ";"
                    "border-radius:9px;padding:12px 16px;margin-bottom:8px;display:flex;align-items:center;gap:14px'>"
                    "<span style='background:" + rc + ";color:#0f172a;font-weight:800;font-size:13px;"
                    "padding:3px 8px;border-radius:4px;flex-shrink:0'>" + risk["level"] + "</span>"
                    "<div><div style='font-size:15px;font-weight:600;color:#e2e8f0'>" + risk["title"] + "</div>"
                    "<div style='font-size:13px;color:#64748b;margin-top:2px'>" + risk["detail"] + "</div>"
                    "</div></div>",
                    unsafe_allow_html=True)

            st.divider()

            # ── Actions ────────────────────────────────────────────────────
            act1, act2 = st.columns(2)
            with act1:
                if st.button("Generate Tender Analysis PDF", type="primary"):
                    try:
                        pdf_buf = generate_tender_analysis_pdf(wjob, analysis)
                        st.download_button("⬇ Download Analysis PDF",
                            data=pdf_buf,
                            file_name=f"TenderAnalysis_{open_job}_{date.today().isoformat()}.pdf",
                            mime="application/pdf",
                            key="tender_pdf_dl")
                    except Exception as e:
                        st.error(f"PDF error: {e}")
            with act2:
                if st.button("Mark as reviewed — ready to send", type="secondary"):
                    upsert_job(open_job, wjob.get("client",""), wjob.get("address",""),
                               wjob.get("estimator",""), "Tender Review")
                    st.success("Job marked as Tender Review."); st.rerun()

        # ── TAB 4: Handover ──────────────────────────────────────────────
        with wtab4:
            wlab3 = labour_metrics(open_job)
            wlp3  = profit_metrics(open_job)
            lb3   = wlab3.get("tender_labour",0)
            mb3   = material_metrics(open_job).get("tender_material",0)

            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Sell Price",      f"${wlp3.get('sell_price',0):,.0f}")
            c2.metric("Labour Budget",   f"${lb3:,.0f}")
            c3.metric("Material Budget", f"${mb3:,.0f}")
            c4.metric("Tender Profit %", f"{wlp3.get('tender_profit_pct',0)*100:.1f}%")
            st.divider()

            emp_ho = fetch_df("SELECT name FROM employees WHERE active=1 ORDER BY name")["name"].tolist()

            htab1, htab2 = st.tabs(["📋 Handover Details", "📄 Generate Handover PDF"])

            with htab1:
                with st.form("handover_f"):
                    hc1,hc2 = st.columns(2)
                    with hc1:
                        h_crew  = st.selectbox("Leading hand", emp_ho if emp_ho else [""])
                        h_days  = st.number_input("Days allowed", min_value=1, value=5, step=1)
                        h_start = st.date_input("Start date", value=date.today(), format="DD/MM/YYYY")
                    with hc2:
                        h_daily = lb3/h_days if h_days else 0
                        st.metric("Daily labour target", f"${h_daily:,.0f}")
                        h_risks = st.text_area("Site risks / Safety notes", placeholder="e.g. Steep roof, harness required. SWMS: refer to site folder.", height=80)
                        h_access = st.text_area("Site access details", placeholder="e.g. Key with PM. Gate code: 1234. Park on street.", height=60)
                    h_notes = st.text_area("Special instructions", placeholder="e.g. Materials booked for Day 1. Client wants daily updates.", height=80)
                    sc1, sc2 = st.columns(2)
                    with sc1:
                        save_notes_only = st.form_submit_button("💾 Save Notes")
                    with sc2:
                        pass
                    if save_notes_only:
                        execute("UPDATE jobs SET handover_crew=?, handover_notes=?, handover_risks=?, handover_access=? WHERE job_id=?",
                            (h_crew, h_notes, h_risks, h_access, open_job))
                        st.success("✅ Notes saved!")
                        st.rerun()
                    if st.form_submit_button("✅ Complete Handover → Live Job", type="primary"):
                        upsert_job(open_job, wjob.get("client",""), wjob.get("address",""),
                                   wjob.get("estimator",""), "Live Job")
                        for bd in pd.bdate_range(h_start, periods=h_days):
                            execute(
                                "INSERT INTO day_assignments (job_id,client,employee,date,note) VALUES (?,?,?,?,?)",
                                (open_job, wjob.get("client",""), h_crew,
                                 bd.date().isoformat(), h_notes or "Handover block"),
                            )
                        # Save handover details to job
                        try:
                            execute("ALTER TABLE jobs ADD COLUMN handover_crew TEXT DEFAULT ''")
                        except: pass
                        try:
                            execute("ALTER TABLE jobs ADD COLUMN handover_notes TEXT DEFAULT ''")
                        except: pass
                        try:
                            execute("ALTER TABLE jobs ADD COLUMN handover_risks TEXT DEFAULT ''")
                        except: pass
                        try:
                            execute("ALTER TABLE jobs ADD COLUMN handover_access TEXT DEFAULT ''")
                        except: pass
                        execute("UPDATE jobs SET handover_crew=?, handover_notes=?, handover_risks=?, handover_access=? WHERE job_id=?",
                            (h_crew, h_notes, h_risks, h_access, open_job))
                        st.success(f"Job is Live. {h_days} days created for {h_crew}."); st.rerun()

            with htab2:
                st.markdown("### 📄 Handover Pack PDF")
                st.caption("Generates a professional handover document for the leading hand — auto-saves to Documents.")

                # Load saved handover details
                ho_saved = fetch_df("SELECT * FROM jobs WHERE job_id=?", (open_job,))
                ho_data = ho_saved.iloc[0].to_dict() if not ho_saved.empty else {}

                # Load scope from estimate lines
                scope_df = fetch_df("""
                    SELECT section, item, qty, uom, material_cost, labour_cost
                    FROM estimate_lines WHERE job_id=? ORDER BY section, id
                """, (open_job,))

                # Load materials from estimate lines with recipe component breakdown
                # First get the estimate lines
                est_lines = fetch_df("""
                    SELECT el.section, el.item, el.qty, el.uom,
                           el.material_cost, el.labour_cost
                    FROM estimate_lines el
                    WHERE el.job_id=? AND el.material_cost > 0
                    ORDER BY el.section, el.item
                """, (open_job,))

                # Build materials list — each line item with its total cost
                if not est_lines.empty:
                    mat_rows_data = []
                    for _, el in est_lines.iterrows():
                        qty = float(el.get("qty",0) or 0)
                        mat_cost = float(el.get("material_cost",0) or 0)
                        total = round(qty * mat_cost, 2)
                        mat_rows_data.append({
                            "section": str(el.get("section","") or "General"),
                            "item": str(el.get("item","") or ""),
                            "qty": qty,
                            "uom": str(el.get("uom","") or ""),
                            "unit_cost": mat_cost,
                            "total_cost": total
                        })
                    import pandas as _pd2
                    mats_df = _pd2.DataFrame(mat_rows_data)
                else:
                    import pandas as _pd2
                    mats_df = _pd2.DataFrame()

                # Preview
                col_prev, col_gen = st.columns(2)
                with col_prev:
                    st.markdown(f"""
                    <div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:16px'>
                        <div style='color:#2dd4bf;font-weight:700;margin-bottom:8px'>HANDOVER PACK PREVIEW</div>
                        <div style='color:#94a3b8;font-size:13px'>
                            <b style='color:#e2e8f0'>Job:</b> {open_job}<br>
                            <b style='color:#e2e8f0'>Client:</b> {wjob.get('client','—')}<br>
                            <b style='color:#e2e8f0'>Address:</b> {wjob.get('address','—')}<br>
                            <b style='color:#e2e8f0'>Leading Hand:</b> {ho_data.get('handover_crew','—')}<br>
                            <b style='color:#e2e8f0'>Scope items:</b> {len(scope_df)} line items<br>
                            <b style='color:#e2e8f0'>Material items:</b> {len(mats_df)} components<br>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)

                with col_gen:
                    if st.button("📄 Generate & Save Handover PDF", type="primary", use_container_width=True):
                        try:
                            from reportlab.lib.pagesizes import A4
                            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
                            from reportlab.lib.units import mm
                            from reportlab.lib import colors
                            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
                            from reportlab.lib.enums import TA_LEFT, TA_CENTER
                            import io as _io

                            buf = _io.BytesIO()
                            doc = SimpleDocTemplate(buf, pagesize=A4,
                                topMargin=15*mm, bottomMargin=15*mm,
                                leftMargin=15*mm, rightMargin=15*mm)

                            styles = getSampleStyleSheet()
                            TEAL   = colors.HexColor("#2dd4bf")
                            DARK   = colors.HexColor("#0f172a")
                            SLATE  = colors.HexColor("#475569")
                            WHITE  = colors.white

                            h1 = ParagraphStyle("h1", fontSize=20, fontName="Helvetica-Bold", textColor=TEAL, spaceAfter=2*mm)
                            h2 = ParagraphStyle("h2", fontSize=12, fontName="Helvetica-Bold", textColor=DARK, spaceBefore=5*mm, spaceAfter=2*mm)
                            body = ParagraphStyle("body", fontSize=9, fontName="Helvetica", textColor=DARK, spaceAfter=1.5*mm, leading=13)
                            label = ParagraphStyle("label", fontSize=8, fontName="Helvetica-Bold", textColor=SLATE)

                            story = []

                            # Header
                            settings_pdf = fetch_df("SELECT * FROM company_settings WHERE id=1")
                            co_name = settings_pdf.iloc[0].get("company_name","Limitless") if not settings_pdf.empty else "Limitless"
                            story.append(Paragraph(co_name, h1))
                            story.append(Paragraph(f"SITE HANDOVER PACK — {open_job}", ParagraphStyle("sub", fontSize=14, fontName="Helvetica-Bold", textColor=DARK)))
                            story.append(HRFlowable(width="100%", thickness=2, color=TEAL, spaceAfter=4*mm))

                            # Job details
                            story.append(Paragraph("JOB DETAILS", h2))
                            details = [
                                ["Job Number:", open_job, "Client:", wjob.get("client","—")],
                                ["Address:", wjob.get("address","—"), "Estimator:", wjob.get("estimator","—")],
                                ["Leading Hand:", ho_data.get("handover_crew","—"), "Date:", date.today().strftime("%-d %b %Y")],
                            ]
                            t = Table(details, colWidths=[35*mm, 65*mm, 35*mm, 55*mm])
                            t.setStyle(TableStyle([
                                ("FONTNAME", (0,0),(-1,-1), "Helvetica"),
                                ("FONTSIZE", (0,0),(-1,-1), 9),
                                ("FONTNAME", (0,0),(0,-1), "Helvetica-Bold"),
                                ("FONTNAME", (2,0),(2,-1), "Helvetica-Bold"),
                                ("TEXTCOLOR", (0,0),(0,-1), SLATE),
                                ("TEXTCOLOR", (2,0),(2,-1), SLATE),
                                ("BOTTOMPADDING", (0,0),(-1,-1), 3),
                            ]))
                            story.append(t)
                            story.append(Spacer(1, 4*mm))

                            # Site access
                            if ho_data.get("handover_access","").strip():
                                story.append(HRFlowable(width="100%", thickness=0.5, color=SLATE))
                                story.append(Paragraph("SITE ACCESS", h2))
                                story.append(Paragraph(str(ho_data.get("handover_access","")), body))

                            # Safety / SWMS
                            if ho_data.get("handover_risks","").strip():
                                story.append(HRFlowable(width="100%", thickness=0.5, color=SLATE))
                                story.append(Paragraph("SAFETY REQUIREMENTS & SWMS", h2))
                                story.append(Paragraph(str(ho_data.get("handover_risks","")), body))

                            # Special instructions
                            if ho_data.get("handover_notes","").strip():
                                story.append(HRFlowable(width="100%", thickness=0.5, color=SLATE))
                                story.append(Paragraph("SPECIAL INSTRUCTIONS", h2))
                                story.append(Paragraph(str(ho_data.get("handover_notes","")), body))

                            # Scope of works
                            story.append(HRFlowable(width="100%", thickness=0.5, color=SLATE))
                            story.append(Paragraph("SCOPE OF WORKS", h2))
                            if not scope_df.empty:
                                current_section = None
                                scope_rows = [["Item", "Qty", "UOM"]]
                                for _, row in scope_df.iterrows():
                                    sec = str(row.get("section","") or "General")
                                    if sec != current_section:
                                        scope_rows.append([f"— {sec} —", "", ""])
                                        current_section = sec
                                    scope_rows.append([
                                        str(row.get("item",""))[:60],
                                        f"{float(row.get('qty',0)):,.2f}",
                                        str(row.get("uom",""))
                                    ])
                                st_table = Table(scope_rows, colWidths=[130*mm, 25*mm, 25*mm])
                                st_table.setStyle(TableStyle([
                                    ("FONTNAME", (0,0),(-1,0), "Helvetica-Bold"),
                                    ("FONTSIZE", (0,0),(-1,-1), 8),
                                    ("BACKGROUND", (0,0),(-1,0), TEAL),
                                    ("TEXTCOLOR", (0,0),(-1,0), WHITE),
                                    ("ROWBACKGROUNDS", (0,1),(-1,-1), [colors.HexColor("#f8fafc"), WHITE]),
                                    ("GRID", (0,0),(-1,-1), 0.3, colors.HexColor("#e2e8f0")),
                                    ("PADDING", (0,0),(-1,-1), 3),
                                    ("FONTNAME", (0,1),(-1,-1), "Helvetica"),
                                ]))
                                story.append(st_table)
                            else:
                                story.append(Paragraph("No scope items found — add items in Quote Builder.", body))

                            # Materials list
                            # Build component breakdown from catalogue (same as Supplier PO)
                            comp_map_ho = {}
                            try:
                                import pandas as _pdho
                                raw_df_ho  = _pdho.read_excel(CATALOGUE_PATH, sheet_name="Raw_Original")
                                raw_lkp_ho = {str(r["DisplayedOrder"]).strip(): float(r["Units"] or 1)
                                              for _, r in raw_df_ho.iterrows()}
                                comp_df_ho = _pdho.read_excel(CATALOGUE_PATH, sheet_name="Component_Breakdown", header=3)
                                for _, cr in comp_df_ho.iterrows():
                                    parent = str(cr.get("ParentDescription","") or "").strip()
                                    ctype  = str(cr.get("ComponentType","") or "").strip()
                                    if ctype == "Labour": continue
                                    cdesc  = str(cr.get("ComponentDescription","") or "").strip()
                                    cuom   = str(cr.get("UOM","") or "").strip()
                                    cord   = str(cr.get("ComponentDisplayedOrder","") or "").strip()
                                    unit_qty = raw_lkp_ho.get(cord, 1.0)
                                    if parent and cdesc:
                                        if parent not in comp_map_ho:
                                            comp_map_ho[parent] = []
                                        comp_map_ho[parent].append({"desc": cdesc, "uom": cuom, "unit_qty": unit_qty})
                            except:
                                comp_map_ho = {}

                            # Build materials list from estimate lines + components
                            ho_mat_rows = [["Section", "Component / Material", "Qty", "UOM"]]
                            ho_mat_count = 0

                            # Load estimate lines for this job
                            ho_est = fetch_df("SELECT section, item, qty FROM estimate_lines WHERE job_id=? ORDER BY section, id", (open_job,))

                            # Group by section
                            ho_sections = {}
                            for _, el in ho_est.iterrows():
                                sec = str(el.get("section","") or "General")
                                item = str(el.get("item","") or "").strip()
                                job_qty = float(el.get("qty",0) or 0)
                                if sec not in ho_sections:
                                    ho_sections[sec] = {}
                                comps = comp_map_ho.get(item, [])
                                if comps:
                                    for comp in comps:
                                        key = comp["desc"] + "||" + comp["uom"]
                                        if key not in ho_sections[sec]:
                                            ho_sections[sec][key] = {"desc": comp["desc"], "uom": comp["uom"], "qty": 0}
                                        ho_sections[sec][key]["qty"] += round(comp["unit_qty"] * job_qty, 2)
                                else:
                                    # No components — show the item itself
                                    key = item + "||"
                                    if key not in ho_sections[sec]:
                                        ho_sections[sec][key] = {"desc": item, "uom": "", "qty": 0}
                                    ho_sections[sec][key]["qty"] += job_qty

                            sec_header_idx = []
                            for sec, comps in ho_sections.items():
                                sec_header_idx.append(len(ho_mat_rows))
                                ho_mat_rows.append([f"— {sec} —", "", "", ""])
                                for comp_data in comps.values():
                                    ho_mat_rows.append([
                                        "", str(comp_data["desc"])[:55],
                                        f"{comp_data['qty']:,.2f}",
                                        str(comp_data["uom"])
                                    ])
                                    ho_mat_count += 1

                            if ho_mat_count > 0:
                                story.append(Spacer(1, 4*mm))
                                story.append(HRFlowable(width="100%", thickness=0.5, color=SLATE))
                                story.append(Paragraph("MATERIALS LIST", h2))
                                mt = Table(ho_mat_rows, colWidths=[35*mm, 105*mm, 25*mm, 25*mm])
                                ts = [
                                    ("FONTNAME", (0,0),(-1,0), "Helvetica-Bold"),
                                    ("FONTSIZE", (0,0),(-1,-1), 8),
                                    ("BACKGROUND", (0,0),(-1,0), TEAL),
                                    ("TEXTCOLOR", (0,0),(-1,0), WHITE),
                                    ("ROWBACKGROUNDS", (0,1),(-1,-1), [colors.HexColor("#f8fafc"), WHITE]),
                                    ("GRID", (0,0),(-1,-1), 0.3, colors.HexColor("#e2e8f0")),
                                    ("PADDING", (0,0),(-1,-1), 3),
                                ]
                                # Style section headers
                                for idx in sec_header_idx:
                                    ts += [
                                        ("BACKGROUND", (0,idx),(-1,idx), colors.HexColor("#1e2d3d")),
                                        ("TEXTCOLOR", (0,idx),(-1,idx), TEAL),
                                        ("FONTNAME", (0,idx),(-1,idx), "Helvetica-Bold"),
                                        ("SPAN", (0,idx),(-1,idx)),
                                    ]
                                mt.setStyle(TableStyle(ts))
                                story.append(mt)

                            # Sign off
                            story.append(Spacer(1, 8*mm))
                            story.append(HRFlowable(width="100%", thickness=0.5, color=SLATE))
                            story.append(Paragraph("SIGN OFF", h2))
                            sign_data = [
                                ["Leading Hand:", "_" * 40, "Date:", "_" * 20],
                                ["Supervisor:", "_" * 40, "Date:", "_" * 20],
                            ]
                            st2 = Table(sign_data, colWidths=[30*mm, 70*mm, 20*mm, 60*mm])
                            st2.setStyle(TableStyle([
                                ("FONTNAME", (0,0),(-1,-1), "Helvetica"),
                                ("FONTSIZE", (0,0),(-1,-1), 9),
                                ("FONTNAME", (0,0),(0,-1), "Helvetica-Bold"),
                                ("FONTNAME", (2,0),(2,-1), "Helvetica-Bold"),
                                ("BOTTOMPADDING", (0,0),(-1,-1), 6),
                                ("TOPPADDING", (0,0),(-1,-1), 8),
                            ]))
                            story.append(st2)

                            doc.build(story)
                            pdf_bytes = buf.getvalue()

                            # Save to job files
                            fname = f"Handover_{open_job}_{date.today().isoformat()}.pdf"
                            execute("""INSERT INTO job_files (job_id, filename, filetype, filedata, uploaded_at)
                                VALUES (?,?,?,?,?)""",
                                (open_job, fname, "application/pdf", pdf_bytes, date.today().isoformat()))

                            # Download button
                            st.download_button("⬇️ Download Handover PDF", data=pdf_bytes,
                                file_name=fname, mime="application/pdf", type="primary")
                            st.success("✅ PDF generated and saved to Documents tab!")

                        except Exception as _he:
                            st.error(f"PDF error: {_he}")

        # ── TAB 5: Labour ────────────────────────────────────────────────
        with wtab5:
            wlog = fetch_df("""
                SELECT id,work_date,employee,hours,hourly_rate,
                       ROUND(hours*hourly_rate,2)AS cost,note
                FROM labour_logs WHERE job_id=? ORDER BY work_date
            """, (open_job,))

            if not wlog.empty:
                st.metric("Total logged labour", f"${(wlog['hours']*wlog['hourly_rate']).sum():,.2f}")

                for _, lr in wlog.iterrows():
                    lid = int(lr["id"])
                    lc1, lc2, lc3 = st.columns([5,1,1])
                    with lc1:
                        st.markdown(
                            "<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:8px;"
                            "padding:10px 16px;margin-bottom:4px;display:flex;gap:16px;align-items:center'>"
                            "<span style='color:#2dd4bf;font-weight:700;min-width:90px'>" + _fmt_date(str(lr["work_date"])) + "</span>"
                            "<span style='color:#e2e8f0'>" + str(lr["employee"]) + "</span>"
                            "<span style='color:#64748b'>" + f"{float(lr['hours']):.1f}h @ ${float(lr['hourly_rate']):.0f}/hr" + "</span>"
                            "<span style='color:#2dd4bf;font-weight:700;margin-left:auto'>$" + f"{float(lr['cost']):,.2f}" + "</span>"
                            + (f"<span style='color:#475569;font-size:13px'>{lr['note']}</span>" if lr.get('note') else "") +
                            "</div>", unsafe_allow_html=True)
                    with lc2:
                        if st.button("✏️", key=f"edit_lab_{lid}", help="Edit"):
                            st.session_state[f"editing_lab"] = lid
                            st.rerun()
                    with lc3:
                        if st.button("🗑", key=f"del_lab_{lid}", help="Delete"):
                            execute("DELETE FROM labour_logs WHERE id=?", (lid,))
                            st.rerun()

                    # Inline edit form
                    if st.session_state.get("editing_lab") == lid:
                        with st.form(f"edit_lab_{lid}"):
                            el1, el2, el3 = st.columns(3)
                            with el1:
                                e_date = st.text_input("Date", value=str(lr["work_date"]))
                                e_emp  = st.text_input("Employee", value=str(lr["employee"]))
                            with el2:
                                e_hrs  = st.number_input("Hours", value=float(lr["hours"]), step=0.5)
                                e_rate = st.number_input("Rate ($/hr)", value=float(lr["hourly_rate"]), step=5.0)
                            with el3:
                                e_note = st.text_input("Note", value=str(lr.get("note","") or ""))
                                st.metric("Cost", f"${e_hrs*e_rate:,.2f}")
                            es1, es2 = st.columns(2)
                            with es1:
                                if st.form_submit_button("💾 Save", type="primary"):
                                    execute("UPDATE labour_logs SET work_date=?,employee=?,hours=?,hourly_rate=?,note=? WHERE id=?",
                                        (e_date, e_emp, e_hrs, e_rate, e_note, lid))
                                    st.session_state.pop("editing_lab", None)
                                    st.rerun()
                            with es2:
                                if st.form_submit_button("Cancel"):
                                    st.session_state.pop("editing_lab", None)
                                    st.rerun()
            else:
                st.info("No labour entries yet.")

            st.divider()
            emp_lab     = fetch_df("SELECT name, hourly_rate FROM employees WHERE active=1 ORDER BY name")
            emp_names   = emp_lab["name"].tolist() if not emp_lab.empty else []
            emp_rates   = {r["name"]: float(r["hourly_rate"] or 0) for _, r in emp_lab.iterrows()}

            if emp_names:
                # Toggle between single and bulk entry
                lab_mode = st.radio("Entry mode", ["Single day", "Date range (bulk)"],
                                    horizontal=True, key=f"lab_mode_{open_job}")

                if lab_mode == "Single day":
                    with st.form("lab_f"):
                        la1,la2,la3 = st.columns(3)
                        with la1:
                            lf_date = st.date_input("Date", value=date.today(), format="DD/MM/YYYY")
                            lf_emp  = st.selectbox("Employee", emp_names)
                        with la2:
                            default_rate = emp_rates.get(emp_names[0], 225.0)
                            lf_hrs  = st.number_input("Hours", min_value=0.0, value=8.0, step=0.5)
                            lf_rate = st.number_input("Rate ($/hr)", min_value=0.0, value=default_rate, step=5.0)
                        with la3:
                            lf_note = st.text_input("Note", value="")
                            st.metric("Cost", f"${lf_hrs*lf_rate:,.2f}")
                        if st.form_submit_button("Add entry", type="primary"):
                            execute(
                                "INSERT INTO labour_logs (work_date,job_id,employee,hours,hourly_rate,note) VALUES (?,?,?,?,?,?)",
                                (lf_date.isoformat(), open_job, lf_emp, lf_hrs, lf_rate, lf_note),
                            )
                            st.success("Added."); st.rerun()

                else:
                    # Bulk date range entry
                    with st.form("lab_bulk_f"):
                        st.subheader("Bulk labour entry")
                        bl1, bl2 = st.columns(2)
                        with bl1:
                            bl_emp   = st.selectbox("Employee", emp_names)
                            bl_from  = st.date_input("From date", value=date.today())
                            bl_to    = st.date_input("To date",   value=date.today())
                        with bl2:
                            default_rate = emp_rates.get(emp_names[0], 225.0)
                            bl_hrs   = st.number_input("Hours per day", min_value=0.0, value=8.0, step=0.5)
                            bl_rate  = st.number_input("Rate ($/hr)", min_value=0.0, value=default_rate, step=5.0)
                            bl_note  = st.text_input("Note (applies to all days)")
                        # Day of week selector
                        st.markdown("**Include which days:**")
                        dc = st.columns(7)
                        day_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
                        day_checks = []
                        for i,d in enumerate(day_names):
                            with dc[i]:
                                day_checks.append(st.checkbox(d, value=(i<5), key=f"bl_day_{i}_{open_job}"))

                        if st.form_submit_button("Log all days", type="primary"):
                            if bl_from > bl_to:
                                st.error("From date must be before To date.")
                            else:
                                import datetime as _dt
                                current = bl_from
                                logged  = 0
                                while current <= bl_to:
                                    if day_checks[current.weekday()]:
                                        execute(
                                            "INSERT INTO labour_logs (work_date,job_id,employee,hours,hourly_rate,note) VALUES (?,?,?,?,?,?)",
                                            (current.isoformat(), open_job, bl_emp, bl_hrs, bl_rate, bl_note),
                                        )
                                        logged += 1
                                    current += _dt.timedelta(days=1)
                                total_cost = logged * bl_hrs * bl_rate
                                st.success(f"✅ Logged {logged} days for {bl_emp} — ${total_cost:,.2f} total")
                                st.rerun()

        # ── TAB 6: Materials ─────────────────────────────────────────────
        with wtab6:
            winv = fetch_df("""
                SELECT id,invoice_date,supplier,invoice_number,amount,status,note
                FROM material_invoices WHERE job_id=? ORDER BY invoice_date
            """, (open_job,))

            if not winv.empty:
                st.metric("Total material spend", f"${winv['amount'].sum():,.2f}")
                # Show each invoice with delete button
                for _, inv in winv.iterrows():
                    inv_id = int(inv["id"])
                    ic1, ic2 = st.columns([8,1])
                    with ic1:
                        st.markdown(
                            "<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:8px;"
                            "padding:10px 16px;margin-bottom:6px;display:flex;gap:20px;align-items:center'>"
                            "<span style='color:#2dd4bf;font-weight:700;font-size:15px'>$" + f"{float(inv['amount']):,.2f}" + "</span>"
                            "<span style='color:#e2e8f0;font-size:14px'>" + str(inv.get('supplier','') or '') + "</span>"
                            "<span style='color:#64748b;font-size:13px'>#" + str(inv.get('invoice_number','') or '') + "</span>"
                            "<span style='color:#64748b;font-size:13px'>" + str(inv.get('invoice_date','') or '') + "</span>"
                            "<span style='background:#1a2d3a;color:#94a3b8;font-size:13px;padding:2px 8px;border-radius:4px'>" + str(inv.get('status','') or '') + "</span>"
                            "</div>",
                            unsafe_allow_html=True)
                    with ic2:
                        if st.button("🗑", key=f"del_inv_{inv_id}", help="Delete invoice"):
                            execute("DELETE FROM material_invoices WHERE id=?", (inv_id,))
                            st.rerun()
            else:
                st.info("No invoices yet.")

            st.divider()
            SINV = ["Entered","Approved","Paid"]

            # ── AI Invoice Scanner ────────────────────────────────────────
            st.subheader("📸 AI Invoice Scanner")
            st.caption("Upload a supplier invoice photo or PDF — AI reads it automatically.")

            ai_upload = st.file_uploader(
                "Upload invoice",
                type=["jpg","jpeg","png","pdf","webp"],
                key=f"ai_inv_{open_job}"
            )

            if ai_upload:
                import base64 as _b64
                import json as _json
                import urllib.request as _urlreq

                with st.spinner("🤖 Reading invoice..."):
                    try:
                        file_bytes = ai_upload.read()
                        file_type  = ai_upload.type
                        b64_data   = _b64.b64encode(file_bytes).decode()

                        extract_prompt = """Extract invoice details and return ONLY valid JSON:
{
  "supplier": "company name",
  "invoice_number": "invoice number or null",
  "invoice_date": "YYYY-MM-DD or null",
  "amount_ex_gst": 0.00,
  "gst_amount": 0.00,
  "amount_inc_gst": 0.00,
  "notes": "any notes or null"
}
No explanation, only JSON."""

                        if "pdf" in file_type:
                            content = [
                                {"type":"document","source":{"type":"base64","media_type":"application/pdf","data":b64_data}},
                                {"type":"text","text":extract_prompt}
                            ]
                        else:
                            content = [
                                {"type":"image","source":{"type":"base64","media_type":file_type,"data":b64_data}},
                                {"type":"text","text":extract_prompt}
                            ]

                        payload = _json.dumps({
                            "model":      "claude-opus-4-5",
                            "max_tokens": 400,
                            "messages":   [{"role":"user","content":content}]
                        }).encode()

                        req = _urlreq.Request(
                            "https://api.anthropic.com/v1/messages",
                            data=payload,
                            headers={"Content-Type":"application/json","anthropic-version":"2023-06-01"},
                            method="POST"
                        )
                        with _urlreq.urlopen(req, timeout=30) as resp:
                            result = _json.loads(resp.read().decode())

                        raw = result["content"][0]["text"].strip()
                        if "```" in raw:
                            raw = raw.split("```")[1]
                            if raw.startswith("json"): raw = raw[4:]
                        extracted = _json.loads(raw.strip())
                        st.session_state[f"ai_inv_{open_job}_data"] = extracted
                        st.success("✅ Invoice read!")

                    except Exception as e:
                        st.error(f"Could not read invoice: {e}")
                        st.info("Add manually below.")

            # Confirmation form
            if f"ai_inv_{open_job}_data" in st.session_state:
                ex = st.session_state[f"ai_inv_{open_job}_data"]
                st.markdown(f"""
                <div style="background:#0d2233;border:2px solid #2dd4bf;border-radius:12px;
                    padding:14px 18px;margin:8px 0">
                    <div style="font-size:13px;font-weight:700;color:#2dd4bf;
                        text-transform:uppercase;letter-spacing:.1em;margin-bottom:10px">
                        🤖 AI extracted — review before saving
                    </div>
                    <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;font-size:13px">
                        <div><div style="color:#64748b">Supplier</div>
                            <div style="color:#e2e8f0;font-weight:600">{ex.get('supplier','—')}</div></div>
                        <div><div style="color:#64748b">Invoice #</div>
                            <div style="color:#e2e8f0;font-weight:600">{ex.get('invoice_number','—')}</div></div>
                        <div><div style="color:#64748b">Date</div>
                            <div style="color:#e2e8f0;font-weight:600">{ex.get('invoice_date','—')}</div></div>
                        <div><div style="color:#64748b">Excl. GST</div>
                            <div style="color:#2dd4bf;font-weight:700">${float(ex.get('amount_ex_gst') or 0):,.2f}</div></div>
                        <div><div style="color:#64748b">GST</div>
                            <div style="color:#f59e0b;font-weight:700">${float(ex.get('gst_amount') or 0):,.2f}</div></div>
                        <div><div style="color:#64748b">Incl. GST</div>
                            <div style="color:#e2e8f0;font-weight:700">${float(ex.get('amount_inc_gst') or 0):,.2f}</div></div>
                    </div>
                </div>""", unsafe_allow_html=True)

                with st.form("ai_confirm_inv"):
                    cc1,cc2,cc3 = st.columns(3)
                    try:
                        ai_d = date.fromisoformat(str(ex.get("invoice_date","") or "")) if ex.get("invoice_date") else date.today()
                    except: ai_d = date.today()
                    with cc1:
                        c_date = st.date_input("Date",       value=ai_d)
                        c_supp = st.text_input("Supplier",   value=str(ex.get("supplier","") or ""))
                    with cc2:
                        c_num  = st.text_input("Invoice #",  value=str(ex.get("invoice_number","") or ""))
                        c_amt  = st.number_input("Amount ($)", min_value=0.0,
                                    value=float(ex.get("amount_ex_gst") or 0), step=1.0)
                    with cc3:
                        c_stat = st.selectbox("Status", SINV)
                        c_note = st.text_input("Note", value=str(ex.get("notes","") or ""))
                    cb1,cb2 = st.columns(2)
                    with cb1:
                        if st.form_submit_button("✅ Confirm & Save", type="primary"):
                            execute(
                                "INSERT INTO material_invoices (invoice_date,job_id,supplier,invoice_number,amount,status,note) VALUES (?,?,?,?,?,?,?)",
                                (c_date.isoformat(),open_job,c_supp,c_num,c_amt,c_stat,c_note))
                            st.session_state.pop(f"ai_inv_{open_job}_data",None)
                            st.success(f"Saved — ${c_amt:,.2f} from {c_supp}"); st.rerun()
                    with cb2:
                        if st.form_submit_button("✗ Discard"):
                            st.session_state.pop(f"ai_inv_{open_job}_data",None); st.rerun()

            st.divider()
            st.subheader("Add manually")
            with st.form("inv_f"):
                ia1,ia2,ia3 = st.columns(3)
                with ia1:
                    if_date = st.date_input("Date",      value=date.today())
                    if_supp = st.text_input("Supplier")
                with ia2:
                    if_num  = st.text_input("Invoice #")
                    if_amt  = st.number_input("Amount ($)", min_value=0.0, value=0.0, step=100.0)
                with ia3:
                    if_stat = st.selectbox("Status", SINV)
                    if_note = st.text_input("Note", value="")
                if st.form_submit_button("Add invoice", type="primary"):
                    execute(
                        "INSERT INTO material_invoices (invoice_date,job_id,supplier,invoice_number,amount,status,note) VALUES (?,?,?,?,?,?,?)",
                        (if_date.isoformat(), open_job, if_supp, if_num, if_amt, if_stat, if_note),
                    )
                    st.success("Added."); st.rerun()

        # ── TAB 7: Performance ───────────────────────────────────────────
        with wtab7:
            wlab7 = labour_metrics(open_job)
            wmat7 = material_metrics(open_job)
            wlp7  = profit_metrics(open_job)

            c1,c2,c3,c4 = st.columns(4)
            c1.metric("Tender Profit %", f"{wlp7.get('tender_profit_pct',0)*100:.1f}%")
            c2.metric("Live Profit %",   f"{wlp7.get('live_profit_pct',0)*100:.1f}%")
            c3.metric("Profit Drift",    f"{wlp7.get('profit_drift',0)*100:+.1f}%")
            c4.metric("Grade",           wlp7.get("grade","—"))
            st.divider()

            p1,p2 = st.columns(2)
            with p1:
                st.subheader("Labour")
                badge(wlab7.get("labour_health","—"))
                st.write(f"Tender: **${wlab7.get('tender_labour',0):,.0f}**")
                st.write(f"Scheduled: **${wlab7.get('scheduled_cost',0):,.0f}**")
                st.write(f"Actual: **${wlab7.get('actual_labour',0):,.0f}**")
                st.write(f"Projected: **${wlab7.get('projected_labour',0):,.0f}**")
            with p2:
                st.subheader("Material")
                badge(wmat7.get("material_health","—"))
                st.write(f"Tender: **${wmat7.get('tender_material',0):,.0f}**")
                st.write(f"Actual: **${wmat7.get('actual_material',0):,.0f}**")
                st.write(f"Variance: **${wmat7.get('material_variance',0):+,.0f}**")

            # Quote PDF + Supplier PO
            w_est7 = get_estimate(open_job)
            if not w_est7.empty:
                st.divider()

                # ── Quote format options ───────────────────────────────
                st.markdown("**Quote format options**")
                qf1, qf2, qf3, qf4 = st.columns(4)
                with qf1:
                    q_cat_totals = st.checkbox("Category totals",
                        value=True, key=f"q_cat_{open_job}",
                        help="Show total per section (e.g. Fascia & Gutter $42,000)")
                with qf2:
                    q_ref_nums = st.checkbox("Reference numbers",
                        value=True, key=f"q_ref_{open_job}",
                        help="Show 1.1, 1.2, 2.1 ref numbers per line")
                with qf3:
                    q_line_amts = st.checkbox("Line item amounts",
                        value=False, key=f"q_amt_{open_job}",
                        help="Show dollar amount per line (off = cleaner for commercial)")
                with qf4:
                    q_terms = st.checkbox("Terms & conditions page",
                        value=True, key=f"q_terms_{open_job}",
                        help="Add T&C page at end of quote")

                quote_opts = {
                    "show_cat_totals":   q_cat_totals,
                    "show_ref_nums":     q_ref_nums,
                    "show_qty_uom":      True,
                    "show_line_amounts": q_line_amts,
                    "show_terms":        q_terms,
                }

                st.divider()
                pdf_col1, pdf_col2 = st.columns(2)
                with pdf_col1:
                    if st.button("Generate Quote PDF", type="primary"):
                        try:
                            pdf_buf = generate_quote_pdf(wjob, w_est7, quote_opts)
                            st.download_button(
                                "⬇ Download Quote PDF", data=pdf_buf,
                                file_name=f"Quote_{open_job}_{date.today().isoformat()}.pdf",
                                mime="application/pdf", type="primary",
                            )
                        except Exception as e:
                            st.error(f"PDF error: {e}")
                with pdf_col2:
                    if st.button("Generate Supplier PO", type="secondary"):
                        try:
                            po_buf = generate_supplier_po_pdf(wjob, w_est7)
                            st.download_button(
                                "⬇ Download Supplier PO", data=po_buf,
                                file_name=f"PO_{open_job}_{date.today().isoformat()}.pdf",
                                mime="application/pdf",
                            )
                        except Exception as e:
                            st.error(f"PO error: {e}")

        # ── TAB 8: Documents ─────────────────────────────────────────────
        with wtab8:
            st.subheader("Job Documents")
            st.caption("Upload roof plans, drawings, site documents and specs.")

            uploaded = st.file_uploader(
                "Upload document",
                type=["pdf","png","jpg","jpeg","dwg","dxf","xlsx","docx"],
                accept_multiple_files=True,
                key=f"upload_{open_job}",
            )
            if uploaded:
                for up in uploaded:
                    filedata = up.read()
                    execute(
                        "INSERT INTO job_files (job_id, filename, filetype, filedata, uploaded_at) VALUES (?,?,?,?,?)",
                        (open_job, up.name, up.type, filedata, date.today().isoformat()),
                    )
                st.success(f"Uploaded {len(uploaded)} file(s)."); st.rerun()

            # Load metadata only — no file data loaded until download clicked
            files_df = fetch_df(
                "SELECT id, filename, filetype, uploaded_at, length(filedata) as filesize FROM job_files WHERE job_id=? ORDER BY id DESC",
                (open_job,)
            )

            if files_df.empty:
                st.info("No documents uploaded yet.")
            else:
                cnt_col, del_all_col = st.columns([4,1])
                with cnt_col:
                    st.markdown(
                        "<div style='font-size:14px;color:#64748b;margin-bottom:8px'>" +
                        str(len(files_df)) + " document(s)</div>",
                        unsafe_allow_html=True)
                with del_all_col:
                    if st.button("🗑 Delete all", key=f"del_all_{open_job}", type="secondary"):
                        execute("DELETE FROM job_files WHERE job_id=?", (open_job,))
                        st.rerun()

                for _, frow in files_df.iterrows():
                    fid   = int(frow["id"])
                    ftype = str(frow["filetype"] or "")
                    fname = str(frow["filename"])
                    fdate = str(frow["uploaded_at"] or "")
                    fsize = int(frow["filesize"] or 0)
                    fsize_str = f"{fsize/1024:.0f} KB" if fsize < 1024*1024 else f"{fsize/1024/1024:.1f} MB"

                    if "pdf" in ftype or fname.lower().endswith(".pdf"):
                        icon = "📄"
                    elif any(fname.lower().endswith(x) for x in [".png",".jpg",".jpeg"]):
                        icon = "🖼️"
                    elif fname.lower().endswith(".xlsx"):
                        icon = "📊"
                    elif fname.lower().endswith(".docx"):
                        icon = "📝"
                    else:
                        icon = "📎"

                    dc1, dc2, dc3 = st.columns([6,1,1])
                    with dc1:
                        st.markdown(
                            "<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:9px;"
                            "padding:12px 16px;display:flex;align-items:center;gap:12px'>"
                            "<span style='font-size:22px'>" + icon + "</span>"
                            "<div style='flex:1'>"
                            "<div style='font-size:15px;font-weight:600;color:#e2e8f0'>" + fname + "</div>"
                            "<div style='font-size:13px;color:#475569'>" + fdate + " · " + fsize_str + "</div>"
                            "</div></div>",
                            unsafe_allow_html=True)
                    with dc2:
                        fdata_row = fetch_df("SELECT filedata, filename, filetype FROM job_files WHERE id=?", (fid,))
                        if not fdata_row.empty:
                            raw = bytes(fdata_row.iloc[0]["filedata"])
                            st.download_button(
                                "⬇",
                                data=raw,
                                file_name=fname,
                                mime=ftype or "application/octet-stream",
                                key=f"dl_{fid}",
                                help="Download"
                            )
                    with dc3:
                        if st.button("🗑", key=f"fdel_{fid}", help="Delete", type="secondary"):
                            execute("DELETE FROM job_files WHERE id=?", (fid,))
                            st.rerun()

        # ── TAB 9: Financials ────────────────────────────────────────────
        with wtab9:
            sell_price_fin = float(wjob.get("sell_price") or 0)

            # VARIATIONS
            st.subheader("Variations")
            st.caption("Track scope changes after the original quote is accepted.")

            # ── Linked variation jobs ──────────────────────────────────
            var_jobs = fetch_df("""
                SELECT job_id, variation_title, stage, sell_price, job_type
                FROM jobs WHERE parent_job=? AND is_variation=1 AND archived=0
                ORDER BY job_id
            """, (open_job,))

            if not var_jobs.empty:
                st.markdown("<div style='font-size:14px;font-weight:700;color:#2dd4bf;margin-bottom:8px'>Variation Jobs</div>", unsafe_allow_html=True)
                for _, vj in var_jobs.iterrows():
                    vj_id    = str(vj["job_id"])
                    vj_title = str(vj.get("variation_title","") or vj_id)
                    vj_val   = float(vj.get("sell_price",0) or 0)
                    vj_stage = str(vj.get("stage","") or "")
                    vcol1, vcol2 = st.columns([5,1])
                    with vcol1:
                        st.markdown(
                            "<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-left:4px solid #f59e0b;"
                            "border-radius:8px;padding:10px 16px;margin-bottom:6px;display:flex;align-items:center;gap:12px'>"
                            "<span style='font-weight:700;color:#f59e0b;font-size:15px'>" + vj_id + "</span>"
                            "<span style='color:#e2e8f0;font-size:14px'>" + vj_title + "</span>"
                            "<span style='margin-left:auto;color:#2dd4bf;font-weight:700'>$" + f"{vj_val:,.2f}" + "</span>"
                            "<span style='background:#2a3d4f;color:#94a3b8;font-size:13px;padding:2px 8px;border-radius:4px'>" + vj_stage + "</span>"
                            "</div>",
                            unsafe_allow_html=True)
                    with vcol2:
                        if st.button("Open", key=f"open_var_{vj_id}"):
                            st.session_state["open_job"] = vj_id
                            st.rerun()
                st.divider()

            # ── Create new variation job ───────────────────────────────
            with st.expander("+ Create Variation Job", expanded=False):
                with st.form("new_var_job_form"):
                    vj1, vj2 = st.columns(2)
                    with vj1:
                        new_var_title  = st.text_input("Variation title *",
                            placeholder="e.g. Extra skylight flashing north face")
                        new_var_prefix = st.text_input("Job prefix", value=str(open_job) + "-V",
                            help="Auto-generates e.g. LES-001-V001")
                    with vj2:
                        new_var_type   = st.selectbox("Job type", JOB_TYPES,
                            index=JOB_TYPES.index(str(wjob.get("job_type","Residential") or "Residential"))
                            if str(wjob.get("job_type","")) in JOB_TYPES else 0)
                        new_var_finish = st.selectbox("Job finish",
                            ["Steel","MATT","ULTRA","Aluminium","VM Zinc","Copper","Zincalume"],
                            index=["Steel","MATT","ULTRA","Aluminium","VM Zinc","Copper","Zincalume"].index(
                                str(wjob.get("job_finish","Steel") or "Steel"))
                            if str(wjob.get("job_finish","Steel")) in ["Steel","MATT","ULTRA","Aluminium","VM Zinc","Copper","Zincalume"] else 0)

                    if st.form_submit_button("Create Variation Job", type="primary"):
                        if not new_var_title.strip():
                            st.error("Variation title required.")
                        else:
                            prefix = (new_var_prefix.strip().upper() or str(open_job) + "-V")
                            var_id = get_next_job_id(prefix)
                            # Direct insert with all variation fields in one shot
                            try:
                                # First ensure columns exist
                                import sqlite3 as _sq
                                _conn = get_conn()
                                _cur  = _conn.cursor()
                                for _col, _def in [
                                    ("parent_job",      "TEXT DEFAULT ''"),
                                    ("is_variation",    "INTEGER DEFAULT 0"),
                                    ("variation_title", "TEXT DEFAULT ''"),
                                ]:
                                    try:
                                        _cur.execute(f"ALTER TABLE jobs ADD COLUMN {_col} {_def}")
                                        _conn.commit()
                                    except:
                                        pass
                                _conn.close()

                                execute("""INSERT INTO jobs
                                    (job_id, client, address, estimator, stage,
                                     job_type, job_finish, parent_job, is_variation, variation_title,
                                     archived, sell_price, running_cost_pct,
                                     tender_material_budget, tender_labour_budget, tender_profit_pct)
                                    VALUES (?,?,?,?,?,?,?,?,1,?,0,0,0.11,0,0,0)""",
                                    (var_id,
                                     str(wjob.get("client","") or ""),
                                     str(wjob.get("address","") or ""),
                                     str(wjob.get("estimator","") or ""),
                                     "Take-off",
                                     new_var_type,
                                     new_var_finish,
                                     str(open_job),
                                     new_var_title.strip()))
                                st.session_state["open_job"] = var_id
                                st.rerun()
                            except Exception as _ve:
                                st.error("Error: " + str(_ve))
                                st.write("var_id:", var_id, "open_job:", open_job)

            var_df = fetch_df("SELECT * FROM variations WHERE job_id=? ORDER BY date_raised, id", (open_job,))
            VAR_STATUSES = ["Pending","Approved","Rejected"]
            approved_total   = float(var_df[var_df["status"]=="Approved"]["value"].sum()) if not var_df.empty else 0
            revised_contract = sell_price_fin + approved_total

            vc1,vc2,vc3,vc4 = st.columns(4)
            vc1.metric("Original Contract",   f"${sell_price_fin:,.2f}")
            vc2.metric("Approved Variations", f"${approved_total:+,.2f}")
            vc3.metric("Revised Contract",    f"${revised_contract:,.2f}")
            vc4.metric("Pending Variations",  str(len(var_df[var_df["status"]=="Pending"]) if not var_df.empty else 0))
            st.divider()

            if not var_df.empty:
                for _, vrow in var_df.iterrows():
                    vid    = int(vrow["id"])
                    vstatus= str(vrow["status"])
                    vval   = float(vrow["value"])
                    vc_map = {"Approved":"#2dd4bf","Pending":"#f59e0b","Rejected":"#f43f5e"}
                    vc     = vc_map.get(vstatus,"#64748b")
                    col_i, col_p, col_e = st.columns([5,1,1])
                    with col_i:
                        st.markdown(
                            f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;"
                            f"border-left:3px solid {vc};border-radius:9px;padding:10px 14px;margin-bottom:6px'>"
                            f"<div style='display:flex;align-items:center;gap:10px;margin-bottom:4px'>"
                            f"<span style='font-weight:700;color:#e2e8f0'>{vrow['var_number']}</span>"
                            f"<span style='background:{vc}22;color:{vc};padding:1px 8px;border-radius:999px;font-size:13px;font-weight:700'>{vstatus}</span>"
                            f"<span style='color:#2dd4bf;font-weight:700;margin-left:auto'>${vval:+,.2f}</span></div>"
                            f"<div style='font-size:13px;color:#94a3b8'>{vrow['description']}</div>"
                            f"<div style='font-size:13px;color:#475569;margin-top:3px'>Raised: {vrow['date_raised']}"
                            f"{'  ·  Approved by: '+str(vrow['approved_by']) if vrow.get('approved_by') else ''}</div>"
                            f"</div>", unsafe_allow_html=True)
                    with col_p:
                        if st.button("PDF", key=f"vpdf_{vid}"):
                            try:
                                vpdf = generate_variation_pdf(wjob, vrow.to_dict(), approved_total)
                                st.download_button("⬇ Download", data=vpdf,
                                    file_name=f"Variation_{vrow['var_number']}_{open_job}.pdf",
                                    mime="application/pdf", key=f"vdl_{vid}")
                            except Exception as e:
                                st.error(str(e))
                    with col_e:
                        if st.button("Edit", key=f"vedit_{vid}"):
                            st.session_state[f"edit_var_{open_job}"] = vid
                    if st.session_state.get(f"edit_var_{open_job}") == vid:
                        with st.form(f"evf_{vid}"):
                            ev1,ev2 = st.columns(2)
                            with ev1:
                                e_vd = st.text_area("Description", value=str(vrow["description"] or ""), height=70)
                                e_vv = st.number_input("Value ($)", value=float(vrow["value"]), step=100.0)
                            with ev2:
                                e_vs = st.selectbox("Status", VAR_STATUSES, index=VAR_STATUSES.index(vstatus) if vstatus in VAR_STATUSES else 0)
                                e_va = st.text_input("Approved by", value=str(vrow.get("approved_by","") or ""))
                            sb1,sb2 = st.columns(2)
                            with sb1:
                                if st.form_submit_button("Save", type="primary"):
                                    execute("UPDATE variations SET description=?,value=?,status=?,approved_by=? WHERE id=?", (e_vd,e_vv,e_vs,e_va,vid))
                                    st.session_state.pop(f"edit_var_{open_job}",None); st.rerun()
                            with sb2:
                                if st.form_submit_button("Delete"):
                                    execute("DELETE FROM variations WHERE id=?", (vid,))
                                    st.session_state.pop(f"edit_var_{open_job}",None); st.rerun()

            next_vnum = f"V{len(var_df)+1:03d}" if not var_df.empty else "V001"
            with st.expander("+ Add variation", expanded=False):
                with st.form(f"add_var_{open_job}"):
                    av1,av2 = st.columns(2)
                    with av1:
                        a_vn = st.text_input("Variation #", value=next_vnum)
                        a_vd = st.text_area("Description", placeholder="e.g. Additional skylight — client request", height=70)
                        a_vv = st.number_input("Value ($)", value=0.0, step=100.0, help="Negative for deductions")
                    with av2:
                        a_vs = st.selectbox("Status", VAR_STATUSES)
                        a_vdt= st.date_input("Date raised", value=date.today())
                        a_va = st.text_input("Approved by")
                    if st.form_submit_button("Add variation", type="primary"):
                        if a_vd.strip():
                            execute("INSERT INTO variations (job_id,var_number,description,value,status,date_raised,approved_by) VALUES (?,?,?,?,?,?,?)",
                                    (open_job,a_vn,a_vd,a_vv,a_vs,a_vdt.isoformat(),a_va))
                            st.success("Added."); st.rerun()

            st.divider()

            # RETENTION
            st.subheader("Retention")
            ret_df = fetch_df("SELECT * FROM job_retention WHERE job_id=?", (open_job,))
            ret    = ret_df.iloc[0].to_dict() if not ret_df.empty else {}
            with st.form("ret_form"):
                rc1,rc2,rc3 = st.columns(3)
                with rc1:
                    r_pct = st.number_input("Retention %", min_value=0.0, max_value=20.0, value=float(ret.get("retention_pct",0) or 0), step=0.5)
                    r_amt = revised_contract * r_pct / 100
                    st.metric("Retention held", f"${r_amt:,.2f}")
                with rc2:
                    r_date= st.text_input("Release date", value=str(ret.get("release_date","") or ""), placeholder="YYYY-MM-DD")
                    r_rel = st.checkbox("Released", value=bool(int(ret.get("released",0) or 0)))
                with rc3:
                    r_note= st.text_area("Notes", value=str(ret.get("notes","") or ""), height=80)
                if st.form_submit_button("Save retention", type="primary"):
                    if ret_df.empty:
                        execute("INSERT INTO job_retention (job_id,retention_pct,retention_amt,release_date,released,notes) VALUES (?,?,?,?,?,?)",
                                (open_job,r_pct,r_amt,r_date,int(r_rel),r_note))
                    else:
                        execute("UPDATE job_retention SET retention_pct=?,retention_amt=?,release_date=?,released=?,notes=? WHERE job_id=?",
                                (r_pct,r_amt,r_date,int(r_rel),r_note,open_job))
                    st.success("Saved."); st.rerun()

            st.divider()

            # PAYMENT SCHEDULE
            st.subheader("Payment Schedule")
            pay_df    = fetch_df("SELECT * FROM payment_schedule WHERE job_id=? ORDER BY id", (open_job,))
            PAY_STATS = ["Unpaid","Invoiced","Paid"]
            net_val   = revised_contract - (revised_contract * float(ret.get("retention_pct",0) or 0) / 100)

            # ── Variation milestone sync ───────────────────────────────
            # All variation jobs linked to this parent
            # Variation JOBS (linked job cards with a price)
            all_var_jobs = fetch_df("""
                SELECT j.job_id, j.variation_title, j.sell_price, j.stage
                FROM jobs j
                WHERE j.parent_job=? AND COALESCE(j.is_variation,0)=1
                AND j.archived=0
                ORDER BY j.job_id
            """, (open_job,))

            # Also get manual variations (from variations table) that are Approved
            approved_manual_vars = fetch_df("""
                SELECT var_number, description, value, status, approved_by
                FROM variations WHERE job_id=? AND status='Approved' AND value > 0
                ORDER BY id
            """, (open_job,))

            has_var_content = not all_var_jobs.empty or not approved_manual_vars.empty
            if has_var_content:
                st.markdown("**Variation jobs & approved variations**")

                # Manual approved variations from variations table
                if not approved_manual_vars.empty:
                    for _, mv in approved_manual_vars.iterrows():
                        mv_num   = str(mv.get("var_number","") or "VAR")
                        mv_desc  = str(mv.get("description","") or "")
                        mv_val   = float(mv.get("value",0) or 0)
                        mv_by    = str(mv.get("approved_by","") or "")
                        mv_key   = f"manual_{mv_num}"
                        existing_mv = fetch_df(
                            "SELECT id FROM payment_schedule WHERE job_id=? AND milestone LIKE ?",
                            (open_job, f"%{mv_num}%"))
                        already_mv = not existing_mv.empty
                        mc1, mc2 = st.columns([5,1])
                        with mc1:
                            st.markdown(
                                "<div style='background:#1e2d3d;border:1px solid " +
                                ("#2dd4bf44" if already_mv else "#f59e0b44") +
                                ";border-radius:8px;padding:10px 16px;display:flex;gap:16px;align-items:center'>"
                                "<span style='font-weight:700;color:#f59e0b'>" + mv_num + "</span>"
                                "<span style='color:#e2e8f0'>" + mv_desc[:50] + "</span>"
                                "<span style='color:#2dd4bf;font-weight:700'>$" + f"{mv_val:,.2f}" + "</span>"
                                "<span style='color:#64748b;font-size:13px'>Approved by " + mv_by + "</span>"
                                "<span style='margin-left:auto;color:" + ("#2dd4bf" if already_mv else "#f59e0b") + ";font-size:13px'>"
                                + ("✅ In schedule" if already_mv else "⏳ Not in schedule") + "</span>"
                                "</div>", unsafe_allow_html=True)
                        with mc2:
                            if not already_mv:
                                if st.button("+ Add", key=f"add_mv_{mv_key}_{open_job}", type="primary"):
                                    execute(
                                        "INSERT INTO payment_schedule (job_id,milestone,pct,amount,status) VALUES (?,?,?,?,?)",
                                        (open_job, f"Variation — {mv_desc[:40]} ({mv_num})", 0, mv_val, "Unpaid"))
                                    st.success(f"✅ {mv_num} added — ${mv_val:,.2f}")
                                    st.rerun()

            if not all_var_jobs.empty:
                pass  # continue to show variation jobs below
                for _, vj in all_var_jobs.iterrows():
                    vj_id    = str(vj["job_id"])
                    vj_title = str(vj.get("variation_title","") or vj_id)
                    vj_val   = float(vj.get("sell_price",0) or 0)
                    vj_stage = str(vj.get("stage","") or "")
                    existing_mil = fetch_df(
                        "SELECT id FROM payment_schedule WHERE job_id=? AND milestone LIKE ?",
                        (open_job, f"%{vj_id}%"))
                    already_added = not existing_mil.empty

                    vc1, vc2 = st.columns([5,1])
                    with vc1:
                        status_col = "#2dd4bf" if already_added else "#f59e0b"
                        status_txt = "✅ In schedule" if already_added else "⏳ Not yet in schedule"
                        st.markdown(
                            "<div style='background:#1e2d3d;border:1px solid " +
                            ("#2dd4bf44" if already_added else "#f59e0b44") +
                            ";border-radius:8px;padding:10px 16px;display:flex;gap:16px;align-items:center'>"
                            "<span style='font-weight:700;color:#f59e0b'>" + vj_id + "</span>"
                            "<span style='color:#e2e8f0'>" + vj_title + "</span>"
                            "<span style='color:#2dd4bf;font-weight:700'>$" + f"{vj_val:,.2f}" + "</span>"
                            "<span style='color:#64748b;font-size:13px'>" + vj_stage + "</span>"
                            "<span style='margin-left:auto;color:" + status_col + ";font-size:13px'>" + status_txt + "</span>"
                            "</div>",
                            unsafe_allow_html=True)
                    with vc2:
                        if not already_added:
                            if st.button("+ Add to schedule", key=f"add_var_mil_{vj_id}", type="primary"):
                                execute(
                                    "INSERT INTO payment_schedule (job_id,milestone,pct,amount,status) VALUES (?,?,?,?,?)",
                                    (open_job, f"Variation — {vj_title} ({vj_id})", 0, vj_val, "Unpaid"))
                                st.success(f"✅ {vj_id} added as milestone — ${vj_val:,.2f}")
                                st.rerun()
                        else:
                            st.markdown("<div style='padding:8px;color:#2dd4bf;font-size:13px'>Added ✓</div>",
                                unsafe_allow_html=True)
                st.divider()

            if pay_df.empty:
                st.info("No payment schedule yet.")
                if st.button("Generate default schedule (10% / 30% / 30% / 30%)", type="primary"):
                    for mil, pct in [("Deposit",10),("Progress 1",30),("Progress 2",30),("Final Claim",30)]:
                        execute("INSERT INTO payment_schedule (job_id,milestone,pct,amount,status) VALUES (?,?,?,?,?)",
                                (open_job,mil,pct,net_val*pct/100,"Unpaid"))
                    st.success("Schedule created."); st.rerun()
            else:
                total_sched    = pay_df["amount"].sum()
                total_paid     = pay_df[pay_df["status"]=="Paid"]["amount"].sum()
                total_inv      = pay_df[pay_df["status"]=="Invoiced"]["amount"].sum()
                total_invoiced_paid = total_paid + total_inv
                # Max invoice guard — total issued can't exceed net contract value
                remaining_to_invoice = max(0, net_val - total_invoiced_paid)
                over_invoiced = total_sched > net_val * 1.001  # 0.1% tolerance

                pm1,pm2,pm3,pm4 = st.columns(4)
                pm1.metric("Contract value", f"${net_val:,.2f}")
                pm2.metric("Paid",           f"${total_paid:,.2f}")
                pm3.metric("Invoiced",       f"${total_inv:,.2f}")
                pm4.metric("Remaining",      f"${remaining_to_invoice:,.2f}",
                    delta=f"-${(net_val - remaining_to_invoice):,.2f} issued" if total_invoiced_paid > 0 else None)

                if over_invoiced:
                    st.warning(f"⚠️ Schedule total (${total_sched:,.2f}) exceeds contract value (${net_val:,.2f}) — check milestone amounts.")

                # Show invoiced % of contract
                total_pct_issued = (total_invoiced_paid / net_val * 100) if net_val else 0
                pct_color = "#f43f5e" if total_pct_issued > 100 else "#2dd4bf" if total_pct_issued > 0 else "#64748b"
                st.markdown(
                    "<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:8px;"
                    "padding:10px 16px;margin-bottom:12px;display:flex;align-items:center;gap:16px'>"
                    "<span style='color:#64748b;font-size:13px'>Invoiced so far:</span>"
                    "<span style='font-size:15px;font-weight:700;color:" + pct_color + "'>"
                    + f"${total_invoiced_paid:,.2f} / {total_pct_issued:.1f}%" +
                    "</span>"
                    + ("<span style='color:#f43f5e;font-size:13px;font-weight:600'>⛔ Over 100% — cannot issue more invoices</span>" if total_pct_issued > 100 else
                       f"<span style='color:#64748b;font-size:13px'>Remaining: ${remaining_to_invoice:,.2f}</span>")
                    + "</div>",
                    unsafe_allow_html=True)

                # Column headers
                st.markdown(
                    "<div style='display:grid;grid-template-columns:2fr 0.6fr 1.2fr 0.8fr 1.2fr 1.2fr 1fr;gap:8px;"
                    "font-size:13px;font-weight:700;color:#475569;text-transform:uppercase;padding:8px 4px 4px'>"
                    "<span>Milestone</span><span>%</span><span>Contract (ex)</span>"
                    "<span>Var (ex)</span><span>Total (ex GST)</span><span>Status</span><span>Due date</span>"
                    "</div>",
                    unsafe_allow_html=True)

                for _, prow in pay_df.iterrows():
                    pid      = int(prow["id"])
                    pstat    = str(prow["status"])
                    is_var_mil = str(prow.get("milestone","")).startswith("Variation")
                    p_pct    = float(prow["pct"] or 0)
                    # Contract portion vs variation portion
                    p_contract_amt = float(sell_price_fin * p_pct / 100) if p_pct > 0 else 0.0
                    p_var_amt      = float(prow["amount"] or 0) - p_contract_amt if is_var_mil else 0.0
                    p_total_ex     = float(prow["amount"] or 0) if p_pct == 0 else float(sell_price_fin * p_pct / 100) + p_var_amt
                    pc_c  = {"Paid":"#2dd4bf","Invoiced":"#f59e0b","Unpaid":"#64748b"}.get(pstat,"#64748b")
                    with st.form(f"prow_{pid}"):
                        pp1,pp2,pp3,pp4,pp5,pp6 = st.columns([2,0.6,1.2,1.2,1.5,1.2])
                        with pp1: e_pm = st.text_input("Milestone", value=str(prow["milestone"]), label_visibility="collapsed")
                        with pp2: e_pp = st.number_input("%", min_value=0.0, max_value=100.0, value=p_pct, step=5.0, label_visibility="collapsed")
                        with pp3:
                            st.markdown(f"<div style='padding:8px 4px;font-size:14px;color:#e2e8f0'>${p_contract_amt:,.2f}</div>", unsafe_allow_html=True)
                        with pp4:
                            var_col = "#f59e0b" if p_var_amt > 0 else "#475569"
                            st.markdown(f"<div style='padding:8px 4px;font-size:14px;color:{var_col}'>${p_var_amt:,.2f}</div>", unsafe_allow_html=True)
                        with pp5:
                            # Use net_val (revised contract) × pct for non-variation milestones
                            _display_amt = float(prow["amount"] or 0) if p_pct == 0 else round(net_val * p_pct / 100, 2)
                            e_pa = st.number_input("Total", min_value=0.0, value=_display_amt, step=100.0, label_visibility="collapsed")
                        with pp6: e_ps = st.selectbox("Status", PAY_STATS, index=PAY_STATS.index(pstat) if pstat in PAY_STATS else 0, label_visibility="collapsed")
                        e_pd = st.text_input("Due date", value=str(prow.get("due_date","") or ""), placeholder="YYYY-MM-DD", label_visibility="collapsed")
                        sb1, sb2 = st.columns([1,1])
                        with sb1:
                            saved_pay = st.form_submit_button("Save")
                        with sb2:
                            issue_inv = st.form_submit_button("🧾 Issue Invoice", type="primary")

                    # Handle outside form so download_button works
                    if saved_pay:
                        execute("UPDATE payment_schedule SET milestone=?,pct=?,amount=?,status=?,due_date=? WHERE id=?",
                                (e_pm,e_pp,e_pa,e_ps,e_pd,pid))
                        st.rerun()
                    if issue_inv:
                        # Max 100% guard — check total already invoiced/paid
                        already_issued = float(fetch_df(
                            "SELECT COALESCE(SUM(amount_ex_gst),0) AS t FROM client_invoices WHERE job_id=?",
                            (open_job,)).iloc[0]["t"])
                        this_amount = float(net_val * float(prow["pct"]) / 100)
                        if already_issued + this_amount > net_val * 1.001:
                            st.error(f"⛔ Cannot issue — would exceed contract value. "
                                     f"Already invoiced: ${already_issued:,.2f} / ${net_val:,.2f}. "
                                     f"Remaining: ${max(0, net_val - already_issued):,.2f}")
                        else:
                            pass  # proceed below
                    if issue_inv and (float(fetch_df("SELECT COALESCE(SUM(amount_ex_gst),0) AS t FROM client_invoices WHERE job_id=?", (open_job,)).iloc[0]["t"]) + float(net_val * float(prow["pct"]) / 100)) <= net_val * 1.001:
                        inv_num   = get_next_invoice_number()
                        amount_ex = float(net_val * float(prow["pct"]) / 100)
                        gst       = round(amount_ex * 0.1, 2)
                        total     = round(amount_ex + gst, 2)
                        inv_data  = {
                            "invoice_number": inv_num,
                            "job_id":         open_job,
                            "milestone":      str(prow["milestone"]),
                            "issue_date":     date.today().isoformat(),
                            "due_date":       str(prow.get("due_date","") or ""),
                            "amount_ex_gst":  amount_ex,
                            "gst":            gst,
                            "total_inc_gst":  total,
                            "status":         "Issued",
                        }
                        execute("""INSERT INTO client_invoices
                            (invoice_number,job_id,milestone_id,issue_date,due_date,
                             amount_ex_gst,gst,total_inc_gst,status,milestone)
                            VALUES (?,?,?,?,?,?,?,?,?,?)""",
                            (inv_num, open_job, pid,
                             inv_data["issue_date"], inv_data["due_date"],
                             amount_ex, gst, total, "Issued",
                             str(prow["milestone"])))
                        execute("UPDATE payment_schedule SET status='Invoiced' WHERE id=?", (pid,))
                        _settings = get_company_settings()
                        pdf_buf   = generate_invoice_pdf(wjob, inv_data, _settings)
                        st.success(f"✅ Invoice {inv_num} — ${total:,.2f} incl. GST")
                        st.download_button(
                            "⬇ Download Invoice " + inv_num,
                            data=pdf_buf,
                            file_name=f"Invoice_{inv_num}_{open_job}.pdf",
                            mime="application/pdf",
                            key=f"inv_dl_{pid}",
                            type="primary",
                        )
                        # Check for billing email on client
                        _cli_billing = fetch_df("""
                            SELECT billing_email, billing_name, company, name
                            FROM clients WHERE name=? OR company=?
                        """, (wjob.get("client",""), wjob.get("client","")))
                        if not _cli_billing.empty:
                            _billing_email = str(_cli_billing.iloc[0].get("billing_email","") or "")
                            _billing_name  = str(_cli_billing.iloc[0].get("billing_name","") or _cli_billing.iloc[0].get("name",""))
                            if _billing_email:
                                _co_name = get_company_settings().get("company_name","Limitless")
                                _subject = f"Invoice {inv_num} — {open_job}"
                                _body = f"Hi {_billing_name},%0D%0A%0D%0APlease find attached Invoice {inv_num} for ${total:,.2f} incl. GST.%0D%0A%0D%0AKind regards,%0D%0A{_co_name}"
                                _mailto = f"mailto:{_billing_email}?subject={_subject}&body={_body}"
                                st.markdown(
                                    f"<a href='{_mailto}' target='_blank'>"
                                    f"<div style='background:#0d2233;border:1px solid #2dd4bf;border-radius:8px;"
                                    f"padding:10px 16px;margin-top:8px;cursor:pointer;text-align:center'>"
                                    f"<span style='color:#2dd4bf;font-weight:700'>📧 Email to {_billing_name} ({_billing_email})</span>"
                                    f"</div></a>",
                                    unsafe_allow_html=True
                                )

                with st.expander("+ Add milestone"):
                    with st.form("add_pay"):
                        ap1,ap2,ap3 = st.columns(3)
                        with ap1: a_pm = st.text_input("Milestone name")
                        with ap2: a_pp = st.number_input("% of contract", min_value=0.0, value=10.0, step=5.0)
                        with ap3: a_pd = st.text_input("Due date", placeholder="YYYY-MM-DD")
                        if st.form_submit_button("Add", type="primary"):
                            execute("INSERT INTO payment_schedule (job_id,milestone,pct,amount,status,due_date) VALUES (?,?,?,?,?,?)",
                                    (open_job,a_pm,a_pp,net_val*a_pp/100,"Unpaid",a_pd))
                            st.rerun()

        # ── TAB 10: Site Diary ──────────────────────────────────────────────
        with wtab10:
            st.subheader("Site Diary")
            st.caption("Daily log of conditions, workers, progress and delays.")

            WEATHER_OPTS = ["Fine","Partly Cloudy","Overcast","Light Rain","Heavy Rain","Wind","Hot","Cold"]
            diary_df = fetch_df(
                "SELECT * FROM site_diary WHERE job_id=? ORDER BY diary_date DESC", (open_job,)
            )

            if not diary_df.empty:
                for _, dr in diary_df.iterrows():
                    did = int(dr["id"])
                    with st.expander(f"📅 {dr['diary_date']} — {dr.get('weather','')} {dr.get('temp','')}",
                                     expanded=False):
                        dc1,dc2 = st.columns(2)
                        with dc1:
                            st.markdown(f"**Workers on site:** {dr.get('workers_on_site','—')}")
                            st.markdown(f"**Hours worked:** {dr.get('hours_worked',0)}")
                            st.markdown(f"**Visitors:** {dr.get('visitors','—') or '—'}")
                        with dc2:
                            st.markdown(f"**Progress:** {dr.get('progress_notes','—')}")
                            if dr.get('delays'):
                                st.markdown(f"**Delays:** {dr.get('delays')}")
                        if st.button("Delete entry", key=f"ddel_{did}"):
                            execute("DELETE FROM site_diary WHERE id=?", (did,))
                            st.rerun()

            st.divider()
            st.subheader("Add diary entry")
            emp_names_d = fetch_df("SELECT name FROM employees WHERE active=1 ORDER BY name")["name"].tolist()

            with st.form("diary_form"):
                df1,df2 = st.columns(2)
                with df1:
                    d_date    = st.date_input("Date", value=date.today(), format="DD/MM/YYYY")
                    d_weather = st.selectbox("Weather", WEATHER_OPTS)
                    d_temp    = st.text_input("Temperature", placeholder="e.g. 28°C")
                    d_workers = st.multiselect("Workers on site", emp_names_d)
                with df2:
                    d_hours   = st.number_input("Total hours worked", min_value=0.0, value=0.0, step=0.5)
                    d_visitors= st.text_input("Visitors / inspections", placeholder="e.g. Builder inspection 10am")
                    d_by      = st.text_input("Entry by")
                d_progress = st.text_area("Progress notes",
                    placeholder="e.g. Completed north face sheeting, started ridge capping", height=80)
                d_delays   = st.text_area("Delays / issues",
                    placeholder="e.g. Material delivery 2hrs late", height=60)

                if st.form_submit_button("Add entry", type="primary"):
                    execute("""INSERT INTO site_diary
                        (job_id,diary_date,weather,temp,workers_on_site,hours_worked,
                         progress_notes,delays,visitors,created_by)
                        VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        (open_job, d_date.isoformat(), d_weather, d_temp,
                         ", ".join(d_workers), d_hours, d_progress,
                         d_delays, d_visitors, d_by))
                    st.success("Diary entry added."); st.rerun()

        # ── TAB 11: Photos ───────────────────────────────────────────────
        with wtab11:
            st.subheader("Photo Log")
            st.caption("Upload and tag site photos by date and category.")

            PHOTO_CATS = ["Progress","Before","After","Defect","Damage","Delivery","Other"]

            photos_df = fetch_df(
                "SELECT id,photo_date,caption,category,filename FROM job_photos WHERE job_id=? ORDER BY photo_date DESC",
                (open_job,)
            )

            # Upload
            ph_col1, ph_col2, ph_col3 = st.columns(3)
            with ph_col1: ph_date = st.date_input("Photo date", value=date.today(), key="ph_date")
            with ph_col2: ph_cat  = st.selectbox("Category", PHOTO_CATS, key="ph_cat")
            with ph_col3: ph_cap  = st.text_input("Caption", placeholder="e.g. North face complete", key="ph_cap")

            ph_upload = st.file_uploader("Upload photos", type=["jpg","jpeg","png","webp"],
                accept_multiple_files=True, key=f"ph_upload_{open_job}")

            if ph_upload:
                for ph in ph_upload:
                    execute("""INSERT INTO job_photos
                        (job_id,photo_date,caption,category,filedata,filename,uploaded_at)
                        VALUES (?,?,?,?,?,?,?)""",
                        (open_job, ph_date.isoformat(), ph_cap, ph_cat,
                         ph.read(), ph.name, date.today().isoformat()))
                st.success(f"{len(ph_upload)} photo(s) uploaded."); st.rerun()

            st.divider()

            if photos_df.empty:
                st.info("No photos uploaded yet.")
            else:
                # Group by date
                for pdate in photos_df["photo_date"].unique():
                    day_photos = photos_df[photos_df["photo_date"]==pdate]
                    st.markdown(f"<div style='font-size:13px;font-weight:700;color:#2dd4bf;"
                                f"margin:12px 0 8px'>📅 {pdate}</div>", unsafe_allow_html=True)
                    pcols = st.columns(3)
                    for i, (_, ph) in enumerate(day_photos.iterrows()):
                        phid = int(ph["id"])
                        with pcols[i % 3]:
                            ph_data = fetch_df("SELECT filedata,filename FROM job_photos WHERE id=?", (phid,))
                            if not ph_data.empty:
                                raw = ph_data.iloc[0]["filedata"]
                                try:
                                    st.image(raw, caption=f"{ph.get('category','')} — {ph.get('caption','')}", width="stretch")
                                except:
                                    st.markdown(f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:8px;padding:20px;text-align:center;color:#64748b'>🖼️<br>{ph.get('filename','')}</div>", unsafe_allow_html=True)
                            if st.button("Delete", key=f"phdel_{phid}"):
                                execute("DELETE FROM job_photos WHERE id=?", (phid,))
                                st.rerun()

        # ── TAB 12: Safety ───────────────────────────────────────────────
        with wtab12:
            st.subheader("Safety Documents")
            st.caption("Attach SWMS, JSAs, safety plans and compliance docs to this job.")

            SAFETY_TYPES = ["SWMS","JSA","Safety Plan","MSDS","Inspection","Permit","Insurance","Other"]

            safety_df = fetch_df(
                "SELECT id,doc_type,title,filename,reviewed,reviewed_by,review_date,uploaded_at FROM safety_docs WHERE job_id=? ORDER BY id DESC",
                (open_job,)
            )

            # Upload
            sf_upload = st.file_uploader("Upload safety document",
                type=["pdf","doc","docx","png","jpg"],
                key=f"sf_upload_{open_job}")

            if sf_upload:
                sc1,sc2,sc3 = st.columns(3)
                with sc1: sf_type  = st.selectbox("Document type", SAFETY_TYPES, key="sf_type")
                with sc2: sf_title = st.text_input("Title", value=sf_upload.name, key="sf_title")
                with sc3: sf_rev   = st.text_input("Reviewed by", key="sf_rev")
                if st.button("Save document", type="primary"):
                    execute("""INSERT INTO safety_docs
                        (job_id,doc_type,title,filename,filetype,filedata,
                         reviewed,reviewed_by,review_date,uploaded_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        (open_job, sf_type, sf_title, sf_upload.name,
                         sf_upload.type, sf_upload.read(),
                         1 if sf_rev else 0, sf_rev,
                         date.today().isoformat() if sf_rev else "",
                         date.today().isoformat()))
                    st.success("Document saved."); st.rerun()

            st.divider()

            if safety_df.empty:
                st.info("No safety documents uploaded yet.")
            else:
                for _, sd in safety_df.iterrows():
                    sdid     = int(sd["id"])
                    reviewed = bool(int(sd.get("reviewed",0) or 0))
                    rev_color= "#2dd4bf" if reviewed else "#f59e0b"
                    rev_label= "REVIEWED" if reviewed else "PENDING REVIEW"

                    col_i, col_dl, col_del = st.columns([5,1,1])
                    with col_i:
                        st.markdown(f"""
                        <div style="background:#1e2d3d;border:1px solid #2a3d4f;
                            border-left:3px solid {rev_color};border-radius:9px;
                            padding:10px 14px;margin-bottom:6px">
                            <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
                                <span style="font-size:16px">📋</span>
                                <span style="font-weight:700;color:#e2e8f0;font-size:13px">{sd.get('title','')}</span>
                                <span style="background:#2a3d4f;color:#94a3b8;padding:1px 8px;
                                    border-radius:999px;font-size:13px">{sd.get('doc_type','')}</span>
                                <span style="background:{rev_color}22;color:{rev_color};padding:1px 8px;
                                    border-radius:999px;font-size:13px;font-weight:700;margin-left:auto">
                                    {rev_label}</span>
                            </div>
                            <div style="font-size:13px;color:#475569">
                                {sd.get('filename','')} · Uploaded {sd.get('uploaded_at','')}
                                {"· Reviewed by "+str(sd.get('reviewed_by','')) if sd.get('reviewed_by') else ""}
                            </div>
                        </div>""", unsafe_allow_html=True)

                    with col_dl:
                        sf_data = fetch_df("SELECT filedata,filetype,filename FROM safety_docs WHERE id=?", (sdid,))
                        if not sf_data.empty:
                            import base64 as _b64
                            raw = sf_data.iloc[0]["filedata"]
                            st.download_button("⬇", data=bytes(raw),
                                file_name=str(sf_data.iloc[0]["filename"]),
                                mime=str(sf_data.iloc[0]["filetype"]),
                                key=f"sfdl_{sdid}")
                    with col_del:
                        if st.button("🗑", key=f"sfdel_{sdid}"):
                            execute("DELETE FROM safety_docs WHERE id=?", (sdid,))
                            st.rerun()

                    if not reviewed:
                        if st.button(f"Mark as reviewed", key=f"sfrev_{sdid}"):
                            execute("UPDATE safety_docs SET reviewed=1, reviewed_by=?, review_date=? WHERE id=?",
                                    ("Reviewed", date.today().isoformat(), sdid))
                            st.rerun()

    # ── Jobs board (no job open) ─────────────────────────────────────────
    else:
        st.title("Jobs")

        # Header row
        hdr1, hdr2 = st.columns([4,1])
        with hdr2:
            if st.button("+ New Job", type="primary"):
                st.session_state["show_new_job"] = True

        # New job form
        if st.session_state.get("show_new_job"):
            # Load full client data OUTSIDE form so selection triggers rerun
            _clients_df = fetch_df("""
                SELECT id, name, company, address, phone, email,
                       billing_name, billing_email,
                       ca_name, ca_email, ca_phone,
                       pm_name, pm_email, pm_phone
                FROM clients ORDER BY company, name
            """)

            # Build display names — company first
            if not _clients_df.empty:
                _clients_df["display"] = _clients_df.apply(
                    lambda r: f"{r.get('company','') or r.get('name','')} — {r.get('name','') or ''}".strip(" —"),
                    axis=1
                )
                _client_options = ["— Select client —"] + _clients_df["display"].tolist()
            else:
                _client_options = ["— Select client —"]

            # Client selector OUTSIDE form — triggers auto-fill
            nj_cl_select = st.selectbox("Select client", _client_options, key="nj_client_select")

            # Auto-fill from client record
            _sel_client = {}
            _sel_name = ""
            if nj_cl_select != "— Select client —" and not _clients_df.empty:
                _idx = _client_options.index(nj_cl_select) - 1
                if 0 <= _idx < len(_clients_df):
                    _sel_client = _clients_df.iloc[_idx].to_dict()
                    _sel_name = str(_sel_client.get("company","") or _sel_client.get("name",""))

            # Show client info card if selected
            if _sel_client:
                _ca = str(_sel_client.get("ca_name","") or "")
                _pm = str(_sel_client.get("pm_name","") or "")
                _bill = str(_sel_client.get("billing_name","") or "")
                _bill_email = str(_sel_client.get("billing_email","") or "")
                # Build client card HTML safely
                _abn_html = f"<div style='color:#475569;font-size:12px'>ABN: {_sel_client.get('abn','')}</div>" if _sel_client.get('abn') else ""
                _ca_html = f"<div><div style='color:#a78bfa;font-size:13px;font-weight:700'>CA</div><div style='color:#94a3b8;font-size:13px'>{_ca}</div></div>" if _ca else ""
                _pm_html = f"<div><div style='color:#60a5fa;font-size:13px;font-weight:700'>PM</div><div style='color:#94a3b8;font-size:13px'>{_pm}</div></div>" if _pm else ""
                _bill_html = f"<div><div style='color:#2dd4bf;font-size:13px;font-weight:700'>Billing</div><div style='color:#94a3b8;font-size:13px'>{_bill} · {_bill_email}</div></div>" if _bill_email else ""
                _card_html = f"""<div style='background:#1e2d3d;border:1px solid #2dd4bf33;border-radius:10px;
                    padding:12px 16px;margin-bottom:12px;display:flex;gap:24px;flex-wrap:wrap'>
                    <div>
                        <div style='color:#2dd4bf;font-size:13px;font-weight:700'>{_sel_name}</div>
                        <div style='color:#64748b;font-size:13px'>{_sel_client.get("address","")}</div>
                        <div style='color:#64748b;font-size:13px'>{_sel_client.get("phone","")} · {_sel_client.get("email","")}</div>
                        {_abn_html}
                    </div>
                    {_ca_html}{_pm_html}{_bill_html}
                </div>"""
                st.markdown(_card_html, unsafe_allow_html=True)

            with st.form("new_job_form"):
                st.subheader("New Job")
                # Job ID row
                jid_col1, jid_col2 = st.columns([1,3])
                with jid_col1:
                    nj_prefix = st.text_input("Prefix", value="LES",
                        help="e.g. LES, ZMH, COM — number auto-increments")
                with jid_col2:
                    st.markdown(
                        "<div style='background:#0d2233;border:2px solid #2dd4bf;border-radius:10px;"
                        "padding:10px 16px;margin-top:4px'>"
                        "<div style='font-size:13px;color:#64748b;text-transform:uppercase;"
                        "letter-spacing:.1em'>Job number will be</div>"
                        "<div style='font-size:22px;font-weight:900;color:#2dd4bf'>"
                        "LES-XXX (auto)</div></div>",
                        unsafe_allow_html=True)

                nj1, nj2 = st.columns(2)
                with nj1:
                    # Manual override fields — pre-filled from client
                    nj_cl   = st.text_input("Client name *",
                        value=str(_sel_client.get("name","") or _sel_name or ""))
                    nj_addr = st.text_input("Site address",
                        value="",
                        placeholder="Enter the actual site/job address",
                        help="Where the work is being done — not the client's office")
                    nj_est  = st.text_input("Estimator",
                        value=str(current_user.get("full_name","") or ""))
                with nj2:
                    nj_stg    = st.selectbox("Stage", STAGES)
                    nj_type   = st.selectbox("Job type", JOB_TYPES)
                    nj_finish = st.selectbox("Job finish",
                        ["Steel","MATT","ULTRA","Aluminium","VM Zinc","Copper","Zincalume"])

                sb1, sb2 = st.columns([1,4])
                with sb1:
                    if st.form_submit_button("Create Job", type="primary"):
                        prefix = (nj_prefix.strip().upper() or "LES")
                        final_id = get_next_job_id(prefix)
                        upsert_job(final_id, nj_cl, nj_addr, nj_est, nj_stg)
                        execute("UPDATE jobs SET job_type=?, job_finish=? WHERE job_id=?",
                                (nj_type, nj_finish, final_id))
                        st.session_state.pop("show_new_job", None)
                        st.session_state.pop("nj_client_select", None)
                        st.session_state["open_job"] = final_id
                        st.success(f"Job {final_id} created!")
                        st.rerun()
                with sb2:
                    if st.form_submit_button("Cancel"):
                        st.session_state.pop("show_new_job", None)
                        st.session_state.pop("nj_client_select", None)
                        st.rerun()

            st.divider()

        # Load all jobs
        all_jobs_df = fetch_df("""
            SELECT j.job_id, j.client, j.address, j.estimator, j.stage,
                   j.sell_price, j.archived, j.job_type,
                   COALESCE(j.is_variation,0) AS is_variation,
                   COALESCE(j.parent_job,'') AS parent_job,
                   COALESCE(j.variation_title,'') AS variation_title,
                   (SELECT COUNT(*) FROM jobs v WHERE v.parent_job=j.job_id AND COALESCE(v.is_variation,0)=1 AND v.archived=0) AS var_count,
                   (SELECT COUNT(*) FROM jobs v WHERE v.parent_job=j.job_id AND COALESCE(v.is_variation,0)=1 AND v.archived=0 AND v.stage!='Completed') AS var_pending,
                   (SELECT MAX(ll.work_date) FROM labour_logs ll WHERE ll.job_id=j.job_id) AS last_labour,
                   (SELECT MAX(mi.invoice_date) FROM material_invoices mi WHERE mi.job_id=j.job_id) AS last_invoice
            FROM jobs j
            WHERE j.archived=0
            ORDER BY j.is_variation, j.job_id
        """)

        show_arch = st.toggle("Show archived", value=False)
        if show_arch:
            arch_df = fetch_df("SELECT job_id,client,stage,estimator FROM jobs WHERE archived=1 ORDER BY job_id")
            if not arch_df.empty:
                st.subheader("Archived")
                for _, ar in arch_df.iterrows():
                    ac1,ac2 = st.columns([6,1])
                    with ac1:
                        st.markdown(f"<div style='color:#475569;font-size:13px;padding:4px 0'>"
                                    f"<b>{ar['job_id']}</b> — {ar['client'] or '—'}</div>",
                                    unsafe_allow_html=True)
                    with ac2:
                        if st.button("Restore", key=f"restore_{ar['job_id']}"):
                            execute("UPDATE jobs SET archived=0 WHERE job_id=?", (ar["job_id"],))
                            st.rerun()
                st.divider()

        if all_jobs_df.empty:
            st.info("No jobs yet — click + New Job to get started.")
        else:
            if "is_variation" in all_jobs_df.columns:
                all_jobs_df["is_variation"] = pd.to_numeric(all_jobs_df["is_variation"], errors="coerce").fillna(0).astype(int)
                var_jobs_board  = all_jobs_df[all_jobs_df["is_variation"] == 1].copy()
                main_jobs_board = all_jobs_df[all_jobs_df["is_variation"] != 1].copy()
            else:
                var_jobs_board  = pd.DataFrame()
                main_jobs_board = all_jobs_df.copy()

            # ── Search bar ──────────────────────────────────────────────
            board_search = st.text_input("🔍 Search jobs", placeholder="Job ID, client, address, estimator...", key="board_search")
            if board_search:
                mask = (
                    main_jobs_board["job_id"].str.contains(board_search, case=False, na=False) |
                    main_jobs_board["client"].str.contains(board_search, case=False, na=False) |
                    main_jobs_board["address"].str.contains(board_search, case=False, na=False) |
                    main_jobs_board["estimator"].str.contains(board_search, case=False, na=False)
                )
                main_jobs_board = main_jobs_board[mask]

            # ── Stage tabs ──────────────────────────────────────────────
            board_stages = ["Lead", "Quoted", "Handover", "Live Job", "Completed"]

            # Stage colours for CSS
            _stage_hex = {
                "Lead":     "#7dd3fc",
                "Quoted":   "#f59e0b",
                "Handover": "#a78bfa",
                "Live Job": "#2dd4bf",
                "Completed":"#4ade80",
            }

            # Inject CSS to colour the tabs
            _tab_css = """<style>
            .stTabs [data-baseweb="tab-list"] {
                gap: 0px;
                width: 100%;
            }
            .stTabs [data-baseweb="tab"] {
                flex: 1;
                justify-content: center;
                font-weight: 700;
                font-size: 13px;
                letter-spacing: 0.05em;
                border-radius: 0px;
                padding: 10px 4px;
                border-bottom: 3px solid transparent;
            }
            </style>"""
            st.markdown(_tab_css, unsafe_allow_html=True)

            # Build tab labels with counts
            tab_labels = []
            for s in board_stages:
                cnt = len(main_jobs_board[main_jobs_board["stage"]==s])
                tab_labels.append(f"{s}  ·  {cnt}")

            stage_tabs = st.tabs(tab_labels)

            for tab, stage in zip(stage_tabs, board_stages):
                sc, tc = STAGE_COLORS.get(stage, ("#1e2d3d","#94a3b8"))
                stage_jobs = main_jobs_board[main_jobs_board["stage"]==stage]
                with tab:
                    # Coloured stage header bar
                    st.markdown(
                        f"<div style='background:{tc}18;border-left:4px solid {tc};"
                        f"border-radius:0 8px 8px 0;padding:10px 16px;margin-bottom:16px;"
                        f"display:flex;align-items:center;gap:12px'>"
                        f"<span style='color:{tc};font-weight:800;font-size:15px'>{stage}</span>"
                        f"<span style='color:#475569;font-size:13px'>{len(stage_jobs)} job{'s' if len(stage_jobs)!=1 else ''}</span>"
                        f"</div>",
                        unsafe_allow_html=True
                    )
                    if stage_jobs.empty:
                        st.info(f"No {stage} jobs yet.")
                    else:
                        # Table header
                        th1,th2,th3,th4,th5,th6 = st.columns([2,3,3,2,2,1])
                        th1.markdown("<div style='font-size:13px;color:#475569;font-weight:700'>JOB ID</div>", unsafe_allow_html=True)
                        th2.markdown("<div style='font-size:13px;color:#475569;font-weight:700'>CLIENT</div>", unsafe_allow_html=True)
                        th3.markdown("<div style='font-size:13px;color:#475569;font-weight:700'>ADDRESS</div>", unsafe_allow_html=True)
                        th4.markdown("<div style='font-size:13px;color:#475569;font-weight:700'>ESTIMATOR</div>", unsafe_allow_html=True)
                        th5.markdown("<div style='font-size:13px;color:#475569;font-weight:700'>VALUE</div>", unsafe_allow_html=True)
                        th6.markdown("", unsafe_allow_html=True)
                        st.divider()

                        for _, jrow in stage_jobs.iterrows():
                            sell = float(jrow.get("sell_price") or 0)
                            var_pending = int(jrow.get("var_pending",0) or 0)
                            jtype = str(jrow.get("job_type","") or "Residential")
                            jtype_col = TYPE_COLORS.get(jtype, "#64748b")
                            _created = _fmt_date(str(jrow.get("created_date","") or ""))

                            c1,c2,c3,c4,c5,c6 = st.columns([2,3,3,2,2,1])
                            with c1:
                                badge = f" <span style='background:#f59e0b22;color:#f59e0b;font-size:12px;padding:1px 4px;border-radius:3px'>{var_pending}V</span>" if var_pending else ""
                                st.markdown(f"<div style='font-weight:800;color:#f1f5f9;font-size:16px'>{jrow['job_id']}{badge}</div>", unsafe_allow_html=True)
                                if _created:
                                    st.markdown(f"<div style='color:#334155;font-size:12px'>{_created}</div>", unsafe_allow_html=True)
                            with c2:
                                st.markdown(f"<div style='color:#e2e8f0;font-size:15px;font-weight:600'>{str(jrow.get('client') or '—')}</div>", unsafe_allow_html=True)
                            with c3:
                                st.markdown(f"<div style='color:#64748b;font-size:14px'>📍 {str(jrow.get('address') or '—')[:35]}</div>", unsafe_allow_html=True)
                            with c4:
                                st.markdown(f"<div style='color:#94a3b8;font-size:14px'>{str(jrow.get('estimator') or '—')}</div>", unsafe_allow_html=True)
                            with c5:
                                if sell > 0:
                                    st.markdown(f"<div style='color:#2dd4bf;font-weight:800;font-size:15px'>${sell:,.0f}</div>", unsafe_allow_html=True)
                            with c6:
                                if st.button("Open →", key=f"open_{jrow['job_id']}", type="primary"):
                                    st.session_state["open_job"] = jrow["job_id"]
                                    st.rerun()
                            st.divider()

            # ── Variation jobs section ─────────────────────────────────
            if not var_jobs_board.empty:
                st.divider()
                st.markdown(
                    "<div style='font-size:13px;font-weight:700;letter-spacing:.12em;"
                    "text-transform:uppercase;color:#f59e0b;margin-bottom:12px'>"
                    "⚠️ Variation Jobs — " + str(len(var_jobs_board)) + " active</div>",
                    unsafe_allow_html=True)
                for i in range(0, len(var_jobs_board), 3):
                    chunk = var_jobs_board.iloc[i:i+3]
                    cols  = st.columns(3)
                    for col, (_, vrow) in zip(cols, chunk.iterrows()):
                        with col:
                            v_sell  = float(vrow.get("sell_price") or 0)
                            v_title = str(vrow.get("variation_title","") or "")
                            v_stage = str(vrow.get("stage","") or "")
                            v_parent= str(vrow.get("parent_job","") or "")
                            st.markdown(
                                "<div style='background:#1e2d3d;border:1px solid #f59e0b44;"
                                "border-top:3px solid #f59e0b;border-radius:10px;"
                                "padding:14px 16px;margin-bottom:8px'>"
                                "<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:4px'>"
                                "<b style='font-size:15px;color:#f59e0b'>" + str(vrow['job_id']) + "</b>"
                                "<span style='background:#f59e0b22;color:#f59e0b;font-size:13px;font-weight:700;padding:2px 8px;border-radius:4px'>VAR</span>"
                                "</div>"
                                "<div style='font-size:13px;color:#e2e8f0;margin-bottom:2px'>" + v_title + "</div>"
                                "<div style='font-size:13px;color:#64748b'>Parent: " + v_parent + " · " + v_stage + "</div>"
                                + ("<div style='font-size:14px;font-weight:800;color:#2dd4bf;margin-top:6px'>$" + f"{v_sell:,.0f}" + "</div>" if v_sell > 0 else "") +
                                "</div>",
                                unsafe_allow_html=True)
                            if st.button("Open →", key=f"open_v_{vrow['job_id']}", type="primary"):
                                st.session_state["open_job"] = vrow["job_id"]
                                st.rerun()

# ─────────────────────────────────────────────
#  PAGE: EMPLOYEES
# ─────────────────────────────────────────────
elif page == "Employees":
    st.title("Employee Register")

    show_inactive = st.toggle("Show inactive", value=False)
    eq = "SELECT id, name, role, hourly_rate, phone, active FROM employees"
    if not show_inactive:
        eq += " WHERE active=1"
    emp_df = fetch_df(eq + " ORDER BY name")
    emp_df["hourly_rate"] = pd.to_numeric(emp_df["hourly_rate"], errors="coerce").fillna(0)
    emp_df["daily_rate"] = (emp_df["hourly_rate"] * 8).round(2)
    st.dataframe(emp_df, width="stretch")

    active_emp = emp_df[emp_df["active"] == 1]
    if not active_emp.empty:
        st.metric("Full team daily cost (est.)", f"${active_emp['daily_rate'].sum():,.0f}")

    st.divider()
    st.subheader("Add / Edit Employee")

    all_emps    = fetch_df("SELECT id, name FROM employees ORDER BY name")
    edit_opts   = ["— New employee —"] + all_emps["name"].tolist()
    edit_choice = st.selectbox("Edit existing or add new", edit_opts, key="emp_edit")

    if edit_choice == "— New employee —":
        pre = {"id": None, "name": "", "role": "Roofer", "hourly_rate": 225.0, "phone": "", "active": 1}
    else:
        r   = fetch_df("SELECT * FROM employees WHERE name=?", (edit_choice,))
        pre = r.iloc[0].to_dict() if not r.empty else {"id": None, "name": edit_choice, "role": "Roofer", "hourly_rate": 225.0, "phone": "", "active": 1}

    ROLES = ["Roofer", "Labourer", "Leading Hand", "Apprentice", "Other"]
    with st.form("emp_form"):
        f_name   = st.text_input("Full name",       value=pre["name"])
        f_role   = st.selectbox("Trade / role",      ROLES, index=ROLES.index(pre["role"]) if pre["role"] in ROLES else 0)
        f_rate   = st.number_input("Hourly rate ($)", min_value=0.0, value=float(pre["hourly_rate"]), step=5.0)
        f_phone  = st.text_input("Phone",            value=pre["phone"] or "")
        f_active = st.checkbox("Active",             value=bool(pre["active"]))

        sa, sb = st.columns(2)
        with sa: saved      = st.form_submit_button("Save employee")
        with sb: deactivate = st.form_submit_button("Deactivate") if pre["id"] else False

        if saved and f_name.strip():
            existing = fetch_df("SELECT id FROM employees WHERE name=?", (f_name.strip(),))
            if existing.empty:
                execute(
                    "INSERT INTO employees (name, role, hourly_rate, phone, active) VALUES (?,?,?,?,?)",
                    (f_name.strip(), f_role, f_rate, f_phone, int(f_active)),
                )
            else:
                execute(
                    "UPDATE employees SET role=?, hourly_rate=?, phone=?, active=? WHERE name=?",
                    (f_role, f_rate, f_phone, int(f_active), f_name.strip()),
                )
            st.success(f"{f_name} saved."); st.rerun()
        elif saved:
            st.warning("Name is required.")

        if deactivate and pre["id"]:
            execute("UPDATE employees SET active=0 WHERE id=?", (pre["id"],))
            st.success(f"{pre['name']} deactivated."); st.rerun()


# ─────────────────────────────────────────────
#  PAGE: SCHEDULE CALENDAR
# ─────────────────────────────────────────────
elif page == "Schedule Calendar":
    st.title("Schedule Calendar")

    # ── Controls ──────────────────────────────────────────────────────────
    cal_col1, cal_col2, cal_col3, cal_col4 = st.columns([2,1,1,2])
    with cal_col1:
        cal_view = st.radio("View", ["Day", "Week", "Month"], horizontal=True, key="cal_view")
    with cal_col2:
        if st.button("◀ Prev"):
            if cal_view == "Month":
                st.session_state["cal_month"] = st.session_state.get("cal_month", pd.Timestamp(_today_aest().replace(day=1))) - pd.DateOffset(months=1)
            elif cal_view == "Week":
                st.session_state["cal_week_start"] = st.session_state.get("cal_week_start", pd.Timestamp(_today_aest() - pd.Timedelta(days=_today_aest().weekday()))) - pd.Timedelta(days=7)
            else:
                cur_day = st.session_state.get("cal_day", _today_aest())
                if hasattr(cur_day,'date'): cur_day = cur_day.date()
                st.session_state["cal_day"] = cur_day - pd.Timedelta(days=1)
            st.rerun()
    with cal_col3:
        if st.button("Next ▶"):
            if cal_view == "Month":
                st.session_state["cal_month"] = st.session_state.get("cal_month", pd.Timestamp(_today_aest().replace(day=1))) + pd.DateOffset(months=1)
            elif cal_view == "Week":
                st.session_state["cal_week_start"] = st.session_state.get("cal_week_start", pd.Timestamp(_today_aest() - pd.Timedelta(days=_today_aest().weekday()))) + pd.Timedelta(days=7)
            else:
                cur_day = st.session_state.get("cal_day", _today_aest())
                if hasattr(cur_day,'date'): cur_day = cur_day.date()
                st.session_state["cal_day"] = cur_day + pd.Timedelta(days=1)
            st.rerun()
    with cal_col4:
        if st.button("Today", key="cal_today"):
            st.session_state.pop("cal_month", None)
            st.session_state.pop("cal_week_start", None)
            st.rerun()

    # ── Load data ──────────────────────────────────────────────────────────
    assignments = fetch_df("""
        SELECT da.id, da.job_id, da.client, da.employee, da.date, da.note,
               COALESCE(j.sell_price, 0) AS sell_price
        FROM day_assignments da
        LEFT JOIN jobs j ON j.job_id = da.job_id
        WHERE da.employee != '__unassigned__'
        ORDER BY da.date, da.employee
    """)

    labour_logs_cal = fetch_df("""
        SELECT work_date, employee, job_id,
               SUM(hours) AS total_hours
        FROM labour_logs
        GROUP BY work_date, employee, job_id
        ORDER BY work_date
    """)

    public_hols = fetch_df("""
        SELECT holiday_date, name FROM public_holidays ORDER BY holiday_date
    """)
    hols_dict = {}
    if not public_hols.empty:
        for _, h in public_hols.iterrows():
            hols_dict[str(h["holiday_date"])] = str(h["name"])

    today_str = _today_aest().isoformat()

    # Colour map for employees
    EMP_COLORS = ["#2dd4bf","#f59e0b","#a78bfa","#f43f5e","#60a5fa","#4ade80","#fb923c","#e879f9"]
    emp_list = fetch_df("SELECT name FROM employees WHERE active=1 ORDER BY name")
    emp_color_map = {}
    if not emp_list.empty:
        for i, name in enumerate(emp_list["name"].tolist()):
            emp_color_map[name] = EMP_COLORS[i % len(EMP_COLORS)]

    def get_day_assignments(date_str):
        if assignments.empty:
            return []
        return assignments[assignments["date"] == date_str].to_dict("records")

    def get_day_labour(date_str):
        if labour_logs_cal.empty:
            return []
        return labour_logs_cal[labour_logs_cal["work_date"] == date_str].to_dict("records")

    def get_day_assignments_timed(date_str):
        """Get assignments with start/end times for a day."""
        if assignments.empty:
            return []
        day = assignments[assignments["date"] == date_str].copy()
        # Load start/end times
        timed = fetch_df("""
            SELECT id, employee, job_id, start_time, end_time, note
            FROM day_assignments WHERE date=? AND employee != '__unassigned__'
            ORDER BY COALESCE(start_time,'07:00'), employee
        """, (date_str,))
        return timed.to_dict("records") if not timed.empty else []

    def render_day_cell(d, compact=True):
        ds = d.isoformat()
        is_today = ds == today_str
        is_hol = ds in hols_dict
        day_assigns = get_day_assignments(ds)
        day_labour = get_day_labour(ds)

        border = "2px solid #2dd4bf" if is_today else "1px solid #1e2d3d"
        bg = "#0d1f2d" if is_today else "#111c27"

        html = f"<div style='background:{bg};border:{border};border-radius:8px;padding:6px;min-height:80px;margin:2px'>"
        html += f"<div style='font-size:13px;font-weight:700;color:{'#2dd4bf' if is_today else '#94a3b8'}'>{d.day}</div>"

        if is_hol:
            html += f"<div style='font-size:13px;background:#f59e0b22;color:#f59e0b;border-radius:3px;padding:1px 4px;margin-bottom:2px'>{hols_dict[ds][:15]}</div>"

        if not compact:
            # Week view — show time-based blocks
            timed_assigns = get_day_assignments_timed(ds)
            if timed_assigns:
                for a in timed_assigns:
                    color = emp_color_map.get(str(a.get("employee","")), "#64748b")
                    st_time = str(a.get("start_time","") or "")
                    en_time = str(a.get("end_time","") or "")
                    time_str = f"{st_time[:5]}–{en_time[:5]}" if st_time and en_time else st_time[:5] if st_time else ""
                    html += f"<div style='font-size:13px;background:{color}22;color:{color};border-left:3px solid {color};border-radius:3px;padding:3px 5px;margin-bottom:3px'>"
                    if time_str:
                        html += f"<div style='font-size:13px;opacity:0.8'>{time_str}</div>"
                    html += f"<b>{str(a.get('employee',''))[:12]}</b><br>{str(a.get('job_id',''))}"
                    if a.get("note"):
                        html += f"<div style='font-size:13px;opacity:0.7'>{str(a.get('note',''))[:18]}</div>"
                    html += "</div>"
            else:
                for a in day_assigns:
                    color = emp_color_map.get(str(a.get("employee","")), "#64748b")
                    html += f"<div style='font-size:13px;background:{color}22;color:{color};border-left:2px solid {color};border-radius:3px;padding:2px 4px;margin-bottom:2px'>"
                    html += f"<b>{str(a.get('employee',''))[:12]}</b> — {str(a.get('job_id',''))}</div>"
            for l in day_labour:
                color = emp_color_map.get(str(l.get("employee","")), "#64748b")
                html += f"<div style='font-size:13px;color:{color};opacity:0.6;padding:1px 4px'>⏱ {str(l.get('employee',''))[:8]}: {float(l.get('total_hours',0)):.1f}h</div>"
        else:
            # Month view — compact
            for a in day_assigns[:3]:
                color = emp_color_map.get(str(a.get("employee","")), "#64748b")
                html += f"<div style='font-size:13px;background:{color}22;color:{color};border-radius:2px;padding:1px 4px;margin-bottom:1px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis'>{str(a.get('employee',''))[:10]}: {str(a.get('job_id',''))}</div>"
            if len(day_assigns) > 3:
                html += f"<div style='font-size:13px;color:#475569'>+{len(day_assigns)-3} more</div>"

        html += "</div>"
        return html

    # ── Month View ─────────────────────────────────────────────────────────
    if cal_view == "Month":
        cur_month = st.session_state.get("cal_month", pd.Timestamp(date.today().replace(day=1)))
        if hasattr(cur_month, 'date'):
            cur_month_date = cur_month.date().replace(day=1)
        else:
            cur_month_date = cur_month.replace(day=1)

        st.markdown(f"<div style='font-size:20px;font-weight:800;color:#e2e8f0;margin-bottom:12px'>{cur_month_date.strftime('%B %Y')}</div>", unsafe_allow_html=True)

        # Day headers
        day_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
        hdr_cols = st.columns(7)
        for col, dn in zip(hdr_cols, day_names):
            col.markdown(f"<div style='text-align:center;font-size:13px;font-weight:700;color:#475569;padding:4px'>{dn}</div>", unsafe_allow_html=True)

        # Build month grid
        import calendar as _cal
        first_day = cur_month_date
        last_day = (first_day + pd.DateOffset(months=1) - pd.DateOffset(days=1)).date()
        start_weekday = first_day.weekday()  # 0=Mon

        # Pad start
        grid_days = [None] * start_weekday
        d = first_day
        while d <= last_day:
            grid_days.append(d)
            d += pd.Timedelta(days=1)
        while len(grid_days) % 7 != 0:
            grid_days.append(None)

        # Render weeks
        for week_start in range(0, len(grid_days), 7):
            week = grid_days[week_start:week_start+7]
            cols = st.columns(7)
            for col, day in zip(cols, week):
                with col:
                    if day:
                        st.markdown(render_day_cell(day, compact=True), unsafe_allow_html=True)
                    else:
                        st.markdown("<div style='min-height:80px'></div>", unsafe_allow_html=True)

    # ── Day View ───────────────────────────────────────────────────────────
    elif cal_view == "Day":
        sel_day = st.session_state.get("cal_day", _today_aest())
        if hasattr(sel_day, 'date'):
            sel_day = sel_day.date()

        st.markdown(
            f"<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:16px'>"
            f"{sel_day.strftime('%A, %d %B %Y')}</div>",
            unsafe_allow_html=True)

        ds = sel_day.isoformat()
        is_hol = ds in hols_dict
        if is_hol:
            st.markdown(f"<div style='background:#f59e0b22;color:#f59e0b;border-radius:8px;padding:8px 16px;margin-bottom:12px'>🏖️ Public Holiday: {hols_dict[ds]}</div>", unsafe_allow_html=True)

        # Load timed assignments for this day
        day_timed = fetch_df("""
            SELECT da.id, da.employee, da.job_id, da.client,
                   COALESCE(da.start_time,'') AS start_time,
                   COALESCE(da.end_time,'') AS end_time,
                   COALESCE(da.note,'') AS note
            FROM day_assignments da
            WHERE da.date=? AND da.employee != '__unassigned__'
            ORDER BY COALESCE(NULLIF(da.start_time,''),'07:00'), da.employee
        """, (ds,))

        # Load labour logs for this day
        day_labour_d = fetch_df("""
            SELECT employee, job_id, hours, note
            FROM labour_logs WHERE work_date=?
            ORDER BY employee
        """, (ds,))

        # Time grid — 6am to 6pm
        hours = list(range(6, 19))
        st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin-bottom:8px'>SCHEDULE</div>", unsafe_allow_html=True)

        if day_timed.empty:
            st.info("No assignments for this day.")
        else:
            for _, row in day_timed.iterrows():
                color = emp_color_map.get(str(row.get("employee","")), "#64748b")
                st_t = str(row.get("start_time","") or "")
                en_t = str(row.get("end_time","") or "")
                time_disp = f"{st_t[:5]} → {en_t[:5]}" if st_t and en_t else st_t[:5] if st_t else "All day"

                # Calculate hours if times set
                hrs_disp = ""
                if st_t and en_t:
                    try:
                        from datetime import datetime as _dtc
                        _s = _dtc.strptime(st_t[:5], "%H:%M")
                        _e = _dtc.strptime(en_t[:5], "%H:%M")
                        _h = (_e - _s).seconds / 3600
                        hrs_disp = f" · {_h:.1f}h"
                    except: pass

                _note_html = f"<div style='color:#64748b;font-size:13px'>{row.get('note','')}</div>" if row.get('note') else ""
                _day_card = (
                    f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;"
                    f"border-left:4px solid {color};border-radius:10px;"
                    f"padding:14px 18px;margin-bottom:8px;"
                    f"display:flex;align-items:center;gap:16px'>"
                    f"<div style='min-width:120px'>"
                    f"<div style='color:{color};font-weight:700;font-size:14px'>{time_disp}</div>"
                    f"<div style='color:#475569;font-size:12px'>{hrs_disp}</div>"
                    f"</div>"
                    f"<div style='flex:1'>"
                    f"<div style='color:#e2e8f0;font-weight:700;font-size:15px'>{row.get('employee','')}</div>"
                    f"<div style='color:#2dd4bf;font-size:13px'>{row.get('job_id','')} — {row.get('client','')}</div>"
                    f"{_note_html}"
                    f"</div></div>"
                )
                st.markdown(_day_card, unsafe_allow_html=True)

        # Labour logged today
        if not day_labour_d.empty:
            st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin:16px 0 8px'>HOURS LOGGED</div>", unsafe_allow_html=True)
            total_h = day_labour_d["hours"].sum()
            st.markdown(f"<div style='color:#475569;font-size:13px;margin-bottom:8px'>Total: <b style='color:#e2e8f0'>{total_h:.1f}h</b></div>", unsafe_allow_html=True)
            for _, ll in day_labour_d.iterrows():
                color = emp_color_map.get(str(ll.get("employee","")), "#64748b")
                st.markdown(f"<div style='display:flex;gap:12px;padding:6px 0;border-bottom:1px solid #1e2d3d'><span style='color:{color};font-weight:700;min-width:140px'>{ll.get('employee','')}</span><span style='color:#e2e8f0'>{ll.get('job_id','')}</span><span style='color:#2dd4bf;margin-left:auto'>{float(ll.get('hours',0)):.1f}h</span></div>", unsafe_allow_html=True)

    # ── Week View ──────────────────────────────────────────────────────────
    else:
        today_dt = date.today()
        week_start = st.session_state.get("cal_week_start",
            pd.Timestamp(today_dt - pd.Timedelta(days=today_dt.weekday())))
        if hasattr(week_start, 'date'):
            week_start_date = week_start.date()
        else:
            week_start_date = week_start

        week_end_date = week_start_date + pd.Timedelta(days=6)
        st.markdown(
            f"<div style='font-size:20px;font-weight:800;color:#e2e8f0;margin-bottom:12px'>"
            f"Week of {week_start_date.strftime('%d %b')} — {week_end_date.strftime('%d %b %Y')}</div>",
            unsafe_allow_html=True)

        week_days = [week_start_date + pd.Timedelta(days=i) for i in range(7)]
        day_names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]

        # Header row
        hdr_cols = st.columns(7)
        for col, d, dn in zip(hdr_cols, week_days, day_names):
            is_today = d.isoformat() == today_str
            color = "#2dd4bf" if is_today else "#94a3b8"
            col.markdown(
                f"<div style='text-align:center;padding:6px'>"
                f"<div style='font-size:13px;font-weight:700;color:{color}'>{dn}</div>"
                f"<div style='font-size:20px;font-weight:900;color:{color}'>{d.day}</div>"
                f"</div>",
                unsafe_allow_html=True)

        # Day cells — tall week view
        day_cols = st.columns(7)
        for col, d in zip(day_cols, week_days):
            with col:
                st.markdown(render_day_cell(d, compact=False), unsafe_allow_html=True)

    st.divider()

    # ── Assignment form ────────────────────────────────────────────────────
    emp_names = fetch_df("SELECT name FROM employees WHERE active=1 ORDER BY name")["name"].tolist() if True else []
    jobs_list_cal = fetch_df("SELECT job_id, client FROM jobs WHERE archived=0 ORDER BY job_id")
    time_options = [""] + [f"{h:02d}:{m:02d}" for h in range(5,20) for m in [0,30]]

    assign_mode = st.radio("Assignment mode", ["Single day", "Date range (bulk)"], horizontal=True, key="assign_mode")

    if assign_mode == "Single day":
        with st.form("cal_assign_form"):
            ac1, ac2, ac3 = st.columns(3)
            with ac1:
                ca_emp = st.selectbox("Employee", emp_names if emp_names else [""])
                ca_job = st.selectbox("Job", jobs_list_cal["job_id"].tolist() if not jobs_list_cal.empty else [""])
                # Show client for selected job
                if ca_job and not jobs_list_cal.empty and ca_job in jobs_list_cal["job_id"].values:
                    _jcli = str(jobs_list_cal[jobs_list_cal["job_id"]==ca_job]["client"].iloc[0] or "")
                    if _jcli:
                        st.markdown(f"<div style='background:#1e2d3d;border-radius:6px;padding:6px 10px;font-size:13px;color:#2dd4bf;margin-top:-8px'>{ca_job} · {_jcli}</div>", unsafe_allow_html=True)
            with ac2:
                ca_date = st.date_input("Date", value=_today_aest(), format="DD/MM/YYYY")
                ca_note = st.text_input("Note", placeholder="e.g. Install gutters")
            with ac3:
                ca_start = st.selectbox("Start time", time_options, index=5)
                ca_end   = st.selectbox("End time",   time_options, index=19)
            if st.form_submit_button("Add Assignment", type="primary"):
                _cli = str(jobs_list_cal[jobs_list_cal["job_id"]==ca_job]["client"].iloc[0]) if not jobs_list_cal.empty and ca_job in jobs_list_cal["job_id"].values else ""
                execute("INSERT INTO day_assignments (job_id,client,employee,date,note,start_time,end_time) VALUES (?,?,?,?,?,?,?)",
                    (ca_job, _cli, ca_emp, ca_date.isoformat(), ca_note, ca_start, ca_end))
                st.success(f"✅ {ca_emp} → {ca_job} on {ca_date.strftime('%d %b')}")
                st.rerun()
    else:
        # ── Bulk date range ────────────────────────────────────────────────
        with st.form("cal_bulk_form"):
            bc1, bc2 = st.columns(2)
            with bc1:
                bulk_emps = st.multiselect("Employees", emp_names, help="Select one or more")
                bulk_job  = st.selectbox("Job", jobs_list_cal["job_id"].tolist() if not jobs_list_cal.empty else [""])
                if bulk_job and not jobs_list_cal.empty and bulk_job in jobs_list_cal["job_id"].values:
                    _bjcli = str(jobs_list_cal[jobs_list_cal["job_id"]==bulk_job]["client"].iloc[0] or "")
                    if _bjcli:
                        st.markdown(f"<div style='background:#1e2d3d;border-radius:6px;padding:6px 10px;font-size:13px;color:#2dd4bf;margin-top:-8px'>{bulk_job} · {_bjcli}</div>", unsafe_allow_html=True)
                bulk_note = st.text_input("Note", placeholder="e.g. Full install")
            with bc2:
                bulk_start_date = st.date_input("From date", value=_today_aest(), key="bulk_from", format="DD/MM/YYYY")
                bulk_end_date   = st.date_input("To date",   value=_today_aest(), key="bulk_to",  format="DD/MM/YYYY")
                bulk_start_time = st.selectbox("Start time", time_options, index=5, key="bulk_st")
                bulk_end_time   = st.selectbox("End time",   time_options, index=19, key="bulk_et")

            # Day checkboxes
            st.markdown("<div style='font-size:13px;font-weight:600;color:#94a3b8;margin-top:8px'>Include days:</div>", unsafe_allow_html=True)
            dc = st.columns(7)
            day_labels = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
            day_checks = []
            for i, (col, lbl) in enumerate(zip(dc, day_labels)):
                with col:
                    day_checks.append(st.checkbox(lbl, value=(i < 5), key=f"bulk_day_{i}"))

            if st.form_submit_button("Add Bulk Assignments", type="primary"):
                if not bulk_emps:
                    st.error("Select at least one employee.")
                elif bulk_end_date < bulk_start_date:
                    st.error("End date must be after start date.")
                else:
                    _cli = str(jobs_list_cal[jobs_list_cal["job_id"]==bulk_job]["client"].iloc[0]) if not jobs_list_cal.empty and bulk_job in jobs_list_cal["job_id"].values else ""
                    count = 0
                    d = bulk_start_date
                    import datetime as _dtt
                    while d <= bulk_end_date:
                        if day_checks[d.weekday()]:
                            for emp in bulk_emps:
                                execute("INSERT INTO day_assignments (job_id,client,employee,date,note,start_time,end_time) VALUES (?,?,?,?,?,?,?)",
                                    (bulk_job, _cli, emp, d.isoformat(), bulk_note, bulk_start_time, bulk_end_time))
                                count += 1
                        d += _dtt.timedelta(days=1)
                    st.success(f"✅ {count} assignments created for {len(bulk_emps)} employee(s) across {(bulk_end_date - bulk_start_date).days + 1} days!")
                    st.rerun()

    # ── Upcoming assignments list ──────────────────────────────────────────
    st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin:16px 0 8px'>UPCOMING ASSIGNMENTS</div>", unsafe_allow_html=True)
    upcoming = fetch_df("""
        SELECT da.date, da.employee, da.job_id, da.client,
               COALESCE(da.start_time,'') AS start_time,
               COALESCE(da.end_time,'') AS end_time,
               COALESCE(da.note,'') AS note, da.id
        FROM day_assignments da
        WHERE da.date >= ? AND da.employee != '__unassigned__'
        ORDER BY da.date, da.start_time, da.employee
        LIMIT 60
    """, (today_str,))

    if upcoming.empty:
        st.info("No upcoming assignments.")
    else:
        # Group by date
        current_date = None
        for _, row in upcoming.iterrows():
            row_date = str(row["date"])
            # Date header when date changes
            if row_date != current_date:
                current_date = row_date
                is_today = row_date == today_str
                date_label = "Today" if is_today else _fmt_date(row_date)
                st.markdown(
                    f"<div style='background:{'#0d2233' if is_today else '#111c27'};"
                    f"border-left:3px solid {'#2dd4bf' if is_today else '#2a3d4f'};"
                    f"padding:8px 14px;margin:10px 0 4px;border-radius:0 6px 6px 0'>"
                    f"<span style='font-weight:700;color:{'#2dd4bf' if is_today else '#94a3b8'};font-size:14px'>"
                    f"{date_label}</span></div>",
                    unsafe_allow_html=True)

            color = emp_color_map.get(str(row.get("employee","")), "#64748b")
            st_t = str(row.get("start_time","") or "")
            en_t = str(row.get("end_time","") or "")
            time_str = f"{st_t[:5]}–{en_t[:5]}" if st_t and en_t else ""

            rc1, rc2 = st.columns([8,1])
            with rc1:
                st.markdown(
                    f"<div style='display:flex;align-items:center;gap:10px;padding:6px 8px;"
                    f"background:#1e2d3d;border-radius:8px;margin-bottom:3px'>"
                    f"<div style='width:8px;height:8px;border-radius:50%;background:{color};flex-shrink:0'></div>"
                    f"<span style='color:{color};font-weight:700;min-width:130px;font-size:14px'>{row.get('employee','')}</span>"
                    f"<span style='color:#e2e8f0;font-size:14px'>{row.get('job_id','')}</span>"
                    f"<span style='color:#64748b;font-size:13px'>{row.get('client','')}</span>"
                    + (f"<span style='color:#f59e0b;font-size:12px;margin-left:auto'>{time_str}</span>" if time_str else "") +
                    (f"<span style='color:#475569;font-size:12px;margin-left:8px'>{row.get('note','')[:30]}</span>" if row.get('note') else "") +
                    f"</div>", unsafe_allow_html=True)
            with rc2:
                if st.button("🗑", key=f"del_assign_{row['id']}"):
                    execute("DELETE FROM day_assignments WHERE id=?", (int(row["id"]),))
                    st.rerun()


# ─────────────────────────────────────────────
#  PAGE: ACTUAL LABOUR LOG
# ─────────────────────────────────────────────
elif page == "Actual Labour Log":
    st.title("Actual Labour Log")

    if not selected_job:
        st.info("No job selected."); st.stop()

    emp_names = fetch_df("SELECT name FROM employees WHERE active=1 ORDER BY name")["name"].tolist()

    log_df = fetch_df("""
        SELECT id, work_date, job_id, employee, hours, hourly_rate,
               ROUND(hours * hourly_rate,2)AS cost, note
        FROM labour_logs WHERE job_id=? ORDER BY work_date
    """, (selected_job,))

    # ── Summary metric ────────────────────────────────────────────────────
    if not log_df.empty:
        st.metric("Total logged labour cost",
                  f"${(log_df['hours'] * log_df['hourly_rate']).sum():,.2f}")
        st.divider()

    # ── Existing entries with inline edit/delete ──────────────────────────
    if not log_df.empty:
        st.subheader("Logged entries")

        edit_id = st.session_state.get("labour_edit_id")

        for _, row in log_df.iterrows():
            rid = int(row["id"])
            is_editing = edit_id == rid

            with st.container():
                if is_editing:
                    # ── Edit form for this row ────────────────────────────
                    st.markdown(
                        "<div style='background:#f9f9f9;border:1.5px solid #1a1a1a;"
                        "border-radius:10px;padding:14px 16px;margin-bottom:10px'>",
                        unsafe_allow_html=True,
                    )
                    with st.form(f"edit_labour_{rid}"):
                        ec1, ec2, ec3 = st.columns(3)
                        with ec1:
                            e_date = st.date_input(
                                "Date",
                                value=date.fromisoformat(str(row["work_date"])),
                                key=f"ed_{rid}",
                            )
                            e_emp = st.selectbox(
                                "Employee",
                                emp_names,
                                index=emp_names.index(row["employee"])
                                if row["employee"] in emp_names else 0,
                                key=f"ee_{rid}",
                            )
                        with ec2:
                            e_hrs = st.number_input(
                                "Hours", min_value=0.0,
                                value=float(row["hours"]), step=0.5,
                                key=f"eh_{rid}",
                            )
                            e_rate = st.number_input(
                                "Hourly rate ($)", min_value=0.0,
                                value=float(row["hourly_rate"]), step=5.0,
                                key=f"er_{rid}",
                            )
                        with ec3:
                            e_note = st.text_input(
                                "Note", value=str(row["note"] or ""),
                                key=f"en_{rid}",
                            )
                            st.metric(
                                "New cost",
                                f"${e_hrs * e_rate:,.2f}",
                            )

                        sb1, sb2, sb3 = st.columns([1, 1, 4])
                        with sb1:
                            save = st.form_submit_button("Save", type="primary")
                        with sb2:
                            cancel = st.form_submit_button("Cancel")

                        if save:
                            execute(
                                """UPDATE labour_logs
                                   SET work_date=?, employee=?, hours=?,
                                       hourly_rate=?, note=?
                                   WHERE id=?""",
                                (e_date.isoformat(), e_emp, e_hrs,
                                 e_rate, e_note, rid),
                            )
                            st.session_state.pop("labour_edit_id", None)
                            st.success("Entry updated.")
                            st.rerun()

                        if cancel:
                            st.session_state.pop("labour_edit_id", None)
                            st.rerun()

                    st.markdown("</div>", unsafe_allow_html=True)

                else:
                    # ── Read-only row with Edit / Delete buttons ──────────
                    col_info, col_edit, col_del = st.columns([6, 1, 1])
                    with col_info:
                        st.markdown(
                            f"<div style='background:#ffffff;border:1px solid #e8e8e8;"
                            f"border-radius:9px;padding:10px 14px;margin-bottom:6px;"
                            f"display:flex;gap:24px;align-items:center;font-size:13px'>"
                            f"<span style='font-weight:600;min-width:90px'>{row['work_date']}</span>"
                            f"<span style='color:#555;min-width:110px'>{row['employee']}</span>"
                            f"<span><b>{row['hours']}h</b> @ ${row['hourly_rate']:.0f}/hr</span>"
                            f"<span style='font-weight:700;color:#1a1a1a'>${row['cost']:,.2f}</span>"
                            f"<span style='color:#999;font-size:13px'>{row['note'] or ''}</span>"
                            f"</div>",
                            unsafe_allow_html=True,
                        )
                    with col_edit:
                        if st.button("Edit", key=f"edit_{rid}"):
                            st.session_state["labour_edit_id"] = rid
                            st.rerun()
                    with col_del:
                        if st.button("Delete", key=f"del_{rid}"):
                            st.session_state["labour_confirm_del"] = rid
                            st.rerun()

                # ── Delete confirmation ───────────────────────────────────
                if st.session_state.get("labour_confirm_del") == rid:
                    st.warning(
                        f"Delete entry for **{row['employee']}** on **{row['work_date']}**? This cannot be undone."
                    )
                    dc1, dc2 = st.columns([1, 1])
                    with dc1:
                        if st.button("Yes, delete", key=f"conf_del_{rid}", type="primary"):
                            execute("DELETE FROM labour_logs WHERE id=?", (rid,))
                            st.session_state.pop("labour_confirm_del", None)
                            st.success("Entry deleted.")
                            st.rerun()
                    with dc2:
                        if st.button("Cancel", key=f"canc_del_{rid}"):
                            st.session_state.pop("labour_confirm_del", None)
                            st.rerun()
    else:
        st.info("No labour entries for this job yet.")

    # ── Add new entry ─────────────────────────────────────────────────────
    st.divider()
    st.subheader("Add entry")

    # Load full employee details so rate auto-fills when employee is picked
    emp_df_full = fetch_df(
        "SELECT name, hourly_rate FROM employees WHERE active=1 ORDER BY name"
    )
    emp_rate_map = dict(zip(emp_df_full["name"], emp_df_full["hourly_rate"].astype(float)))         if not emp_df_full.empty else {}

    # Employee selectbox lives outside the form so changing it updates the rate
    if not emp_names:
        st.warning("No active employees found — go to the **Employees** page and add your team first, then come back here.")
        st.stop()
    f_emp = st.selectbox("Employee", emp_names, key="new_entry_emp")
    default_rate = emp_rate_map.get(f_emp, 225.0)

    with st.form("labour_form"):
        fc1, fc2, fc3 = st.columns(3)
        with fc1:
            f_date = st.date_input("Work date", value=date.today())
        with fc2:
            f_hrs  = st.number_input("Hours", min_value=0.0, value=8.0, step=0.5)
            f_rate = st.number_input(
                "Hourly rate ($)",
                min_value=0.0,
                value=default_rate,
                step=5.0,
                help=f"Auto-filled from {f_emp}'s employee record — override if needed",
            )
        with fc3:
            f_note = st.text_input("Note", value="")
            st.metric("Cost preview", f"${f_hrs * f_rate:,.2f}")

        if st.form_submit_button("Add entry", type="primary"):
            execute(
                "INSERT INTO labour_logs (work_date, job_id, employee, hours, hourly_rate, note) VALUES (?,?,?,?,?,?)",
                (f_date.isoformat(), selected_job, f_emp, f_hrs, f_rate, f_note),
            )
            st.success(f"Entry added — {f_emp}, {f_hrs}h @ ${f_rate:.0f}/hr.")
            st.rerun()


# ─────────────────────────────────────────────
#  PAGE: MATERIAL INVOICE LOG
# ─────────────────────────────────────────────
elif page == "Material Invoice Log":
    st.title("Material Invoice Log")

    if not selected_job:
        st.info("No job selected."); st.stop()

    inv_df = fetch_df("""
        SELECT id, invoice_date, job_id, supplier, invoice_number, amount, status, note
        FROM material_invoices WHERE job_id=? ORDER BY invoice_date
    """, (selected_job,))

    if not inv_df.empty:
        st.metric("Total material spend", f"${inv_df['amount'].sum():,.2f}")
        st.divider()

    st.subheader("Invoices")
    STATUS_OPTS = ["Entered", "Approved", "Paid"]
    inv_edit_id = st.session_state.get("inv_edit_id")

    if not inv_df.empty:
        for _, row in inv_df.iterrows():
            rid       = int(row["id"])
            is_editing = inv_edit_id == rid

            if is_editing:
                st.markdown(
                    "<div style='background:#1e2d3d;border:1.5px solid #2dd4bf;"
                    "border-radius:10px;padding:14px 16px;margin-bottom:10px'>",
                    unsafe_allow_html=True,
                )
                with st.form(f"edit_inv_{rid}"):
                    ic1, ic2, ic3 = st.columns(3)
                    with ic1:
                        e_date  = st.date_input("Date",           value=date.fromisoformat(str(row["invoice_date"])))
                        e_supp  = st.text_input("Supplier",       value=str(row["supplier"] or ""))
                    with ic2:
                        e_num   = st.text_input("Invoice #",      value=str(row["invoice_number"] or ""))
                        e_amt   = st.number_input("Amount ($)",   min_value=0.0, value=float(row["amount"]), step=100.0)
                    with ic3:
                        e_stat  = st.selectbox("Status", STATUS_OPTS,
                                    index=STATUS_OPTS.index(row["status"]) if row["status"] in STATUS_OPTS else 0)
                        e_note  = st.text_input("Note",           value=str(row["note"] or ""))

                    sb1, sb2 = st.columns([1, 1])
                    with sb1: save   = st.form_submit_button("Save",   type="primary")
                    with sb2: cancel = st.form_submit_button("Cancel")

                    if save:
                        execute("""
                            UPDATE material_invoices
                            SET invoice_date=?, supplier=?, invoice_number=?,
                                amount=?, status=?, note=?
                            WHERE id=?
                        """, (e_date.isoformat(), e_supp, e_num, e_amt, e_stat, e_note, rid))
                        st.session_state.pop("inv_edit_id", None)
                        st.success("Invoice updated."); st.rerun()
                    if cancel:
                        st.session_state.pop("inv_edit_id", None); st.rerun()
                st.markdown("</div>", unsafe_allow_html=True)
            else:
                col_info, col_edit, col_del = st.columns([6, 1, 1])
                with col_info:
                    status_color = {"Entered": "#64748b", "Approved": "#f59e0b", "Paid": "#2dd4bf"}.get(str(row["status"]), "#64748b")
                    st.markdown(
                        f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;"
                        f"border-radius:9px;padding:10px 14px;margin-bottom:6px;"
                        f"display:flex;gap:20px;align-items:center;font-size:13px'>"
                        f"<span style='color:#94a3b8;min-width:90px'>{row['invoice_date']}</span>"
                        f"<span style='color:#e2e8f0;font-weight:600;min-width:120px'>{row['supplier'] or '—'}</span>"
                        f"<span style='color:#94a3b8;min-width:100px'>{row['invoice_number'] or '—'}</span>"
                        f"<span style='color:#2dd4bf;font-weight:700'>${row['amount']:,.2f}</span>"
                        f"<span style='background:#1a2d3a;color:{status_color};padding:2px 10px;"
                        f"border-radius:999px;font-size:13px;font-weight:600'>{row['status']}</span>"
                        f"<span style='color:#475569;font-size:13px'>{row['note'] or ''}</span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                with col_edit:
                    if st.button("Edit", key=f"inv_edit_{rid}"):
                        st.session_state["inv_edit_id"] = rid; st.rerun()
                with col_del:
                    if st.button("Delete", key=f"inv_del_{rid}"):
                        st.session_state["inv_confirm_del"] = rid; st.rerun()

            if st.session_state.get("inv_confirm_del") == rid:
                st.warning(f"Delete invoice **{row['invoice_number'] or rid}** from {row['supplier']}?")
                dc1, dc2 = st.columns([1, 1])
                with dc1:
                    if st.button("Yes, delete", key=f"inv_conf_{rid}", type="primary"):
                        execute("DELETE FROM material_invoices WHERE id=?", (rid,))
                        st.session_state.pop("inv_confirm_del", None)
                        st.success("Deleted."); st.rerun()
                with dc2:
                    if st.button("Cancel", key=f"inv_canc_{rid}"):
                        st.session_state.pop("inv_confirm_del", None); st.rerun()
    else:
        st.info("No invoices for this job yet.")

    st.divider()
    st.subheader("Add invoice")
    with st.form("inv_form"):
        ic1, ic2, ic3 = st.columns(3)
        with ic1:
            f_date = st.date_input("Invoice date", value=date.today())
            f_supp = st.text_input("Supplier")
        with ic2:
            f_num  = st.text_input("Invoice number")
            f_amt  = st.number_input("Amount ($)", min_value=0.0, value=0.0, step=100.0)
        with ic3:
            f_status = st.selectbox("Status", STATUS_OPTS)
            f_note   = st.text_input("Note", value="")
        if st.form_submit_button("Add invoice", type="primary"):
            execute(
                "INSERT INTO material_invoices (invoice_date, job_id, supplier, invoice_number, amount, status, note) VALUES (?,?,?,?,?,?,?)",
                (f_date.isoformat(), selected_job, f_supp, f_num, f_amt, f_status, f_note),
            )
            st.success("Invoice added."); st.rerun()


# ─────────────────────────────────────────────
#  PAGE: QUOTE PDF
# ─────────────────────────────────────────────
elif page == "Quote PDF":
    st.title("Quote PDF")
    st.caption("Generate a professional quote to send to your client.")

    if not selected_job or not job:
        st.info("No job selected."); st.stop()

    if estimate.empty:
        st.warning("No estimate saved for this job — go to Quote Builder first.")
        st.stop()

    sell       = float(job.get("sell_price") or 0)
    mat_total  = estimate["Material Cost"].sum()
    lab_total  = estimate["Labour Cost"].sum()
    cost_total = mat_total + lab_total
    markup_pct = ((sell - cost_total) / cost_total * 100) if cost_total else 0

    # ── Preview ───────────────────────────────────────────────────────────
    st.subheader("Quote summary")
    pc1, pc2, pc3, pc4 = st.columns(4)
    pc1.metric("Client",        job.get("client","—"))
    pc2.metric("Cost Total",    f"${cost_total:,.2f}")
    pc3.metric("Markup",        f"{markup_pct:.1f}%")
    pc4.metric("Quote Total",   f"${sell:,.2f}")

    st.divider()

    # Adjust sell price if needed
    st.subheader("Adjust before generating")
    adj_sell = st.number_input(
        "Final sell price ($)",
        min_value=0.0,
        value=float(sell) if sell > 0 else float(cost_total * 1.2),
        step=100.0,
    )
    if adj_sell != sell:
        if st.button("Update job sell price"):
            execute("UPDATE jobs SET sell_price=? WHERE job_id=?", (adj_sell, selected_job))
            st.success("Sell price updated."); st.rerun()

    st.divider()
    st.subheader("Line items")
    st.dataframe(estimate, width="stretch")

    st.divider()

    # ── Generate PDF ──────────────────────────────────────────────────────
    if st.button("Generate Quote PDF", type="primary"):
        job_for_pdf = dict(job)
        job_for_pdf["sell_price"] = adj_sell
        try:
            pdf_buf = generate_quote_pdf(job_for_pdf, estimate)
            st.download_button(
                label="Download Quote PDF",
                data=pdf_buf,
                file_name=f"Quote_{selected_job}_{date.today().isoformat()}.pdf",
                mime="application/pdf",
                type="primary",
            )
            st.success("Quote PDF ready — click Download above.")
        except Exception as e:
            st.error(f"PDF generation failed: {e}")


# ─────────────────────────────────────────────
#  PAGE: PERFORMANCE CENTRE
# ─────────────────────────────────────────────
elif page == "Performance Centre":
    st.title("Performance Centre")

    if not selected_job or not job:
        st.info("No job selected."); st.stop()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tender Profit %", f"{lp.get('tender_profit_pct', 0) * 100:.1f}%")
    c2.metric("Live Profit %",   f"{lp.get('live_profit_pct', 0) * 100:.1f}%")
    c3.metric("Profit Drift",    f"{lp.get('profit_drift', 0) * 100:+.1f}%")
    c4.metric("Grade",           lp.get("grade", "—"))

    st.divider()
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Estimator")
        st.write(f"Sell price: **${lp.get('sell_price', 0):,.0f}**")
        st.write(f"Tender profit: **{lp.get('tender_profit_pct', 0) * 100:.1f}%**")
        st.write(f"Live profit: **{lp.get('live_profit_pct', 0) * 100:.1f}%**")
        st.write(f"Running cost: **${lp.get('running_cost', 0):,.0f}**")
        st.write(f"Live cost base: **${lp.get('live_cost', 0):,.0f}**")

    with col2:
        st.subheader("Labour")
        badge(lab.get("labour_health", "—"))
        st.write(f"Tender budget: **${lab.get('tender_labour', 0):,.0f}**")
        st.write(f"Scheduled days: **{lab.get('scheduled_days', 0)}**")
        st.write(f"Scheduled cost: **${lab.get('scheduled_cost', 0):,.0f}**")
        st.write(f"Actual logged: **${lab.get('actual_labour', 0):,.0f}**")
        st.write(f"Projected: **${lab.get('projected_labour', 0):,.0f}**")
        st.write(f"Variance: **${lab.get('labour_variance', 0):+,.0f}**")

    st.divider()
    st.subheader("Material")
    badge(mat.get("material_health", "—"))
    m1, m2, m3 = st.columns(3)
    m1.metric("Tender budget", f"${mat.get('tender_material', 0):,.0f}")
    m2.metric("Actual spend",  f"${mat.get('actual_material', 0):,.0f}")
    m3.metric("Variance",      f"${mat.get('material_variance', 0):+,.0f}")

    if not estimate.empty:
        st.divider()
        st.subheader("Estimate lines")
        st.dataframe(estimate, width="stretch")


# ─────────────────────────────────────────────
#  PAGE: TENDER REVIEW
# ─────────────────────────────────────────────
elif page == "Tender Review":
    st.title("Tender Review")
    st.caption("Full risk check before sending a quote. Review labour, material and margin.")

    if not selected_job or not job:
        st.info("No job selected."); st.stop()

    if estimate.empty:
        st.warning("No estimate saved for this job yet — go to Quote Builder first.")
        st.stop()

    sell       = lp.get("sell_price", 0)
    mat_budget = mat.get("tender_material", 0)
    lab_budget = lab.get("tender_labour", 0)
    rc         = lp.get("running_cost", 0)
    true_cost  = mat_budget + lab_budget + rc
    net_profit = sell - true_cost
    net_pct    = (net_profit / sell * 100) if sell else 0

    # ── Header metrics ────────────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Sell Price",      f"${sell:,.0f}")
    c2.metric("Material Budget", f"${mat_budget:,.0f}")
    c3.metric("Labour Budget",   f"${lab_budget:,.0f}")
    c4.metric("Running Cost",    f"${rc:,.0f}")
    c5.metric("Net Profit %",    f"{net_pct:.1f}%")

    st.divider()

    # ── Risk checks ───────────────────────────────────────────────────────
    st.subheader("Risk checks")

    def risk_row(label, status, comment):
        color = {"PASS": "#2dd4bf", "WARN": "#f59e0b", "FAIL": "#f43f5e"}.get(status, "#64748b")
        icon  = {"PASS": "✓", "WARN": "!", "FAIL": "✗"}.get(status, "?")
        st.markdown(
            f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:9px;"
            f"padding:10px 16px;margin-bottom:6px;display:flex;align-items:center;gap:14px'>"
            f"<span style='background:{color};color:#0f172a;font-weight:800;font-size:13px;"
            f"width:24px;height:24px;border-radius:50%;display:flex;align-items:center;"
            f"justify-content:center;flex-shrink:0'>{icon}</span>"
            f"<span style='font-weight:600;font-size:13px;color:#e2e8f0;min-width:200px'>{label}</span>"
            f"<span style='font-size:13px;color:#94a3b8'>{comment}</span>"
            f"</div>",
            unsafe_allow_html=True,
        )

    # Material check
    if mat_budget > 0:
        risk_row("Material coverage", "PASS", f"Material budget ${mat_budget:,.0f} derived from estimate lines.")
    else:
        risk_row("Material coverage", "FAIL", "No material budget set — run estimate in Quote Builder first.")

    # Labour check
    if lab_budget > 0:
        risk_row("Labour coverage", "PASS", f"Labour budget ${lab_budget:,.0f} derived from estimate lines.")
    else:
        risk_row("Labour coverage", "FAIL", "No labour budget set — run estimate in Quote Builder first.")

    # Margin check
    if net_pct >= 20:
        risk_row("Profit level", "PASS", f"{net_pct:.1f}% net profit after running costs — healthy margin.")
    elif net_pct >= 10:
        risk_row("Profit level", "WARN", f"{net_pct:.1f}% net profit — acceptable but review before sending.")
    else:
        risk_row("Profit level", "FAIL", f"{net_pct:.1f}% net profit — below benchmark. Check your rates.")

    # Sell price check
    if sell > 0:
        risk_row("Sell price set", "PASS", f"Quote value ${sell:,.0f} saved to job.")
    else:
        risk_row("Sell price set", "FAIL", "No sell price — save estimate to job first.")

    st.divider()

    # ── Quoted item breakdown ─────────────────────────────────────────────
    st.subheader("Quoted item breakdown")
    review_df = estimate.copy()
    review_df["Review"] = "REVIEWED"
    st.dataframe(review_df, width="stretch")

    st.divider()

    # ── Estimator notes ───────────────────────────────────────────────────
    st.subheader("Estimator notes")
    tender_note = st.text_area(
        "Notes before sending",
        placeholder="e.g. Allow for access issues on north side. Crane required week 2.",
        height=100,
        key="tender_note",
    )
    if st.button("Mark as reviewed and ready to send", type="primary"):
        upsert_job(
            selected_job,
            job.get("client", ""), job.get("address", ""),
            job.get("estimator", ""), "Tender Review"
        )
        st.success(f"Job {selected_job} marked as Tender Review. Ready to generate quote.")


# ─────────────────────────────────────────────
#  PAGE: PRE-LIVE HANDOVER
# ─────────────────────────────────────────────
elif page == "Pre-Live Handover":
    st.title("Pre-Live Handover")
    st.caption("Bridge between estimating and operations. Lock in days allowed, daily target and site notes.")

    if not selected_job or not job:
        st.info("No job selected."); st.stop()

    sell       = lp.get("sell_price", 0)
    lab_budget = lab.get("tender_labour", 0)
    mat_budget = mat.get("tender_material", 0)
    net_pct    = lp.get("tender_profit_pct", 0)

    # ── Locked financials from tender ────────────────────────────────────
    st.subheader("Locked from tender")
    lc1, lc2, lc3, lc4 = st.columns(4)
    lc1.metric("Sell Price",      f"${sell:,.0f}")
    lc2.metric("Labour Budget",   f"${lab_budget:,.0f}")
    lc3.metric("Material Budget", f"${mat_budget:,.0f}")
    lc4.metric("Tender Profit %", f"{net_pct * 100:.1f}%")

    st.divider()

    # ── Ops planning inputs ───────────────────────────────────────────────
    st.subheader("Operations plan")

    emp_names_ho = fetch_df(
        "SELECT name FROM employees WHERE active=1 ORDER BY name"
    )["name"].tolist()

    with st.form("handover_form"):
        hc1, hc2 = st.columns(2)
        with hc1:
            h_crew      = st.selectbox("Assigned crew / leading hand",
                            emp_names_ho if emp_names_ho else [""])
            h_days      = st.number_input("Days allowed on site", min_value=1, value=5, step=1)
            h_start     = st.date_input("Expected start date", value=date.today())
        with hc2:
            h_daily_target = lab_budget / h_days if h_days else 0
            st.metric("Daily labour target", f"${h_daily_target:,.0f}")
            h_risks     = st.text_area("Site risks / access notes",
                            placeholder="e.g. Steep roof — harness required. No site access Fridays.",
                            height=80)
            h_notes     = st.text_area("Handover notes to ops",
                            placeholder="e.g. Material delivery booked for Day 1 morning.",
                            height=80)

        if st.form_submit_button("Complete handover — move to Live Job", type="primary"):
            upsert_job(
                selected_job,
                job.get("client", ""), job.get("address", ""),
                job.get("estimator", ""), "Live Job"
            )
            # Create schedule blocks for the days allowed
            bdate = pd.bdate_range(h_start, periods=h_days)
            for bd in bdate:
                execute(
                    "INSERT INTO day_assignments (job_id, client, employee, date, note) VALUES (?,?,?,?,?)",
                    (selected_job, job.get("client",""), h_crew,
                     bd.date().isoformat(), h_notes or "Handover block"),
                )
            st.success(
                f"Handover complete. {selected_job} is now Live. "
                f"{h_days} day blocks created for {h_crew} starting {h_start.isoformat()}."
            )
            st.rerun()

    st.divider()

    # ── Handover summary card ─────────────────────────────────────────────
    st.subheader("Estimate breakdown for ops")
    if not estimate.empty:
        # Show only section totals — ops doesn't need line-by-line
        summary = estimate.groupby("Section", as_index=False)[
            ["Material Cost", "Labour Cost", "Total Cost"]
        ].sum()
        st.dataframe(summary, width="stretch")


# ─────────────────────────────────────────────
#  PAGE: RECIPES
# ─────────────────────────────────────────────
elif page == "Assemblies":
    st.title("Recipe Builder")
    st.caption("Build reusable assemblies. Apply them in Quote Builder to auto-populate quantities.")

    ITEM_TYPES = ["Material", "Labour", "MatLab"]
    COMMON_UOMS = ["m2","lm","ea","Ea","each","m","kg","hr","set","roll","sheet","bag"]

    # ── Create new recipe ─────────────────────────────────────────────────
    with st.expander("+ Create new recipe", expanded=False):
        with st.form("new_recipe_form"):
            nc1, nc2 = st.columns(2)
            with nc1:
                nr_name  = st.text_input("Recipe name", placeholder="e.g. Colorbond Roof Install")
                nr_uom   = st.text_input("Unit measure", value="m2",
                               help="The single measurement you enter per job (m2, lm, ea)")
            with nc2:
                nr_notes = st.text_area("Notes", height=80, placeholder="Internal notes about this recipe")
            if st.form_submit_button("Create recipe", type="primary"):
                if nr_name.strip():
                    execute(
                        "INSERT INTO recipes (name, unit_measure, notes) VALUES (?,?,?)",
                        (nr_name.strip(), nr_uom.strip() or "m2", nr_notes.strip()),
                    )
                    st.success(f"Recipe '{nr_name}' created."); st.rerun()
                else:
                    st.warning("Recipe name is required.")

    # ── List recipes ──────────────────────────────────────────────────────
    recipes_df = fetch_df("SELECT id, name, unit_measure, notes FROM recipes ORDER BY name")

    if recipes_df.empty:
        st.info("No recipes yet — create your first one above.")
    else:
        for _, rec in recipes_df.iterrows():
            rid   = int(rec["id"])
            r_items_df = fetch_df(
                "SELECT * FROM recipe_items WHERE recipe_id=? ORDER BY sort_order, id", (rid,)
            )
            unit_price = sum(
                float(r["unit_qty"]) * (float(r["material_rate"]) + float(r["labour_rate"]))
                for _, r in r_items_df.iterrows()
            ) if not r_items_df.empty else 0

            with st.expander(
                f"**{rec['name']}** — per {rec['unit_measure']}  |  "
                f"${unit_price:,.2f} / {rec['unit_measure']}  |  "
                f"{len(r_items_df)} items",
                expanded=False
            ):
                # Edit recipe header
                with st.form(f"edit_recipe_{rid}"):
                    ec1, ec2, ec3 = st.columns([3,1,3])
                    with ec1:
                        e_name  = st.text_input("Name",         value=rec["name"])
                    with ec2:
                        e_uom   = st.text_input("Unit measure",  value=rec["unit_measure"])
                    with ec3:
                        e_notes = st.text_input("Notes",         value=rec["notes"] or "")
                    sb1, sb2 = st.columns([1,4])
                    with sb1:
                        if st.form_submit_button("Save", type="primary"):
                            execute("UPDATE recipes SET name=?, unit_measure=?, notes=? WHERE id=?",
                                    (e_name, e_uom, e_notes, rid))
                            st.success("Saved."); st.rerun()
                    with sb2:
                        if st.form_submit_button("Delete recipe"):
                            execute("DELETE FROM recipe_items WHERE recipe_id=?", (rid,))
                            execute("DELETE FROM recipes WHERE id=?", (rid,))
                            st.success("Deleted."); st.rerun()

                st.divider()

                # ── Recipe items ──────────────────────────────────────────
                st.markdown(
                    "<div style='display:flex;gap:8px;font-size:13px;font-weight:700;"
                    "color:#475569;text-transform:uppercase;letter-spacing:.06em;"
                    "padding:4px 0;margin-bottom:4px'>"
                    "<span style='flex:3'>Description</span>"
                    "<span style='flex:1'>Type</span>"
                    "<span style='flex:1'>Unit Qty</span>"
                    "<span style='flex:1'>UOM</span>"
                    "<span style='flex:1'>Mat rate $</span>"
                    "<span style='flex:1'>Lab rate $</span>"
                    "<span style='flex:1'>Unit cost</span>"
                    "<span style='width:60px'></span>"
                    "</div>",
                    unsafe_allow_html=True,
                )

                if not r_items_df.empty:
                    for _, ri in r_items_df.iterrows():
                        riid = int(ri["id"])
                        with st.form(f"edit_ri_{riid}"):
                            rc = st.columns([3,1,1,1,1,1,1,1])
                            with rc[0]: e_desc  = st.text_input("", value=str(ri["description"]), label_visibility="collapsed")
                            with rc[1]: e_type  = st.selectbox("", ITEM_TYPES,
                                            index=ITEM_TYPES.index(ri["item_type"]) if ri["item_type"] in ITEM_TYPES else 0,
                                            label_visibility="collapsed")
                            with rc[2]: e_uqty  = st.number_input("", value=float(ri["unit_qty"]),  min_value=0.0, step=0.001, format="%.3f", label_visibility="collapsed")
                            with rc[3]: e_iuom  = st.text_input("",  value=str(ri["uom"]),           label_visibility="collapsed")
                            with rc[4]: e_mrate = st.number_input("", value=float(ri["material_rate"]), min_value=0.0, step=0.5, label_visibility="collapsed")
                            with rc[5]: e_lrate = st.number_input("", value=float(ri["labour_rate"]),   min_value=0.0, step=0.5, label_visibility="collapsed")
                            with rc[6]:
                                unit_cost = e_uqty * (e_mrate + e_lrate)
                                st.markdown(f"<div style='padding:8px 0;font-size:13px;color:#2dd4bf;font-weight:700'>${unit_cost:,.2f}</div>", unsafe_allow_html=True)
                            sc1, sc2 = st.columns([1,1])
                            with sc1:
                                if st.form_submit_button("Save"):
                                    execute("""UPDATE recipe_items SET description=?,item_type=?,
                                               unit_qty=?,uom=?,material_rate=?,labour_rate=?
                                               WHERE id=?""",
                                            (e_desc,e_type,e_uqty,e_iuom,e_mrate,e_lrate,riid))
                                    st.rerun()
                            with sc2:
                                if st.form_submit_button("Remove"):
                                    execute("DELETE FROM recipe_items WHERE id=?", (riid,))
                                    st.rerun()

                st.divider()

                # ── Add item to recipe ────────────────────────────────────
                st.subheader("Add item")
                with st.form(f"add_ri_{rid}"):
                    ac = st.columns([3,1,1,1,1,1])
                    with ac[0]: a_desc  = st.text_input("Description", placeholder="e.g. Kliplok 0.48 STD Colorbond")
                    with ac[1]: a_type  = st.selectbox("Type",      ITEM_TYPES)
                    with ac[2]: a_uqty  = st.number_input("Unit qty", min_value=0.0, value=1.0, step=0.001, format="%.3f",
                                            help=f"Qty per 1 {rec['unit_measure']}")
                    with ac[3]: a_uom   = st.selectbox("UOM", COMMON_UOMS)
                    with ac[4]: a_mrate = st.number_input("Mat rate", min_value=0.0, value=0.0, step=0.5)
                    with ac[5]: a_lrate = st.number_input("Lab rate", min_value=0.0, value=0.0, step=0.5)
                    if st.form_submit_button("Add item", type="primary"):
                        if a_desc.strip():
                            execute("""INSERT INTO recipe_items
                                        (recipe_id,description,item_type,unit_qty,uom,material_rate,labour_rate)
                                        VALUES (?,?,?,?,?,?,?)""",
                                    (rid, a_desc.strip(), a_type, a_uqty, a_uom, a_mrate, a_lrate))
                            st.success("Item added."); st.rerun()
                        else:
                            st.warning("Description required.")


# ─────────────────────────────────────────────
#  PAGE: PIPELINE
# ─────────────────────────────────────────────
elif page == "Pipeline":
    st.title("Pipeline Planner")
    st.caption("Drag cards between months. Click to edit. Track follow-ups, status and secured jobs.")

    PIPELINE_PATH = Path(__file__).with_name("pipeline.html")

    # ── View toggle ───────────────────────────────────────────────────────
    view_mode = st.radio("View", ["List View", "Kanban Board (beta)"], horizontal=True, label_visibility="collapsed")

    # ── Add job form ──────────────────────────────────────────────────────
    with st.expander("+ Add job to pipeline", expanded=False):
        jobs_avail = fetch_df("""
            SELECT j.job_id, j.client, j.sell_price, j.stage,
                   j.address, j.estimator
            FROM jobs j
            WHERE j.archived=0
            AND j.stage IN ('Quoted','Pre-Live Handover','Handover','Live Job','Completed')
            ORDER BY j.job_id
        """)

        # Job selector OUTSIDE form so value auto-updates
        p_job = st.selectbox("Select job",
            jobs_avail["job_id"].tolist() if not jobs_avail.empty else [""],
            key="pipe_job_select")

        # Auto-pull sell price, client and contacts when job selected
        _pipe_sell   = 0.0
        _pipe_client = ""
        _pipe_contact = ""
        _pipe_phone   = ""
        _pipe_email   = ""
        if p_job and not jobs_avail.empty and p_job in jobs_avail["job_id"].values:
            _prow = jobs_avail[jobs_avail["job_id"]==p_job].iloc[0]
            _pipe_client = str(_prow.get("client","") or "")
            if _prow.get("sell_price") and float(_prow.get("sell_price",0) or 0) > 0:
                _pipe_sell = float(_prow["sell_price"])
            # Pull contact from client register
            _cli_rec = fetch_df("""
                SELECT ca_name, ca_phone, ca_email,
                       pm_name, pm_phone, pm_email,
                       billing_name, billing_email, billing_phone, name, phone, email
                FROM clients WHERE name=? OR company=?
            """, (_pipe_client, _pipe_client))
            if not _cli_rec.empty:
                _cr = _cli_rec.iloc[0]
                # Prefer CA, then PM, then main contact
                _pipe_contact = str(_cr.get("ca_name","") or _cr.get("pm_name","") or _cr.get("name","") or "")
                _pipe_phone   = str(_cr.get("ca_phone","") or _cr.get("pm_phone","") or _cr.get("phone","") or "")
                _pipe_email   = str(_cr.get("ca_email","") or _cr.get("pm_email","") or _cr.get("email","") or "")

        # Show auto-filled value
        if _pipe_sell > 0:
            st.markdown(
                "<div style='background:#0d2233;border:1px solid #2dd4bf;border-radius:8px;"
                "padding:8px 14px;margin-bottom:8px;font-size:15px;color:#2dd4bf'>"
                "✅ Auto-filled from job estimate: <strong>$" + f"{_pipe_sell:,.0f}" + "</strong></div>",
                unsafe_allow_html=True)

        with st.form("pipe_form"):
            pc1, pc2, pc3 = st.columns(3)
            with pc1:
                p_client  = st.text_input("Client", value=_pipe_client)
                p_value   = st.number_input("Value ($)", min_value=0.0,
                    value=_pipe_sell, step=1000.0)
            with pc2:
                p_prob    = st.slider("Probability %", 0, 100, 50, step=5)
                today_p   = date.today()
                months_p  = [f"{y}-{str(m).zfill(2)}" for y in range(today_p.year, today_p.year+3) for m in range(1,13)]
                p_month   = st.selectbox("Target month", months_p, index=today_p.month-1)
                p_secured = st.checkbox("Secured / confirmed")
            with pc3:
                p_contact = st.text_input("Contact name", value=_pipe_contact)
                p_phone   = st.text_input("Contact phone", value=_pipe_phone)
                p_email   = st.text_input("Contact email", value=_pipe_email)
                p_followup= st.date_input("Follow-up date", value=date.today())
                p_notes   = st.text_area("Status notes", height=60,
                    placeholder="e.g. Verbally accepted — price may change")

            if st.form_submit_button("Add to pipeline", type="primary"):
                final_value = p_value if p_value > 0 else _pipe_sell
                execute("""
                    INSERT INTO pipeline
                        (job_id,client,value,probability_pct,target_month,notes,
                         secured,contact_name,contact_phone,contact_email,
                         follow_up_date,status_notes)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                """, (p_job, p_client, final_value, p_prob, p_month, "",
                      int(p_secured), p_contact, p_phone, p_email,
                      p_followup.isoformat(), p_notes))
                st.success(f"{p_job} added — ${final_value:,.0f}"); st.rerun()

    # ── Sync pipeline values from jobs (keep values current) ─────────────
    # If a job has a sell_price, update the pipeline value to match
    sync_jobs = fetch_df("""
        SELECT p.id, j.sell_price, j.client
        FROM pipeline p
        JOIN jobs j ON j.job_id = p.job_id
        WHERE p.archived=0 AND j.sell_price > 0
        AND (p.value = 0 OR p.value IS NULL)
    """)
    for _, sj in sync_jobs.iterrows():
        execute("UPDATE pipeline SET value=?, client=? WHERE id=?",
                (float(sj["sell_price"]), str(sj["client"] or ""), int(sj["id"])))

    # ── Load data ─────────────────────────────────────────────────────────
    pipe_df   = fetch_df("SELECT * FROM pipeline WHERE archived=0 ORDER BY target_month, job_id")
    today_k   = date.today()
    all_months= [f"{y}-{str(m).zfill(2)}" for y in range(today_k.year, today_k.year+2) for m in range(1,13)]
    has_entry = set(pipe_df["target_month"].tolist()) if not pipe_df.empty else set()
    show_months = sorted(set(all_months[:9]) | has_entry)
    month_names_map = {
        f"{y}-{str(m).zfill(2)}": f"{pycal.month_name[m]} {y}"
        for y in range(today_k.year, today_k.year+3) for m in range(1,13)
    }
    today_str = today_k.isoformat()

    # Follow-up alerts
    if not pipe_df.empty:
        overdue = pipe_df[
            (pipe_df.get("follow_up_date","") != "") &
            (pipe_df["follow_up_date"].fillna("") <= today_str) &
            (pipe_df["follow_up_date"].fillna("") != "")
        ] if "follow_up_date" in pipe_df.columns else pd.DataFrame()
        if not overdue.empty:
            st.warning(f"⚠️  **{len(overdue)} follow-up{'s' if len(overdue)>1 else ''}** due today or overdue — check List View for details.")

    # ── Summary stats ─────────────────────────────────────────────────────
    if not pipe_df.empty:
        total_val  = pipe_df["value"].sum()
        wtd_val    = (pipe_df["value"] * pipe_df["probability_pct"] / 100).sum()
        secured_val = pipe_df[pipe_df["secured"]==1]["value"].sum() if "secured" in pipe_df.columns else 0
        job_count  = len(pipe_df)
        sc1,sc2,sc3,sc4 = st.columns(4)
        sc1.metric("Jobs in pipeline", job_count)
        sc2.metric("Total value",      f"${total_val:,.0f}")
        sc3.metric("Weighted value",   f"${wtd_val:,.0f}")
        sc4.metric("Secured",          f"${secured_val:,.0f}")
        st.divider()

    # ── KANBAN VIEW ───────────────────────────────────────────────────────
    if view_mode == "Kanban Board (beta)":
        if not PIPELINE_PATH.exists():
            st.error("pipeline.html not found next to app.py.")
        else:
            cards_list = []
            if not pipe_df.empty:
                for _, r in pipe_df.iterrows():
                    cards_list.append({
                        "id":             int(r["id"]),
                        "job_id":         str(r["job_id"] or ""),
                        "client":         str(r["client"] or ""),
                        "value":          float(r["value"] or 0),
                        "probability_pct":int(r["probability_pct"] or 0),
                        "target_month":   str(r["target_month"] or ""),
                        "notes":          str(r.get("status_notes") or r.get("notes") or ""),
                        "secured":        int(r.get("secured",0) or 0),
                    })

            pipe_html = PIPELINE_PATH.read_text()
            pipe_html = pipe_html.replace("PIPELINE_MONTHS", json.dumps(show_months))
            pipe_html = pipe_html.replace("PIPELINE_CARDS",  json.dumps(cards_list))
            pipe_html = pipe_html.replace("MONTH_NAMES_MAP", json.dumps(month_names_map))

            event = components.html(pipe_html, height=620, scrolling=False)

            if event and isinstance(event, dict):
                action = event.get("action")
                card   = event.get("card", {})
                cid    = int(card.get("id", 0))
                if action == "move" and cid:
                    execute("UPDATE pipeline SET target_month=? WHERE id=?",
                            (card.get("target_month"), cid))
                    st.rerun()
                elif action == "update" and cid:
                    execute("""UPDATE pipeline SET value=?,probability_pct=?,
                               target_month=?,notes=?,status_notes=?,secured=? WHERE id=?""",
                            (card.get("value",0), card.get("probability_pct",0),
                             card.get("target_month",""), card.get("notes",""),
                             card.get("notes",""), int(card.get("secured",0) or 0), cid))
                    st.rerun()
                elif action == "delete" and cid:
                    execute("UPDATE pipeline SET archived=1 WHERE id=?", (cid,))
                    st.rerun()

    # ── LIST VIEW ─────────────────────────────────────────────────────────
    elif view_mode == "List View":
        if pipe_df.empty:
            st.info("No pipeline entries yet.")
        else:
            # Filter controls
            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                f_month = st.selectbox("Filter month", ["All months"] + show_months,
                    format_func=lambda x: x if x=="All months" else month_names_map.get(x,x))
            with fc2:
                f_secured = st.selectbox("Filter status", ["All","Secured only","Unsecured only"])
            with fc3:
                f_followup = st.selectbox("Follow-up", ["All","Due today","Overdue","This week"])

            filtered = pipe_df.copy()
            if f_month != "All months":
                filtered = filtered[filtered["target_month"]==f_month]
            if f_secured == "Secured only":
                filtered = filtered[filtered.get("secured",0)==1] if "secured" in filtered.columns else filtered
            elif f_secured == "Unsecured only":
                filtered = filtered[filtered.get("secured",0)==0] if "secured" in filtered.columns else filtered

            if "follow_up_date" in filtered.columns and f_followup != "All":
                import datetime as _dt2
                today_d = today_k
                week_end = today_d + __import__("datetime").timedelta(days=7)
                filtered["follow_up_date"] = filtered["follow_up_date"].fillna("")
                if f_followup == "Due today":
                    filtered = filtered[filtered["follow_up_date"]==today_str]
                elif f_followup == "Overdue":
                    filtered = filtered[(filtered["follow_up_date"]<today_str) & (filtered["follow_up_date"]!="")]
                elif f_followup == "This week":
                    filtered = filtered[
                        (filtered["follow_up_date"]>=today_str) &
                        (filtered["follow_up_date"]<=week_end.isoformat())
                    ]

            st.markdown(f"**{len(filtered)} jobs**")

            for _, row in filtered.iterrows():
                rid      = int(row["id"])
                prob     = int(row["probability_pct"])
                is_sec   = int(row.get("secured",0) or 0) == 1
                fu_date  = str(row.get("follow_up_date","") or "")
                is_overdue = fu_date and fu_date < today_str
                is_due_today = fu_date == today_str

                if is_sec:
                    border = "2px solid #2dd4bf"
                    bg     = "#0d2a1f"
                elif is_overdue:
                    border = "2px solid #f43f5e"
                    bg     = "#2a0d0d"
                elif is_due_today:
                    border = "2px solid #f59e0b"
                    bg     = "#2a1f0d"
                else:
                    border = "1px solid #2a3d4f"
                    bg     = "#1e2d3d"

                pc = "#2dd4bf" if prob>=75 else "#f59e0b" if prob>=40 else "#f43f5e"
                sec_badge = "<span style='background:#2dd4bf22;color:#2dd4bf;padding:1px 8px;border-radius:999px;font-size:13px;font-weight:700'>SECURED</span>" if is_sec else ""
                overdue_badge = "<span style='background:#f43f5e22;color:#f43f5e;padding:1px 8px;border-radius:999px;font-size:13px;font-weight:700'>FOLLOW-UP OVERDUE</span>" if is_overdue else ""
                due_badge = "<span style='background:#f59e0b22;color:#f59e0b;padding:1px 8px;border-radius:999px;font-size:13px;font-weight:700'>FOLLOW-UP TODAY</span>" if is_due_today else ""

                _contact = str(row.get('contact_name','') or '')
                _phone   = str(row.get('contact_phone','') or '')
                _email   = str(row.get('contact_email','') or '')
                _fuspan  = f"📅 {_fmt_date(fu_date)}" if fu_date else ""
                _notes   = str(row.get('status_notes','') or '')
                _month   = str(month_names_map.get(str(row['target_month']), row['target_month']))

                # Probability bar
                _bar = f"<div style='background:#0f172a;border-radius:999px;height:4px;margin-top:8px'><div style='background:{pc};width:{prob}%;height:4px;border-radius:999px'></div></div>"

                _contact_section = ""
                if _contact or _phone or _email:
                    _contact_section = (
                        "<div style='display:flex;gap:16px;flex-wrap:wrap;margin-top:8px;"
                        "padding:8px 12px;background:#111c27;border-radius:8px'>"
                        + (f"<span style='color:#e2e8f0;font-size:14px;font-weight:600'>👤 {_contact}</span>" if _contact else "")
                        + (f"<span style='color:#64748b;font-size:14px'>📞 {_phone}</span>" if _phone else "")
                        + (f"<span style='color:#64748b;font-size:14px'>✉️ {_email}</span>" if _email else "")
                        + "</div>"
                    )

                st.markdown(
                    "<div style='background:" + bg + ";border:" + border + ";border-radius:12px;"
                    "padding:16px 20px;margin-bottom:6px'>"
                    "<div style='display:flex;align-items:center;gap:10px;margin-bottom:6px;flex-wrap:wrap'>"
                    "<span style='font-weight:800;font-size:18px;color:#f1f5f9'>" + str(row['job_id']) + "</span>"
                    "<span style='font-size:15px;font-weight:600;color:#e2e8f0'>" + str(row.get('client','') or '') + "</span>"
                    "<span style='font-size:13px;background:#1a2d3a;color:#64748b;"
                    "padding:2px 10px;border-radius:999px'>" + _month + "</span>"
                    + sec_badge + overdue_badge + due_badge +
                    "<span style='margin-left:auto;font-size:20px;font-weight:800;color:#2dd4bf'>$" + f"{float(row['value']):,.0f}" + "</span>"
                    "</div>"
                    "<div style='display:flex;gap:16px;align-items:center;flex-wrap:wrap'>"
                    "<span style='color:" + pc + ";font-weight:700;font-size:15px'>" + str(prob) + "%</span>"
                    + (_bar) +
                    (f"<span style='color:#475569;font-size:13px;margin-left:auto'>{_fuspan}</span>" if _fuspan else "") +
                    "</div>"
                    + _contact_section
                    + (f"<div style='font-size:13px;color:#f59e0b;margin-top:6px;font-style:italic'>{_notes}</div>" if _notes else "")
                    + "</div>",
                    unsafe_allow_html=True)

                # Quick action buttons
                qb1, qb2, qb3 = st.columns([1,1,4])
                with qb1:
                    if not is_sec:
                        if st.button("🔒 Secure", key=f"sec_{rid}", type="primary"):
                            execute("UPDATE pipeline SET secured=1 WHERE id=?", (rid,))
                            st.success(f"{row['job_id']} marked as secured!"); st.rerun()
                    else:
                        if st.button("🔓 Unsecure", key=f"unsec_{rid}"):
                            execute("UPDATE pipeline SET secured=0 WHERE id=?", (rid,))
                            st.rerun()
                with qb2:
                    pass

                # Inline edit
                with st.expander(f"Edit {row['job_id']}", expanded=False):
                    with st.form(f"edit_pipe_{rid}"):
                        ep1,ep2,ep3 = st.columns(3)
                        with ep1:
                            e_val    = st.number_input("Value ($)",    min_value=0.0, value=float(row["value"]), step=1000.0)
                            e_prob   = st.slider("Probability %",      0, 100, int(row["probability_pct"]), step=5)
                            e_month  = st.selectbox("Month", show_months,
                                index=show_months.index(row["target_month"]) if row["target_month"] in show_months else 0,
                                format_func=lambda x: month_names_map.get(x,x))
                        with ep2:
                            e_sec    = st.checkbox("Secured",          value=bool(int(row.get("secured",0) or 0)))
                            e_fu     = st.text_input("Follow-up date",  value=str(row.get("follow_up_date","") or ""),
                                         placeholder="YYYY-MM-DD")
                            e_notes  = st.text_area("Status notes",    value=str(row.get("status_notes","") or ""), height=80)
                        with ep3:
                            e_cname  = st.text_input("Contact name",   value=str(row.get("contact_name","") or ""))
                            e_cphone = st.text_input("Contact phone",  value=str(row.get("contact_phone","") or ""))
                            e_cemail = st.text_input("Contact email",  value=str(row.get("contact_email","") or ""))

                        sb1,sb2 = st.columns([1,1])
                        with sb1:
                            if st.form_submit_button("Save", type="primary"):
                                execute("""UPDATE pipeline SET
                                    value=?,probability_pct=?,target_month=?,secured=?,
                                    follow_up_date=?,status_notes=?,
                                    contact_name=?,contact_phone=?,contact_email=?
                                    WHERE id=?""",
                                    (e_val,e_prob,e_month,int(e_sec),
                                     e_fu,e_notes,e_cname,e_cphone,e_cemail,rid))
                                st.success("Saved."); st.rerun()
                        with sb2:
                            if st.form_submit_button("Remove from pipeline"):
                                execute("UPDATE pipeline SET archived=1 WHERE id=?", (rid,))
                                st.rerun()

    # ── Summary table ─────────────────────────────────────────────────────
    if not pipe_df.empty:
        st.divider()
        st.subheader("Monthly summary")
        summary_rows = []
        for mo in show_months:
            mo_df = pipe_df[pipe_df["target_month"]==mo]
            if mo_df.empty: continue
            sec_val = mo_df[mo_df.get("secured",pd.Series(0))==1]["value"].sum() if "secured" in mo_df.columns else 0
            summary_rows.append({
                "Month":       month_names_map.get(mo,mo),
                "Jobs":        len(mo_df),
                "Total ($)":   f"${mo_df['value'].sum():,.0f}",
                "Weighted ($)":f"${(mo_df['value']*mo_df['probability_pct']/100).sum():,.0f}",
                "Secured ($)": f"${sec_val:,.0f}",
            })
        if summary_rows:
            st.dataframe(pd.DataFrame(summary_rows), width="stretch", hide_index=True)


# ─────────────────────────────────────────────
#  PAGE: BUDGET PLANNER
# ─────────────────────────────────────────────
elif page == "Budget Planner":
    st.title("Budget Planner")
    st.caption("Set monthly revenue targets and track secured vs pipeline vs target.")

    from datetime import datetime as _dtbp

    today_b   = date.today()
    # Show 12 months from current month
    months_b  = [f"{(today_b.replace(day=1) + __import__('datetime').timedelta(days=32*i)).strftime('%Y-%m')}"
                 for i in range(12)]
    # Fix month generation cleanly
    months_b = []
    y, m = today_b.year, today_b.month
    for _ in range(12):
        months_b.append(f"{y}-{str(m).zfill(2)}")
        m += 1
        if m > 12:
            m = 1; y += 1

    month_labels = {mo: _dtbp.strptime(mo, "%Y-%m").strftime("%b %Y") for mo in months_b}

    # ── Load / save monthly targets ───────────────────────────────────────
    targets_df = fetch_df("SELECT month, target FROM monthly_targets")
    targets    = dict(zip(targets_df["month"], targets_df["target"].astype(float))) if not targets_df.empty else {}

    # ── Set targets form ──────────────────────────────────────────────────
    with st.expander("Set monthly targets", expanded=not bool(targets)):
        st.caption("Enter your revenue target for each month. Leave at 0 to skip.")
        with st.form("targets_form"):
            cols_t = st.columns(4)
            target_inputs = {}
            for i, mo in enumerate(months_b):
                with cols_t[i % 4]:
                    target_inputs[mo] = st.number_input(
                        month_labels[mo],
                        min_value=0.0,
                        value=float(targets.get(mo, 0)),
                        step=10000.0,
                        format="%.0f",
                        key=f"tgt_{mo}",
                    )
            if st.form_submit_button("Save targets", type="primary"):
                for mo, val in target_inputs.items():
                    existing = fetch_df("SELECT month FROM monthly_targets WHERE month=?", (mo,))
                    if existing.empty:
                        execute("INSERT INTO monthly_targets (month, target) VALUES (?,?)", (mo, val))
                    else:
                        execute("UPDATE monthly_targets SET target=? WHERE month=?", (val, mo))
                st.success("Targets saved."); st.rerun()

    # ── Load pipeline + job data ──────────────────────────────────────────
    pipe_df_b = fetch_df("SELECT * FROM pipeline WHERE archived=0")
    jobs_df_b = fetch_df("""
        SELECT job_id, client, stage, sell_price
        FROM jobs WHERE archived=0 AND stage IN ('Live Job','Completed')
    """)

    # Reload targets after save
    targets_df = fetch_df("SELECT month, target FROM monthly_targets")
    targets    = dict(zip(targets_df["month"], targets_df["target"].astype(float))) if not targets_df.empty else {}

    # ── Build month-by-month data ─────────────────────────────────────────
    month_data = []
    ytd_target   = 0.0
    ytd_secured  = 0.0
    ytd_weighted = 0.0
    cur_mo_str   = f"{today_b.year}-{str(today_b.month).zfill(2)}"

    for mo in months_b:
        tgt = float(targets.get(mo, 0))

        # Secured = Live Job or Completed with sell_price, matched to pipeline target_month
        pipe_mo = pipe_df_b[pipe_df_b["target_month"] == mo] if not pipe_df_b.empty else __import__('pandas').DataFrame()

        # Weighted pipeline (all jobs in this month × probability)
        wtd = float((pipe_mo["value"] * pipe_mo["probability_pct"] / 100).sum()) if not pipe_mo.empty else 0.0

        # Secured from pipeline (prob >= 75%) OR from jobs board (Live/Completed)
        high_prob = float(pipe_mo[pipe_mo["probability_pct"] >= 75]["value"].sum()) if not pipe_mo.empty else 0.0

        # Also count Live/Completed jobs whose pipeline entry is in this month
        secured_job_ids = set(pipe_mo[pipe_mo["probability_pct"] >= 75]["job_id"].tolist()) if not pipe_mo.empty else set()
        confirmed_jobs  = jobs_df_b[jobs_df_b["job_id"].isin(secured_job_ids)] if not jobs_df_b.empty else __import__('pandas').DataFrame()
        secured = float(confirmed_jobs["sell_price"].fillna(0).sum()) if not confirmed_jobs.empty else high_prob

        variance   = secured - tgt
        wtd_gap    = wtd - tgt
        is_past    = mo < cur_mo_str
        is_current = mo == cur_mo_str

        if is_past or is_current:
            ytd_target   += tgt
            ytd_secured  += secured
            ytd_weighted += wtd

        month_data.append({
            "mo": mo, "label": month_labels[mo], "target": tgt,
            "secured": secured, "weighted": wtd,
            "variance": variance, "wtd_gap": wtd_gap,
            "is_current": is_current, "is_past": is_past,
        })

    # ── YTD summary banner ────────────────────────────────────────────────
    st.markdown("""
    <div style="font-size:13px;font-weight:700;letter-spacing:0.12em;
        text-transform:uppercase;color:#2dd4bf;margin-bottom:10px">
        Year to date
    </div>""", unsafe_allow_html=True)

    ytd_var = ytd_secured - ytd_target
    ytd_pct = (ytd_secured / ytd_target * 100) if ytd_target else 0
    yc1,yc2,yc3,yc4 = st.columns(4)
    yc1.metric("YTD Target",   f"${ytd_target:,.0f}")
    yc2.metric("YTD Secured",  f"${ytd_secured:,.0f}")
    yc3.metric("YTD Weighted", f"${ytd_weighted:,.0f}")
    yc4.metric("YTD Variance", f"${ytd_var:+,.0f}")

    st.divider()

    # ── Month cards ───────────────────────────────────────────────────────
    st.markdown("""
    <div style="font-size:13px;font-weight:700;letter-spacing:0.12em;
        text-transform:uppercase;color:#2dd4bf;margin-bottom:12px">
        Monthly breakdown
    </div>""", unsafe_allow_html=True)

    for row in month_data:
        tgt = row["target"]
        if tgt == 0 and row["secured"] == 0 and row["weighted"] == 0:
            continue  # skip months with no data and no target

        secured_pct = min((row["secured"] / tgt * 100) if tgt else 0, 100)
        wtd_pct     = min((row["weighted"] / tgt * 100) if tgt else 0, 100)
        var         = row["variance"]

        if row["is_current"]:
            border = "2px solid #2dd4bf"
            bg     = "#0d2233"
        elif row["is_past"]:
            border = "1px solid #2a3d4f"
            bg     = "#131f2e"
        else:
            border = "1px solid #1e2d3d"
            bg     = "#161f2e"

        if tgt == 0:
            var_color = "#64748b"; var_label = "No target set"
        elif var >= 0:
            var_color = "#2dd4bf"; var_label = f"+${var:,.0f} ahead"
        elif var >= -tgt * 0.1:
            var_color = "#f59e0b"; var_label = f"${abs(var):,.0f} short"
        else:
            var_color = "#f43f5e"; var_label = f"${abs(var):,.0f} short"

        cur_badge = "<span style='background:#2dd4bf22;color:#2dd4bf;font-size:13px;font-weight:700;padding:2px 8px;border-radius:999px;margin-left:8px'>THIS MONTH</span>" if row["is_current"] else ""

        st.markdown(f"""
        <div style="background:{bg};border:{border};border-radius:12px;
            padding:16px 20px;margin-bottom:10px">
            <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:12px">
                <div style="font-size:14px;font-weight:700;color:#e2e8f0">
                    {row['label']}{cur_badge}
                </div>
                <div style="font-size:13px;font-weight:700;color:{var_color}">{var_label}</div>
            </div>
            <div style="display:flex;gap:32px;font-size:13px;margin-bottom:10px">
                <div><span style="color:#475569">Target</span>
                    <span style="color:#e2e8f0;font-weight:700;margin-left:6px">${tgt:,.0f}</span></div>
                <div><span style="color:#475569">Secured</span>
                    <span style="color:#2dd4bf;font-weight:700;margin-left:6px">${row['secured']:,.0f}</span></div>
                <div><span style="color:#475569">Weighted pipeline</span>
                    <span style="color:#f59e0b;font-weight:700;margin-left:6px">${row['weighted']:,.0f}</span></div>
            </div>
            <div style="margin-bottom:5px">
                <div style="display:flex;justify-content:space-between;font-size:13px;color:#475569;margin-bottom:3px">
                    <span>Secured</span><span>{secured_pct:.0f}% of target</span>
                </div>
                <div style="background:#0f172a;border-radius:999px;height:8px">
                    <div style="background:#2dd4bf;width:{secured_pct:.0f}%;height:8px;border-radius:999px"></div>
                </div>
            </div>
            <div>
                <div style="display:flex;justify-content:space-between;font-size:13px;color:#475569;margin-bottom:3px">
                    <span>Weighted pipeline</span><span>{wtd_pct:.0f}% of target</span>
                </div>
                <div style="background:#0f172a;border-radius:999px;height:8px">
                    <div style="background:#f59e0b44;width:{wtd_pct:.0f}%;height:8px;border-radius:999px;
                        border:1px solid #f59e0b66"></div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # ── Summary table ─────────────────────────────────────────────────────
    st.divider()
    st.subheader("Summary table")
    import pandas as _pd
    summary_rows = []
    for row in month_data:
        if row["target"] == 0 and row["secured"] == 0 and row["weighted"] == 0:
            continue
        summary_rows.append({
            "Month":            row["label"],
            "Target ($)":       f"${row['target']:,.0f}",
            "Secured ($)":      f"${row['secured']:,.0f}",
            "Weighted ($)":     f"${row['weighted']:,.0f}",
            "Variance ($)":     f"${row['variance']:+,.0f}",
        })
    if summary_rows:
        st.dataframe(_pd.DataFrame(summary_rows), width="stretch", hide_index=True)

# ─────────────────────────────────────────────
#  PAGE: JOB COSTING REPORT (P&L)
# ─────────────────────────────────────────────
elif page == "Job Costing Report":
    st.title("Job Costing Report")
    st.caption("Full P&L per job — tender vs actual vs revised.")

    all_jobs_pl = fetch_df("""
        SELECT j.job_id, j.client, j.stage, j.job_type,
               j.sell_price, j.tender_material_budget, j.tender_labour_budget,
               j.tender_profit_pct, j.running_cost_pct
        FROM jobs j WHERE j.archived=0 ORDER BY j.job_id
    """)

    if all_jobs_pl.empty:
        st.info("No jobs yet.")
        st.stop()

    # Filter
    fc1,fc2 = st.columns(2)
    with fc1:
        pl_stage = st.selectbox("Filter stage", ["All"] + ["Lead","Take-off","Tender Review","Pre-Live Handover","Live Job","Completed"])
    with fc2:
        pl_type  = st.selectbox("Filter type", ["All"] + ["Residential","Commercial","Industrial","Maintenance","Insurance Repair","Renovation","Variation"])

    if pl_stage != "All": all_jobs_pl = all_jobs_pl[all_jobs_pl["stage"]==pl_stage]
    if pl_type  != "All": all_jobs_pl = all_jobs_pl[all_jobs_pl["job_type"]==pl_type]

    # Build P&L rows
    pl_rows = []
    for _, jr in all_jobs_pl.iterrows():
        jid      = jr["job_id"]
        sell     = float(jr.get("sell_price") or 0)
        t_mat    = float(jr.get("tender_material_budget") or 0)
        t_lab    = float(jr.get("tender_labour_budget") or 0)
        t_cost   = t_mat + t_lab
        rc_pct   = float(jr.get("running_cost_pct") or 0)
        rc       = sell * rc_pct

        # Actuals
        act_mat  = fetch_df("SELECT COALESCE(SUM(amount),0) AS v FROM material_invoices WHERE job_id=? AND status='Entered'", (jid,)).iloc[0]["v"]
        act_lab  = fetch_df("SELECT COALESCE(SUM(hours*hourly_rate),0) AS v FROM labour_logs WHERE job_id=?", (jid,)).iloc[0]["v"]
        act_cost = float(act_mat) + float(act_lab) + rc

        # Variations
        var_appr = fetch_df("SELECT COALESCE(SUM(value),0) AS v FROM variations WHERE job_id=? AND status='Approved'", (jid,)).iloc[0]["v"]
        revised  = sell + float(var_appr)

        # Tender profit
        t_profit = sell - t_cost - rc if sell else 0
        t_pct    = (t_profit/sell*100) if sell else 0

        # Live profit
        l_profit = revised - act_cost
        l_pct    = (l_profit/revised*100) if revised else 0
        drift    = l_pct - t_pct

        if l_pct >= t_pct:          grade = "A"
        elif l_pct >= t_pct - 3:    grade = "B"
        else:                       grade = "C"

        pl_rows.append({
            "Job":           jid,
            "Client":        jr["client"] or "",
            "Type":          jr.get("job_type","") or "",
            "Stage":         jr["stage"] or "",
            "Contract ($)":  f"${sell:,.0f}",
            "Variations ($)":f"${float(var_appr):+,.0f}",
            "Revised ($)":   f"${revised:,.0f}",
            "Tender Cost":   f"${t_cost:,.0f}",
            "Actual Cost":   f"${act_cost:,.0f}",
            "Tender Profit": f"{t_pct:.1f}%",
            "Live Profit":   f"{l_pct:.1f}%",
            "Drift":         f"{drift:+.1f}%",
            "Grade":         grade,
        })

    if pl_rows:
        import pandas as _pd
        pl_df = _pd.DataFrame(pl_rows)
        st.dataframe(pl_df, width="stretch", hide_index=True)
        st.divider()

        # Portfolio totals
        total_contract = all_jobs_pl["sell_price"].fillna(0).sum()
        st.markdown(f"""
        <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-radius:12px;padding:16px 20px">
            <div style="font-size:13px;font-weight:700;color:#2dd4bf;text-transform:uppercase;
                letter-spacing:.1em;margin-bottom:10px">Portfolio summary — {len(pl_rows)} jobs</div>
            <div style="display:flex;gap:32px;font-size:13px">
                <div><span style="color:#64748b">Total contract value</span>
                    <span style="color:#e2e8f0;font-weight:700;margin-left:8px">${total_contract:,.0f}</span></div>
                <div><span style="color:#64748b">A grade jobs</span>
                    <span style="color:#2dd4bf;font-weight:700;margin-left:8px">{sum(1 for r in pl_rows if r['Grade']=='A')}</span></div>
                <div><span style="color:#64748b">B grade jobs</span>
                    <span style="color:#f59e0b;font-weight:700;margin-left:8px">{sum(1 for r in pl_rows if r['Grade']=='B')}</span></div>
                <div><span style="color:#64748b">C grade jobs</span>
                    <span style="color:#f43f5e;font-weight:700;margin-left:8px">{sum(1 for r in pl_rows if r['Grade']=='C')}</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  PAGE: NOTIFICATIONS
# ─────────────────────────────────────────────
elif page == "Notifications":
    st.title("Notifications")
    st.caption("Follow-up reminders and job alerts. Email/SMS delivery requires configuration.")

    today_n = date.today()

    # ── Pipeline follow-ups ───────────────────────────────────────────────
    st.subheader("Pipeline follow-ups")
    pipe_notif = fetch_df("""
        SELECT job_id, client, follow_up_date, status_notes,
               contact_name, contact_phone, contact_email
        FROM pipeline
        WHERE archived=0 AND follow_up_date != '' AND follow_up_date IS NOT NULL
        ORDER BY follow_up_date
    """)

    if not pipe_notif.empty:
        overdue  = pipe_notif[pipe_notif["follow_up_date"] < today_n.isoformat()]
        due_today= pipe_notif[pipe_notif["follow_up_date"] == today_n.isoformat()]
        upcoming = pipe_notif[pipe_notif["follow_up_date"] > today_n.isoformat()]

        for label, df_n, color in [
            ("Overdue",    overdue,   "#f43f5e"),
            ("Due today",  due_today, "#f59e0b"),
            ("Upcoming",   upcoming,  "#2dd4bf"),
        ]:
            if df_n.empty: continue
            st.markdown(f"<div style='font-size:13px;font-weight:700;color:{color};"
                        f"text-transform:uppercase;letter-spacing:.1em;margin:12px 0 6px'>"
                        f"{label} — {len(df_n)}</div>", unsafe_allow_html=True)
            for _, nr in df_n.iterrows():
                st.markdown(f"""
                <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-left:3px solid {color};
                    border-radius:9px;padding:10px 14px;margin-bottom:6px">
                    <div style="display:flex;justify-content:space-between;align-items:center">
                        <div>
                            <span style="font-weight:700;color:#e2e8f0">{nr['job_id']}</span>
                            <span style="color:#64748b;margin-left:8px">{nr['client'] or ''}</span>
                        </div>
                        <span style="color:{color};font-size:13px;font-weight:600">{nr['follow_up_date']}</span>
                    </div>
                    {"<div style='font-size:13px;color:#f59e0b;margin-top:4px'>"+str(nr['status_notes'])+"</div>" if nr.get('status_notes') else ""}
                    {"<div style='font-size:13px;color:#475569;margin-top:4px'>"+str(nr['contact_name'])+" · "+str(nr['contact_phone'] or '')+"</div>" if nr.get('contact_name') else ""}
                </div>
                """, unsafe_allow_html=True)
    else:
        st.info("No follow-up dates set — add them in the Pipeline page.")

    st.divider()

    # ── Payment schedule alerts ───────────────────────────────────────────
    st.subheader("Payment schedule alerts")
    pay_alerts = fetch_df("""
        SELECT ps.job_id, j.client, ps.milestone, ps.amount, ps.due_date, ps.status
        FROM payment_schedule ps
        LEFT JOIN jobs j ON j.job_id = ps.job_id
        WHERE ps.status != 'Paid' AND ps.due_date != '' AND ps.due_date IS NOT NULL
        ORDER BY ps.due_date
    """)

    if not pay_alerts.empty:
        for _, pa in pay_alerts.iterrows():
            due   = str(pa["due_date"] or "")
            is_od = due < today_n.isoformat() if due else False
            is_td = due == today_n.isoformat() if due else False
            pc    = "#f43f5e" if is_od else "#f59e0b" if is_td else "#64748b"
            badge = "OVERDUE" if is_od else "DUE TODAY" if is_td else due
            st.markdown(f"""
            <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-left:3px solid {pc};
                border-radius:9px;padding:10px 14px;margin-bottom:6px;
                display:flex;align-items:center;gap:16px">
                <div style="flex:1">
                    <span style="font-weight:700;color:#e2e8f0">{pa['job_id']}</span>
                    <span style="color:#64748b;margin-left:8px">{pa['client'] or ''}</span>
                    <span style="color:#94a3b8;margin-left:8px;font-size:13px">{pa['milestone']}</span>
                </div>
                <div style="text-align:right">
                    <div style="color:#2dd4bf;font-weight:700">${float(pa['amount']):,.2f}</div>
                    <div style="color:{pc};font-size:13px;font-weight:600">{badge}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No upcoming payment milestones set.")

    st.divider()

    # ── Email/SMS config ──────────────────────────────────────────────────
    st.subheader("Email & SMS setup")
    st.markdown("""
    <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:16px 20px">
        <div style="font-size:13px;color:#94a3b8;line-height:1.8">
            <div style="color:#e2e8f0;font-weight:600;margin-bottom:8px">Coming soon — ready to wire up</div>
            <div>📧 <strong style="color:#e2e8f0">Email notifications</strong> via SendGrid (free up to 100 emails/day)</div>
            <div>📱 <strong style="color:#e2e8f0">SMS notifications</strong> via Twilio (pay per SMS)</div>
            <div style="margin-top:8px;color:#475569">
                Once deployed online, add your API keys and follow-up reminders will send automatically.
                Requires: SENDGRID_API_KEY and TWILIO credentials in environment variables.
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  PAGE: CLIENTS
# ─────────────────────────────────────────────
elif page == "Clients":
    st.title("Client Register")
    st.caption("Manage your clients, contacts and interaction history.")

    CLIENT_TYPES   = ["Builder","Developer","Owner","Property Manager","Insurance","Other"]
    INTERACT_TYPES = ["Call","Email","Meeting","Site Visit","Quote Sent","Follow-up","Other"]

    # ── Open client or list ───────────────────────────────────────────────
    open_client = st.session_state.get("open_client")

    if open_client:
        cli_df = fetch_df("SELECT * FROM clients WHERE id=?", (open_client,))
        if cli_df.empty:
            st.session_state.pop("open_client",None); st.rerun()
        cli = cli_df.iloc[0].to_dict()

        if st.button("← All Clients"):
            st.session_state.pop("open_client",None); st.rerun()

        # Client header — company first
        st.markdown(f"""
        <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-radius:12px;
            padding:16px 20px;margin-bottom:1.2rem;display:flex;align-items:center;gap:16px">
            <div style="width:48px;height:48px;border-radius:50%;background:#1a3a3a;
                border:2px solid #2dd4bf;display:flex;align-items:center;justify-content:center;
                font-size:18px;font-weight:800;color:#2dd4bf;flex-shrink:0">
                {(cli.get('company') or cli.get('name') or 'C')[0].upper()}
            </div>
            <div style="flex:1">
                <div style="font-size:20px;font-weight:800;color:#f1f5f9">{cli.get('company') or cli.get('name') or ''}</div>
                <div style="font-size:13px;color:#94a3b8;margin-top:2px">
                    {cli.get('name') or ''}
                    {(' · ' + cli.get('client_type','')) if cli.get('client_type') else ''}
                </div>
            </div>
            <div style="text-align:right;font-size:13px;color:#64748b">
                <div>{cli.get('phone') or ''}</div>
                <div>{cli.get('email') or ''}</div>
                {("<div style='color:#2dd4bf'>💳 " + str(cli.get('billing_email','')) + "</div>") if cli.get('billing_email') else ''}
            </div>
        </div>""", unsafe_allow_html=True)

        ctab1, ctab2, ctab3, ctab4 = st.tabs(["Details", "Contacts", "Jobs", "Interactions"])

        with ctab1:
            with st.form("edit_client"):
                cc1,cc2 = st.columns(2)
                with cc1:
                    e_comp  = st.text_input("Company *", value=cli.get("company",""))
                    e_name  = st.text_input("Contact name", value=cli.get("name",""))
                    e_type  = st.selectbox("Type", CLIENT_TYPES,
                        index=CLIENT_TYPES.index(cli["client_type"]) if cli.get("client_type") in CLIENT_TYPES else 0)
                    e_abn   = st.text_input("ABN", value=str(cli.get("abn","") or ""), help="Used on invoices")
                    e_phone = st.text_input("Phone", value=cli.get("phone",""))
                with cc2:
                    e_email = st.text_input("Email",   value=cli.get("email",""))
                    e_addr  = st.text_input("Office address", value=cli.get("address",""))
                    e_notes = st.text_area("Notes",    value=cli.get("notes",""), height=100)
                if st.form_submit_button("Save", type="primary"):
                    execute("""UPDATE clients SET name=?,company=?,client_type=?,phone=?,
                               email=?,address=?,notes=?,abn=? WHERE id=?""",
                            (e_name,e_comp,e_type,e_phone,e_email,e_addr,e_notes,e_abn,open_client))
                    st.success("Saved."); st.rerun()

        with ctab2:
            st.markdown("### Contacts")
            with st.form("edit_contacts"):
                st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin-bottom:8px'>💳 BILLING / ACCOUNTS</div>", unsafe_allow_html=True)
                bc1,bc2,bc3 = st.columns(3)
                with bc1:
                    e_bill_name  = st.text_input("Billing contact name",  value=str(cli.get("billing_name","") or ""))
                with bc2:
                    e_bill_email = st.text_input("Billing email", value=str(cli.get("billing_email","") or ""), help="Invoices will be emailed here")
                with bc3:
                    e_bill_phone = st.text_input("Billing phone", value=str(cli.get("billing_phone","") or ""))

                st.markdown("<div style='font-size:13px;font-weight:700;color:#a78bfa;margin:12px 0 8px'>📋 CONTRACT ADMINISTRATOR</div>", unsafe_allow_html=True)
                ca1,ca2,ca3 = st.columns(3)
                with ca1:
                    e_ca_name  = st.text_input("CA name",  value=str(cli.get("ca_name","") or ""))
                with ca2:
                    e_ca_email = st.text_input("CA email", value=str(cli.get("ca_email","") or ""))
                with ca3:
                    e_ca_phone = st.text_input("CA phone", value=str(cli.get("ca_phone","") or ""))

                st.markdown("<div style='font-size:13px;font-weight:700;color:#60a5fa;margin:12px 0 8px'>🏗️ PROJECT MANAGER</div>", unsafe_allow_html=True)
                pm1,pm2,pm3 = st.columns(3)
                with pm1:
                    e_pm_name  = st.text_input("PM name",  value=str(cli.get("pm_name","") or ""))
                with pm2:
                    e_pm_email = st.text_input("PM email", value=str(cli.get("pm_email","") or ""))
                with pm3:
                    e_pm_phone = st.text_input("PM phone", value=str(cli.get("pm_phone","") or ""))

                if st.form_submit_button("💾 Save Contacts", type="primary"):
                    execute("""UPDATE clients SET
                        billing_name=?, billing_email=?, billing_phone=?,
                        ca_name=?, ca_email=?, ca_phone=?,
                        pm_name=?, pm_email=?, pm_phone=?
                        WHERE id=?""",
                        (e_bill_name, e_bill_email, e_bill_phone,
                         e_ca_name, e_ca_email, e_ca_phone,
                         e_pm_name, e_pm_email, e_pm_phone,
                         open_client))
                    st.success("✅ Contacts saved!")
                    st.rerun()

            # Show contact cards
            if cli.get("billing_email"):
                st.markdown(f"""
                <div style='background:#1e2d3d;border:1px solid #2dd4bf33;border-radius:8px;padding:10px 16px;margin-top:8px'>
                    <div style='color:#2dd4bf;font-size:13px;font-weight:700'>💳 BILLING</div>
                    <div style='color:#e2e8f0'>{cli.get('billing_name','—')}</div>
                    <div style='color:#64748b;font-size:13px'>{cli.get('billing_email','')} · {cli.get('billing_phone','')}</div>
                </div>""", unsafe_allow_html=True)

        with ctab3:
            cli_jobs = fetch_df("""
                SELECT job_id, stage, job_type, sell_price FROM jobs
                WHERE client=? AND archived=0 ORDER BY job_id
            """, (cli.get("name",""),))
            if cli_jobs.empty:
                st.info("No jobs linked to this client yet.")
            else:
                total_val = cli_jobs["sell_price"].fillna(0).sum()
                jc1,jc2 = st.columns(2)
                jc1.metric("Total jobs",    len(cli_jobs))
                jc2.metric("Total value",   f"${total_val:,.0f}")
                st.dataframe(cli_jobs, width="stretch", hide_index=True)

        with ctab4:
            interactions = fetch_df("""
                SELECT * FROM client_interactions WHERE client_id=?
                ORDER BY interaction_date DESC
            """, (open_client,))

            if not interactions.empty:
                for _, ir in interactions.iterrows():
                    itype = str(ir.get("type",""))
                    ic_map= {"Call":"#7dd3fc","Email":"#2dd4bf","Meeting":"#a78bfa",
                             "Site Visit":"#4ade80","Quote Sent":"#f59e0b",
                             "Follow-up":"#fb923c","Other":"#64748b"}
                    ic = ic_map.get(itype,"#64748b")
                    st.markdown(f"""
                    <div style="background:#1e2d3d;border:1px solid #2a3d4f;
                        border-left:3px solid {ic};border-radius:9px;
                        padding:10px 14px;margin-bottom:8px">
                        <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px">
                            <span style="background:{ic}22;color:{ic};padding:1px 8px;
                                border-radius:999px;font-size:13px;font-weight:700">{itype}</span>
                            <span style="font-size:13px;color:#64748b">{ir['interaction_date']}</span>
                            {"<span style='font-size:13px;color:#475569;margin-left:auto'>Job: "+str(ir['job_id'])+"</span>" if ir.get('job_id') else ""}
                        </div>
                        <div style="font-size:13px;color:#94a3b8">{ir['notes'] or ''}</div>
                        {"<div style='font-size:13px;color:#f59e0b;margin-top:4px'>Follow-up: "+str(ir['follow_up_date'])+"</div>" if ir.get('follow_up_date') else ""}
                    </div>""", unsafe_allow_html=True)

            st.divider()
            st.subheader("Log interaction")
            jobs_for_client = fetch_df("SELECT job_id FROM jobs WHERE client=? AND archived=0", (cli.get("name",""),))
            job_opts = ["—"] + jobs_for_client["job_id"].tolist() if not jobs_for_client.empty else ["—"]
            with st.form("add_interaction"):
                ic1,ic2 = st.columns(2)
                with ic1:
                    i_type  = st.selectbox("Type", INTERACT_TYPES)
                    i_date  = st.date_input("Date", value=date.today(), format="DD/MM/YYYY")
                    i_job   = st.selectbox("Linked job", job_opts)
                with ic2:
                    i_notes = st.text_area("Notes", height=100,
                        placeholder="e.g. Called re quote — verbally accepted, wants to start March")
                    i_fu    = st.text_input("Follow-up date", placeholder="YYYY-MM-DD")
                if st.form_submit_button("Log", type="primary"):
                    if i_notes.strip():
                        execute("""INSERT INTO client_interactions
                            (client_id,interaction_date,type,notes,follow_up_date,job_id)
                            VALUES (?,?,?,?,?,?)""",
                            (open_client, i_date.isoformat(), i_type, i_notes,
                             i_fu, i_job if i_job != "—" else ""))
                        st.success("Logged."); st.rerun()

    else:
        # ── Client list ───────────────────────────────────────────────────
        hdr1, hdr2 = st.columns([4,1])
        with hdr2:
            if st.button("+ New Client", type="primary"):
                st.session_state["show_new_client"] = True

        if st.session_state.get("show_new_client"):
            with st.form("new_client_form"):
                st.subheader("New Client")
                nc1,nc2 = st.columns(2)
                with nc1:
                    nc_comp = st.text_input("Company *")
                    nc_name = st.text_input("Contact name")
                    nc_type = st.selectbox("Type", CLIENT_TYPES)
                with nc2:
                    nc_phone= st.text_input("Phone")
                    nc_email= st.text_input("Email")
                    nc_addr = st.text_input("Address")
                sb1,sb2 = st.columns([1,4])
                with sb1:
                    if st.form_submit_button("Create", type="primary"):
                        if nc_name.strip():
                            execute("""INSERT INTO clients
                                (name,company,client_type,phone,email,address,created_date)
                                VALUES (?,?,?,?,?,?,?)""",
                                (nc_name,nc_comp,nc_type,nc_phone,nc_email,nc_addr,date.today().isoformat()))
                            st.session_state.pop("show_new_client",None)
                            st.success(f"Client {nc_name} created."); st.rerun()
                        else:
                            st.warning("Name required.")
                with sb2:
                    if st.form_submit_button("Cancel"):
                        st.session_state.pop("show_new_client",None); st.rerun()
            st.divider()

        # Search
        search_c = st.text_input("Search clients", placeholder="Name, company or phone...")

        clients_df = fetch_df("SELECT * FROM clients ORDER BY name")
        if search_c:
            mask = (
                clients_df["name"].str.contains(search_c, case=False, na=False) |
                clients_df["company"].str.contains(search_c, case=False, na=False) |
                clients_df["phone"].str.contains(search_c, case=False, na=False)
            )
            clients_df = clients_df[mask]

        if clients_df.empty:
            st.info("No clients yet — click + New Client to add your first one.")
        else:
            # Alphabetical full-width list
            TYPE_COLORS_CLI = {"Builder":"#7dd3fc","Developer":"#a78bfa","Owner":"#4ade80",
                               "Property Manager":"#f59e0b","Insurance":"#fb923c","Other":"#64748b"}

            # Sort alphabetically by company/name
            clients_df["_sort"] = clients_df.apply(
                lambda r: str(r.get("company") or r.get("name") or "").upper(), axis=1)
            clients_df = clients_df.sort_values("_sort").reset_index(drop=True)

            st.markdown(f"<div style='color:#475569;font-size:14px;margin-bottom:12px'>{len(clients_df)} clients</div>", unsafe_allow_html=True)

            # Column headers
            hc1,hc2,hc3,hc4,hc5,hc6 = st.columns([3,2,2,2,1,1])
            hc1.markdown("<div style='color:#475569;font-size:13px;font-weight:700'>COMPANY / NAME</div>", unsafe_allow_html=True)
            hc2.markdown("<div style='color:#475569;font-size:13px;font-weight:700'>TYPE</div>", unsafe_allow_html=True)
            hc3.markdown("<div style='color:#475569;font-size:13px;font-weight:700'>PHONE</div>", unsafe_allow_html=True)
            hc4.markdown("<div style='color:#475569;font-size:13px;font-weight:700'>EMAIL</div>", unsafe_allow_html=True)
            hc5.markdown("<div style='color:#475569;font-size:13px;font-weight:700'>JOBS</div>", unsafe_allow_html=True)
            hc6.markdown("", unsafe_allow_html=True)
            st.divider()

            for _, cr in clients_df.iterrows():
                cid = int(cr["id"])
                tc_col = TYPE_COLORS_CLI.get(str(cr.get("client_type","")), "#64748b")
                njobs = fetch_df("SELECT COUNT(*) AS n FROM jobs WHERE client=? AND archived=0",
                                (cr.get("name",""),)).iloc[0]["n"]

                cl1,cl2,cl3,cl4,cl5,cl6 = st.columns([3,2,2,2,1,1])
                with cl1:
                    company = str(cr.get("company","") or cr.get("name","") or "")
                    contact = str(cr.get("name","") or "")
                    st.markdown(
                        f"<div style='font-weight:700;font-size:15px;color:#f1f5f9'>{company}</div>"
                        f"<div style='font-size:13px;color:#64748b'>{contact if contact != company else ''}</div>",
                        unsafe_allow_html=True)
                with cl2:
                    st.markdown(f"<span style='background:{tc_col}22;color:{tc_col};border-radius:999px;padding:2px 10px;font-size:13px;font-weight:700'>{cr.get('client_type','')}</span>", unsafe_allow_html=True)
                with cl3:
                    st.markdown(f"<div style='color:#94a3b8;font-size:14px'>{cr.get('phone','') or '—'}</div>", unsafe_allow_html=True)
                with cl4:
                    st.markdown(f"<div style='color:#94a3b8;font-size:13px'>{str(cr.get('email','') or '—')[:25]}</div>", unsafe_allow_html=True)
                with cl5:
                    st.markdown(f"<div style='color:#2dd4bf;font-weight:700;font-size:14px'>{njobs}</div>", unsafe_allow_html=True)
                with cl6:
                    if st.button("Open →", key=f"cli_{cid}", type="primary"):
                        st.session_state["open_client"] = cid; st.rerun()
                st.divider()

# ─────────────────────────────────────────────
#  PAGE: TIMESHEETS
# ─────────────────────────────────────────────
elif page == "Timesheets":
    st.title("Timesheets")
    st.caption("Weekly view of all employee hours across all jobs.")

    import datetime as _dtt

    today_ts = date.today()
    # Get Monday of current week
    monday   = today_ts - _dtt.timedelta(days=today_ts.weekday())

    # Week navigation
    wk1, wk2, wk3 = st.columns([1,2,1])
    with wk1:
        if st.button("← Previous week"):
            st.session_state["ts_week_offset"] = st.session_state.get("ts_week_offset",0) - 1
    with wk3:
        if st.button("Next week →"):
            st.session_state["ts_week_offset"] = st.session_state.get("ts_week_offset",0) + 1
    with wk2:
        if st.button("This week", type="primary"):
            st.session_state["ts_week_offset"] = 0

    offset  = st.session_state.get("ts_week_offset", 0)
    monday  = monday + _dtt.timedelta(weeks=offset)
    week_days = [monday + _dtt.timedelta(days=i) for i in range(7)]
    week_start = monday.isoformat()
    week_end   = week_days[6].isoformat()

    st.markdown(f"""
    <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;
        padding:12px 20px;margin-bottom:1rem;text-align:center">
        <div style="font-size:16px;font-weight:700;color:#e2e8f0">
            Week of {monday.strftime('%d %B %Y')}
        </div>
        <div style="font-size:13px;color:#64748b;margin-top:2px">
            {monday.strftime('%d %b')} — {week_days[6].strftime('%d %b %Y')}
        </div>
    </div>""", unsafe_allow_html=True)

    # Load all labour logs for this week
    week_logs = fetch_df("""
        SELECT ll.employee, ll.work_date, ll.job_id, ll.hours, ll.hourly_rate,
               ROUND(ll.hours*ll.hourly_rate,2)AS cost, ll.note
        FROM labour_logs ll
        WHERE ll.work_date >= ? AND ll.work_date <= ?
        ORDER BY ll.employee, ll.work_date
    """, (week_start, week_end))

    # Load active employees
    emp_df = fetch_df("SELECT name, hourly_rate FROM employees WHERE active=1 ORDER BY name")

    if emp_df.empty:
        st.info("No active employees.")
        st.stop()

    # ── Weekly grid ───────────────────────────────────────────────────────
    day_labels = [d.strftime("%a %d") for d in week_days]

    # Header row
    header_cols = st.columns([2] + [1]*7 + [1])
    header_cols[0].markdown("<div style='font-size:13px;font-weight:700;color:#475569;text-transform:uppercase'>Employee</div>", unsafe_allow_html=True)
    for i, dl in enumerate(day_labels):
        is_today = week_days[i] == today_ts
        color = "#2dd4bf" if is_today else "#475569"
        header_cols[i+1].markdown(f"<div style='font-size:13px;font-weight:700;color:{color};text-align:center'>{dl}</div>", unsafe_allow_html=True)
    header_cols[8].markdown("<div style='font-size:13px;font-weight:700;color:#475569;text-align:center'>TOTAL</div>", unsafe_allow_html=True)

    st.markdown("<hr style='border-color:#2a3d4f;margin:6px 0'>", unsafe_allow_html=True)

    week_total_hrs  = 0
    week_total_cost = 0

    for _, emp in emp_df.iterrows():
        emp_name = emp["name"]
        emp_logs = week_logs[week_logs["employee"]==emp_name] if not week_logs.empty else week_logs.__class__()

        emp_total_hrs  = float(emp_logs["hours"].sum()) if not emp_logs.empty else 0
        emp_total_cost = float(emp_logs["cost"].sum()) if not emp_logs.empty else 0
        week_total_hrs  += emp_total_hrs
        week_total_cost += emp_total_cost

        row_cols = st.columns([2] + [1]*7 + [1])
        row_cols[0].markdown(
            f"<div style='font-size:13px;font-weight:600;color:#e2e8f0;padding:6px 0'>{emp_name}</div>"
            f"<div style='font-size:13px;color:#475569'>${emp['hourly_rate']:.0f}/hr</div>",
            unsafe_allow_html=True)

        for i, day in enumerate(week_days):
            day_str  = day.isoformat()
            day_logs = emp_logs[emp_logs["work_date"]==day_str] if not emp_logs.empty else emp_logs.__class__()
            day_hrs  = float(day_logs["hours"].sum()) if not day_logs.empty else 0
            is_today = day == today_ts

            if day_hrs > 0:
                jobs_on_day = ", ".join(day_logs["job_id"].tolist()) if not day_logs.empty else ""
                bg = "#1a3a3a" if is_today else "#1e2d3d"
                row_cols[i+1].markdown(
                    f"<div style='background:{bg};border:1px solid #2dd4bf;border-radius:6px;"
                    f"padding:4px;text-align:center;margin:2px'>"
                    f"<div style='font-size:13px;font-weight:700;color:#2dd4bf'>{day_hrs:.1f}h</div>"
                    f"<div style='font-size:13px;color:#64748b;white-space:nowrap;overflow:hidden;"
                    f"text-overflow:ellipsis'>{jobs_on_day}</div></div>",
                    unsafe_allow_html=True)
            else:
                bg = "#161f2e" if is_today else "transparent"
                row_cols[i+1].markdown(
                    f"<div style='background:{bg};border:1px solid #1e2d3d;border-radius:6px;"
                    f"padding:4px;text-align:center;margin:2px;min-height:40px'>"
                    f"<div style='font-size:13px;color:#2a3d4f'>—</div></div>",
                    unsafe_allow_html=True)

        row_cols[8].markdown(
            f"<div style='text-align:center;padding:6px 0'>"
            f"<div style='font-size:13px;font-weight:700;color:#e2e8f0'>{emp_total_hrs:.1f}h</div>"
            f"<div style='font-size:13px;color:#2dd4bf'>${emp_total_cost:,.0f}</div></div>",
            unsafe_allow_html=True)

    st.markdown("<hr style='border-color:#2a3d4f;margin:6px 0'>", unsafe_allow_html=True)

    # Totals row
    tot_cols = st.columns([2] + [1]*7 + [1])
    tot_cols[0].markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf'>WEEK TOTAL</div>", unsafe_allow_html=True)
    for i, day in enumerate(week_days):
        day_str  = day.isoformat()
        day_tot  = float(week_logs[week_logs["work_date"]==day_str]["hours"].sum()) if not week_logs.empty else 0
        if day_tot > 0:
            tot_cols[i+1].markdown(
                f"<div style='text-align:center;font-size:13px;font-weight:700;color:#f1f5f9'>{day_tot:.1f}h</div>",
                unsafe_allow_html=True)
    tot_cols[8].markdown(
        f"<div style='text-align:center'>"
        f"<div style='font-size:14px;font-weight:800;color:#2dd4bf'>{week_total_hrs:.1f}h</div>"
        f"<div style='font-size:13px;color:#e2e8f0;font-weight:700'>${week_total_cost:,.0f}</div></div>",
        unsafe_allow_html=True)

    st.divider()

    # ── Breakdown by job ──────────────────────────────────────────────────
    st.subheader("Hours by job this week")
    if not week_logs.empty:
        job_summary = week_logs.groupby("job_id").agg(
            Hours=("hours","sum"),
            Cost=("cost","sum"),
            Employees=("employee", lambda x: ", ".join(sorted(set(x))))
        ).reset_index()
        job_summary.columns = ["Job","Hours","Cost ($)","Employees"]
        job_summary["Cost ($)"] = job_summary["Cost ($)"].apply(lambda x: f"${x:,.2f}")
        job_summary["Hours"]    = job_summary["Hours"].apply(lambda x: f"{x:.1f}h")
        st.dataframe(job_summary, width="stretch", hide_index=True)
    else:
        st.info("No hours logged this week.")

    st.divider()

    # ── Quick add entry ───────────────────────────────────────────────────
    st.subheader("Quick add")
    jobs_ts = fetch_df("SELECT job_id FROM jobs WHERE archived=0 ORDER BY job_id")["job_id"].tolist()
    emp_names_ts = emp_df["name"].tolist()

    with st.form("ts_quick_add"):
        qa1,qa2,qa3,qa4,qa5 = st.columns(5)
        with qa1: qa_date = st.date_input("Date", value=today_ts)
        with qa2: qa_emp  = st.selectbox("Employee", emp_names_ts)
        with qa3: qa_job  = st.selectbox("Job", jobs_ts if jobs_ts else [""])
        with qa4: qa_hrs  = st.number_input("Hours", min_value=0.0, value=8.0, step=0.5)
        with qa5: qa_note = st.text_input("Note", value="")
        # Auto-fill rate from employee
        emp_rate = float(emp_df[emp_df["name"]==qa_emp]["hourly_rate"].iloc[0]) if qa_emp in emp_df["name"].values else 225.0
        if st.form_submit_button("Add entry", type="primary"):
            execute("INSERT INTO labour_logs (work_date,job_id,employee,hours,hourly_rate,note) VALUES (?,?,?,?,?,?)",
                    (qa_date.isoformat(), qa_job, qa_emp, qa_hrs, emp_rate, qa_note))
            st.success(f"Added {qa_hrs}h for {qa_emp} on {qa_job}."); st.rerun()

    st.divider()

    # ── CSV Export ────────────────────────────────────────────────────────
    st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin-bottom:8px'>EXPORT FOR ACCOUNTANT</div>", unsafe_allow_html=True)
    ex1, ex2, ex3 = st.columns(3)
    with ex1:
        export_from = st.date_input("From", value=monday, format="DD/MM/YYYY", key="ts_exp_from")
    with ex2:
        export_to = st.date_input("To", value=week_days[6], format="DD/MM/YYYY", key="ts_exp_to")
    with ex3:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        if st.button("📊 Generate CSV", type="primary"):
            export_logs = fetch_df("""
                SELECT ll.work_date AS Date, ll.employee AS Employee,
                       ll.job_id AS Job, j.client AS Client,
                       ll.hours AS Hours, ll.hourly_rate AS Rate,
                       ROUND(ll.hours*ll.hourly_rate,2) AS Cost,
                       ll.note AS Note
                FROM labour_logs ll
                LEFT JOIN jobs j ON j.job_id=ll.job_id
                WHERE ll.work_date >= ? AND ll.work_date <= ?
                ORDER BY ll.work_date, ll.employee
            """, (export_from.isoformat(), export_to.isoformat()))
            if not export_logs.empty:
                # Format dates as Australian
                export_logs["Date"] = export_logs["Date"].apply(_fmt_date)
                csv_data = export_logs.to_csv(index=False)
                st.download_button(
                    "⬇️ Download Timesheet CSV",
                    data=csv_data,
                    file_name=f"timesheets_{export_from.strftime('%d%m%Y')}_{export_to.strftime('%d%m%Y')}.csv",
                    mime="text/csv",
                    type="primary"
                )
                # Summary stats
                total_hrs = export_logs["Hours"].sum()
                total_cost = export_logs["Cost"].sum()
                st.success(f"{len(export_logs)} entries · {total_hrs:.1f}h total · ${total_cost:,.2f}")
            else:
                st.info("No hours logged in that date range.")

# ─────────────────────────────────────────────
#  PAGE: COMPANY SETTINGS
# ─────────────────────────────────────────────
elif page == "Company Settings":
    st.title("Company Settings")
    st.caption("Your company details appear on all invoices, quotes and purchase orders.")

    settings = get_company_settings()
    sid = int(settings.get("id", 1))

    # Director-only warning
    if user_role not in ["Admin"]:
        st.warning("⚠️ Company settings are restricted to Admin/Director only.")
        st.stop()

    with st.form("company_settings_form"):
        st.subheader("Company details")
        cs1, cs2 = st.columns(2)
        with cs1:
            s_name  = st.text_input("Company name",   value=settings.get("company_name",""))
            s_abn   = st.text_input("ABN",             value=settings.get("abn",""), placeholder="XX XXX XXX XXX")
            s_addr  = st.text_input("Address",         value=settings.get("address",""))
            s_phone = st.text_input("Phone",           value=settings.get("phone",""))
            s_email = st.text_input("Email",           value=settings.get("email",""))
        with cs2:
            s_logo  = st.text_input("Logo text (shown on PDFs)", value=settings.get("logo_text","LIMITLESS"))
            s_terms = st.number_input("Payment terms (days)", min_value=1, max_value=90,
                        value=int(settings.get("payment_terms",14) or 14), step=1)

        st.divider()
        st.subheader("Bank details")
        st.caption("Shown on client invoices for EFT payment.")
        bc1, bc2 = st.columns(2)
        with bc1:
            s_bank  = st.text_input("Bank name",       value=settings.get("bank_name",""))
            s_bsb   = st.text_input("BSB",             value=settings.get("bsb",""), placeholder="XXX-XXX")
        with bc2:
            s_acct  = st.text_input("Account number",  value=settings.get("account_number",""))
            s_aname = st.text_input("Account name",    value=settings.get("account_name",""))

        st.divider()
        st.subheader("Business financials")
        st.caption("Director only — these figures feed into Company P&L, job costing and Financial Health.")
        bf1, bf2 = st.columns(2)
        with bf1:
            s_overhead = st.number_input(
                "Overhead / running cost %",
                min_value=0.0, max_value=50.0,
                value=float(settings.get("overhead_pct", 11.0) or 11.0),
                step=0.5,
                help="Applied to all jobs in P&L. Covers vehicles, insurance, office, tools etc.")
        with bf2:
            s_markup = st.number_input(
                "Default markup %",
                min_value=0.0, max_value=200.0,
                value=float(settings.get("markup_default", 30.0) or 30.0),
                step=1.0,
                help="Default markup applied in Quote Builder. Override per job.")

        st.divider()
        st.subheader("Terms & conditions")
        st.caption("Shown on the last page of every quote PDF. Leave blank to use the default T&C.")
        s_tc      = st.text_area("Terms & conditions text", height=200,
            value=str(settings.get("terms_conditions","") or ""),
            placeholder="Leave blank to use default terms and conditions...")
        s_website = st.text_input("Website (shown on PDFs)",
            value=str(settings.get("website","") or ""))

        if st.form_submit_button("Save settings", type="primary"):
            execute("""UPDATE company_settings SET
                company_name=?, abn=?, address=?, phone=?, email=?,
                bank_name=?, bsb=?, account_number=?, account_name=?,
                payment_terms=?, logo_text=?, overhead_pct=?, markup_default=?,
                terms_conditions=?, website=?
                WHERE id=?""",
                (s_name, s_abn, s_addr, s_phone, s_email,
                 s_bank, s_bsb, s_acct, s_aname, s_terms, s_logo,
                 s_overhead, s_markup, s_tc, s_website, sid))
            st.success("Settings saved — all future PDFs and P&L will use these figures.")

    st.divider()
    st.subheader("Company logo")
    st.caption("Upload your logo to appear on all PDFs — quotes, invoices, purchase orders.")

    logo_col1, logo_col2 = st.columns([1,2])
    with logo_col1:
        logo_upload = st.file_uploader("Upload logo",
            type=["png","jpg","jpeg","svg"],
            key="logo_uploader")
        if logo_upload:
            logo_bytes = logo_upload.read()
            execute("UPDATE company_settings SET logo_data=?, logo_filename=? WHERE id=?",
                    (logo_bytes, logo_upload.name, sid))
            st.success("Logo saved!"); st.rerun()

        # Show current logo or remove button
        current_logo = fetch_df("SELECT logo_data, logo_filename FROM company_settings WHERE id=?", (sid,))
        if not current_logo.empty and current_logo.iloc[0]["logo_data"] is not None:
            try:
                import io as _io2
                logo_data = current_logo.iloc[0]["logo_data"]
                st.image(bytes(logo_data), width=200)
                st.caption(str(current_logo.iloc[0]["logo_filename"] or ""))
                if st.button("Remove logo"):
                    execute("UPDATE company_settings SET logo_data=NULL, logo_filename='' WHERE id=?", (sid,))
                    st.rerun()
            except:
                st.info("Logo saved but preview unavailable.")
        else:
            st.info("No logo uploaded yet.")
    with logo_col2:
        st.markdown("""
        <div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:16px;font-size:14px;color:#94a3b8;line-height:1.9'>
            <div style='color:#e2e8f0;font-weight:600;margin-bottom:8px'>Logo tips</div>
            <div>✅ PNG with transparent background works best</div>
            <div>✅ Minimum 300px wide for crisp PDF rendering</div>
            <div>✅ Landscape/horizontal logos look better on PDFs</div>
            <div>✅ Max file size 2MB</div>
            <div style='margin-top:8px;color:#475569'>Without a logo, your company name text shows instead.</div>
        </div>""", unsafe_allow_html=True)

    st.divider()
    st.subheader("Preview")
    _logo    = str(settings.get('logo_text','LIMITLESS'))
    _coname  = str(settings.get('company_name',''))
    _abn     = str(settings.get('abn','—') or '—')
    _phone   = str(settings.get('phone','—') or '—')
    _email   = str(settings.get('email','—') or '—')
    _addr    = str(settings.get('address','—') or '—')
    _bank    = str(settings.get('bank_name','—') or '—')
    _bsb     = str(settings.get('bsb','—') or '—')
    _acct    = str(settings.get('account_number','—') or '—')
    _aname   = str(settings.get('account_name','—') or '—')
    _terms   = str(settings.get('payment_terms',14))
    st.markdown(
        "<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:12px;padding:24px'>"
        "<div style='font-size:28px;font-weight:800;color:#e2e8f0'>" + _logo + "</div>"
        "<div style='font-size:16px;color:#64748b;margin-top:4px'>" + _coname + "</div>"
        "<div style='font-size:15px;color:#475569;margin-top:10px'>"
        "ABN: " + _abn + " &nbsp;·&nbsp; " + _phone + " &nbsp;·&nbsp; " + _email + "</div>"
        "<div style='font-size:15px;color:#475569;margin-top:4px'>" + _addr + "</div>"
        "<div style='margin-top:12px;padding-top:12px;border-top:1px solid #2a3d4f;"
        "font-size:15px;color:#475569'>"
        "Bank: " + _bank + " &nbsp;·&nbsp; BSB: " + _bsb + " &nbsp;·&nbsp; "
        "Account: " + _acct + " &nbsp;·&nbsp; " + _aname + "</div>"
        "<div style='font-size:15px;color:#2dd4bf;margin-top:6px;font-weight:600'>"
        "Payment terms: " + _terms + " days</div>"
        "</div>",
        unsafe_allow_html=True)

# ─────────────────────────────────────────────
#  PAGE: COMPANY P&L
# ─────────────────────────────────────────────
elif page == "Company P&L":
    st.title("Company P&L")
    st.caption("Full business financial overview — revenue, costs, pipeline and labour utilisation.")

    import datetime as _dtpl

    # ── Date range filter ─────────────────────────────────────────────────
    today_pl  = date.today()
    fy_start  = date(today_pl.year if today_pl.month >= 7 else today_pl.year - 1, 7, 1)

    fc1, fc2, fc3 = st.columns([2,2,2])
    with fc1:
        date_from = st.date_input("From", value=fy_start)
    with fc2:
        date_to   = st.date_input("To",   value=today_pl)
    with fc3:
        period_label = st.selectbox("Quick select", [
            "Custom", "This financial year", "Last 90 days", "Last 30 days", "All time"
        ])

    if period_label == "This financial year":
        date_from = fy_start; date_to = today_pl
    elif period_label == "Last 90 days":
        date_from = today_pl - _dtpl.timedelta(days=90); date_to = today_pl
    elif period_label == "Last 30 days":
        date_from = today_pl - _dtpl.timedelta(days=30); date_to = today_pl
    elif period_label == "All time":
        date_from = date(2020, 1, 1); date_to = today_pl

    ds = date_from.isoformat()
    de = date_to.isoformat()

    st.markdown(f"<div style='font-size:13px;color:#64748b;margin-bottom:1rem'>"
                f"Showing: {date_from.strftime('%d %b %Y')} — {date_to.strftime('%d %b %Y')}"
                f"</div>", unsafe_allow_html=True)

    # ── Pull all data ─────────────────────────────────────────────────────
    # Jobs in period
    jobs_pl = fetch_df("""
        SELECT j.job_id, j.client, j.stage, j.job_type,
               j.sell_price, j.running_cost_pct,
               j.tender_material_budget, j.tender_labour_budget
        FROM jobs j WHERE j.archived=0
        AND j.stage IN ('Live Job','Completed','Pre-Live Handover')
        AND COALESCE(j.is_variation,0)=0
    """)

    # Revenue — invoiced and collected
    inv_pl = fetch_df("""
        SELECT ci.issue_date,
               COALESCE(ci.amount_ex_gst, 0) AS amount,
               COALESCE(ci.gst, 0) AS gst,
               COALESCE(ci.total_inc_gst, 0) AS total_inc_gst,
               ci.status, ci.job_id
        FROM client_invoices ci
        WHERE ci.issue_date >= ? AND ci.issue_date <= ?
    """, (ds, de))

    # Approved variations
    var_pl = fetch_df("""
        SELECT v.job_id, v.value FROM variations v
        WHERE v.status='Approved'
    """)

    # Material costs
    mat_pl = fetch_df("""
        SELECT mi.invoice_date, mi.amount, mi.job_id
        FROM material_invoices mi
        WHERE mi.invoice_date >= ? AND mi.invoice_date <= ?
        AND mi.status='Entered'
    """, (ds, de))

    # Labour costs
    lab_pl = fetch_df("""
        SELECT ll.work_date, ll.hours, ll.hourly_rate,
               ROUND(ll.hours*ll.hourly_rate,2)AS cost, ll.job_id, ll.employee
        FROM labour_logs ll
        WHERE ll.work_date >= ? AND ll.work_date <= ?
    """, (ds, de))

    # Pipeline
    pipe_pl = fetch_df("""
        SELECT value, probability_pct, secured, target_month
        FROM pipeline WHERE archived=0
    """)

    # ── Calculate totals ──────────────────────────────────────────────────
    total_contract   = float(jobs_pl["sell_price"].fillna(0).sum())
    total_variations = float(var_pl["value"].sum()) if not var_pl.empty else 0
    total_invoiced   = float(inv_pl["amount"].sum()) if not inv_pl.empty else 0
    # Count both Issued and Paid as revenue (Issued = sent to client, Paid = received)
    total_collected  = float(inv_pl[inv_pl["status"].isin(["Paid","Issued"])]["amount"].sum()) if not inv_pl.empty else 0
    total_gst_coll   = float(inv_pl[inv_pl["status"].isin(["Paid","Issued"])]["gst"].sum()) if not inv_pl.empty else 0

    total_mat_cost   = float(mat_pl["amount"].sum()) if not mat_pl.empty else 0
    total_lab_cost   = float(lab_pl["cost"].sum()) if not lab_pl.empty else 0

    # Expenses
    exp_pl = fetch_df("""
        SELECT amount, category FROM expenses
        WHERE expense_date >= ? AND expense_date <= ? AND status != 'Rejected'
    """, (ds, de))
    total_expenses = float(exp_pl["amount"].sum()) if not exp_pl.empty else 0
    _co_settings     = get_company_settings()
    _overhead_rate   = float(_co_settings.get("overhead_pct", 11.0) or 11.0) / 100
    total_overhead   = total_contract * _overhead_rate

    total_costs      = total_mat_cost + total_lab_cost + total_overhead
    gross_profit     = total_collected - total_costs
    gross_margin     = (gross_profit / total_collected * 100) if total_collected else 0

    wtd_pipeline     = float((pipe_pl["value"] * pipe_pl["probability_pct"] / 100).sum()) if not pipe_pl.empty else 0
    secured_pipeline = float(pipe_pl[pipe_pl["secured"]==1]["value"].sum()) if not pipe_pl.empty else 0

    total_hours      = float(lab_pl["hours"].sum()) if not lab_pl.empty else 0
    total_employees  = fetch_df("SELECT COUNT(*) AS n FROM employees WHERE active=1").iloc[0]["n"]
    days_in_period   = (date_to - date_from).days
    available_hours  = float(total_employees) * days_in_period * (5/7) * 8
    utilisation      = (total_hours / available_hours * 100) if available_hours else 0

    # ── Hero metrics ──────────────────────────────────────────────────────
    st.markdown("""<div style="font-size:13px;font-weight:700;letter-spacing:.12em;
        text-transform:uppercase;color:#2dd4bf;margin-bottom:10px">Business snapshot</div>""",
        unsafe_allow_html=True)

    h1,h2,h3,h4 = st.columns(4)
    h1.metric("Total collected",  f"${total_collected:,.0f}")
    h2.metric("Total costs",      f"${total_costs:,.0f}")
    h3.metric("Gross profit",     f"${gross_profit:,.0f}")
    h4.metric("Gross margin",     f"{gross_margin:.1f}%")

    st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)

    # ── Two column layout ─────────────────────────────────────────────────
    left_pl, right_pl = st.columns([1,1])

    with left_pl:
        # REVENUE BREAKDOWN
        st.markdown("""<div style="font-size:13px;font-weight:700;letter-spacing:.12em;
            text-transform:uppercase;color:#2dd4bf;margin:1rem 0 8px">Revenue breakdown</div>""",
            unsafe_allow_html=True)

        rev_rows = [
            ("Total contract value",  total_contract,   "#e2e8f0"),
            ("Approved variations",   total_variations, "#a78bfa"),
            ("Total invoiced",        total_invoiced,   "#f59e0b"),
            ("Collected (paid)",      total_collected,  "#2dd4bf"),
            ("GST collected",         total_gst_coll,   "#64748b"),
            ("Outstanding",           total_invoiced - total_collected, "#f43f5e"),
        ]
        rev_html = ""
        for label, val, color in rev_rows:
            rev_html += f"""
            <div style="display:flex;justify-content:space-between;align-items:center;
                padding:8px 0;border-bottom:1px solid #1e2d3d">
                <span style="font-size:13px;color:#94a3b8">{label}</span>
                <span style="font-size:13px;font-weight:700;color:{color}">${val:,.0f}</span>
            </div>"""
        st.markdown(f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;"
                    f"border-radius:10px;padding:12px 16px'>{rev_html}</div>",
                    unsafe_allow_html=True)

        # PIPELINE FORWARD VIEW
        st.markdown("""<div style="font-size:13px;font-weight:700;letter-spacing:.12em;
            text-transform:uppercase;color:#2dd4bf;margin:1.2rem 0 8px">Pipeline forward view</div>""",
            unsafe_allow_html=True)

        pipe_rows = [
            ("Total pipeline value",   float(pipe_pl["value"].sum()) if not pipe_pl.empty else 0,  "#e2e8f0"),
            ("Weighted (probability)", wtd_pipeline,    "#f59e0b"),
            ("Secured / confirmed",    secured_pipeline,"#2dd4bf"),
            ("Jobs in pipeline",       len(pipe_pl) if not pipe_pl.empty else 0, "#94a3b8"),
        ]
        pipe_html = ""
        for label, val, color in pipe_rows:
            display = f"${val:,.0f}" if isinstance(val, float) else str(int(val))
            pipe_html += f"""
            <div style="display:flex;justify-content:space-between;align-items:center;
                padding:8px 0;border-bottom:1px solid #1e2d3d">
                <span style="font-size:13px;color:#94a3b8">{label}</span>
                <span style="font-size:13px;font-weight:700;color:{color}">{display}</span>
            </div>"""
        st.markdown(f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;"
                    f"border-radius:10px;padding:12px 16px'>{pipe_html}</div>",
                    unsafe_allow_html=True)

    with right_pl:
        # COST BREAKDOWN
        st.markdown("""<div style="font-size:13px;font-weight:700;letter-spacing:.12em;
            text-transform:uppercase;color:#2dd4bf;margin:1rem 0 8px">Cost breakdown</div>""",
            unsafe_allow_html=True)

        cost_rows = [
            ("Material costs",    total_mat_cost,  "#f59e0b"),
            ("Labour costs",      total_lab_cost,  "#7dd3fc"),
            ("Overhead / running",total_overhead,  "#a78bfa"),
            ("Total costs",       total_costs,     "#f43f5e"),
        ]
        max_cost = max(total_mat_cost, total_lab_cost, total_overhead, 1)
        cost_html = ""
        for label, val, color in cost_rows:
            bar_w = min(val/max_cost*100, 100) if val != total_costs else 0
            pct   = (val/total_costs*100) if total_costs else 0
            pct_str  = "" if label == "Total costs" else "<span style='color:#475569;font-weight:400'>(" + f"{pct:.0f}%" + ")</span>"
            bar_str  = "" if label == "Total costs" else "<div style='background:#0f172a;border-radius:4px;height:6px'><div style='background:" + color + ";width:" + f"{bar_w:.0f}" + "%;height:6px;border-radius:4px'></div></div>"
            cost_html += (
                "<div style='margin-bottom:10px'>"
                "<div style='display:flex;justify-content:space-between;font-size:13px;margin-bottom:3px'>"
                "<span style='color:#94a3b8'>" + label + "</span>"
                "<span style='font-weight:700;color:" + color + "'>$" + f"{val:,.0f} " + pct_str + "</span>"
                "</div>" + bar_str + "</div>"
            )
        st.markdown(f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;"
                    f"border-radius:10px;padding:12px 16px'>{cost_html}</div>",
                    unsafe_allow_html=True)

        # LABOUR UTILISATION
        st.markdown("""<div style="font-size:13px;font-weight:700;letter-spacing:.12em;
            text-transform:uppercase;color:#2dd4bf;margin:1.2rem 0 8px">Labour utilisation</div>""",
            unsafe_allow_html=True)

        util_color = "#2dd4bf" if utilisation >= 70 else "#f59e0b" if utilisation >= 50 else "#f43f5e"
        util_w     = min(utilisation, 100)

        # Top employees by hours
        if not lab_pl.empty:
            emp_hours = lab_pl.groupby("employee").agg(
                Hours=("hours","sum"),
                Cost=("cost","sum")
            ).reset_index().sort_values("Hours", ascending=False).head(5)
        else:
            emp_hours = None

        util_html = f"""
        <div style="margin-bottom:12px">
            <div style="display:flex;justify-content:space-between;font-size:13px;margin-bottom:6px">
                <span style="color:#94a3b8">Utilisation rate</span>
                <span style="font-weight:800;font-size:16px;color:{util_color}">{utilisation:.0f}%</span>
            </div>
            <div style="background:#0f172a;border-radius:6px;height:10px">
                <div style="background:{util_color};width:{util_w:.0f}%;height:10px;border-radius:6px"></div>
            </div>
        </div>
        <div style="display:flex;gap:20px;font-size:13px;margin-bottom:12px;padding-bottom:10px;border-bottom:1px solid #2a3d4f">
            <div><span style="color:#64748b">Logged hours</span>
                <span style="color:#e2e8f0;font-weight:700;margin-left:6px">{total_hours:,.0f}h</span></div>
            <div><span style="color:#64748b">Available</span>
                <span style="color:#e2e8f0;font-weight:700;margin-left:6px">{available_hours:,.0f}h</span></div>
            <div><span style="color:#64748b">Active staff</span>
                <span style="color:#e2e8f0;font-weight:700;margin-left:6px">{total_employees}</span></div>
        </div>"""

        if emp_hours is not None and not emp_hours.empty:
            util_html += "<div style='font-size:13px;font-weight:700;color:#475569;text-transform:uppercase;margin-bottom:6px'>Top by hours</div>"
            max_emp_hrs = float(emp_hours["Hours"].max()) or 1
            for _, er in emp_hours.iterrows():
                bar = float(er["Hours"])/max_emp_hrs*100
                util_html += f"""
                <div style="display:flex;align-items:center;gap:8px;margin-bottom:5px">
                    <div style="font-size:13px;color:#94a3b8;width:90px;flex-shrink:0;
                        white-space:nowrap;overflow:hidden;text-overflow:ellipsis">{er['employee']}</div>
                    <div style="flex:1;background:#0f172a;border-radius:3px;height:6px">
                        <div style="background:#7dd3fc;width:{bar:.0f}%;height:6px;border-radius:3px"></div>
                    </div>
                    <div style="font-size:13px;color:#7dd3fc;font-weight:600;width:35px;text-align:right">
                        {float(er['Hours']):.0f}h</div>
                </div>"""

        st.markdown(f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;"
                    f"border-radius:10px;padding:12px 16px'>{util_html}</div>",
                    unsafe_allow_html=True)

    st.divider()

    # ── P&L Summary table ─────────────────────────────────────────────────
    st.subheader("P&L Summary")
    import pandas as _pdpl

    pl_summary = _pdpl.DataFrame([
        {"Category": "REVENUE",               "Item": "Total contracts",         "Amount": f"${total_contract:,.2f}"},
        {"Category": "REVENUE",               "Item": "+ Approved variations",   "Amount": f"${total_variations:,.2f}"},
        {"Category": "REVENUE",               "Item": "Total invoiced",          "Amount": f"${total_invoiced:,.2f}"},
        {"Category": "REVENUE",               "Item": "Collected (excl. GST)",   "Amount": f"${total_collected:,.2f}"},
        {"Category": "COSTS",                 "Item": "Material costs",          "Amount": f"${total_mat_cost:,.2f}"},
        {"Category": "COSTS",                 "Item": "Labour costs",            "Amount": f"${total_lab_cost:,.2f}"},
        {"Category": "COSTS",                 "Item": "Overhead / running costs","Amount": f"${total_overhead:,.2f}"},
        {"Category": "COSTS",                 "Item": "Total costs",             "Amount": f"${total_costs:,.2f}"},
        {"Category": "PROFIT",                "Item": "Gross profit",            "Amount": f"${gross_profit:,.2f}"},
        {"Category": "PROFIT",                "Item": "Gross margin %",          "Amount": f"{gross_margin:.1f}%"},
        {"Category": "PIPELINE",              "Item": "Weighted pipeline",       "Amount": f"${wtd_pipeline:,.2f}"},
        {"Category": "PIPELINE",              "Item": "Secured pipeline",        "Amount": f"${secured_pipeline:,.2f}"},
        {"Category": "LABOUR",                "Item": "Total hours logged",      "Amount": f"{total_hours:,.0f}h"},
        {"Category": "LABOUR",                "Item": "Utilisation rate",        "Amount": f"{utilisation:.1f}%"},
    ])
    st.dataframe(pl_summary, width="stretch", hide_index=True)

    st.caption("Note: Overhead calculated at 11% of contract value (default). "
               "Adjust per-job running cost % in job settings. "
               "Costs reflect logged entries only — not all jobs may have full data.")

# ─────────────────────────────────────────────
#  PAGE: USER MANAGEMENT
# ─────────────────────────────────────────────
elif page == "User Management":
    if user_role != "Admin":
        st.error("Admin access required."); st.stop()

    st.title("User Management")
    st.caption("Manage who can access Limitless and what they can see.")

    ROLES = ["Admin", "Estimator", "Ops"]

    # ── Existing users ────────────────────────────────────────────────────
    users_df = fetch_df("SELECT id, username, full_name, role, active, created_date FROM users ORDER BY id")

    if not users_df.empty:
        for _, ur in users_df.iterrows():
            uid     = int(ur["id"])
            is_self = ur["username"] == current_user["username"]
            active  = bool(int(ur.get("active",1) or 1))
            role_c  = {"Admin":"#f43f5e","Estimator":"#2dd4bf","Ops":"#f59e0b"}.get(ur["role"],"#64748b")

            with st.expander(
                f"{'🟢' if active else '⚫'} {ur['full_name'] or ur['username']} "
                f"— {ur['role']} {'(you)' if is_self else ''}",
                expanded=False
            ):
                with st.form(f"edit_user_{uid}"):
                    uc1, uc2 = st.columns(2)
                    with uc1:
                        e_fname = st.text_input("Full name",  value=str(ur.get("full_name","") or ""))
                        e_uname = st.text_input("Username",   value=str(ur["username"]),
                                    disabled=is_self)
                        e_role  = st.selectbox("Role", ROLES,
                                    index=ROLES.index(ur["role"]) if ur["role"] in ROLES else 2,
                                    disabled=is_self)
                    with uc2:
                        e_pw    = st.text_input("New password", type="password",
                                    placeholder="Leave blank to keep current")
                        e_active= st.checkbox("Active", value=active, disabled=is_self)

                    sb1, sb2 = st.columns([1,1])
                    with sb1:
                        if st.form_submit_button("Save", type="primary"):
                            updates = [
                                "full_name=?", "role=?", "active=?"
                            ]
                            vals = [e_fname, e_role, int(e_active)]
                            if not is_self:
                                updates.append("username=?")
                                vals.append(e_uname)
                            if e_pw.strip():
                                updates.append("password_hash=?")
                                vals.append(hash_password(e_pw.strip()))
                            vals.append(uid)
                            execute(f"UPDATE users SET {', '.join(updates)} WHERE id=?", vals)
                            st.success("User updated.")
                            if is_self and e_pw.strip():
                                st.info("Password changed — you'll need to log in again.")
                            st.rerun()
                    with sb2:
                        if not is_self:
                            if st.form_submit_button("Deactivate"):
                                execute("UPDATE users SET active=0 WHERE id=?", (uid,))
                                st.rerun()

    st.divider()

    # ── Add new user ──────────────────────────────────────────────────────
    st.subheader("Add new user")
    with st.form("add_user_form"):
        nu1, nu2 = st.columns(2)
        with nu1:
            n_fname = st.text_input("Full name")
            n_uname = st.text_input("Username")
        with nu2:
            n_role  = st.selectbox("Role", ROLES, index=2)
            n_pw    = st.text_input("Password", type="password")
            n_pw2   = st.text_input("Confirm password", type="password")

        if st.form_submit_button("Create user", type="primary"):
            if not n_uname.strip():
                st.warning("Username required.")
            elif not n_pw.strip():
                st.warning("Password required.")
            elif n_pw != n_pw2:
                st.error("Passwords don't match.")
            else:
                existing = fetch_df("SELECT id FROM users WHERE username=?", (n_uname.strip(),))
                if not existing.empty:
                    st.error("Username already exists.")
                else:
                    execute("""INSERT INTO users (username, password_hash, full_name, role, active, created_date)
                               VALUES (?,?,?,?,?,?)""",
                            (n_uname.strip(), hash_password(n_pw.strip()),
                             n_fname.strip(), n_role, 1, date.today().isoformat()))
                    st.success(f"User {n_uname} created with role {n_role}."); st.rerun()

    st.divider()

    # ── Role permissions reference ────────────────────────────────────────
    st.subheader("Role permissions")
    for role, pages_list in ROLE_PAGES.items():
        rc = {"Admin":"#f43f5e","Estimator":"#2dd4bf","Ops":"#f59e0b"}.get(role,"#64748b")
        st.markdown(
            f"<div style='background:#1e2d3d;border:1px solid #2a3d4f;border-left:3px solid {rc};"
            f"border-radius:9px;padding:10px 14px;margin-bottom:8px'>"
            f"<div style='font-weight:700;color:{rc};margin-bottom:6px'>{role}</div>"
            f"<div style='font-size:13px;color:#64748b'>{' · '.join(pages_list)}</div>"
            f"</div>",
            unsafe_allow_html=True)


# ─────────────────────────────────────────────
#  PAGE: EXPENSES
# ─────────────────────────────────────────────
elif page == "Expenses":
    st.title("Expenses")
    st.caption("Track business expenditure — tools, client lunches, fuel, subscriptions and more.")

    EXP_CATEGORIES = [
        "Tools & Equipment", "Client Entertainment", "Fuel & Vehicle",
        "Phone & Subscriptions", "Office & Admin", "Travel & Accommodation",
        "Training & Education", "Marketing", "Subcontractors", "Other"
    ]
    EXP_STATUSES = ["Pending", "Approved", "Rejected", "Reimbursed"]

    # ── Summary metrics ────────────────────────────────────────────────────
    exp_summary = fetch_df("""
        SELECT
            COALESCE(SUM(amount),0) AS total,
            COALESCE(SUM(gst),0) AS total_gst,
            COALESCE(SUM(CASE WHEN status='Pending' THEN amount ELSE 0 END),0) AS pending,
            COALESCE(SUM(CASE WHEN status='Approved' THEN amount ELSE 0 END),0) AS approved
        FROM expenses WHERE expense_date >= date('now', '-30 days')
    """)
    if not exp_summary.empty:
        es = exp_summary.iloc[0]
        em1,em2,em3,em4 = st.columns(4)
        em1.metric("Total (30 days)", f"${float(es['total']):,.2f}")
        em2.metric("GST claimable", f"${float(es['total_gst']):,.2f}")
        em3.metric("Pending approval", f"${float(es['pending']):,.2f}")
        em4.metric("Approved", f"${float(es['approved']):,.2f}")

    st.divider()

    # ── Add expense ────────────────────────────────────────────────────────
    with st.expander("+ Log New Expense", expanded=False):
        with st.form("add_expense_form"):
            ec1, ec2 = st.columns(2)
            with ec1:
                ex_date = st.date_input("Date", value=_today_aest())
                ex_cat  = st.selectbox("Category", EXP_CATEGORIES)
                ex_desc = st.text_input("Description *", placeholder="e.g. Client lunch — Peter Smith Builder")
                ex_by   = st.text_input("Submitted by", value=str(current_user.get("full_name","") or current_user.get("username","")))
            with ec2:
                ex_amount = st.number_input("Amount (inc. GST)", min_value=0.0, step=1.0, value=0.0)
                ex_gst_inc = st.checkbox("GST included", value=True)
                ex_gst = round(ex_amount / 11, 2) if ex_gst_inc else 0.0
                st.metric("GST component", f"${ex_gst:.2f}")
                jobs_exp = fetch_df("SELECT job_id FROM jobs WHERE archived=0 ORDER BY job_id")
                job_opts = ["— Company expense —"] + (jobs_exp["job_id"].tolist() if not jobs_exp.empty else [])
                ex_job = st.selectbox("Link to job (optional)", job_opts)
                ex_notes = st.text_area("Notes", height=68)
            if st.form_submit_button("Save Expense", type="primary"):
                if ex_desc.strip() and ex_amount > 0:
                    job_id = "" if ex_job.startswith("—") else ex_job
                    execute("""INSERT INTO expenses
                        (expense_date,category,description,amount,gst,job_id,submitted_by,status,notes,created_at)
                        VALUES (?,?,?,?,?,?,?,?,?,?)""",
                        (ex_date.isoformat(), ex_cat, ex_desc.strip(),
                         ex_amount, ex_gst, job_id, ex_by, "Pending",
                         ex_notes.strip(), _today_aest().isoformat()))
                    st.success(f"Expense logged!")
                    st.rerun()
                else:
                    st.error("Description and amount required.")

    # ── Filter ─────────────────────────────────────────────────────────────
    fc1, fc2, fc3 = st.columns(3)
    with fc1: exp_cat_f = st.selectbox("Category", ["All"] + EXP_CATEGORIES, key="exp_cf")
    with fc2: exp_status_f = st.selectbox("Status", ["All"] + EXP_STATUSES, key="exp_sf")
    with fc3: exp_search = st.text_input("Search", placeholder="Description or person...", key="exp_s")

    exp_df = fetch_df("""SELECT id,expense_date,category,description,amount,gst,
        job_id,submitted_by,status,notes FROM expenses ORDER BY expense_date DESC, id DESC""")

    if not exp_df.empty:
        if exp_cat_f != "All":
            exp_df = exp_df[exp_df["category"] == exp_cat_f]
        if exp_status_f != "All":
            exp_df = exp_df[exp_df["status"] == exp_status_f]
        if exp_search:
            mask = (exp_df["description"].str.contains(exp_search, case=False, na=False) |
                    exp_df["submitted_by"].str.contains(exp_search, case=False, na=False))
            exp_df = exp_df[mask]

    STATUS_COLORS = {"Pending":"#f59e0b","Approved":"#2dd4bf","Rejected":"#f43f5e","Reimbursed":"#4ade80"}
    CAT_ICONS = {"Tools & Equipment":"🔧","Client Entertainment":"🍽️","Fuel & Vehicle":"⛽",
        "Phone & Subscriptions":"📱","Office & Admin":"🏢","Travel & Accommodation":"✈️",
        "Training & Education":"📚","Marketing":"📣","Subcontractors":"👷","Other":"💳"}

    if exp_df.empty:
        st.info("No expenses yet.")
    else:
        total_shown = exp_df["amount"].sum()
        st.markdown(f"<div style='color:#475569;font-size:14px;margin-bottom:12px'>{len(exp_df)} expenses · Total: <b style='color:#e2e8f0'>${total_shown:,.2f}</b></div>", unsafe_allow_html=True)

        for _, ex in exp_df.iterrows():
            eid = int(ex["id"])
            sc = STATUS_COLORS.get(str(ex.get("status","Pending")), "#64748b")
            icon = CAT_ICONS.get(str(ex.get("category","")), "💳")
            job_badge = f" · <span style='color:#2dd4bf'>{ex.get('job_id','')}</span>" if ex.get("job_id") else ""

            ecl1, ecl2, ecl3 = st.columns([6,2,1])
            with ecl1:
                st.markdown(f"""
                <div style='background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:12px 16px;margin-bottom:6px'>
                    <div style='display:flex;align-items:center;gap:12px'>
                        <span style='font-size:22px'>{icon}</span>
                        <div style='flex:1'>
                            <div style='font-weight:700;color:#e2e8f0;font-size:15px'>{ex.get("description","")} {job_badge}</div>
                            <div style='color:#64748b;font-size:13px'>{ex.get("category","")} · {ex.get("submitted_by","")} · {ex.get("expense_date","")}</div>
                        </div>
                        <div style='text-align:right'>
                            <div style='font-size:18px;font-weight:800;color:#e2e8f0'>${float(ex.get("amount",0)):,.2f}</div>
                            <div style='font-size:12px;color:#475569'>GST ${float(ex.get("gst",0)):,.2f}</div>
                        </div>
                        <span style='background:{sc}22;color:{sc};border-radius:999px;padding:3px 12px;font-size:12px;font-weight:700'>{ex.get("status","Pending")}</span>
                    </div>
                </div>""", unsafe_allow_html=True)

            with ecl2:
                new_st = st.selectbox("Status", EXP_STATUSES,
                    index=EXP_STATUSES.index(str(ex.get("status","Pending"))) if str(ex.get("status","Pending")) in EXP_STATUSES else 0,
                    key=f"exp_st_{eid}", label_visibility="collapsed")
                if new_st != str(ex.get("status","Pending")):
                    execute("UPDATE expenses SET status=? WHERE id=?", (new_st, eid))
                    st.rerun()

            with ecl3:
                if st.button("🗑", key=f"del_ex_{eid}"):
                    execute("DELETE FROM expenses WHERE id=?", (eid,))
                    st.rerun()

        st.divider()
        csv_exp = exp_df.to_csv(index=False)
        st.download_button("📊 Export CSV", data=csv_exp,
            file_name=f"expenses_{_today_aest().isoformat()}.csv", mime="text/csv")

# ─────────────────────────────────────────────
#  PAGE: FINANCIAL HEALTH
# ─────────────────────────────────────────────
elif page == "Financial Health":
    st.title("Financial Health")
    st.caption("Know what to set aside, what you owe, and where your cash is going — before your accountant tells you.")

    import datetime as _dtfh

    today_fh  = date.today()

    # ── BAS quarter detection ─────────────────────────────────────────────
    # Australian BAS quarters: Q1 Jul-Sep, Q2 Oct-Dec, Q3 Jan-Mar, Q4 Apr-Jun
    month = today_fh.month
    year  = today_fh.year
    if month in [7,8,9]:
        q_start = date(year, 7, 1); q_end = date(year, 9, 30); q_label = "Q1 (Jul–Sep)"
        q_due   = date(year, 10, 28)
    elif month in [10,11,12]:
        q_start = date(year, 10, 1); q_end = date(year, 12, 31); q_label = "Q2 (Oct–Dec)"
        q_due   = date(year+1, 2, 28)
    elif month in [1,2,3]:
        q_start = date(year, 1, 1); q_end = date(year, 3, 31); q_label = "Q3 (Jan–Mar)"
        q_due   = date(year, 4, 28)
    else:
        q_start = date(year, 4, 1); q_end = date(year, 6, 30); q_label = "Q4 (Apr–Jun)"
        q_due   = date(year, 7, 28)

    days_to_bas = (q_due - today_fh).days

    # Allow custom date range
    with st.expander("⚙️ Adjust period", expanded=False):
        fc1,fc2 = st.columns(2)
        with fc1: q_start = st.date_input("From", value=q_start)
        with fc2: q_end   = st.date_input("To",   value=q_end)

    ds = q_start.isoformat()
    de = q_end.isoformat()

    # ── Pull data ─────────────────────────────────────────────────────────
    # GST collected — from issued/paid invoices within period
    gst_collected_df = fetch_df("""
        SELECT COALESCE(SUM(gst),0) AS gst,
               COALESCE(SUM(total_inc_gst),0) AS total,
               COALESCE(SUM(amount_ex_gst),0) AS ex_gst
        FROM client_invoices
        WHERE status IN ('Paid','Issued') AND issue_date >= ? AND issue_date <= ?
    """, (ds, de))
    gst_collected  = float(gst_collected_df.iloc[0]["gst"])    if not gst_collected_df.empty else 0
    total_collected= float(gst_collected_df.iloc[0]["ex_gst"]) if not gst_collected_df.empty else 0

    # GST paid — on material invoices (assume all supplier invoices include GST)
    gst_paid_df = fetch_df("""
        SELECT COALESCE(SUM(amount * 0.1),0) AS gst_paid,
               COALESCE(SUM(amount),0) AS total_spend
        FROM material_invoices
        WHERE invoice_date >= ? AND invoice_date <= ?
        AND status='Entered'
    """, (ds, de))
    gst_paid   = float(gst_paid_df.iloc[0]["gst_paid"]) if not gst_paid_df.empty else 0
    mat_spend  = float(gst_paid_df.iloc[0]["total_spend"]) if not gst_paid_df.empty else 0

    bas_owing  = gst_collected - gst_paid

    # Labour for super calculation (11% super on gross wages)
    SUPER_RATE = 0.11
    labour_df  = fetch_df("""
        SELECT COALESCE(SUM(hours * hourly_rate),0) AS wages
        FROM labour_logs
        WHERE work_date >= ? AND work_date <= ?
    """, (ds, de))
    total_wages  = float(labour_df.iloc[0]["wages"]) if not labour_df.empty else 0
    super_owing  = total_wages * SUPER_RATE

    # Company tax estimate (25% small business rate on net profit)
    # Use FY data for tax
    fy_start_tax = date(year if month >= 7 else year-1, 7, 1)
    fy_end_tax   = date(year+1 if month >= 7 else year, 6, 30)

    fy_collected = fetch_df("""
        SELECT COALESCE(SUM(amount_ex_gst),0) AS v FROM client_invoices
        WHERE status IN ('Paid','Issued') AND issue_date >= ? AND issue_date <= ?
    """, (fy_start_tax.isoformat(), today_fh.isoformat())).iloc[0]["v"]

    fy_mat_cost  = fetch_df("""
        SELECT COALESCE(SUM(amount),0) AS v FROM material_invoices
        WHERE invoice_date >= ? AND invoice_date <= ? AND status='Entered'
    """, (fy_start_tax.isoformat(), today_fh.isoformat())).iloc[0]["v"]

    fy_lab_cost  = fetch_df("""
        SELECT COALESCE(SUM(hours*hourly_rate),0) AS v FROM labour_logs
        WHERE work_date >= ? AND work_date <= ?
    """, (fy_start_tax.isoformat(), today_fh.isoformat())).iloc[0]["v"]

    fy_revenue   = float(fy_collected)
    _fh_settings  = get_company_settings()
    _fh_ovhd_rate = float(_fh_settings.get("overhead_pct", 11.0) or 11.0) / 100
    fy_costs     = float(fy_mat_cost) + float(fy_lab_cost) + (fy_revenue * _fh_ovhd_rate)
    fy_profit    = fy_revenue - fy_costs
    tax_estimate = max(fy_profit * 0.25, 0)  # 25% small business rate

    total_set_aside = bas_owing + super_owing

    # ── BAS countdown banner ──────────────────────────────────────────────
    bas_color = "#f43f5e" if days_to_bas <= 14 else "#f59e0b" if days_to_bas <= 30 else "#2dd4bf"
    st.markdown(f"""
    <div style="background:linear-gradient(135deg,#1a2332,#1e3040);
        border:1px solid {bas_color};border-radius:12px;
        padding:16px 20px;margin-bottom:1.2rem;
        display:flex;align-items:center;justify-content:space-between">
        <div>
            <div style="font-size:13px;font-weight:700;color:{bas_color};
                text-transform:uppercase;letter-spacing:.1em;margin-bottom:4px">
                BAS Due — {q_label}
            </div>
            <div style="font-size:28px;font-weight:800;color:#f1f5f9">
                {days_to_bas} days
            </div>
            <div style="font-size:13px;color:#64748b">Due {q_due.strftime('%d %B %Y')}</div>
        </div>
        <div style="text-align:right">
            <div style="font-size:13px;color:#64748b;margin-bottom:4px">Estimated BAS owing</div>
            <div style="font-size:32px;font-weight:800;color:{bas_color}">${bas_owing:,.0f}</div>
            <div style="font-size:13px;color:#475569">GST collected − GST paid</div>
        </div>
    </div>""", unsafe_allow_html=True)

    # ── SET ASIDE dashboard ───────────────────────────────────────────────
    st.markdown("""<div style="font-size:13px;font-weight:700;letter-spacing:.12em;
        text-transform:uppercase;color:#2dd4bf;margin-bottom:10px">
        💰 What to set aside right now</div>""", unsafe_allow_html=True)

    set_aside_items = [
        ("BAS — GST owing",        bas_owing,      "#f59e0b",
         f"GST collected ${gst_collected:,.0f} minus GST paid on materials ${gst_paid:,.0f}"),
        ("Super obligations",      super_owing,    "#a78bfa",
         f"{SUPER_RATE*100:.0f}% of wages paid (${total_wages:,.0f}) this period"),
        ("Company tax (estimate)", tax_estimate,   "#7dd3fc",
         f"25% of estimated FY net profit ${fy_profit:,.0f} — consult your accountant"),
    ]

    total_quarantine = bas_owing + super_owing + tax_estimate

    for label, amount, color, explanation in set_aside_items:
        pct = (amount / total_quarantine * 100) if total_quarantine else 0
        st.markdown(f"""
        <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-left:4px solid {color};
            border-radius:10px;padding:14px 18px;margin-bottom:8px">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:6px">
                <span style="font-weight:700;font-size:14px;color:#e2e8f0">{label}</span>
                <span style="font-size:22px;font-weight:800;color:{color}">${amount:,.0f}</span>
            </div>
            <div style="font-size:13px;color:#64748b;margin-bottom:8px">{explanation}</div>
            <div style="background:#0f172a;border-radius:4px;height:6px">
                <div style="background:{color};width:{pct:.0f}%;height:6px;border-radius:4px"></div>
            </div>
        </div>""", unsafe_allow_html=True)

    # Total set aside box
    st.markdown(f"""
    <div style="background:#0d2233;border:2px solid #2dd4bf;border-radius:12px;
        padding:16px 20px;margin:12px 0;text-align:center">
        <div style="font-size:13px;font-weight:700;color:#2dd4bf;text-transform:uppercase;
            letter-spacing:.1em;margin-bottom:6px">Total to quarantine</div>
        <div style="font-size:36px;font-weight:900;color:#2dd4bf">${total_quarantine:,.0f}</div>
        <div style="font-size:13px;color:#64748b;margin-top:4px">
            Move this to a separate account today. Don't touch it.
        </div>
    </div>""", unsafe_allow_html=True)

    st.divider()

    # ── BAS breakdown ─────────────────────────────────────────────────────
    st.subheader("BAS breakdown")
    st.caption(f"Quarter: {q_label} — {q_start.strftime('%d %b')} to {q_end.strftime('%d %b %Y')}")

    bc1,bc2,bc3,bc4 = st.columns(4)
    bc1.metric("GST collected",     f"${gst_collected:,.2f}")
    bc2.metric("GST paid (inputs)", f"${gst_paid:,.2f}")
    bc3.metric("Net GST owing",     f"${bas_owing:,.2f}")
    bc4.metric("Total invoiced",    f"${total_collected:,.2f}")

    st.markdown(f"""
    <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;
        padding:16px 20px;margin-top:8px">
        <div style="font-size:13px;color:#94a3b8;line-height:2">
            <div style="display:flex;justify-content:space-between;border-bottom:1px solid #2a3d4f;padding-bottom:6px;margin-bottom:6px">
                <span>1A — GST on sales (collected from clients)</span>
                <span style="font-weight:700;color:#e2e8f0">${gst_collected:,.2f}</span>
            </div>
            <div style="display:flex;justify-content:space-between;border-bottom:1px solid #2a3d4f;padding-bottom:6px;margin-bottom:6px">
                <span>1B — GST on purchases (paid to suppliers)</span>
                <span style="font-weight:700;color:#e2e8f0">${gst_paid:,.2f}</span>
            </div>
            <div style="display:flex;justify-content:space-between">
                <span style="font-weight:700;color:#f59e0b">Net amount payable to ATO</span>
                <span style="font-weight:800;font-size:16px;color:#f59e0b">${bas_owing:,.2f}</span>
            </div>
        </div>
    </div>""", unsafe_allow_html=True)

    st.divider()

    # ── Super breakdown ───────────────────────────────────────────────────
    st.subheader("Superannuation obligations")
    st.caption(f"Based on wages logged this period. Super guarantee rate: {SUPER_RATE*100:.0f}%")

    # Per employee super
    emp_super = fetch_df("""
        SELECT employee,
               COALESCE(SUM(hours*hourly_rate),0) AS wages
        FROM labour_logs
        WHERE work_date >= ? AND work_date <= ?
        GROUP BY employee ORDER BY wages DESC
    """, (ds, de))

    sc1,sc2,sc3 = st.columns(3)
    sc1.metric("Total wages this period", f"${total_wages:,.2f}")
    sc2.metric(f"Super rate",             f"{SUPER_RATE*100:.0f}%")
    sc3.metric("Super owing",             f"${super_owing:,.2f}")

    if not emp_super.empty:
        st.markdown("<div style='height:.5rem'></div>", unsafe_allow_html=True)
        for _, er in emp_super.iterrows():
            emp_sup = float(er["wages"]) * SUPER_RATE
            st.markdown(f"""
            <div style="display:flex;justify-content:space-between;align-items:center;
                background:#1e2d3d;border:1px solid #2a3d4f;border-radius:8px;
                padding:10px 14px;margin-bottom:6px">
                <span style="font-size:13px;font-weight:600;color:#e2e8f0">{er['employee']}</span>
                <div style="text-align:right">
                    <div style="font-size:13px;color:#64748b">Wages: ${float(er['wages']):,.2f}</div>
                    <div style="font-size:13px;font-weight:700;color:#a78bfa">
                        Super: ${emp_sup:,.2f}</div>
                </div>
            </div>""", unsafe_allow_html=True)

    st.divider()

    # ── Education callout ─────────────────────────────────────────────────
    st.markdown("""
    <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-radius:12px;padding:18px 20px">
        <div style="font-size:13px;font-weight:700;color:#2dd4bf;margin-bottom:10px">
            📚 Understanding your obligations
        </div>
        <div style="font-size:13px;color:#94a3b8;line-height:1.9">
            <div style="margin-bottom:8px">
                <strong style="color:#e2e8f0">What is BAS?</strong><br>
                Business Activity Statement — you report and pay GST to the ATO every quarter.
                You collect 10% GST from clients, pay 10% GST to suppliers, and remit the difference.
                If you collected more than you paid — you owe the ATO. Simple.
            </div>
            <div style="margin-bottom:8px">
                <strong style="color:#e2e8f0">What is Superannuation?</strong><br>
                You must pay 11% of each employee's ordinary time earnings into their super fund.
                Super is paid quarterly. Miss it and the ATO charges interest and penalties.
            </div>
            <div>
                <strong style="color:#e2e8f0">The golden rule</strong><br>
                Every time money hits your account — move the set aside amount to a separate account immediately.
                Treat it like it was never yours. Your accountant will thank you.
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    st.caption("⚠️ This is an estimate only based on data entered in Limitless. "
               "Always confirm with your registered tax agent or accountant before lodging.")

# ─────────────────────────────────────────────
#  PAGE: PAYROLL RULES
# ─────────────────────────────────────────────
elif page == "Payroll Rules":
    st.title("Payroll Rules")
    st.caption("Set award conditions per employee. Calculate true on-costs and weekly pay.")

    import datetime as _dtpay

    # ── Seed NSW public holidays ──────────────────────────────────────────
    ph_count = fetch_df("SELECT COUNT(*) AS n FROM public_holidays").iloc[0]["n"]
    if ph_count == 0:
        nsw_ph_2026 = [
            ("2026-01-01","New Year's Day"),("2026-01-26","Australia Day"),
            ("2026-04-03","Good Friday"),("2026-04-04","Easter Saturday"),
            ("2026-04-05","Easter Sunday"),("2026-04-06","Easter Monday"),
            ("2026-04-25","ANZAC Day"),("2026-06-08","King's Birthday"),
            ("2026-08-03","Bank Holiday"),("2026-10-05","Labour Day"),
            ("2026-12-25","Christmas Day"),("2026-12-26","Boxing Day"),
            ("2026-12-28","Boxing Day (substitute)"),
        ]
        for d, n in nsw_ph_2026:
            execute("INSERT INTO public_holidays (holiday_date,name,state) VALUES (?,?,?)",
                    (d, n, "NSW"))

    def is_public_holiday(d):
        df = fetch_df("SELECT id FROM public_holidays WHERE holiday_date=?", (d,))
        return not df.empty

    def calculate_hours(work_date, start_time, end_time, break_mins, rules):
        """Calculate ordinary, overtime, saturday, sunday, PH hours."""
        from datetime import datetime as _dt, timedelta as _td
        import calendar as _cal

        d        = date.fromisoformat(work_date)
        weekday  = d.weekday()  # 0=Mon, 6=Sun
        is_ph    = is_public_holiday(work_date)
        is_sat   = weekday == 5
        is_sun   = weekday == 6

        fmt      = "%H:%M"
        start    = _dt.strptime(start_time, fmt)
        end      = _dt.strptime(end_time, fmt)
        if end < start:  end += _td(hours=24)

        total_mins  = (end - start).seconds // 60 - int(break_mins)
        total_hrs   = max(total_mins / 60, 0)

        ord_hrs = ot_hrs = sat_hrs = sun_hrs = ph_hrs = 0.0
        std_hrs = float(rules.get("ordinary_hours", 8.0))

        if is_ph:
            ph_hrs  = total_hrs
        elif is_sat:
            sat_hrs = total_hrs
        elif is_sun:
            sun_hrs = total_hrs
        else:
            ord_hrs = min(total_hrs, std_hrs)
            ot_hrs  = max(total_hrs - std_hrs, 0)

        return ord_hrs, ot_hrs, sat_hrs, sun_hrs, ph_hrs, total_hrs

    def calculate_gross(emp_rate, ord_h, ot_h, sat_h, sun_h, ph_h,
                        rules, travel=0, tool=0, meal=0):
        """Calculate gross pay from hours and rules."""
        ot_rate = float(rules.get("overtime_rate", 1.5))
        sat_rate= float(rules.get("saturday_rate", 2.0))
        sun_rate= float(rules.get("sunday_rate", 2.0))
        ph_rate = float(rules.get("public_holiday_rate", 2.0))

        gross = (
            ord_h * emp_rate +
            ot_h  * emp_rate * ot_rate +
            sat_h * emp_rate * sat_rate +
            sun_h * emp_rate * sun_rate +
            ph_h  * emp_rate * ph_rate +
            travel + tool + meal
        )
        return gross

    # ── Tabs ──────────────────────────────────────────────────────────────
    pr_tab1, pr_tab2, pr_tab3, pr_tab4 = st.tabs([
        "Award Rules", "True On-Cost Calculator", "Weekly Timesheets", "Public Holidays"
    ])

    # ── TAB 1: Award Rules per employee ───────────────────────────────────
    with pr_tab1:
        st.subheader("Award conditions per employee")
        st.caption("Set the Building & Construction General On-site Award conditions for each employee.")

        emp_df_pr = fetch_df("""
            SELECT e.id, e.name, e.role, e.hourly_rate,
                   pr.id AS rule_id,
                   pr.award_name, pr.standard_start, pr.standard_end,
                   pr.break_mins, pr.ordinary_hours,
                   pr.overtime_rate, pr.saturday_rate,
                   pr.public_holiday_rate,
                   pr.workcover_pct, pr.leave_loading_pct,
                   pr.travel_allowance, pr.tool_allowance, pr.meal_allowance
            FROM employees e
            LEFT JOIN payroll_rules pr ON pr.employee_id = e.id
            WHERE e.active=1 ORDER BY e.name
        """)

        if emp_df_pr.empty:
            st.info("No active employees — add them in the Employees page first.")
        else:
            for _, er in emp_df_pr.iterrows():
                eid     = int(er["id"])
                import math as _math
                rule_id_raw = er.get("rule_id")
                has_rule = (rule_id_raw is not None and 
                           str(rule_id_raw) not in ("None","nan","") and
                           not (isinstance(rule_id_raw, float) and _math.isnan(rule_id_raw)))
                rid = int(rule_id_raw) if has_rule else None

                with st.expander(
                    f"**{er['name']}** — {er['role']} — ${er['hourly_rate']:.2f}/hr "
                    f"{'✅ Rules set' if has_rule else '⚠️ No rules set'}",
                    expanded=not has_rule
                ):
                    with st.form(f"pr_form_{eid}"):
                        pc1,pc2,pc3 = st.columns(3)
                        with pc1:
                            p_award  = st.text_input("Award name",
                                value=safe_str(er.get("award_name"),"Building & Construction General On-site Award"))
                            p_start  = st.text_input("Standard start",
                                value=safe_str(er.get("standard_start"),"07:00"),
                                help="24hr format e.g. 07:00")
                            p_end    = st.text_input("Standard end",
                                value=safe_str(er.get("standard_end"),"15:30"))
                            p_break  = st.number_input("Break (mins)",
                                min_value=0, max_value=60,
                                value=safe_int(er.get("break_mins"),30), step=5)
                            p_ohrs   = st.number_input("Ordinary hours/day",
                                min_value=1.0, max_value=12.0,
                                value=safe_float(er.get("ordinary_hours"),8.0), step=0.5)
                        with pc2:
                            p_ot     = st.number_input("Overtime rate (×)",
                                min_value=1.0, max_value=3.0,
                                value=safe_float(er.get("overtime_rate"),1.5), step=0.25)
                            p_sat    = st.number_input("Saturday rate (×)",
                                min_value=1.0, max_value=3.0,
                                value=safe_float(er.get("saturday_rate"),2.0), step=0.25)
                            p_sun    = st.number_input("Sunday rate (×)",
                                min_value=1.0, max_value=3.0,
                                value=2.0, step=0.25)
                            p_ph     = st.number_input("Public holiday rate (×)",
                                min_value=1.0, max_value=3.0,
                                value=safe_float(er.get("public_holiday_rate"),2.0), step=0.25)
                        with pc3:
                            p_wc     = st.number_input("WorkCover %",
                                min_value=0.0, max_value=10.0,
                                value=safe_float(er.get("workcover_pct"),2.0), step=0.1)
                            p_ll     = st.number_input("Leave loading %",
                                min_value=0.0, max_value=30.0,
                                value=safe_float(er.get("leave_loading_pct"),17.5), step=0.5)
                            p_travel = st.number_input("Travel allowance ($/day)",
                                min_value=0.0,
                                value=safe_float(er.get("travel_allowance"),0.0), step=5.0)
                            p_tool   = st.number_input("Tool allowance ($/day)",
                                min_value=0.0,
                                value=safe_float(er.get("tool_allowance"),0.0), step=5.0)
                            p_meal   = st.number_input("Meal allowance ($/day)",
                                min_value=0.0,
                                value=safe_float(er.get("meal_allowance"),0.0), step=5.0)

                        if st.form_submit_button("Save rules", type="primary"):
                            if has_rule and rid:
                                execute("""UPDATE payroll_rules SET
                                    award_name=?,standard_start=?,standard_end=?,break_mins=?,
                                    ordinary_hours=?,overtime_rate=?,saturday_rate=?,
                                    sunday_rate=?,public_holiday_rate=?,workcover_pct=?,
                                    leave_loading_pct=?,travel_allowance=?,tool_allowance=?,
                                    meal_allowance=? WHERE id=?""",
                                    (p_award,p_start,p_end,p_break,p_ohrs,p_ot,p_sat,
                                     p_sun,p_ph,p_wc,p_ll,p_travel,p_tool,p_meal,rid))
                            else:
                                execute("""INSERT INTO payroll_rules
                                    (employee_id,award_name,standard_start,standard_end,
                                     break_mins,ordinary_hours,overtime_rate,saturday_rate,
                                     sunday_rate,public_holiday_rate,workcover_pct,
                                     leave_loading_pct,travel_allowance,tool_allowance,meal_allowance)
                                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                                    (eid,p_award,p_start,p_end,p_break,p_ohrs,p_ot,p_sat,
                                     p_sun,p_ph,p_wc,p_ll,p_travel,p_tool,p_meal))
                            st.success(f"Rules saved for {er['name']}."); st.rerun()

    # ── TAB 2: True On-Cost Calculator ────────────────────────────────────
    with pr_tab2:
        st.subheader("True Employee On-Cost Calculator")
        st.caption("What an employee REALLY costs vs what you quote. Most tradies lose money here.")

        emp_select = fetch_df("SELECT id, name, hourly_rate FROM employees WHERE active=1 ORDER BY name")
        if emp_select.empty:
            st.info("No active employees.")
        else:
            sel_emp = st.selectbox("Select employee", emp_select["name"].tolist())
            emp_row = emp_select[emp_select["name"]==sel_emp].iloc[0]
            eid_oc  = int(emp_row["id"])
            base_rate = float(emp_row["hourly_rate"])

            rules_oc = fetch_df("SELECT * FROM payroll_rules WHERE employee_id=?", (eid_oc,))
            r = rules_oc.iloc[0].to_dict() if not rules_oc.empty else {}

            wc_pct  = float(r.get("workcover_pct", 2.0) or 2.0) / 100
            ll_pct  = float(r.get("leave_loading_pct", 17.5) or 17.5) / 100
            super_r = 0.11
            ann_leave_weeks = 4
            sick_days = 10
            ph_days   = 12
            work_weeks = 52 - ann_leave_weeks
            work_days  = work_weeks * 5
            work_hrs   = work_days * float(r.get("ordinary_hours", 8.0) or 8.0)

            # On-cost components per hour
            super_ph     = base_rate * super_r
            wc_ph        = base_rate * wc_pct
            ann_leave_ph = (base_rate * ann_leave_weeks * 5 *
                           float(r.get("ordinary_hours",8.0) or 8.0) * (1 + ll_pct)) / work_hrs
            sick_ph      = (base_rate * sick_days *
                           float(r.get("ordinary_hours",8.0) or 8.0)) / work_hrs
            ph_ph        = (base_rate * ph_days *
                           float(r.get("ordinary_hours",8.0) or 8.0)) / work_hrs
            travel_ph    = float(r.get("travel_allowance",0) or 0) / float(r.get("ordinary_hours",8.0) or 8.0)
            tool_ph      = float(r.get("tool_allowance",0) or 0) / float(r.get("ordinary_hours",8.0) or 8.0)

            true_cost    = base_rate + super_ph + wc_ph + ann_leave_ph + sick_ph + ph_ph + travel_ph + tool_ph
            oncost_pct   = ((true_cost - base_rate) / base_rate * 100)

            # Display
            st.markdown(f"""
            <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-radius:12px;
                padding:20px;margin-bottom:1rem">
                <div style="font-size:13px;font-weight:700;color:#2dd4bf;text-transform:uppercase;
                    letter-spacing:.1em;margin-bottom:12px">{sel_emp} — True hourly cost</div>
            """, unsafe_allow_html=True)

            components = [
                ("Base hourly rate",       base_rate,    "#e2e8f0", "What you pay per hour"),
                ("Superannuation (11%)",   super_ph,     "#a78bfa", "Mandatory super guarantee"),
                ("WorkCover ({:.1f}%)".format(wc_pct*100), wc_ph, "#fb923c", "Workers compensation insurance"),
                ("Annual leave (4 wks)",   ann_leave_ph, "#7dd3fc", "4 weeks + 17.5% leave loading"),
                ("Sick leave (10 days)",   sick_ph,      "#4ade80", "10 days personal leave"),
                ("Public holidays",        ph_ph,        "#f59e0b", "~12 public holidays/year"),
            ]
            if travel_ph > 0:
                components.append(("Travel allowance", travel_ph, "#2dd4bf", "Daily travel allowance"))
            if tool_ph > 0:
                components.append(("Tool allowance", tool_ph, "#2dd4bf", "Daily tool allowance"))

            comp_html = ""
            for label, val, color, desc in components:
                comp_html += f"""
                <div style="display:flex;align-items:center;justify-content:space-between;
                    padding:8px 0;border-bottom:1px solid #2a3d4f">
                    <div>
                        <div style="font-size:13px;font-weight:600;color:#e2e8f0">{label}</div>
                        <div style="font-size:13px;color:#475569">{desc}</div>
                    </div>
                    <span style="font-size:14px;font-weight:700;color:{color}">${val:.2f}/hr</span>
                </div>"""

            st.markdown(f"<div>{comp_html}</div>", unsafe_allow_html=True)

            st.markdown(f"""
            <div style="background:#0d2233;border:2px solid #2dd4bf;border-radius:10px;
                padding:14px 18px;margin-top:12px;display:flex;justify-content:space-between;
                align-items:center">
                <div>
                    <div style="font-size:13px;color:#64748b;text-transform:uppercase;
                        letter-spacing:.1em">TRUE COST PER HOUR</div>
                    <div style="font-size:36px;font-weight:900;color:#2dd4bf">${true_cost:.2f}</div>
                </div>
                <div style="text-align:right">
                    <div style="font-size:13px;color:#64748b">You quote at</div>
                    <div style="font-size:28px;font-weight:700;color:#f43f5e">${base_rate:.2f}</div>
                    <div style="font-size:13px;color:#f43f5e;font-weight:700">
                        {oncost_pct:.1f}% gap
                    </div>
                </div>
            </div>
            <div style="font-size:13px;color:#475569;text-align:center;margin-top:8px">
                Every hour you quote at ${base_rate:.2f} actually costs you ${true_cost:.2f}
                — a ${true_cost-base_rate:.2f} shortfall per hour
            </div>
            </div>""", unsafe_allow_html=True)

            st.divider()

            # Annual cost summary
            st.subheader("Annual cost summary")
            annual_base  = base_rate * work_hrs
            annual_super = super_ph * work_hrs
            annual_wc    = wc_ph * work_hrs
            annual_leave = ann_leave_ph * work_hrs
            annual_sick  = sick_ph * work_hrs
            annual_ph    = ph_ph * work_hrs
            annual_true  = true_cost * work_hrs

            ac1,ac2,ac3 = st.columns(3)
            ac1.metric("Annual base wages",  f"${annual_base:,.0f}")
            ac2.metric("Total on-costs",     f"${annual_true-annual_base:,.0f}")
            ac3.metric("TRUE annual cost",   f"${annual_true:,.0f}")

    # ── TAB 3: Weekly Timesheets with payroll calc ─────────────────────────
    with pr_tab3:
        st.subheader("Weekly Payroll Timesheets")
        st.caption("Clock in/out times → automatic ordinary/overtime/Saturday calculation → gross pay")

        import datetime as _dttw

        today_ts2 = date.today()
        monday2   = today_ts2 - _dttw.timedelta(days=today_ts2.weekday())
        offset2   = st.session_state.get("pr_week_offset", 0)
        monday2   = monday2 + _dttw.timedelta(weeks=offset2)
        week_days2= [monday2 + _dttw.timedelta(days=i) for i in range(7)]

        wk1,wk2,wk3 = st.columns([1,2,1])
        with wk1:
            if st.button("← Prev", key="pr_prev"):
                st.session_state["pr_week_offset"] = offset2 - 1; st.rerun()
        with wk2:
            st.markdown(f"<div style='text-align:center;font-weight:700;color:#e2e8f0'>"
                        f"Week of {monday2.strftime('%d %b %Y')}</div>", unsafe_allow_html=True)
        with wk3:
            if st.button("Next →", key="pr_next"):
                st.session_state["pr_week_offset"] = offset2 + 1; st.rerun()

        emp_pr = fetch_df("""
            SELECT e.id, e.name, e.hourly_rate,
                   pr.standard_start, pr.standard_end, pr.break_mins,
                   pr.ordinary_hours, pr.overtime_rate, pr.saturday_rate,
                   pr.sunday_rate, pr.public_holiday_rate,
                   pr.travel_allowance, pr.tool_allowance, pr.meal_allowance
            FROM employees e
            LEFT JOIN payroll_rules pr ON pr.employee_id = e.id
            WHERE e.active=1 ORDER BY e.name
        """)

        if emp_pr.empty:
            st.info("No employees found.")
        else:
            week_total_gross = 0

            for _, er in emp_pr.iterrows():
                eid_pr   = int(er["id"])
                emp_name = er["name"]
                base_rate= float(er["hourly_rate"])
                rules_pr = er.to_dict()

                with st.expander(f"**{emp_name}** — ${base_rate:.2f}/hr", expanded=False):
                    emp_week_gross = 0
                    emp_week_ord   = 0
                    emp_week_ot    = 0
                    emp_week_sat   = 0
                    emp_week_ph    = 0

                    # Header
                    st.markdown("""<div style='display:flex;gap:6px;font-size:13px;
                        font-weight:700;color:#475569;text-transform:uppercase;
                        padding:4px 0;margin-bottom:4px'>
                        <span style='width:70px'>Date</span>
                        <span style='width:55px'>Start</span>
                        <span style='width:55px'>End</span>
                        <span style='width:45px'>Break</span>
                        <span style='width:45px'>Ord</span>
                        <span style='width:45px'>OT</span>
                        <span style='width:45px'>Sat</span>
                        <span style='width:45px'>PH</span>
                        <span style='flex:1'>Gross</span>
                        </div>""", unsafe_allow_html=True)

                    for day in week_days2:
                        day_str  = day.isoformat()
                        day_label= day.strftime("%a %d")
                        is_sat   = day.weekday() == 5
                        is_sun   = day.weekday() == 6
                        is_ph    = is_public_holiday(day_str)

                        # Check existing entry
                        existing = fetch_df("""
                            SELECT * FROM timesheet_entries
                            WHERE employee_id=? AND work_date=?
                        """, (eid_pr, day_str))
                        ex = existing.iloc[0].to_dict() if not existing.empty else {}

                        day_color = "#f43f5e" if is_ph else "#f59e0b" if is_sat else "#e2e8f0"
                        day_bg    = "#2a0d0d" if is_ph else "#2a1f0d" if is_sat else "transparent"

                        col_date,col_s,col_e,col_b,col_ord,col_ot,col_sat,col_ph,col_gross,col_save = st.columns([2,1.5,1.5,1.2,1.2,1.2,1.2,1.2,1.5,1])

                        with col_date:
                            ph_tag = " 🎉" if is_ph else " 🏗" if is_sat else ""
                            st.markdown(f"<div style='font-size:13px;color:{day_color};"
                                        f"background:{day_bg};padding:4px 6px;border-radius:4px'>"
                                        f"{day_label}{ph_tag}</div>", unsafe_allow_html=True)

                        with col_s:
                            def_start = str(er.get("standard_start","07:00") or "07:00")
                            s_time = st.text_input("", value=str(ex.get("start_time","") or def_start),
                                key=f"st_{eid_pr}_{day_str}", label_visibility="collapsed")
                        with col_e:
                            def_end = str(er.get("standard_end","15:30") or "15:30")
                            e_time = st.text_input("", value=str(ex.get("end_time","") or def_end),
                                key=f"et_{eid_pr}_{day_str}", label_visibility="collapsed")
                        with col_b:
                            bk_min = st.number_input("", min_value=0, max_value=120,
                                value=int(ex.get("break_mins",30) or 30),
                                key=f"bk_{eid_pr}_{day_str}", label_visibility="collapsed")

                        # Calculate hours
                        try:
                            ord_h,ot_h,sat_h,sun_h,ph_h,tot_h = calculate_hours(
                                day_str, s_time, e_time, bk_min, rules_pr)
                            gross = calculate_gross(base_rate, ord_h, ot_h, sat_h,
                                sun_h, ph_h, rules_pr,
                                float(er.get("travel_allowance",0) or 0) if tot_h > 0 else 0,
                                float(er.get("tool_allowance",0) or 0) if tot_h > 0 else 0,
                                float(er.get("meal_allowance",0) or 0) if tot_h > 0 else 0)
                        except:
                            ord_h=ot_h=sat_h=sun_h=ph_h=gross=0.0

                        with col_ord: st.markdown(f"<div style='font-size:13px;color:#2dd4bf;padding:8px 0'>{ord_h:.1f}</div>", unsafe_allow_html=True)
                        with col_ot:  st.markdown(f"<div style='font-size:13px;color:#f59e0b;padding:8px 0'>{ot_h:.1f}</div>", unsafe_allow_html=True)
                        with col_sat: st.markdown(f"<div style='font-size:13px;color:#fb923c;padding:8px 0'>{sat_h:.1f}</div>", unsafe_allow_html=True)
                        with col_ph:  st.markdown(f"<div style='font-size:13px;color:#f43f5e;padding:8px 0'>{ph_h:.1f}</div>", unsafe_allow_html=True)
                        with col_gross: st.markdown(f"<div style='font-size:13px;font-weight:700;color:#e2e8f0;padding:8px 0'>${gross:,.2f}</div>", unsafe_allow_html=True)

                        with col_save:
                            if st.button("💾", key=f"save_{eid_pr}_{day_str}"):
                                if existing.empty:
                                    execute("""INSERT INTO timesheet_entries
                                        (employee_id,job_id,work_date,start_time,end_time,
                                         break_mins,ordinary_hours,overtime_hours,saturday_hours,
                                         sunday_hours,ph_hours,gross_pay)
                                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                                        (eid_pr,"",day_str,s_time,e_time,bk_min,
                                         ord_h,ot_h,sat_h,sun_h,ph_h,gross))
                                else:
                                    execute("""UPDATE timesheet_entries SET
                                        start_time=?,end_time=?,break_mins=?,
                                        ordinary_hours=?,overtime_hours=?,saturday_hours=?,
                                        sunday_hours=?,ph_hours=?,gross_pay=?
                                        WHERE employee_id=? AND work_date=?""",
                                        (s_time,e_time,bk_min,ord_h,ot_h,sat_h,
                                         sun_h,ph_h,gross,eid_pr,day_str))
                                st.rerun()

                        emp_week_gross += gross
                        emp_week_ord   += ord_h
                        emp_week_ot    += ot_h
                        emp_week_sat   += sat_h
                        emp_week_ph    += ph_h

                    week_total_gross += emp_week_gross

                    # Employee week summary
                    st.markdown(f"""
                    <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-radius:8px;
                        padding:10px 14px;margin-top:8px;display:flex;gap:24px;font-size:13px">
                        <span>Ordinary: <b style="color:#2dd4bf">{emp_week_ord:.1f}h</b></span>
                        <span>Overtime: <b style="color:#f59e0b">{emp_week_ot:.1f}h</b></span>
                        <span>Saturday: <b style="color:#fb923c">{emp_week_sat:.1f}h</b></span>
                        <span>Public Holiday: <b style="color:#f43f5e">{emp_week_ph:.1f}h</b></span>
                        <span style="margin-left:auto">Week gross:
                            <b style="color:#e2e8f0;font-size:14px">${emp_week_gross:,.2f}</b></span>
                    </div>""", unsafe_allow_html=True)

            # Week total
            st.markdown(f"""
            <div style="background:#0d2233;border:2px solid #2dd4bf;border-radius:10px;
                padding:14px 20px;margin-top:12px;text-align:center">
                <div style="font-size:13px;color:#64748b;text-transform:uppercase;
                    letter-spacing:.1em">Total weekly payroll</div>
                <div style="font-size:32px;font-weight:900;color:#2dd4bf">${week_total_gross:,.2f}</div>
                <div style="font-size:13px;color:#475569;margin-top:4px">
                    Excl. super, WorkCover and on-costs
                </div>
            </div>""", unsafe_allow_html=True)

    # ── TAB 4: Public Holidays ─────────────────────────────────────────────
    with pr_tab4:
        st.subheader("Public Holidays")
        st.caption("NSW public holidays — used for automatic public holiday rate calculation.")

        ph_df = fetch_df("SELECT * FROM public_holidays ORDER BY holiday_date")

        if not ph_df.empty:
            for _, ph in ph_df.iterrows():
                phid = int(ph["id"])
                ph_d = date.fromisoformat(str(ph["holiday_date"]))
                is_upcoming = ph_d >= date.today()
                color = "#2dd4bf" if is_upcoming else "#475569"
                st.markdown(
                    f"<div style='display:flex;justify-content:space-between;align-items:center;"
                    f"padding:7px 0;border-bottom:1px solid #1e2d3d'>"
                    f"<span style='font-size:13px;color:{color}'>"
                    f"{'📅' if is_upcoming else '✓'} {ph['name']}</span>"
                    f"<span style='font-size:13px;color:{color};font-weight:600'>"
                    f"{ph_d.strftime('%d %b %Y')}</span>"
                    f"</div>", unsafe_allow_html=True)

        st.divider()
        st.subheader("Add public holiday")
        with st.form("add_ph"):
            ph1,ph2 = st.columns(2)
            with ph1: a_ph_name = st.text_input("Holiday name")
            with ph2: a_ph_date = st.date_input("Date", value=date.today(), format="DD/MM/YYYY")
            if st.form_submit_button("Add", type="primary"):
                if a_ph_name.strip():
                    execute("INSERT INTO public_holidays (holiday_date,name,state) VALUES (?,?,?)",
                            (a_ph_date.isoformat(), a_ph_name.strip(), "NSW"))
                    st.success("Added."); st.rerun()

# ─────────────────────────────────────────────
#  PAGE: STACKCT IMPORT
# ─────────────────────────────────────────────
elif page == "StackCT Import":
    st.title("StackCT Import")
    st.caption("Import your StackCT takeoff quantities directly into a job's scan sheet.")

    import io as _io
    import csv as _csv

    # ── How to export from StackCT ────────────────────────────────────────
    with st.expander("📖 How to export from StackCT", expanded=False):
        st.markdown("""
        <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-radius:10px;padding:14px 18px;font-size:13px;color:#94a3b8;line-height:1.9">
            <div style="color:#e2e8f0;font-weight:700;margin-bottom:8px">Steps to export from StackCT:</div>
            <div>1. Open your project in StackCT</div>
            <div>2. Click <strong style="color:#2dd4bf">Reports</strong> in the top menu</div>
            <div>3. Select <strong style="color:#2dd4bf">Takeoff Quantity</strong></div>
            <div>4. Click <strong style="color:#2dd4bf">Export</strong> → <strong style="color:#2dd4bf">CSV (All Data)</strong></div>
            <div>5. Save the file and upload it below</div>
            <div style="margin-top:8px;color:#475569">
                The CSV needs columns: <strong style="color:#94a3b8">Takeoff Name</strong>,
                <strong style="color:#94a3b8">Takeoff Quantity</strong>,
                <strong style="color:#94a3b8">Takeoff Unit</strong>
            </div>
        </div>
        """, unsafe_allow_html=True)

    # ── Select target job ─────────────────────────────────────────────────
    jobs_sct = fetch_df("SELECT job_id, client, job_finish FROM jobs WHERE archived=0 ORDER BY job_id")
    if jobs_sct.empty:
        st.warning("No jobs yet — create a job first."); st.stop()

    sc1,sc2 = st.columns(2)
    with sc1:
        target_job = st.selectbox("Import into job",
            jobs_sct["job_id"].tolist(),
            format_func=lambda x: f"{x} — {jobs_sct.loc[jobs_sct['job_id']==x,'client'].iloc[0]}")
    with sc2:
        job_row    = jobs_sct[jobs_sct["job_id"]==target_job].iloc[0]
        cur_finish = str(job_row.get("job_finish","") or "Steel")
        finishes_s = fetch_df("SELECT finish_name FROM material_finishes ORDER BY sort_order")
        fin_opts   = finishes_s["finish_name"].tolist() if not finishes_s.empty else ["Steel","MATT","ULTRA","Aluminium","VM Zinc"]
        sel_finish = st.selectbox("Job finish",
            fin_opts,
            index=fin_opts.index(cur_finish) if cur_finish in fin_opts else 0,
            help="This finish will apply to all imported items")
        if sel_finish != cur_finish:
            execute("UPDATE jobs SET job_finish=? WHERE job_id=?", (sel_finish, target_job))
            st.rerun()

    # Finish badge
    fc_map = {"Steel":"#94a3b8","MATT":"#2dd4bf","ULTRA":"#a78bfa",
              "Aluminium":"#7dd3fc","VM Zinc":"#f59e0b","Copper":"#fb923c","Zincalume":"#4ade80"}
    fc = fc_map.get(sel_finish,"#64748b")
    st.markdown(f"""
    <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-left:4px solid {fc};
        border-radius:8px;padding:10px 14px;margin:8px 0;font-size:13px;color:#94a3b8">
        Importing into <strong style="color:#e2e8f0">{target_job}</strong> with finish
        <strong style="color:{fc}">{sel_finish}</strong> — all items will use {sel_finish} rates.
        Override individual lines after import if needed.
    </div>""", unsafe_allow_html=True)

    st.divider()

    # ── Upload CSV ────────────────────────────────────────────────────────
    st.subheader("Upload StackCT export")
    uploaded_csv = st.file_uploader("Upload CSV or Excel from StackCT",
        type=["csv","xlsx","xls"], key="stackct_upload")

    if uploaded_csv:
        try:
            # Parse file
            if uploaded_csv.name.endswith(".csv"):
                content = uploaded_csv.read().decode("utf-8-sig", errors="ignore")
                reader  = _csv.DictReader(_io.StringIO(content))
                rows    = list(reader)
            else:
                import pandas as _pdsc
                df_sc = _pdsc.read_excel(uploaded_csv)
                rows  = df_sc.to_dict("records")

            # Find the right columns (flexible naming)
            if not rows:
                st.error("Empty file."); st.stop()

            first_row = rows[0]
            col_keys  = list(first_row.keys())

            # Map columns
            name_col = next((k for k in col_keys if "name" in k.lower() or "description" in k.lower() or "takeoff" in k.lower() and "name" in k.lower()), col_keys[0])
            qty_col  = next((k for k in col_keys if "qty" in k.lower() or "quantity" in k.lower()), col_keys[1] if len(col_keys)>1 else col_keys[0])
            unit_col = next((k for k in col_keys if "unit" in k.lower() or "uom" in k.lower()), col_keys[2] if len(col_keys)>2 else None)

            # Parse rows
            parsed = []
            for row in rows:
                name = str(row.get(name_col,"") or "").strip()
                qty_raw = str(row.get(qty_col,"") or "0").strip().replace(",","")
                unit = str(row.get(unit_col,"") or "") if unit_col else ""
                # Normalise unit
                unit = unit.replace("Ln M","lm").replace("Sq M","m2").replace("EA","ea").replace("Ea","ea")
                try:
                    qty = float(qty_raw)
                except:
                    qty = 0.0
                if name and qty > 0:
                    parsed.append({"name":name,"qty":qty,"unit":unit})

            st.success(f"✅ Found {len(parsed)} items with quantities > 0")

            # Load mapping table
            mapping_df = fetch_df("SELECT stackct_name, catalogue_item, catalogue_section FROM stackct_mapping")
            mapping    = {r["stackct_name"]: {"item":r["catalogue_item"],"section":r["catalogue_section"]}
                         for _,r in mapping_df.iterrows()} if not mapping_df.empty else {}

            # Load catalogue for matching
            try:
                cat_sc = load_catalogue()
                cat_items = cat_sc["Description"].tolist()
                cat_map   = {str(r["Description"]):(str(r["Category"]),float(r["MaterialCost"]),float(r["LabourCost"]))
                             for _,r in cat_sc.iterrows()}
            except:
                cat_items = []
                cat_map   = {}

            # ── Preview table ─────────────────────────────────────────────
            st.subheader("Review before importing")
            st.caption("Match each StackCT item to your catalogue. Items already mapped are auto-filled.")

            import_rows = []
            for item in parsed:
                name = item["name"]
                # Check mapping table first
                if name in mapping:
                    mapped_item    = mapping[name]["item"]
                    mapped_section = mapping[name]["section"]
                    mat_rate = cat_map.get(mapped_item,(None,0,0))[1]
                    lab_rate = cat_map.get(mapped_item,(None,0,0))[2]
                    status   = "✅ Mapped"
                else:
                    # Try fuzzy match on catalogue
                    mapped_item = ""
                    mapped_section = ""
                    mat_rate = 0.0
                    lab_rate = 0.0
                    status   = "⚠️ Unmapped"
                    # Simple prefix match — CF-, DP-, FG-, RI-, SL-
                    prefix = name.split(" - ")[0].strip() + " -" if " - " in name else ""
                    for ci in cat_items:
                        if prefix and ci.upper().startswith(prefix.upper()):
                            mapped_item    = ci
                            mapped_section = cat_map.get(ci,("",0,0))[0] if isinstance(cat_map.get(ci),tuple) else ""
                            mapped_section = str(cat_sc.loc[cat_sc["Description"]==ci,"Category"].iloc[0]) if ci in cat_sc["Description"].values else ""
                            mat_rate = float(cat_map.get(ci,(None,0,0))[1])
                            lab_rate = float(cat_map.get(ci,(None,0,0))[2])
                            status   = "🔄 Auto-matched"
                            break

                import_rows.append({
                    "StackCT Name": name,
                    "Qty":          item["qty"],
                    "Unit":         item["unit"],
                    "→ Catalogue":  mapped_item or "— not matched —",
                    "Mat rate":     f"${mat_rate:.2f}",
                    "Lab rate":     f"${lab_rate:.2f}",
                    "Status":       status,
                })

            import pandas as _pdsc2
            preview_df = _pdsc2.DataFrame(import_rows)
            st.dataframe(preview_df, width="stretch", hide_index=True)

            # Summary
            mapped_count   = sum(1 for r in import_rows if "✅" in r["Status"] or "🔄" in r["Status"])
            unmapped_count = sum(1 for r in import_rows if "⚠️" in r["Status"])

            st.markdown(f"""
            <div style="display:flex;gap:20px;margin:10px 0;font-size:13px">
                <span style="color:#2dd4bf;font-weight:700">✅ {mapped_count} matched</span>
                <span style="color:#f59e0b;font-weight:700">⚠️ {unmapped_count} unmatched — will be imported as-is</span>
            </div>""", unsafe_allow_html=True)

            # ── Unmapped items — set mapping ──────────────────────────────
            unmapped = [r for r in import_rows if "⚠️" in r["Status"]]
            if unmapped and cat_items:
                with st.expander(f"🔧 Map {len(unmapped)} unmatched items", expanded=unmapped_count>0):
                    st.caption("Map these StackCT names to your catalogue once — saved forever.")
                    for um in unmapped:
                        with st.form(f"map_{um['StackCT Name']}"):
                            um1,um2 = st.columns([2,3])
                            with um1:
                                st.markdown(f"<div style='font-weight:600;color:#f59e0b;font-size:13px;padding:8px 0'>{um['StackCT Name']}</div>", unsafe_allow_html=True)
                            with um2:
                                cat_choice = st.selectbox("Map to catalogue item",
                                    ["— skip —"] + cat_items,
                                    key=f"map_sel_{um['StackCT Name']}")
                            if st.form_submit_button("Save mapping"):
                                if cat_choice != "— skip —":
                                    sec = str(cat_sc.loc[cat_sc["Description"]==cat_choice,"Category"].iloc[0]) if cat_choice in cat_sc["Description"].values else ""
                                    existing_map = fetch_df("SELECT id FROM stackct_mapping WHERE stackct_name=?", (um["StackCT Name"],))
                                    if existing_map.empty:
                                        execute("INSERT INTO stackct_mapping (stackct_name,catalogue_item,catalogue_section) VALUES (?,?,?)",
                                                (um["StackCT Name"], cat_choice, sec))
                                    else:
                                        execute("UPDATE stackct_mapping SET catalogue_item=?,catalogue_section=? WHERE stackct_name=?",
                                                (cat_choice, sec, um["StackCT Name"]))
                                    st.success(f"Mapped: {um['StackCT Name']} → {cat_choice}"); st.rerun()

            st.divider()

            # ── Import button ─────────────────────────────────────────────
            col_imp1, col_imp2 = st.columns([2,4])
            with col_imp1:
                import_mode = st.radio("Import mode",
                    ["Add to existing scan sheet","Replace scan sheet"],
                    help="Add = merges with existing quantities. Replace = clears and starts fresh.")
            with col_imp2:
                st.markdown(f"""
                <div style="background:#1e2d3d;border:1px solid #2a3d4f;border-radius:8px;
                    padding:10px 14px;margin-top:4px;font-size:13px;color:#94a3b8">
                    <strong style="color:#e2e8f0">{len(import_rows)} items</strong> from StackCT →
                    importing into <strong style="color:#2dd4bf">{target_job}</strong> with
                    <strong style="color:{fc}">{sel_finish}</strong> finish
                </div>""", unsafe_allow_html=True)

            if st.button("🚀 Import into Quote Builder", type="primary"):
                # Reload mapping after any saves
                mapping_df2 = fetch_df("SELECT stackct_name,catalogue_item,catalogue_section FROM stackct_mapping")
                mapping2    = {r["stackct_name"]:{"item":r["catalogue_item"],"section":r["catalogue_section"]}
                               for _,r in mapping_df2.iterrows()} if not mapping_df2.empty else {}

                if "Replace" in import_mode:
                    st.session_state.pop(f"scan_{target_job}", None)

                # Get or init scan sheet
                ss_key = f"scan_{target_job}"
                if ss_key not in st.session_state:
                    st.session_state[ss_key] = {}
                scan = st.session_state[ss_key]

                imported = 0
                for item in parsed:
                    name = item["name"]
                    qty  = item["qty"]
                    unit = item["unit"]

                    # Get catalogue match
                    if name in mapping2:
                        cat_item = mapping2[name]["item"]
                        cat_sec  = mapping2[name]["section"]
                    else:
                        cat_item = name
                        cat_sec  = name.split(" - ")[0] if " - " in name else "Imported"

                    # Get rates from catalogue
                    mat_r = 0.0; lab_r = 0.0
                    if cat_item in cat_map:
                        mat_r = float(cat_map[cat_item][1])
                        lab_r = float(cat_map[cat_item][2])

                    # Check for finish-specific rates
                    finish_rate = fetch_df("""
                        SELECT cf.material_rate, cf.labour_rate
                        FROM catalogue_finishes cf
                        JOIN material_finishes mf ON mf.id = cf.finish_id
                        WHERE cf.catalogue_item=? AND mf.finish_name=?
                    """, (cat_item, sel_finish))
                    if not finish_rate.empty:
                        mat_r = float(finish_rate.iloc[0]["material_rate"])
                        lab_r = float(finish_rate.iloc[0]["labour_rate"])

                    if cat_item in scan and "Add" in import_mode:
                        scan[cat_item]["qty"] += qty
                    else:
                        scan[cat_item] = {
                            "section": cat_sec,
                            "uom":     unit or "lm",
                            "qty":     qty,
                            "mat":     mat_r,
                            "lab":     lab_r,
                        }
                    imported += 1

                st.success(f"✅ {imported} items imported into {target_job} scan sheet with {sel_finish} finish!")
                st.info(f"Go to Jobs → open {target_job} → Quote Builder tab to review and save the estimate.")

        except Exception as e:
            st.error(f"Error reading file: {e}")
            st.info("Make sure you exported 'CSV (All Data)' from StackCT Reports → Takeoff Quantity")

    st.divider()

    # ── Saved mappings ────────────────────────────────────────────────────
    st.subheader("Saved StackCT mappings")
    st.caption("These are remembered forever — build them up over time.")
    saved_map_df = fetch_df("SELECT stackct_name, catalogue_item, catalogue_section FROM stackct_mapping ORDER BY stackct_name")
    if saved_map_df.empty:
        st.info("No mappings saved yet — they build up as you import jobs.")
    else:
        st.dataframe(saved_map_df, width="stretch", hide_index=True)
        if st.button("Clear all mappings"):
            execute("DELETE FROM stackct_mapping")
            st.rerun()

    st.divider()

    # ── Finish rates manager ──────────────────────────────────────────────
    st.subheader("Finish rates")
    st.caption("Set different material rates per finish for any catalogue item. Leave blank to use catalogue default.")

    with st.expander("+ Add finish rate override", expanded=False):
        try:
            cat_fr = load_catalogue()
            cat_items_fr = cat_fr["Description"].tolist()
        except:
            cat_items_fr = []

        finishes_fr = fetch_df("SELECT id, finish_name FROM material_finishes ORDER BY sort_order")

        with st.form("add_finish_rate"):
            fr1,fr2,fr3,fr4 = st.columns(4)
            with fr1: fr_item   = st.selectbox("Catalogue item", cat_items_fr if cat_items_fr else [""])
            with fr2: fr_finish = st.selectbox("Finish", finishes_fr["finish_name"].tolist() if not finishes_fr.empty else ["Steel"])
            with fr3: fr_mat    = st.number_input("Material rate ($/unit)", min_value=0.0, value=0.0, step=0.5)
            with fr4: fr_lab    = st.number_input("Labour rate ($/unit)", min_value=0.0, value=0.0, step=0.5)

            if st.form_submit_button("Save rate", type="primary"):
                fin_id = int(finishes_fr.loc[finishes_fr["finish_name"]==fr_finish,"id"].iloc[0]) if not finishes_fr.empty else 1
                cat_sec_fr = str(cat_fr.loc[cat_fr["Description"]==fr_item,"Category"].iloc[0]) if fr_item in cat_fr["Description"].values else ""
                existing_fr = fetch_df("SELECT id FROM catalogue_finishes WHERE catalogue_item=? AND finish_id=?", (fr_item, fin_id))
                if existing_fr.empty:
                    execute("INSERT INTO catalogue_finishes (catalogue_item,catalogue_section,finish_id,material_rate,labour_rate) VALUES (?,?,?,?,?)",
                            (fr_item, cat_sec_fr, fin_id, fr_mat, fr_lab))
                else:
                    execute("UPDATE catalogue_finishes SET material_rate=?,labour_rate=? WHERE catalogue_item=? AND finish_id=?",
                            (fr_mat, fr_lab, fr_item, fin_id))
                st.success(f"Rate saved: {fr_item} — {fr_finish} — ${fr_mat:.2f} mat / ${fr_lab:.2f} lab"); st.rerun()

    # Show existing finish rates
    fr_df = fetch_df("""
        SELECT cf.catalogue_item, mf.finish_name, cf.material_rate, cf.labour_rate
        FROM catalogue_finishes cf
        JOIN material_finishes mf ON mf.id = cf.finish_id
        ORDER BY cf.catalogue_item, mf.sort_order
    """)
    if not fr_df.empty:
        st.dataframe(fr_df, width="stretch", hide_index=True)
