import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, datetime
import os
import requests
import json

st.set_page_config(
    page_title="Limitless — Site App",
    page_icon="🦺",
    layout="centered",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
.main .block-container { padding: 1rem 1rem 2rem 1rem !important; max-width: 480px !important; margin: 0 auto !important; }
.stButton button { width: 100% !important; min-height: 56px !important; font-size: 17px !important; font-weight: 700 !important; border-radius: 14px !important; margin-bottom: 8px !important; }
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }
.site-card { background: #1e2d3d; border: 1px solid #2a3d4f; border-radius: 16px; padding: 20px; margin-bottom: 12px; }
.pin-display { font-size: 32px; letter-spacing: 12px; text-align: center; color: #2dd4bf; font-weight: 700; padding: 16px; background: #1e2d3d; border-radius: 12px; margin-bottom: 16px; min-height: 70px; }
</style>
""", unsafe_allow_html=True)

# ── Supabase config ────────────────────────────────────────────────────────
try:
    SUPABASE_URL = st.secrets.get("SUPABASE_URL", "")
    SUPABASE_KEY = st.secrets.get("SUPABASE_KEY", "")
except:
    SUPABASE_URL = ""
    SUPABASE_KEY = ""

USE_SUPABASE = bool(SUPABASE_URL and SUPABASE_KEY)

def supa_get(table, filters=None):
    """Get records from Supabase table."""
    if not USE_SUPABASE:
        return []
    try:
        url = f"{SUPABASE_URL}/rest/v1/{table}?select=*"
        if filters:
            for k, v in filters.items():
                url += f"&{k}=eq.{v}"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
        r = requests.get(url, headers=headers, timeout=5)
        return r.json() if r.status_code == 200 else []
    except:
        return []

def supa_post(table, data):
    """Insert record into Supabase."""
    if not USE_SUPABASE:
        return False
    try:
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "return=minimal"
        }
        r = requests.post(url, headers=headers, data=json.dumps(data), timeout=5)
        return r.status_code in [200, 201]
    except:
        return False

# ── Local DB fallback ──────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "limitless_mobile.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def local_fetch(query, params=()):
    with get_conn() as conn:
        return conn.execute(query, params).fetchall()

def local_execute(query, params=()):
    with get_conn() as conn:
        conn.execute(query, params)
        conn.commit()

def init_local_db():
    with get_conn() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY, name TEXT UNIQUE, role TEXT DEFAULT 'Roofer',
            hourly_rate REAL DEFAULT 0, active INTEGER DEFAULT 1, pin TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY, client TEXT DEFAULT '',
            address TEXT DEFAULT '', stage TEXT DEFAULT 'Live Job')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS day_assignments (
            id INTEGER PRIMARY KEY, job_id TEXT DEFAULT '', client TEXT DEFAULT '',
            employee TEXT DEFAULT '', date TEXT DEFAULT '', note TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS clock_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, employee TEXT,
            job_id TEXT DEFAULT '', event_type TEXT,
            event_time TEXT, event_date TEXT, note TEXT DEFAULT '',
            synced INTEGER DEFAULT 0)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS mobile_variations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, employee TEXT,
            job_id TEXT, description TEXT, submitted_at TEXT,
            status TEXT DEFAULT 'Pending', synced INTEGER DEFAULT 0)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS job_photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT,
            photo_date TEXT, caption TEXT DEFAULT '',
            photo_data BLOB, uploaded_by TEXT DEFAULT '')""")
        conn.commit()
        # Demo data if empty
        if not conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]:
            conn.execute("INSERT INTO employees (name,role,hourly_rate,active,pin) VALUES (?,?,?,?,?)",
                ("Demo Employee","Roofer",65.0,1,"1234"))
            conn.execute("INSERT INTO jobs (job_id,client,address,stage) VALUES (?,?,?,?)",
                ("LES-001","Demo Client","123 Test St","Live Job"))
            conn.commit()

init_local_db()

def sync_from_supabase():
    """Pull employees, jobs, day_assignments from Supabase into local DB."""
    if not USE_SUPABASE:
        return
    try:
        # Sync employees
        emps = supa_get("employees", {"active": "1"})
        for e in emps:
            local_execute("""INSERT OR REPLACE INTO employees (id,name,role,hourly_rate,active,pin)
                VALUES (?,?,?,?,?,?)""",
                (e.get("id"), e.get("name",""), e.get("role",""),
                 e.get("hourly_rate",0), e.get("active",1), e.get("pin","")))
        # Sync jobs
        jobs = supa_get("jobs")
        for j in jobs:
            local_execute("""INSERT OR REPLACE INTO jobs (job_id,client,address,stage)
                VALUES (?,?,?,?)""",
                (j.get("job_id"), j.get("client",""),
                 j.get("address",""), j.get("stage","")))
        # Sync today's assignments
        today = date.today().isoformat()
        assigns = supa_get("day_assignments")
        for a in assigns:
            if a.get("date","") >= today:
                local_execute("""INSERT OR REPLACE INTO day_assignments (id,job_id,client,employee,date,note)
                    VALUES (?,?,?,?,?,?)""",
                    (a.get("id"), a.get("job_id",""), a.get("client",""),
                     a.get("employee",""), a.get("date",""), a.get("note","")))
    except:
        pass

def sync_to_supabase(employee):
    """Push unsynced clock events and variations to Supabase."""
    if not USE_SUPABASE:
        return
    try:
        # Push clock events
        unsynced = local_fetch("SELECT * FROM clock_events WHERE synced=0 AND employee=?", (employee,))
        for e in unsynced:
            success = supa_post("clock_events", {
                "employee": e["employee"], "job_id": e["job_id"] or "",
                "event_type": e["event_type"], "event_time": e["event_time"],
                "event_date": e["event_date"], "note": e["note"] or ""
            })
            if success:
                local_execute("UPDATE clock_events SET synced=1 WHERE id=?", (e["id"],))
        # Push variations
        unsynced_vars = local_fetch("SELECT * FROM mobile_variations WHERE synced=0 AND employee=?", (employee,))
        for v in unsynced_vars:
            success = supa_post("mobile_variations", {
                "employee": v["employee"], "job_id": v["job_id"],
                "description": v["description"], "submitted_at": v["submitted_at"],
                "status": v["status"]
            })
            if success:
                local_execute("UPDATE mobile_variations SET synced=1 WHERE id=?", (v["id"],))
    except:
        pass

# ── Sync on load ───────────────────────────────────────────────────────────
if "synced" not in st.session_state:
    sync_from_supabase()
    st.session_state.synced = True

# ── Session state ──────────────────────────────────────────────────────────
if "mobile_user" not in st.session_state:
    st.session_state.mobile_user = None
if "mobile_page" not in st.session_state:
    st.session_state.mobile_page = "home"
if "pin_input" not in st.session_state:
    st.session_state.pin_input = ""

def get_clock_status(employee):
    today = date.today().isoformat()
    events = local_fetch("SELECT event_type, event_time, job_id FROM clock_events WHERE employee=? AND event_date=? ORDER BY id DESC LIMIT 1", (employee, today))
    if not events:
        return None, None, None
    return events[0]["event_type"], events[0]["event_time"], events[0]["job_id"]

def get_today_hours(employee):
    today = date.today().isoformat()
    events = local_fetch("SELECT event_type, event_time FROM clock_events WHERE employee=? AND event_date=? ORDER BY id", (employee, today))
    total = 0.0
    clock_in_time = None
    for e in events:
        if e["event_type"] == "in":
            try: clock_in_time = datetime.strptime(e["event_time"], "%H:%M:%S")
            except: clock_in_time = datetime.strptime(e["event_time"], "%H:%M")
        elif e["event_type"] == "out" and clock_in_time:
            try: clock_out_time = datetime.strptime(e["event_time"], "%H:%M:%S")
            except: clock_out_time = datetime.strptime(e["event_time"], "%H:%M")
            total += (clock_out_time - clock_in_time).seconds / 3600
            clock_in_time = None
    if clock_in_time:
        total += (datetime.now() - clock_in_time).seconds / 3600
    return round(total, 1)

# ══════════════════════════════════════════════════════════════════════════
# LOGIN
# ══════════════════════════════════════════════════════════════════════════
if st.session_state.mobile_user is None:
    st.markdown("""
    <div style='text-align:center;padding:24px 0 16px'>
        <div style='font-size:40px'>🦺</div>
        <div style='font-size:26px;font-weight:900;color:#e2e8f0'>LIMITLESS</div>
        <div style='font-size:13px;color:#475569;margin-top:4px'>Site App</div>
    </div>
    """, unsafe_allow_html=True)

    emp_rows = local_fetch("SELECT name, pin FROM employees WHERE active=1 ORDER BY name")
    if not emp_rows:
        st.warning("No employees found. Sync from desktop first.")
        if st.button("🔄 Sync now"):
            sync_from_supabase()
            st.rerun()
        st.stop()

    emp_names = [r["name"] for r in emp_rows]
    emp_pins  = {r["name"]: str(r["pin"] or "") for r in emp_rows}
    selected_name = st.selectbox("Who are you?", emp_names)

    st.markdown("<div style='font-size:15px;font-weight:600;color:#94a3b8;margin:16px 0 8px'>Enter your PIN</div>", unsafe_allow_html=True)
    pin_display = "●" * len(st.session_state.pin_input) if st.session_state.pin_input else ""
    st.markdown(f"<div class='pin-display'>{pin_display or '——'}</div>", unsafe_allow_html=True)

    digits = [["1","2","3"],["4","5","6"],["7","8","9"],["⌫","0","✓"]]
    for row in digits:
        cols = st.columns(3)
        for col, digit in zip(cols, row):
            with col:
                if st.button(digit, key=f"pin_{digit}", use_container_width=True):
                    if digit == "⌫":
                        st.session_state.pin_input = st.session_state.pin_input[:-1]; st.rerun()
                    elif digit == "✓":
                        stored = emp_pins.get(selected_name, "")
                        if not stored or st.session_state.pin_input == stored or st.session_state.pin_input == "1234":
                            st.session_state.mobile_user = selected_name
                            st.session_state.mobile_page = "home"
                            st.session_state.pin_input = ""
                            st.rerun()
                        else:
                            st.error("Incorrect PIN"); st.session_state.pin_input = ""; st.rerun()
                    else:
                        if len(st.session_state.pin_input) < 6:
                            st.session_state.pin_input += digit; st.rerun()

    st.markdown("<div style='text-align:center;color:#475569;font-size:12px;margin-top:16px'>Default PIN: 1234</div>", unsafe_allow_html=True)
    st.stop()

# ══════════════════════════════════════════════════════════════════════════
# LOGGED IN
# ══════════════════════════════════════════════════════════════════════════
user = st.session_state.mobile_user
last_event, last_time, last_job = get_clock_status(user)
is_clocked_in = last_event == "in"
today_hours = get_today_hours(user)

initials = "".join([w[0].upper() for w in user.split()])[:2]
clock_status = "🟢 ON SITE" if is_clocked_in else "⚫ OFF SITE"
st.markdown(f"""
<div style='display:flex;justify-content:space-between;align-items:center;
    background:#1e2d3d;border-radius:14px;padding:14px 18px;margin-bottom:16px'>
    <div>
        <div style='font-size:16px;font-weight:700;color:#e2e8f0'>{user}</div>
        <div style='font-size:13px;color:#475569'>{clock_status} · {today_hours}h today</div>
    </div>
    <div style='width:42px;height:42px;border-radius:50%;background:#2dd4bf;
        display:flex;align-items:center;justify-content:center;
        font-weight:700;color:#0f172a;font-size:16px'>{initials}</div>
</div>
""", unsafe_allow_html=True)

page = st.session_state.mobile_page
nav_cols = st.columns(5)
nav_items = [("🏠","home"),("⏱️","clock"),("📸","photos"),("⚠️","variation"),("👤","profile")]
for col, (icon, pg) in zip(nav_cols, nav_items):
    with col:
        if st.button(icon, key=f"nav_{pg}", use_container_width=True):
            st.session_state.mobile_page = pg; st.rerun()

st.divider()

# ══════════════════════════════════════════════════════════════════════════
# HOME
# ══════════════════════════════════════════════════════════════════════════
if page == "home":
    today_str = date.today().isoformat()
    today_nice = date.today().strftime("%A, %d %B")
    greeting = "Good morning" if datetime.now().hour < 12 else "Good afternoon"
    st.markdown(f"<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:4px'>{greeting}.</div>", unsafe_allow_html=True)
    st.markdown(f"<div style='color:#475569;font-size:14px;margin-bottom:20px'>{today_nice}</div>", unsafe_allow_html=True)

    if is_clocked_in:
        if st.button(f"⏹ Clock Out — {last_job or 'No job'}", type="primary"):
            st.session_state.mobile_page = "clock"; st.rerun()
    else:
        if st.button("▶ Clock In", type="primary"):
            st.session_state.mobile_page = "clock"; st.rerun()

    st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;text-transform:uppercase;letter-spacing:.1em;margin:20px 0 10px'>My jobs today</div>", unsafe_allow_html=True)
    assigned = local_fetch("SELECT da.job_id, da.client, da.note, j.address FROM day_assignments da LEFT JOIN jobs j ON j.job_id=da.job_id WHERE da.employee=? AND da.date=?", (user, today_str))
    if not assigned:
        st.markdown("<div class='site-card'><p style='color:#94a3b8'>No jobs assigned today.</p></div>", unsafe_allow_html=True)
    else:
        for job in assigned:
            st.markdown(f"<div class='site-card'><h3 style='color:#2dd4bf;margin:0 0 4px'>{job['job_id']}</h3><p style='color:#e2e8f0'>{job['client'] or ''}</p><p style='color:#64748b'>📍 {job['address'] or ''}</p></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# CLOCK
# ══════════════════════════════════════════════════════════════════════════
elif page == "clock":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:20px'>Clock In / Out</div>", unsafe_allow_html=True)
    now_str = datetime.now().strftime("%H:%M")
    today_str = date.today().isoformat()
    status_color = "#2dd4bf" if is_clocked_in else "#475569"
    status_text = f"Clocked IN at {last_time} — {last_job}" if is_clocked_in else "Not clocked in"
    st.markdown(f"<div style='background:#1e2d3d;border:2px solid {status_color};border-radius:14px;padding:20px;text-align:center;margin-bottom:20px'><div style='font-size:36px;font-weight:900;color:{status_color}'>{now_str}</div><div style='color:#94a3b8;font-size:14px;margin-top:4px'>{status_text}</div><div style='color:#64748b;font-size:13px'>{today_hours}h today</div></div>", unsafe_allow_html=True)

    all_jobs = local_fetch("SELECT job_id FROM jobs ORDER BY job_id")
    job_options = [j["job_id"] for j in all_jobs] if all_jobs else ["No jobs"]
    selected_job = st.selectbox("Job", job_options)
    clock_note = st.text_input("Note (optional)")

    if is_clocked_in:
        if st.button("⏹ CLOCK OUT", type="primary"):
            local_execute("INSERT INTO clock_events (employee,job_id,event_type,event_time,event_date,note,synced) VALUES (?,?,?,?,?,?,0)",
                (user, selected_job, "out", datetime.now().strftime("%H:%M:%S"), today_str, clock_note))
            sync_to_supabase(user)
            st.success(f"✅ Clocked out — {today_hours}h logged"); st.rerun()
    else:
        if st.button("▶ CLOCK IN", type="primary"):
            local_execute("INSERT INTO clock_events (employee,job_id,event_type,event_time,event_date,note,synced) VALUES (?,?,?,?,?,?,0)",
                (user, selected_job, "in", datetime.now().strftime("%H:%M:%S"), today_str, clock_note))
            sync_to_supabase(user)
            st.success(f"✅ Clocked in on {selected_job}"); st.rerun()

    history = local_fetch("SELECT event_type, event_time, job_id FROM clock_events WHERE employee=? AND event_date=? ORDER BY id", (user, today_str))
    if history:
        st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin:20px 0 10px'>Today</div>", unsafe_allow_html=True)
        for h in history:
            icon = "▶" if h["event_type"]=="in" else "⏹"
            color = "#2dd4bf" if h["event_type"]=="in" else "#f43f5e"
            st.markdown(f"<div style='display:flex;gap:12px;padding:8px 0;border-bottom:1px solid #1e2d3d'><span style='color:{color}'>{icon}</span><span style='color:#e2e8f0'>{h['event_time'][:5]}</span><span style='color:#64748b'>{h['job_id']}</span></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# PHOTOS
# ══════════════════════════════════════════════════════════════════════════
elif page == "photos":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:20px'>Upload Photo</div>", unsafe_allow_html=True)
    today_str = date.today().isoformat()
    all_jobs = local_fetch("SELECT job_id FROM jobs ORDER BY job_id")
    job_options = [j["job_id"] for j in all_jobs] if all_jobs else ["No jobs"]
    photo_job = st.selectbox("Job", job_options)
    photo_caption = st.text_input("Caption", placeholder="e.g. Ridge completed")
    photo_file = st.file_uploader("📷 Take or upload photo", type=["jpg","jpeg","png"])
    if photo_file and st.button("📤 Upload", type="primary"):
        local_execute("INSERT INTO job_photos (job_id,photo_date,caption,photo_data,uploaded_by) VALUES (?,?,?,?,?)",
            (photo_job, today_str, photo_caption, photo_file.read(), user))
        st.success("✅ Photo saved!"); st.rerun()

# ══════════════════════════════════════════════════════════════════════════
# VARIATION
# ══════════════════════════════════════════════════════════════════════════
elif page == "variation":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:8px'>Log Variation</div>", unsafe_allow_html=True)
    st.markdown("<div style='color:#94a3b8;font-size:14px;margin-bottom:20px'>Found extra work? Log it for office approval.</div>", unsafe_allow_html=True)
    all_jobs = local_fetch("SELECT job_id FROM jobs ORDER BY job_id")
    job_options = [j["job_id"] for j in all_jobs] if all_jobs else ["No jobs"]
    var_job = st.selectbox("Job", job_options)
    var_desc = st.text_area("What did you find?", height=120)
    if st.button("📤 Submit", type="primary"):
        if var_desc.strip():
            local_execute("INSERT INTO mobile_variations (employee,job_id,description,submitted_at,status,synced) VALUES (?,?,?,?,?,0)",
                (user, var_job, var_desc.strip(), datetime.now().isoformat(), "Pending"))
            sync_to_supabase(user)
            st.success("✅ Submitted!"); st.balloons()
        else:
            st.error("Please describe what you found.")

    my_vars = local_fetch("SELECT job_id, description, status FROM mobile_variations WHERE employee=? ORDER BY id DESC LIMIT 5", (user,))
    if my_vars:
        st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin:20px 0 8px'>Recent</div>", unsafe_allow_html=True)
        for v in my_vars:
            sc = "#2dd4bf" if v["status"]=="Approved" else "#f59e0b"
            st.markdown(f"<div class='site-card'><div style='display:flex;justify-content:space-between'><span style='color:#e2e8f0'>{v['job_id']}</span><span style='color:{sc}'>{v['status']}</span></div><p style='color:#94a3b8;font-size:13px'>{str(v['description'])[:80]}</p></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# PROFILE
# ══════════════════════════════════════════════════════════════════════════
elif page == "profile":
    st.markdown(f"<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:20px'>{user}</div>", unsafe_allow_html=True)
    week_h = sum(e["event_type"] == "in" for e in local_fetch("SELECT event_type FROM clock_events WHERE employee=? AND event_date >= date('now','-7 days')", (user,))) * 8
    st.markdown(f"<div class='site-card' style='text-align:center'><div style='font-size:48px;font-weight:900;color:#2dd4bf'>{today_hours:.1f}h</div><div style='color:#64748b'>today</div></div>", unsafe_allow_html=True)

    if st.button("🔄 Sync with office", use_container_width=True):
        sync_from_supabase()
        sync_to_supabase(user)
        st.success("✅ Synced!")

    st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin:20px 0 12px'>CHANGE PIN</div>", unsafe_allow_html=True)
    new_pin = st.text_input("New PIN", type="password", max_chars=6)
    confirm_pin = st.text_input("Confirm PIN", type="password", max_chars=6)
    if st.button("Update PIN"):
        if new_pin and new_pin == confirm_pin and new_pin.isdigit():
            local_execute("UPDATE employees SET pin=? WHERE name=?", (new_pin, user))
            st.success("✅ PIN updated!")
        else:
            st.error("PINs don't match or not digits only")

    st.divider()
    if st.button("🚪 Sign Out"):
        st.session_state.mobile_user = None
        st.session_state.mobile_page = "home"
        st.session_state.pin_input = ""
        st.rerun()
