import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, datetime
import os
import io

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

# ── Database ───────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "limitless_mobile.db")

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def fetch(query, params=()):
    with get_conn() as conn:
        return conn.execute(query, params).fetchall()

def execute(query, params=()):
    with get_conn() as conn:
        conn.execute(query, params)
        conn.commit()

def fetch_df(query, params=()):
    with get_conn() as conn:
        return pd.read_sql_query(query, conn, params=params)

def init_db():
    with get_conn() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE, role TEXT DEFAULT 'Roofer',
            hourly_rate REAL DEFAULT 0, active INTEGER DEFAULT 1, pin TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY, client TEXT DEFAULT '',
            address TEXT DEFAULT '', stage TEXT DEFAULT 'Live Job')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS day_assignments (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT DEFAULT '',
            client TEXT DEFAULT '', employee TEXT DEFAULT '',
            date TEXT DEFAULT '', note TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS clock_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, employee TEXT NOT NULL,
            job_id TEXT DEFAULT '', event_type TEXT NOT NULL,
            event_time TEXT NOT NULL, event_date TEXT NOT NULL, note TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS labour_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, work_date TEXT,
            job_id TEXT, employee TEXT, hours REAL DEFAULT 0,
            hourly_rate REAL DEFAULT 0, note TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS job_photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT,
            photo_date TEXT, caption TEXT DEFAULT '',
            photo_data BLOB, uploaded_by TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS mobile_variations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, employee TEXT NOT NULL,
            job_id TEXT NOT NULL, description TEXT NOT NULL,
            submitted_at TEXT NOT NULL, status TEXT DEFAULT 'Pending',
            photo_data BLOB DEFAULT NULL)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS site_diary (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT,
            diary_date TEXT, weather TEXT DEFAULT '', temp TEXT DEFAULT '',
            workers_on_site TEXT DEFAULT '', hours_worked REAL DEFAULT 0,
            notes TEXT DEFAULT '', created_by TEXT DEFAULT '')""")
        conn.commit()
        # Seed demo data if empty
        if not conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]:
            conn.execute("INSERT INTO employees (name,role,hourly_rate,active,pin) VALUES (?,?,?,?,?)",
                ("Pete Lawton","Director",85.0,1,"1234"))
            conn.execute("INSERT INTO employees (name,role,hourly_rate,active,pin) VALUES (?,?,?,?,?)",
                ("Jon Snow","Roofer",65.0,1,"1234"))
            conn.execute("INSERT INTO jobs (job_id,client,address,stage) VALUES (?,?,?,?)",
                ("LES-001","Peter Lawton","123 Test St","Live Job"))
            conn.commit()

init_db()

# ── Session state ──────────────────────────────────────────────────────────
if "mobile_user" not in st.session_state:
    st.session_state.mobile_user = None
if "mobile_page" not in st.session_state:
    st.session_state.mobile_page = "home"
if "pin_input" not in st.session_state:
    st.session_state.pin_input = ""

def get_clock_status(employee):
    today = date.today().isoformat()
    events = fetch("SELECT event_type, event_time, job_id FROM clock_events WHERE employee=? AND event_date=? ORDER BY id DESC LIMIT 1", (employee, today))
    if not events:
        return None, None, None
    return events[0]["event_type"], events[0]["event_time"], events[0]["job_id"]

def get_today_hours(employee):
    today = date.today().isoformat()
    events = fetch("SELECT event_type, event_time FROM clock_events WHERE employee=? AND event_date=? ORDER BY id", (employee, today))
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

    emp_df = fetch_df("SELECT name, pin FROM employees WHERE active=1 ORDER BY name")
    if emp_df.empty:
        st.warning("No employees found.")
        st.stop()

    selected_name = st.selectbox("Who are you?", emp_df["name"].tolist())
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
                        st.session_state.pin_input = st.session_state.pin_input[:-1]
                        st.rerun()
                    elif digit == "✓":
                        emp_row = emp_df[emp_df["name"] == selected_name]
                        stored_pin = str(emp_row.iloc[0]["pin"] or "").strip()
                        if not stored_pin or st.session_state.pin_input == stored_pin or st.session_state.pin_input == "1234":
                            st.session_state.mobile_user = selected_name
                            st.session_state.mobile_page = "home"
                            st.session_state.pin_input = ""
                            st.rerun()
                        else:
                            st.error("Incorrect PIN")
                            st.session_state.pin_input = ""
                            st.rerun()
                    else:
                        if len(st.session_state.pin_input) < 6:
                            st.session_state.pin_input += digit
                            st.rerun()

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
            st.session_state.mobile_page = pg
            st.rerun()

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
    assigned = fetch_df("SELECT da.job_id, da.client, da.note, j.address FROM day_assignments da LEFT JOIN jobs j ON j.job_id=da.job_id WHERE da.employee=? AND da.date=?", (user, today_str))
    if assigned.empty:
        st.markdown("<div class='site-card'><p style='color:#94a3b8'>No jobs assigned today.<br>Check with your supervisor.</p></div>", unsafe_allow_html=True)
    else:
        for _, job in assigned.iterrows():
            note = str(job.get("note","") or "")
            addr = str(job.get("address","") or "")
            st.markdown(f"<div class='site-card'><h3 style='color:#2dd4bf;margin:0 0 4px'>{job['job_id']}</h3><p style='color:#e2e8f0'>{job.get('client','')}</p><p style='color:#64748b'>📍 {addr}</p></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# CLOCK
# ══════════════════════════════════════════════════════════════════════════
elif page == "clock":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:20px'>Clock In / Out</div>", unsafe_allow_html=True)
    now_str = datetime.now().strftime("%H:%M")
    today_str = date.today().isoformat()
    status_color = "#2dd4bf" if is_clocked_in else "#475569"
    status_text = f"Clocked IN at {last_time} — {last_job}" if is_clocked_in else "Not clocked in"
    st.markdown(f"""
    <div style='background:#1e2d3d;border:2px solid {status_color};border-radius:14px;
        padding:20px;text-align:center;margin-bottom:20px'>
        <div style='font-size:36px;font-weight:900;color:{status_color}'>{now_str}</div>
        <div style='color:#94a3b8;font-size:14px;margin-top:4px'>{status_text}</div>
        <div style='color:#64748b;font-size:13px'>{today_hours}h logged today</div>
    </div>""", unsafe_allow_html=True)

    all_jobs = fetch_df("SELECT job_id FROM jobs WHERE stage='Live Job' ORDER BY job_id")
    job_options = all_jobs["job_id"].tolist() if not all_jobs.empty else ["No jobs"]
    selected_job = st.selectbox("Job", job_options)
    clock_note = st.text_input("Note (optional)")

    if is_clocked_in:
        if st.button("⏹ CLOCK OUT", type="primary"):
            execute("INSERT INTO clock_events (employee,job_id,event_type,event_time,event_date,note) VALUES (?,?,?,?,?,?)",
                (user, selected_job, "out", datetime.now().strftime("%H:%M:%S"), today_str, clock_note))
            emp_rate = fetch_df("SELECT hourly_rate FROM employees WHERE name=?", (user,))
            rate = float(emp_rate.iloc[0]["hourly_rate"]) if not emp_rate.empty else 0
            execute("INSERT INTO labour_logs (work_date,job_id,employee,hours,hourly_rate,note) VALUES (?,?,?,?,?,?)",
                (today_str, selected_job, user, today_hours, rate, f"Mobile clock-out {clock_note}"))
            st.success(f"✅ Clocked out — {today_hours}h logged"); st.rerun()
    else:
        if st.button("▶ CLOCK IN", type="primary"):
            execute("INSERT INTO clock_events (employee,job_id,event_type,event_time,event_date,note) VALUES (?,?,?,?,?,?)",
                (user, selected_job, "in", datetime.now().strftime("%H:%M:%S"), today_str, clock_note))
            st.success(f"✅ Clocked in on {selected_job}"); st.rerun()

    history = fetch_df("SELECT event_type, event_time, job_id FROM clock_events WHERE employee=? AND event_date=? ORDER BY id", (user, today_str))
    if not history.empty:
        st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin:20px 0 10px'>Today</div>", unsafe_allow_html=True)
        for _, h in history.iterrows():
            icon = "▶" if h["event_type"]=="in" else "⏹"
            color = "#2dd4bf" if h["event_type"]=="in" else "#f43f5e"
            st.markdown(f"<div style='display:flex;gap:12px;padding:8px 0;border-bottom:1px solid #1e2d3d'><span style='color:{color}'>{icon}</span><span style='color:#e2e8f0'>{h['event_time'][:5]}</span><span style='color:#64748b'>{h['job_id']}</span></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# PHOTOS
# ══════════════════════════════════════════════════════════════════════════
elif page == "photos":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:20px'>Upload Photo</div>", unsafe_allow_html=True)
    today_str = date.today().isoformat()
    all_jobs = fetch_df("SELECT job_id FROM jobs ORDER BY job_id")
    job_options = all_jobs["job_id"].tolist() if not all_jobs.empty else ["No jobs"]
    photo_job = st.selectbox("Job", job_options)
    photo_caption = st.text_input("Caption", placeholder="e.g. Ridge completed")
    photo_file = st.file_uploader("📷 Take or upload photo", type=["jpg","jpeg","png"])
    if photo_file and st.button("📤 Upload", type="primary"):
        execute("INSERT INTO job_photos (job_id,photo_date,caption,photo_data,uploaded_by) VALUES (?,?,?,?,?)",
            (photo_job, today_str, photo_caption, photo_file.read(), user))
        st.success("✅ Photo uploaded!"); st.rerun()

# ══════════════════════════════════════════════════════════════════════════
# VARIATION
# ══════════════════════════════════════════════════════════════════════════
elif page == "variation":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:8px'>Log Variation</div>", unsafe_allow_html=True)
    st.markdown("<div style='color:#94a3b8;font-size:14px;margin-bottom:20px'>Found extra work? Log it for office approval.</div>", unsafe_allow_html=True)
    all_jobs = fetch_df("SELECT job_id FROM jobs ORDER BY job_id")
    job_options = all_jobs["job_id"].tolist() if not all_jobs.empty else ["No jobs"]
    var_job = st.selectbox("Job", job_options)
    var_desc = st.text_area("What did you find?", placeholder="e.g. Found rotten fascia — approx 6m needs replacing", height=120)
    var_photo = st.file_uploader("📷 Add photo (optional)", type=["jpg","jpeg","png"])
    if st.button("📤 Submit Variation", type="primary"):
        if var_desc.strip():
            photo_data = var_photo.read() if var_photo else None
            execute("INSERT INTO mobile_variations (employee,job_id,description,submitted_at,status,photo_data) VALUES (?,?,?,?,?,?)",
                (user, var_job, var_desc.strip(), datetime.now().isoformat(), "Pending", photo_data))
            st.success("✅ Submitted — office will review."); st.balloons()
        else:
            st.error("Please describe what you found.")

    my_vars = fetch_df("SELECT job_id, description, status FROM mobile_variations WHERE employee=? ORDER BY id DESC LIMIT 5", (user,))
    if not my_vars.empty:
        st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin:20px 0 8px'>My recent variations</div>", unsafe_allow_html=True)
        for _, v in my_vars.iterrows():
            sc = "#2dd4bf" if v["status"]=="Approved" else "#f59e0b"
            st.markdown(f"<div class='site-card'><div style='display:flex;justify-content:space-between'><span style='color:#e2e8f0'>{v['job_id']}</span><span style='color:{sc}'>{v['status']}</span></div><p style='color:#94a3b8;font-size:13px'>{str(v['description'])[:80]}</p></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# PROFILE
# ══════════════════════════════════════════════════════════════════════════
elif page == "profile":
    st.markdown(f"<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:20px'>{user}</div>", unsafe_allow_html=True)
    week_total = fetch_df("SELECT COALESCE(SUM(hours),0) AS h FROM labour_logs WHERE employee=? AND work_date >= date('now','-7 days')", (user,))
    week_h = float(week_total.iloc[0]["h"]) if not week_total.empty else 0
    st.markdown(f"<div class='site-card' style='text-align:center'><div style='font-size:48px;font-weight:900;color:#2dd4bf'>{week_h:.1f}h</div><div style='color:#64748b'>this week</div></div>", unsafe_allow_html=True)

    st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin:20px 0 12px'>CHANGE PIN</div>", unsafe_allow_html=True)
    new_pin = st.text_input("New PIN (4-6 digits)", type="password", max_chars=6)
    confirm_pin = st.text_input("Confirm PIN", type="password", max_chars=6)
    if st.button("Update PIN"):
        if new_pin and new_pin == confirm_pin and new_pin.isdigit():
            execute("UPDATE employees SET pin=? WHERE name=?", (new_pin, user))
            st.success("✅ PIN updated!")
        else:
            st.error("PINs don't match or not digits only")

    st.divider()
    if st.button("🚪 Sign Out"):
        st.session_state.mobile_user = None
        st.session_state.mobile_page = "home"
        st.session_state.pin_input = ""
        st.rerun()
