import streamlit as st
import sqlite3
import hashlib
import pandas as pd
from datetime import date, datetime, time
import os
import io

st.set_page_config(
    page_title="Limitless — Site App",
    page_icon="🦺",
    layout="centered",
    initial_sidebar_state="collapsed"
)

# ── Mobile CSS ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* Mobile-first styling */
* { box-sizing: border-box; }
.main .block-container { 
    padding: 1rem 1rem 2rem 1rem !important; 
    max-width: 480px !important;
    margin: 0 auto !important;
}
body { background: #0f172a !important; }

/* Big tap targets */
.stButton button {
    width: 100% !important;
    min-height: 60px !important;
    font-size: 18px !important;
    font-weight: 700 !important;
    border-radius: 14px !important;
    margin-bottom: 8px !important;
}
.stTextInput input, .stSelectbox select, .stTextArea textarea {
    font-size: 16px !important;
    min-height: 52px !important;
    border-radius: 10px !important;
}
/* Hide streamlit branding */
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }

/* Card style */
.site-card {
    background: #1e2d3d;
    border: 1px solid #2a3d4f;
    border-radius: 16px;
    padding: 20px;
    margin-bottom: 12px;
}
.site-card h3 { color: #2dd4bf; margin: 0 0 4px 0; font-size: 20px; }
.site-card p { color: #94a3b8; margin: 0; font-size: 14px; }

/* Status badge */
.badge-green { background: #0f2d1a; color: #2dd4bf; border: 1px solid #2dd4bf; 
    border-radius: 999px; padding: 4px 12px; font-size: 13px; font-weight: 700; }
.badge-amber { background: #2d1f0f; color: #f59e0b; border: 1px solid #f59e0b;
    border-radius: 999px; padding: 4px 12px; font-size: 13px; font-weight: 700; }

/* PIN pad */
.pin-display {
    font-size: 32px; letter-spacing: 12px; text-align: center;
    color: #2dd4bf; font-weight: 700; padding: 16px;
    background: #1e2d3d; border-radius: 12px; margin-bottom: 16px;
    min-height: 70px;
}
</style>
""", unsafe_allow_html=True)

# ── Database ───────────────────────────────────────────────────────────────
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "limitless.db")

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

# ── Init mobile tables ─────────────────────────────────────────────────────
def init_mobile_db():
    with get_conn() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS clock_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee TEXT NOT NULL,
            job_id TEXT DEFAULT '',
            event_type TEXT NOT NULL,  -- 'in' or 'out'
            event_time TEXT NOT NULL,
            event_date TEXT NOT NULL,
            note TEXT DEFAULT '',
            lat REAL DEFAULT NULL,
            lng REAL DEFAULT NULL
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS mobile_variations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            employee TEXT NOT NULL,
            job_id TEXT NOT NULL,
            description TEXT NOT NULL,
            submitted_at TEXT NOT NULL,
            status TEXT DEFAULT 'Pending',
            photo_data BLOB DEFAULT NULL
        )""")
        # Add PIN column to employees if not exists
        try:
            conn.execute("ALTER TABLE employees ADD COLUMN pin TEXT DEFAULT ''")
        except:
            pass
        conn.commit()

init_mobile_db()

# ── Session state ──────────────────────────────────────────────────────────
if "mobile_user" not in st.session_state:
    st.session_state.mobile_user = None
if "mobile_page" not in st.session_state:
    st.session_state.mobile_page = "login"
if "pin_input" not in st.session_state:
    st.session_state.pin_input = ""

# ── Helper: get today's clock status ──────────────────────────────────────
def get_clock_status(employee):
    today = date.today().isoformat()
    events = fetch("""
        SELECT event_type, event_time, job_id FROM clock_events 
        WHERE employee=? AND event_date=? ORDER BY id DESC LIMIT 1
    """, (employee, today))
    if not events:
        return None, None, None
    return events[0]["event_type"], events[0]["event_time"], events[0]["job_id"]

def get_today_hours(employee):
    today = date.today().isoformat()
    events = fetch("""
        SELECT event_type, event_time FROM clock_events
        WHERE employee=? AND event_date=? ORDER BY id
    """, (employee, today))
    total = 0.0
    clock_in_time = None
    for e in events:
        if e["event_type"] == "in":
            try:
                clock_in_time = datetime.strptime(e["event_time"], "%H:%M:%S")
            except:
                clock_in_time = datetime.strptime(e["event_time"], "%H:%M")
        elif e["event_type"] == "out" and clock_in_time:
            try:
                clock_out_time = datetime.strptime(e["event_time"], "%H:%M:%S")
            except:
                clock_out_time = datetime.strptime(e["event_time"], "%H:%M")
            total += (clock_out_time - clock_in_time).seconds / 3600
            clock_in_time = None
    # Still clocked in
    if clock_in_time:
        total += (datetime.now() - clock_in_time).seconds / 3600
    return round(total, 1)

# ══════════════════════════════════════════════════════════════════════════
# LOGIN PAGE
# ══════════════════════════════════════════════════════════════════════════
if st.session_state.mobile_user is None:
    st.markdown("""
    <div style='text-align:center;padding:24px 0 16px'>
        <div style='font-size:40px'>🦺</div>
        <div style='font-size:26px;font-weight:900;color:#e2e8f0'>LIMITLESS</div>
        <div style='font-size:13px;color:#475569;margin-top:4px'>Site App</div>
    </div>
    """, unsafe_allow_html=True)

    # Employee selector
    emp_df = fetch_df("SELECT name, pin FROM employees WHERE active=1 ORDER BY name")
    if emp_df.empty:
        st.warning("No active employees found. Set up employees in the desktop app first.")
        st.stop()

    emp_names = emp_df["name"].tolist()
    selected_name = st.selectbox("Who are you?", emp_names, key="login_name")

    # PIN entry
    st.markdown("<div style='font-size:15px;font-weight:600;color:#94a3b8;margin:16px 0 8px'>Enter your PIN</div>", unsafe_allow_html=True)
    
    pin_display = "●" * len(st.session_state.pin_input) if st.session_state.pin_input else ""
    st.markdown(f"<div class='pin-display'>{pin_display or '——'}</div>", unsafe_allow_html=True)

    # PIN pad
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
                        # Verify PIN
                        emp_row = emp_df[emp_df["name"] == selected_name]
                        stored_pin = str(emp_row.iloc[0]["pin"] or "").strip()
                        
                        if not stored_pin:
                            # No PIN set — first time setup or use default 1234
                            if st.session_state.pin_input == "1234" or st.session_state.pin_input == "":
                                st.session_state.mobile_user = selected_name
                                st.session_state.mobile_page = "home"
                                st.session_state.pin_input = ""
                                st.rerun()
                            else:
                                st.error("Incorrect PIN. Default PIN is 1234.")
                                st.session_state.pin_input = ""
                                st.rerun()
                        elif st.session_state.pin_input == stored_pin:
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

    st.markdown("<div style='text-align:center;color:#475569;font-size:12px;margin-top:16px'>Default PIN: 1234 — change in Settings</div>", unsafe_allow_html=True)
    st.stop()

# ══════════════════════════════════════════════════════════════════════════
# LOGGED IN — NAVIGATION
# ══════════════════════════════════════════════════════════════════════════
user = st.session_state.mobile_user
last_event, last_time, last_job = get_clock_status(user)
is_clocked_in = last_event == "in"
today_hours = get_today_hours(user)

# Top bar
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

# Bottom nav
page = st.session_state.mobile_page
nav_cols = st.columns(5)
nav_items = [
    ("🏠", "home", "Home"),
    ("⏱️", "clock", "Clock"),
    ("📸", "photos", "Photos"),
    ("⚠️", "variation", "Variation"),
    ("👤", "profile", "Profile"),
]
for col, (icon, pg, label) in zip(nav_cols, nav_items):
    with col:
        active = "color:#2dd4bf" if page == pg else "color:#475569"
        if st.button(icon, key=f"nav_{pg}", use_container_width=True, help=label):
            st.session_state.mobile_page = pg
            st.rerun()

st.divider()

# ══════════════════════════════════════════════════════════════════════════
# HOME PAGE
# ══════════════════════════════════════════════════════════════════════════
if page == "home":
    today_str = date.today().isoformat()
    today_nice = date.today().strftime("%A, %d %B")
    
    st.markdown(f"<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:4px'>Good {'morning' if datetime.now().hour < 12 else 'afternoon'}.</div>", unsafe_allow_html=True)
    st.markdown(f"<div style='color:#475569;font-size:14px;margin-bottom:20px'>{today_nice}</div>", unsafe_allow_html=True)

    # Clock in/out quick action
    if is_clocked_in:
        if st.button(f"⏹ Clock Out — {last_job or 'No job'}", type="primary", use_container_width=True):
            st.session_state.mobile_page = "clock"
            st.rerun()
    else:
        if st.button("▶ Clock In", type="primary", use_container_width=True):
            st.session_state.mobile_page = "clock"
            st.rerun()

    # Today's jobs
    st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;text-transform:uppercase;letter-spacing:.1em;margin:20px 0 10px'>My jobs today</div>", unsafe_allow_html=True)
    
    assigned = fetch_df("""
        SELECT da.job_id, da.client, da.note, j.address
        FROM day_assignments da
        LEFT JOIN jobs j ON j.job_id = da.job_id
        WHERE da.employee=? AND da.date=?
        ORDER BY da.id
    """, (user, today_str))

    if assigned.empty:
        st.markdown("<div class='site-card'><p>No jobs assigned today.<br>Check with your supervisor.</p></div>", unsafe_allow_html=True)
    else:
        for _, job in assigned.iterrows():
            note = str(job.get("note","") or "")
            addr = str(job.get("address","") or "")
            st.markdown(f"""
            <div class='site-card'>
                <h3>{job['job_id']}</h3>
                <p style='color:#e2e8f0;font-size:15px'>{job.get('client','')}</p>
                <p>📍 {addr}</p>
                {f"<p style='color:#f59e0b'>📋 {note}</p>" if note else ""}
            </div>
            """, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# CLOCK PAGE
# ══════════════════════════════════════════════════════════════════════════
elif page == "clock":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:20px'>Clock In / Out</div>", unsafe_allow_html=True)
    
    now_str = datetime.now().strftime("%H:%M")
    today_str = date.today().isoformat()

    # Current status
    status_color = "#2dd4bf" if is_clocked_in else "#475569"
    status_text = f"Clocked IN at {last_time} — {last_job}" if is_clocked_in else "Not clocked in"
    st.markdown(f"""
    <div style='background:#1e2d3d;border:2px solid {status_color};border-radius:14px;
        padding:20px;text-align:center;margin-bottom:20px'>
        <div style='font-size:36px;font-weight:900;color:{status_color}'>{now_str}</div>
        <div style='color:#94a3b8;font-size:14px;margin-top:4px'>{status_text}</div>
        <div style='color:#64748b;font-size:13px'>{today_hours}h logged today</div>
    </div>
    """, unsafe_allow_html=True)

    # Job selector
    today_jobs = fetch_df("""
        SELECT DISTINCT da.job_id, da.client FROM day_assignments da
        WHERE da.employee=? AND da.date=?
    """, (user, today_str))
    
    job_options = today_jobs["job_id"].tolist() if not today_jobs.empty else []
    if not job_options:
        all_jobs = fetch_df("SELECT job_id FROM jobs WHERE archived=0 AND stage='Live Job' ORDER BY job_id")
        job_options = all_jobs["job_id"].tolist() if not all_jobs.empty else ["No jobs"]

    selected_job = st.selectbox("Job", job_options)
    clock_note = st.text_input("Note (optional)", placeholder="e.g. Started on gutters")

    if is_clocked_in:
        if st.button("⏹ CLOCK OUT", type="primary", use_container_width=True):
            execute("""INSERT INTO clock_events (employee, job_id, event_type, event_time, event_date, note)
                VALUES (?,?,?,?,?,?)""",
                (user, selected_job, "out", datetime.now().strftime("%H:%M:%S"), today_str, clock_note))
            # Also log to labour_logs
            execute("""INSERT INTO labour_logs (work_date, job_id, employee, hours, hourly_rate, note)
                SELECT ?, ?, ?, ?, COALESCE(e.hourly_rate, 0), ?
                FROM employees e WHERE e.name=?""",
                (today_str, selected_job, user, today_hours, f"Mobile clock-out {clock_note}", user))
            st.success(f"✅ Clocked out at {datetime.now().strftime('%H:%M')} — {today_hours}h logged")
            st.rerun()
    else:
        if st.button("▶ CLOCK IN", type="primary", use_container_width=True):
            execute("""INSERT INTO clock_events (employee, job_id, event_type, event_time, event_date, note)
                VALUES (?,?,?,?,?,?)""",
                (user, selected_job, "in", datetime.now().strftime("%H:%M:%S"), today_str, clock_note))
            st.success(f"✅ Clocked in at {datetime.now().strftime('%H:%M')} on {selected_job}")
            st.rerun()

    # Today's clock history
    history = fetch_df("""
        SELECT event_type, event_time, job_id, note FROM clock_events
        WHERE employee=? AND event_date=? ORDER BY id
    """, (user, today_str))
    
    if not history.empty:
        st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;text-transform:uppercase;margin:20px 0 10px'>Today's history</div>", unsafe_allow_html=True)
        for _, h in history.iterrows():
            icon = "▶" if h["event_type"] == "in" else "⏹"
            color = "#2dd4bf" if h["event_type"] == "in" else "#f43f5e"
            st.markdown(f"""
            <div style='display:flex;gap:12px;align-items:center;padding:10px 0;
                border-bottom:1px solid #1e2d3d'>
                <span style='color:{color};font-size:18px'>{icon}</span>
                <span style='color:#e2e8f0;font-weight:600'>{h['event_time'][:5]}</span>
                <span style='color:#64748b'>{h['job_id']}</span>
                <span style='color:#475569;font-size:13px'>{h.get('note','')}</span>
            </div>
            """, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# PHOTOS PAGE
# ══════════════════════════════════════════════════════════════════════════
elif page == "photos":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:20px'>Upload Photo</div>", unsafe_allow_html=True)

    today_str = date.today().isoformat()
    today_jobs = fetch_df("""
        SELECT DISTINCT da.job_id FROM day_assignments da
        WHERE da.employee=? AND da.date=?
    """, (user, today_str))
    job_options = today_jobs["job_id"].tolist() if not today_jobs.empty else []
    if not job_options:
        all_jobs = fetch_df("SELECT job_id FROM jobs WHERE archived=0 ORDER BY job_id")
        job_options = all_jobs["job_id"].tolist() if not all_jobs.empty else []

    photo_job = st.selectbox("Job", job_options if job_options else ["No jobs"])
    photo_caption = st.text_input("Caption", placeholder="e.g. Ridge completed, north face")
    
    photo_file = st.file_uploader("📷 Take or upload photo", 
        type=["jpg","jpeg","png","heic"],
        help="Tap to open camera on mobile")
    
    if photo_file and st.button("📤 Upload Photo", type="primary", use_container_width=True):
        photo_bytes = photo_file.read()
        execute("""INSERT INTO job_photos (job_id, photo_date, caption, photo_data, uploaded_by)
            VALUES (?,?,?,?,?)""",
            (photo_job, today_str, photo_caption, photo_bytes, user))
        st.success("✅ Photo uploaded!")
        st.rerun()

    # Recent photos for today
    recent = fetch_df("""
        SELECT caption, photo_date, uploaded_by FROM job_photos
        WHERE uploaded_by=? ORDER BY id DESC LIMIT 5
    """, (user,))
    if not recent.empty:
        st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin:16px 0 8px'>Recent uploads</div>", unsafe_allow_html=True)
        for _, p in recent.iterrows():
            st.markdown(f"<div style='color:#94a3b8;font-size:13px;padding:6px 0'>📷 {p['caption'] or 'No caption'} — {p['photo_date']}</div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# VARIATION PAGE
# ══════════════════════════════════════════════════════════════════════════
elif page == "variation":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:8px'>Log Variation</div>", unsafe_allow_html=True)
    st.markdown("<div style='color:#94a3b8;font-size:14px;margin-bottom:20px'>Found extra work? Log it here for office approval.</div>", unsafe_allow_html=True)

    today_str = date.today().isoformat()
    today_jobs = fetch_df("""
        SELECT DISTINCT da.job_id FROM day_assignments da
        WHERE da.employee=? AND da.date=?
    """, (user, today_str))
    job_options = today_jobs["job_id"].tolist() if not today_jobs.empty else []
    if not job_options:
        all_jobs = fetch_df("SELECT job_id FROM jobs WHERE archived=0 AND stage='Live Job' ORDER BY job_id")
        job_options = all_jobs["job_id"].tolist() if not all_jobs.empty else []

    var_job = st.selectbox("Job", job_options if job_options else ["No jobs"])
    var_desc = st.text_area("What did you find?", 
        placeholder="e.g. Found rotten fascia board on north face — approx 6m needs replacing",
        height=120)
    var_photo = st.file_uploader("📷 Add photo (optional)", type=["jpg","jpeg","png"])

    if st.button("📤 Submit Variation", type="primary", use_container_width=True):
        if var_desc.strip():
            photo_data = var_photo.read() if var_photo else None
            execute("""INSERT INTO mobile_variations (employee, job_id, description, submitted_at, status, photo_data)
                VALUES (?,?,?,?,?,?)""",
                (user, var_job, var_desc.strip(), datetime.now().isoformat(), "Pending", photo_data))
            st.success("✅ Variation submitted — office will review and approve.")
            st.balloons()
        else:
            st.error("Please describe what you found.")

    # My pending variations
    my_vars = fetch_df("""
        SELECT job_id, description, status, submitted_at FROM mobile_variations
        WHERE employee=? ORDER BY id DESC LIMIT 5
    """, (user,))
    if not my_vars.empty:
        st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin:20px 0 8px'>My recent variations</div>", unsafe_allow_html=True)
        for _, v in my_vars.iterrows():
            status_col = "#2dd4bf" if v["status"] == "Approved" else "#f59e0b" if v["status"] == "Pending" else "#f43f5e"
            st.markdown(f"""
            <div class='site-card'>
                <div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:6px'>
                    <span style='font-weight:700;color:#e2e8f0'>{v['job_id']}</span>
                    <span style='color:{status_col};font-size:13px;font-weight:700'>{v['status']}</span>
                </div>
                <p>{v['description'][:80]}{'...' if len(str(v['description'])) > 80 else ''}</p>
            </div>
            """, unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# PROFILE PAGE
# ══════════════════════════════════════════════════════════════════════════
elif page == "profile":
    st.markdown(f"<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:20px'>{user}</div>", unsafe_allow_html=True)

    # This week's hours
    week_hours = fetch_df("""
        SELECT event_date, 
               SUM(CASE WHEN event_type='in' THEN 1 ELSE 0 END) as clock_ins
        FROM clock_events
        WHERE employee=? AND event_date >= date('now', '-7 days')
        GROUP BY event_date ORDER BY event_date DESC
    """, (user,))

    st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin-bottom:12px'>THIS WEEK</div>", unsafe_allow_html=True)
    
    week_total = fetch_df("""
        SELECT COALESCE(SUM(hours),0) AS h FROM labour_logs
        WHERE employee=? AND work_date >= date('now', '-7 days')
    """, (user,))
    week_h = float(week_total.iloc[0]["h"]) if not week_total.empty else 0

    st.markdown(f"""
    <div class='site-card' style='text-align:center'>
        <div style='font-size:48px;font-weight:900;color:#2dd4bf'>{week_h:.1f}h</div>
        <div style='color:#64748b'>logged this week</div>
    </div>
    """, unsafe_allow_html=True)

    # Change PIN
    st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin:20px 0 12px'>CHANGE PIN</div>", unsafe_allow_html=True)
    new_pin = st.text_input("New PIN (4-6 digits)", type="password", max_chars=6)
    confirm_pin = st.text_input("Confirm PIN", type="password", max_chars=6)
    if st.button("Update PIN", use_container_width=True):
        if new_pin and new_pin == confirm_pin and new_pin.isdigit():
            execute("UPDATE employees SET pin=? WHERE name=?", (new_pin, user))
            st.success("✅ PIN updated!")
        elif new_pin != confirm_pin:
            st.error("PINs don't match")
        else:
            st.error("PIN must be digits only")

    # Site diary quick entry
    st.markdown("<div style='font-size:13px;font-weight:700;color:#2dd4bf;margin:20px 0 12px'>SITE DIARY</div>", unsafe_allow_html=True)
    today_str = date.today().isoformat()
    today_jobs2 = fetch_df("SELECT DISTINCT job_id FROM day_assignments WHERE employee=? AND date=?", (user, today_str))
    if not today_jobs2.empty:
        diary_job = st.selectbox("Job", today_jobs2["job_id"].tolist(), key="diary_job")
        diary_note = st.text_area("Notes", placeholder="What happened on site today?", height=100)
        if st.button("Save diary entry", use_container_width=True):
            if diary_note.strip():
                execute("""INSERT INTO site_diary (job_id, diary_date, weather, temp, workers_on_site, hours_worked, notes, created_by)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (diary_job, today_str, "", "", user, today_hours, diary_note.strip(), user))
                st.success("✅ Saved!")

    st.divider()
    if st.button("🚪 Sign Out", use_container_width=True):
        st.session_state.mobile_user = None
        st.session_state.mobile_page = "login"
        st.session_state.pin_input = ""
        st.rerun()

