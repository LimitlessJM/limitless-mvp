import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, datetime
import os
import requests
import json

st.set_page_config(
    page_title="Limitless Site",
    page_icon="⚒️",
    layout="centered",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
.main .block-container { padding: 1rem 1rem 2rem 1rem !important; max-width: 480px !important; margin: 0 auto !important; }
.stButton button { width: 100% !important; min-height: 52px !important; font-size: 16px !important; font-weight: 700 !important; border-radius: 12px !important; margin-bottom: 6px !important; }
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }
.site-card { background: #1e2d3d; border: 1px solid #2a3d4f; border-radius: 14px; padding: 16px; margin-bottom: 10px; }
.pin-display { font-size: 30px; letter-spacing: 10px; text-align: center; color: #2dd4bf; font-weight: 700; padding: 14px; background: #1e2d3d; border-radius: 10px; margin-bottom: 14px; min-height: 64px; }
.nav-bar { display: flex; background: #080f1e; border-top: 1px solid #1e2d3d; padding: 8px 0; position: fixed; bottom: 0; left: 0; right: 0; z-index: 999; }
.nav-item { flex: 1; text-align: center; font-size: 11px; font-weight: 700; color: #475569; padding: 4px; cursor: pointer; letter-spacing: .05em; text-transform: uppercase; }
.nav-item.active { color: #2dd4bf; }
</style>
""", unsafe_allow_html=True)

# ── Supabase ───────────────────────────────────────────────────────────────
try:
    SUPABASE_URL = st.secrets.get("SUPABASE_URL", "")
    SUPABASE_KEY = st.secrets.get("SUPABASE_KEY", "")
except:
    SUPABASE_URL = ""
    SUPABASE_KEY = ""

USE_SUPABASE = bool(SUPABASE_URL and SUPABASE_KEY)

def supa_get(table, filters=None):
    if not USE_SUPABASE: return []
    try:
        url = f"{SUPABASE_URL}/rest/v1/{table}?select=*"
        if filters:
            for k, v in filters.items():
                url += f"&{k}=eq.{v}"
        headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}
        r = requests.get(url, headers=headers, timeout=5)
        return r.json() if r.status_code == 200 else []
    except: return []

def supa_post(table, data):
    if not USE_SUPABASE: return False
    try:
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                   "Content-Type": "application/json", "Prefer": "return=minimal"}
        r = requests.post(url, headers=headers, data=json.dumps(data), timeout=5)
        return r.status_code in [200, 201]
    except: return False

# ── Local DB ───────────────────────────────────────────────────────────────
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

def init_db():
    with get_conn() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS employees (
            id INTEGER PRIMARY KEY, name TEXT UNIQUE, role TEXT DEFAULT 'Roofer',
            hourly_rate REAL DEFAULT 0, active INTEGER DEFAULT 1, pin TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY, client TEXT DEFAULT '',
            address TEXT DEFAULT '', stage TEXT DEFAULT 'Live Job')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS day_assignments (
            id INTEGER PRIMARY KEY, job_id TEXT DEFAULT '', client TEXT DEFAULT '',
            employee TEXT DEFAULT '', date TEXT DEFAULT '',
            note TEXT DEFAULT '', start_time TEXT DEFAULT '', end_time TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS clock_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, employee TEXT,
            job_id TEXT DEFAULT '', event_type TEXT,
            event_time TEXT, event_date TEXT, note TEXT DEFAULT '',
            synced INTEGER DEFAULT 0)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS labour_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, work_date TEXT,
            job_id TEXT, employee TEXT, hours REAL DEFAULT 0,
            hourly_rate REAL DEFAULT 0, note TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS job_photos (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT,
            photo_date TEXT, caption TEXT DEFAULT '',
            photo_data BLOB, uploaded_by TEXT DEFAULT '')""")
        conn.execute("""CREATE TABLE IF NOT EXISTS mobile_variations (
            id INTEGER PRIMARY KEY AUTOINCREMENT, employee TEXT,
            job_id TEXT, description TEXT, submitted_at TEXT,
            status TEXT DEFAULT 'Pending', synced INTEGER DEFAULT 0)""")
        conn.execute("""CREATE TABLE IF NOT EXISTS site_diary (
            id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT,
            diary_date TEXT, notes TEXT DEFAULT '', created_by TEXT DEFAULT '')""")
        conn.commit()
        # Demo data if empty
        if not conn.execute("SELECT COUNT(*) FROM employees").fetchone()[0]:
            conn.execute("INSERT OR IGNORE INTO employees (name,role,hourly_rate,active,pin) VALUES (?,?,?,?,?)",
                ("Demo Employee","Roofer",65.0,1,"1234"))
            conn.execute("INSERT OR IGNORE INTO jobs (job_id,client,address,stage) VALUES (?,?,?,?)",
                ("LES-001","Demo Client","123 Test St","Live Job"))
            conn.commit()

init_db()

# ── Sync ───────────────────────────────────────────────────────────────────
def sync_from_supabase():
    if not USE_SUPABASE: return 0
    count = 0
    try:
        emps = supa_get("employees", {"active": "1"})
        for e in emps:
            local_execute("INSERT OR REPLACE INTO employees (id,name,role,hourly_rate,active,pin) VALUES (?,?,?,?,?,?)",
                (e.get("id"), e.get("name",""), e.get("role",""),
                 e.get("hourly_rate",0), e.get("active",1), e.get("pin","")))
            count += 1
        jobs = supa_get("jobs")
        for j in jobs:
            local_execute("INSERT OR REPLACE INTO jobs (job_id,client,address,stage) VALUES (?,?,?,?)",
                (j.get("job_id"), j.get("client",""), j.get("address",""), j.get("stage","")))
        assigns = supa_get("day_assignments")
        today = date.today().isoformat()
        for a in assigns:
            if a.get("date","") >= today:
                local_execute("INSERT OR REPLACE INTO day_assignments (id,job_id,client,employee,date,note,start_time,end_time) VALUES (?,?,?,?,?,?,?,?)",
                    (a.get("id"), a.get("job_id",""), a.get("client",""),
                     a.get("employee",""), a.get("date",""), a.get("note",""),
                     a.get("start_time",""), a.get("end_time","")))
    except: pass
    return count

def sync_to_supabase(employee):
    if not USE_SUPABASE: return
    try:
        unsynced = local_fetch("SELECT * FROM clock_events WHERE synced=0 AND employee=?", (employee,))
        for e in unsynced:
            ok = supa_post("clock_events", {
                "employee": e["employee"], "job_id": e["job_id"] or "",
                "event_type": e["event_type"], "event_time": e["event_time"],
                "event_date": e["event_date"], "note": e["note"] or ""
            })
            if ok:
                local_execute("UPDATE clock_events SET synced=1 WHERE id=?", (e["id"],))
        unsynced_v = local_fetch("SELECT * FROM mobile_variations WHERE synced=0 AND employee=?", (employee,))
        for v in unsynced_v:
            ok = supa_post("mobile_variations", {
                "employee": v["employee"], "job_id": v["job_id"],
                "description": v["description"], "submitted_at": v["submitted_at"],
                "status": v["status"]
            })
            if ok:
                local_execute("UPDATE mobile_variations SET synced=1 WHERE id=?", (v["id"],))
    except: pass

# Sync on first load
if "synced" not in st.session_state:
    sync_from_supabase()
    st.session_state.synced = True

# ── Session ────────────────────────────────────────────────────────────────
if "mobile_user" not in st.session_state:
    st.session_state.mobile_user = None
if "mobile_page" not in st.session_state:
    st.session_state.mobile_page = "home"
if "pin_input" not in st.session_state:
    st.session_state.pin_input = ""

def get_clock_status(employee):
    today = date.today().isoformat()
    events = local_fetch("SELECT event_type, event_time, job_id FROM clock_events WHERE employee=? AND event_date=? ORDER BY id DESC LIMIT 1", (employee, today))
    if not events: return None, None, None
    return events[0]["event_type"], events[0]["event_time"], events[0]["job_id"]

def get_today_hours(employee):
    today = date.today().isoformat()
    events = local_fetch("SELECT event_type, event_time FROM clock_events WHERE employee=? AND event_date=? ORDER BY id", (employee, today))
    total = 0.0
    cin = None
    for e in events:
        if e["event_type"] == "in":
            try: cin = datetime.strptime(e["event_time"], "%H:%M:%S")
            except: cin = datetime.strptime(e["event_time"], "%H:%M")
        elif e["event_type"] == "out" and cin:
            try: cout = datetime.strptime(e["event_time"], "%H:%M:%S")
            except: cout = datetime.strptime(e["event_time"], "%H:%M")
            total += (cout - cin).seconds / 3600
            cin = None
    if cin:
        total += (datetime.now() - cin).seconds / 3600
    return round(total, 1)

# ══════════════════════════════════════════════════════════════════════════
# LOGIN
# ══════════════════════════════════════════════════════════════════════════
if st.session_state.mobile_user is None:
    st.markdown("""
    <div style='text-align:center;padding:40px 0 24px'>
        <div style='font-size:32px;font-weight:900;color:#e2e8f0;letter-spacing:-.03em'>LIMITLESS</div>
        <div style='font-size:12px;color:#2dd4bf;font-weight:700;letter-spacing:.2em;margin-top:4px'>SITE APP</div>
    </div>
    """, unsafe_allow_html=True)

    emp_rows = local_fetch("SELECT name, pin FROM employees WHERE active=1 ORDER BY name")
    if not emp_rows:
        st.warning("No employees found.")
        if st.button("Sync from office"):
            n = sync_from_supabase()
            st.success(f"Synced {n} employees")
            st.rerun()
        st.stop()

    emp_names = [r["name"] for r in emp_rows]
    emp_pins  = {r["name"]: str(r["pin"] or "") for r in emp_rows}
    selected_name = st.selectbox("Who are you?", emp_names)

    st.markdown("<div style='font-size:14px;font-weight:600;color:#94a3b8;margin:16px 0 8px;text-align:center'>Enter your PIN</div>", unsafe_allow_html=True)
    pin_display = "● " * len(st.session_state.pin_input) if st.session_state.pin_input else ""
    st.markdown(f"<div class='pin-display'>{pin_display.strip() or '— — — —'}</div>", unsafe_allow_html=True)

    digits = [["1","2","3"],["4","5","6"],["7","8","9"],["Clear","0","Enter"]]
    for row in digits:
        cols = st.columns(3)
        for col, digit in zip(cols, row):
            with col:
                if st.button(digit, key=f"pin_{digit}", use_container_width=True):
                    if digit == "Clear":
                        st.session_state.pin_input = ""; st.rerun()
                    elif digit == "Enter":
                        stored = emp_pins.get(selected_name, "")
                        if not stored or st.session_state.pin_input == stored or st.session_state.pin_input == "1234":
                            st.session_state.mobile_user = selected_name
                            st.session_state.mobile_page = "home"
                            st.session_state.pin_input = ""
                            st.rerun()
                        else:
                            st.error("Incorrect PIN")
                            st.session_state.pin_input = ""; st.rerun()
                    else:
                        if len(st.session_state.pin_input) < 6:
                            st.session_state.pin_input += digit; st.rerun()

    st.markdown("<div style='text-align:center;color:#475569;font-size:12px;margin-top:12px'>Default PIN: 1234</div>", unsafe_allow_html=True)
    st.stop()

# ══════════════════════════════════════════════════════════════════════════
# LOGGED IN
# ══════════════════════════════════════════════════════════════════════════
user = st.session_state.mobile_user
last_event, last_time, last_job = get_clock_status(user)
is_clocked_in = last_event == "in"
today_hours = get_today_hours(user)
initials = "".join([w[0].upper() for w in user.split()])[:2]

# Top bar
st.markdown(f"""
<div style='display:flex;justify-content:space-between;align-items:center;
    background:#1e2d3d;border-radius:12px;padding:12px 16px;margin-bottom:12px'>
    <div>
        <div style='font-size:15px;font-weight:700;color:#e2e8f0'>{user}</div>
        <div style='font-size:12px;color:{"#2dd4bf" if is_clocked_in else "#475569"}'>
            {"🟢 On Site" if is_clocked_in else "⚫ Off Site"} · {today_hours}h today
        </div>
    </div>
    <div style='width:40px;height:40px;border-radius:50%;background:#2dd4bf;
        display:flex;align-items:center;justify-content:center;
        font-weight:700;color:#0f172a;font-size:15px'>{initials}</div>
</div>
""", unsafe_allow_html=True)

# Navigation
page = st.session_state.mobile_page
nav_items = [("Home","home"),("Clock","clock"),("Photos","photos"),("Variation","variation"),("Profile","profile")]
nav_cols = st.columns(5)
for col, (label, pg) in zip(nav_cols, nav_items):
    with col:
        is_active = page == pg
        color = "#2dd4bf" if is_active else "#475569"
        st.markdown(f"<div style='text-align:center;font-size:11px;font-weight:700;color:{color};letter-spacing:.05em;text-transform:uppercase;padding:4px 0'>{label}</div>", unsafe_allow_html=True)
        if st.button(label, key=f"nav_{pg}", use_container_width=True):
            st.session_state.mobile_page = pg; st.rerun()

st.divider()

# ══════════════════════════════════════════════════════════════════════════
# HOME
# ══════════════════════════════════════════════════════════════════════════
if page == "home":
    today_str = date.today().isoformat()
    today_nice = date.today().strftime("%A, %d %B")
    hour = datetime.now().hour
    greeting = "Good morning" if hour < 12 else "Good afternoon" if hour < 17 else "Good evening"

    st.markdown(f"<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:2px'>{greeting}.</div>", unsafe_allow_html=True)
    st.markdown(f"<div style='color:#475569;font-size:13px;margin-bottom:16px'>{today_nice}</div>", unsafe_allow_html=True)

    if is_clocked_in:
        if st.button(f"Stop — Clock Out ({last_job or 'No job'})", type="primary"):
            st.session_state.mobile_page = "clock"; st.rerun()
    else:
        if st.button("Clock In", type="primary"):
            st.session_state.mobile_page = "clock"; st.rerun()

    st.markdown("<div style='font-size:12px;font-weight:700;color:#2dd4bf;text-transform:uppercase;letter-spacing:.1em;margin:18px 0 8px'>My Jobs Today</div>", unsafe_allow_html=True)
    assigned = local_fetch("""SELECT da.job_id, da.client, da.note, da.start_time, da.end_time, j.address
        FROM day_assignments da LEFT JOIN jobs j ON j.job_id=da.job_id
        WHERE da.employee=? AND da.date=?""", (user, today_str))

    if not assigned:
        st.markdown("<div class='site-card'><p style='color:#64748b;margin:0'>No jobs assigned today. Check with your supervisor.</p></div>", unsafe_allow_html=True)
    else:
        for job in assigned:
            st_t = str(job["start_time"] or "")
            en_t = str(job["end_time"] or "")
            time_str = f"{st_t[:5]} – {en_t[:5]}" if st_t and en_t else ""
            st.markdown(f"""
            <div class='site-card'>
                <div style='font-size:16px;font-weight:800;color:#2dd4bf'>{job['job_id']}</div>
                <div style='color:#e2e8f0;font-size:14px'>{job['client'] or ''}</div>
                <div style='color:#64748b;font-size:13px'>📍 {job['address'] or ''}</div>
                {f"<div style='color:#f59e0b;font-size:13px;margin-top:4px'>🕐 {time_str}</div>" if time_str else ""}
                {f"<div style='color:#94a3b8;font-size:13px'>{job['note']}</div>" if job['note'] else ""}
            </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# CLOCK
# ══════════════════════════════════════════════════════════════════════════
elif page == "clock":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:16px'>Clock In / Out</div>", unsafe_allow_html=True)
    now_str = datetime.now().strftime("%I:%M %p")
    today_str = date.today().isoformat()
    status_color = "#2dd4bf" if is_clocked_in else "#475569"

    st.markdown(f"""
    <div style='background:#1e2d3d;border:2px solid {status_color};border-radius:14px;
        padding:20px;text-align:center;margin-bottom:20px'>
        <div style='font-size:42px;font-weight:900;color:{status_color}'>{now_str}</div>
        <div style='color:#94a3b8;font-size:14px;margin-top:6px'>
            {"Clocked IN at " + (last_time or "")[:5] + " on " + (last_job or "") if is_clocked_in else "Not clocked in"}
        </div>
        <div style='color:#64748b;font-size:13px'>{today_hours}h logged today</div>
    </div>""", unsafe_allow_html=True)

    all_jobs = local_fetch("SELECT job_id, client FROM jobs ORDER BY job_id")
    job_options = [j["job_id"] for j in all_jobs] if all_jobs else ["No jobs"]
    selected_job = st.selectbox("Job", job_options)
    clock_note = st.text_input("Note (optional)", placeholder="e.g. Started on gutters")

    if is_clocked_in:
        if st.button("Clock Out", type="primary"):
            local_execute("INSERT INTO clock_events (employee,job_id,event_type,event_time,event_date,note,synced) VALUES (?,?,?,?,?,?,0)",
                (user, selected_job, "out", datetime.now().strftime("%H:%M:%S"), today_str, clock_note))
            emp = local_fetch("SELECT hourly_rate FROM employees WHERE name=?", (user,))
            rate = float(emp[0]["hourly_rate"]) if emp else 0
            local_execute("INSERT INTO labour_logs (work_date,job_id,employee,hours,hourly_rate,note) VALUES (?,?,?,?,?,?)",
                (today_str, selected_job, user, today_hours, rate, f"Mobile {clock_note}"))
            sync_to_supabase(user)
            st.success(f"Clocked out — {today_hours}h logged"); st.rerun()
    else:
        if st.button("Clock In", type="primary"):
            local_execute("INSERT INTO clock_events (employee,job_id,event_type,event_time,event_date,note,synced) VALUES (?,?,?,?,?,?,0)",
                (user, selected_job, "in", datetime.now().strftime("%H:%M:%S"), today_str, clock_note))
            sync_to_supabase(user)
            st.success(f"Clocked in on {selected_job}"); st.rerun()

    history = local_fetch("SELECT event_type, event_time, job_id FROM clock_events WHERE employee=? AND event_date=? ORDER BY id", (user, today_str))
    if history:
        st.markdown("<div style='font-size:12px;font-weight:700;color:#2dd4bf;text-transform:uppercase;margin:16px 0 8px'>Today</div>", unsafe_allow_html=True)
        for h in history:
            color = "#2dd4bf" if h["event_type"]=="in" else "#f43f5e"
            label = "In" if h["event_type"]=="in" else "Out"
            st.markdown(f"<div style='display:flex;gap:12px;padding:8px 0;border-bottom:1px solid #1e2d3d'><span style='color:{color};font-weight:700;min-width:30px'>{label}</span><span style='color:#e2e8f0'>{h['event_time'][:5]}</span><span style='color:#64748b'>{h['job_id']}</span></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# PHOTOS
# ══════════════════════════════════════════════════════════════════════════
elif page == "photos":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:16px'>Upload Photo</div>", unsafe_allow_html=True)
    today_str = date.today().isoformat()
    all_jobs = local_fetch("SELECT job_id FROM jobs ORDER BY job_id")
    job_options = [j["job_id"] for j in all_jobs] if all_jobs else ["No jobs"]
    photo_job = st.selectbox("Job", job_options)
    photo_caption = st.text_input("Caption", placeholder="e.g. Ridge completed, north face")
    photo_file = st.file_uploader("Take or upload photo", type=["jpg","jpeg","png","heic"])
    if photo_file and st.button("Upload Photo", type="primary"):
        local_execute("INSERT INTO job_photos (job_id,photo_date,caption,photo_data,uploaded_by) VALUES (?,?,?,?,?)",
            (photo_job, today_str, photo_caption, photo_file.read(), user))
        st.success("Photo uploaded!"); st.rerun()

    recent = local_fetch("SELECT caption, photo_date, job_id FROM job_photos WHERE uploaded_by=? ORDER BY id DESC LIMIT 5", (user,))
    if recent:
        st.markdown("<div style='font-size:12px;font-weight:700;color:#2dd4bf;text-transform:uppercase;margin:16px 0 8px'>Recent Uploads</div>", unsafe_allow_html=True)
        for p in recent:
            st.markdown(f"<div style='color:#94a3b8;font-size:13px;padding:6px 0;border-bottom:1px solid #1e2d3d'>📷 {p['caption'] or 'No caption'} · {p['job_id']} · {p['photo_date']}</div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# VARIATION
# ══════════════════════════════════════════════════════════════════════════
elif page == "variation":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:6px'>Log Variation</div>", unsafe_allow_html=True)
    st.markdown("<div style='color:#94a3b8;font-size:13px;margin-bottom:16px'>Found extra work on site? Log it here for office approval.</div>", unsafe_allow_html=True)

    all_jobs = local_fetch("SELECT job_id FROM jobs ORDER BY job_id")
    job_options = [j["job_id"] for j in all_jobs] if all_jobs else ["No jobs"]
    var_job = st.selectbox("Job", job_options)
    var_desc = st.text_area("What did you find?", placeholder="e.g. Found rotten fascia board on north face — approx 6m needs replacing", height=120)

    if st.button("Submit Variation", type="primary"):
        if var_desc.strip():
            local_execute("INSERT INTO mobile_variations (employee,job_id,description,submitted_at,status,synced) VALUES (?,?,?,?,?,0)",
                (user, var_job, var_desc.strip(), datetime.now().isoformat(), "Pending"))
            sync_to_supabase(user)
            st.success("Variation submitted — office will review and approve.")
            st.balloons()
        else:
            st.error("Please describe what you found.")

    my_vars = local_fetch("SELECT job_id, description, status FROM mobile_variations WHERE employee=? ORDER BY id DESC LIMIT 5", (user,))
    if my_vars:
        st.markdown("<div style='font-size:12px;font-weight:700;color:#2dd4bf;text-transform:uppercase;margin:16px 0 8px'>Recent Variations</div>", unsafe_allow_html=True)
        for v in my_vars:
            sc = "#2dd4bf" if v["status"]=="Approved" else "#f59e0b" if v["status"]=="Pending" else "#f43f5e"
            st.markdown(f"""
            <div class='site-card'>
                <div style='display:flex;justify-content:space-between;align-items:center'>
                    <span style='color:#e2e8f0;font-weight:700'>{v['job_id']}</span>
                    <span style='color:{sc};font-size:12px;font-weight:700'>{v['status']}</span>
                </div>
                <div style='color:#94a3b8;font-size:13px;margin-top:4px'>{str(v['description'])[:80]}</div>
            </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# PROFILE
# ══════════════════════════════════════════════════════════════════════════
elif page == "profile":
    st.markdown(f"<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:16px'>{user}</div>", unsafe_allow_html=True)

    # Hours this week
    week_total = local_fetch("SELECT SUM(hours) AS h FROM labour_logs WHERE employee=? AND work_date >= date('now','-7 days')", (user,))
    week_h = float(week_total[0]["h"] or 0) if week_total and week_total[0]["h"] else 0

    st.markdown(f"""
    <div class='site-card' style='text-align:center'>
        <div style='font-size:48px;font-weight:900;color:#2dd4bf'>{today_hours}h</div>
        <div style='color:#64748b;font-size:14px'>today</div>
        <div style='font-size:24px;font-weight:700;color:#94a3b8;margin-top:8px'>{week_h:.1f}h</div>
        <div style='color:#64748b;font-size:13px'>this week</div>
    </div>""", unsafe_allow_html=True)

    if st.button("Sync with Office", use_container_width=True):
        sync_from_supabase()
        sync_to_supabase(user)
        st.success("Synced!")

    st.markdown("<div style='font-size:12px;font-weight:700;color:#2dd4bf;text-transform:uppercase;margin:20px 0 10px'>Change PIN</div>", unsafe_allow_html=True)
    new_pin = st.text_input("New PIN (4-6 digits)", type="password", max_chars=6)
    confirm_pin = st.text_input("Confirm PIN", type="password", max_chars=6)
    if st.button("Update PIN"):
        if new_pin and new_pin == confirm_pin and new_pin.isdigit():
            local_execute("UPDATE employees SET pin=? WHERE name=?", (new_pin, user))
            st.success("PIN updated!")
        else:
            st.error("PINs must match and be digits only")

    st.divider()
    if st.button("Sign Out"):
        st.session_state.mobile_user = None
        st.session_state.mobile_page = "home"
        st.session_state.pin_input = ""
        st.rerun()
