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
.main .block-container { padding: 1rem 1rem 5rem 1rem !important; max-width: 480px !important; margin: 0 auto !important; }
.stButton button { width: 100% !important; min-height: 52px !important; font-size: 16px !important; font-weight: 700 !important; border-radius: 12px !important; margin-bottom: 6px !important; }
#MainMenu, footer, header { visibility: hidden; }
.stDeployButton { display: none; }
.site-card { background: #1e2d3d; border: 1px solid #2a3d4f; border-radius: 14px; padding: 16px; margin-bottom: 10px; }
.pin-display { font-size: 36px; letter-spacing: 12px; text-align: center; color: #2dd4bf; font-weight: 700; padding: 16px; background: #111c27; border-radius: 12px; margin-bottom: 14px; min-height: 72px; border: 1px solid #2a3d4f; }
.clock-btn-in { background: #2dd4bf !important; color: #0f172a !important; font-size: 20px !important; min-height: 70px !important; border-radius: 16px !important; }
.clock-btn-out { background: #f43f5e !important; color: #fff !important; font-size: 20px !important; min-height: 70px !important; border-radius: 16px !important; }
.status-badge-in { background: #0d2a1f; border: 1px solid #2dd4bf; border-radius: 8px; padding: 6px 14px; color: #2dd4bf; font-weight: 700; font-size: 13px; display: inline-block; }
.status-badge-out { background: #2d0f1a; border: 1px solid #f43f5e; border-radius: 8px; padding: 6px 14px; color: #f43f5e; font-weight: 700; font-size: 13px; display: inline-block; }
</style>
""", unsafe_allow_html=True)

LOGO_B64 = "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"

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
            status TEXT DEFAULT 'Pending',
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
        conn.commit()
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
        # Pull back approval status for clock events
        clock_updates = supa_get("clock_events")
        for ce in clock_updates:
            cid = ce.get("id")
            status = ce.get("status","Pending")
            if cid and status in ("Approved","Rejected"):
                local_execute("UPDATE clock_events SET status=? WHERE id=?", (status, cid))
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
                "event_date": e["event_date"], "note": e["note"] or "",
                "status": "Pending"
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
    st.markdown(f"""
    <div style='text-align:center;padding:40px 0 8px'>
        <img src="data:image/png;base64,{LOGO_B64}" style="width:140px;margin-bottom:8px">
        <div style='font-size:12px;color:#2dd4bf;font-weight:700;letter-spacing:.2em;margin-top:4px;text-transform:uppercase'>Site App</div>
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

# Top bar with logo + user
st.markdown(f"""
<div style='display:flex;justify-content:space-between;align-items:center;
    background:#111c27;border-radius:14px;padding:10px 14px;margin-bottom:14px;
    border:1px solid #1e2d3d'>
    <img src="data:image/png;base64,{LOGO_B64}" style="height:36px">
    <div style='text-align:right'>
        <div style='font-size:14px;font-weight:700;color:#e2e8f0'>{user}</div>
        <div style='font-size:12px;color:{"#2dd4bf" if is_clocked_in else "#475569"}'>
            {"🟢 On Site" if is_clocked_in else "⚫ Off Site"} · {today_hours}h today
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

# Navigation
page = st.session_state.mobile_page
nav_items = [("🏠 Home","home"),("⏱ Clock","clock"),("📷 Photos","photos"),("⚠️ Variation","variation"),("👤 Profile","profile")]
nav_cols = st.columns(5)
for col, (label, pg) in zip(nav_cols, nav_items):
    with col:
        is_active = page == pg
        color = "#2dd4bf" if is_active else "#475569"
        icon, txt = label.split(" ", 1)
        st.markdown(f"<div style='text-align:center;font-size:18px'>{icon}</div><div style='text-align:center;font-size:10px;font-weight:700;color:{color};letter-spacing:.05em;text-transform:uppercase'>{txt}</div>", unsafe_allow_html=True)
        if st.button(txt, key=f"nav_{pg}", use_container_width=True):
            st.session_state.mobile_page = pg; st.rerun()

st.divider()

# ══════════════════════════════════════════════════════════════════════════
# HOME
# ══════════════════════════════════════════════════════════════════════════
if page == "home":
    today_str = date.today().isoformat()
    today_nice = date.today().strftime("%A, %d %B")
    hour = datetime.now().hour
    greeting = "Good morning" if hour < 12 else "Good afternoon" if hour < 17 else "G'day"

    st.markdown(f"<div style='font-size:24px;font-weight:800;color:#e2e8f0;margin-bottom:2px'>{greeting}, {user.split()[0]}.</div>", unsafe_allow_html=True)
    st.markdown(f"<div style='color:#475569;font-size:13px;margin-bottom:16px'>{today_nice}</div>", unsafe_allow_html=True)

    # Big clock status card
    if is_clocked_in:
        st.markdown(f"""
        <div style='background:linear-gradient(135deg,#0d2a1f,#1a3a2a);border:2px solid #2dd4bf;
            border-radius:16px;padding:20px;text-align:center;margin-bottom:16px'>
            <div style='font-size:13px;font-weight:700;color:#2dd4bf;letter-spacing:.1em;text-transform:uppercase'>On Site</div>
            <div style='font-size:48px;font-weight:900;color:#2dd4bf;line-height:1.1'>{today_hours}h</div>
            <div style='color:#94a3b8;font-size:13px'>Clocked in at {(last_time or "")[:5]} · {last_job or ""}</div>
        </div>""", unsafe_allow_html=True)
        if st.button("⏹ Clock Out Now", type="primary"):
            st.session_state.mobile_page = "clock"; st.rerun()
    else:
        st.markdown(f"""
        <div style='background:#111c27;border:1px solid #2a3d4f;
            border-radius:16px;padding:20px;text-align:center;margin-bottom:16px'>
            <div style='font-size:13px;font-weight:700;color:#475569;letter-spacing:.1em;text-transform:uppercase'>Not On Site</div>
            <div style='font-size:48px;font-weight:900;color:#475569;line-height:1.1'>{today_hours}h</div>
            <div style='color:#64748b;font-size:13px'>Ready to start</div>
        </div>""", unsafe_allow_html=True)
        if st.button("▶ Clock In", type="primary"):
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
                <div style='font-size:18px;font-weight:800;color:#2dd4bf'>{job['job_id']}</div>
                <div style='color:#e2e8f0;font-size:15px;font-weight:600'>{job['client'] or ''}</div>
                <div style='color:#64748b;font-size:13px;margin-top:4px'>📍 {job['address'] or ''}</div>
                {f"<div style='color:#f59e0b;font-size:13px;margin-top:4px'>🕐 {time_str}</div>" if time_str else ""}
                {f"<div style='color:#94a3b8;font-size:13px'>{job['note']}</div>" if job['note'] else ""}
            </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# CLOCK
# ══════════════════════════════════════════════════════════════════════════
elif page == "clock":
    now_str = datetime.now().strftime("%I:%M %p")
    today_str = date.today().isoformat()

    st.markdown(f"""
    <div style='background:#111c27;border:2px solid {"#2dd4bf" if is_clocked_in else "#2a3d4f"};
        border-radius:16px;padding:24px;text-align:center;margin-bottom:20px'>
        <div style='font-size:52px;font-weight:900;color:#e2e8f0;letter-spacing:-.02em'>{now_str}</div>
        <div style='margin-top:8px'>
            <span class='{"status-badge-in" if is_clocked_in else "status-badge-out"}'>
                {"🟢 ON SITE" if is_clocked_in else "⚫ OFF SITE"}
            </span>
        </div>
        {f"<div style='color:#94a3b8;font-size:13px;margin-top:10px'>Clocked in at {(last_time or '')[:5]} on {last_job or ''}</div>" if is_clocked_in else ""}
        <div style='font-size:28px;font-weight:800;color:#2dd4bf;margin-top:8px'>{today_hours}h today</div>
    </div>""", unsafe_allow_html=True)

    all_jobs = local_fetch("SELECT job_id, client FROM jobs WHERE stage='Live Job' ORDER BY job_id")
    if not all_jobs:
        all_jobs = local_fetch("SELECT job_id, client FROM jobs ORDER BY job_id")
    job_options = [f"{j['job_id']} — {j['client']}" for j in all_jobs] if all_jobs else ["No jobs"]
    job_ids     = [j["job_id"] for j in all_jobs] if all_jobs else [""]
    selected_idx = st.selectbox("Job", range(len(job_options)), format_func=lambda x: job_options[x])
    selected_job = job_ids[selected_idx] if job_ids else ""
    clock_note = st.text_input("Note (optional)", placeholder="e.g. Started ridge capping")

    if is_clocked_in:
        if st.button("⏹  Clock Out", type="primary", use_container_width=True):
            now = datetime.now()
            local_execute("INSERT INTO clock_events (employee,job_id,event_type,event_time,event_date,note,status,synced) VALUES (?,?,?,?,?,?,?,0)",
                (user, selected_job, "out", now.strftime("%H:%M:%S"), today_str, clock_note, "Pending"))
            emp = local_fetch("SELECT hourly_rate FROM employees WHERE name=?", (user,))
            rate = float(emp[0]["hourly_rate"]) if emp else 0
            sync_to_supabase(user)
            st.success(f"✅ Clocked out — {today_hours}h logged · Pending director approval")
            st.rerun()
    else:
        if st.button("▶  Clock In", type="primary", use_container_width=True):
            local_execute("INSERT INTO clock_events (employee,job_id,event_type,event_time,event_date,note,status,synced) VALUES (?,?,?,?,?,?,?,0)",
                (user, selected_job, "in", datetime.now().strftime("%H:%M:%S"), today_str, clock_note, "Pending"))
            sync_to_supabase(user)
            st.success(f"✅ Clocked in on {selected_job}")
            st.rerun()

    history = local_fetch("SELECT event_type, event_time, job_id, status FROM clock_events WHERE employee=? AND event_date=? ORDER BY id", (user, today_str))
    if history:
        st.markdown("<div style='font-size:12px;font-weight:700;color:#2dd4bf;text-transform:uppercase;margin:20px 0 8px'>Today\'s Log</div>", unsafe_allow_html=True)
        for h in history:
            color = "#2dd4bf" if h["event_type"]=="in" else "#f43f5e"
            label = "IN" if h["event_type"]=="in" else "OUT"
            status_col = "#f59e0b" if h["status"]=="Pending" else "#2dd4bf" if h["status"]=="Approved" else "#f43f5e"
            st.markdown(f"""
            <div style='display:flex;gap:12px;align-items:center;padding:10px 0;border-bottom:1px solid #1e2d3d'>
                <span style='color:{color};font-weight:800;font-size:12px;min-width:32px'>{label}</span>
                <span style='color:#e2e8f0;font-size:15px;font-weight:700'>{h["event_time"][:5]}</span>
                <span style='color:#64748b;font-size:13px;flex:1'>{h["job_id"]}</span>
                <span style='color:{status_col};font-size:11px;font-weight:700'>{h["status"] or "Pending"}</span>
            </div>""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# PHOTOS
# ══════════════════════════════════════════════════════════════════════════
elif page == "photos":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:16px'>📷 Site Photos</div>", unsafe_allow_html=True)
    today_str = date.today().isoformat()
    all_jobs = local_fetch("SELECT job_id FROM jobs ORDER BY job_id")
    job_options = [j["job_id"] for j in all_jobs] if all_jobs else ["No jobs"]
    photo_job = st.selectbox("Job", job_options)
    photo_caption = st.text_input("Caption", placeholder="e.g. Ridge completed, north face")
    photo_file = st.file_uploader("Take or upload photo", type=["jpg","jpeg","png","heic"])
    if photo_file and st.button("Upload Photo", type="primary"):
        local_execute("INSERT INTO job_photos (job_id,photo_date,caption,photo_data,uploaded_by) VALUES (?,?,?,?,?)",
            (photo_job, today_str, photo_caption, photo_file.read(), user))
        st.success("✅ Photo uploaded!"); st.rerun()

    recent = local_fetch("SELECT caption, photo_date, job_id FROM job_photos WHERE uploaded_by=? ORDER BY id DESC LIMIT 5", (user,))
    if recent:
        st.markdown("<div style='font-size:12px;font-weight:700;color:#2dd4bf;text-transform:uppercase;margin:16px 0 8px'>Recent Uploads</div>", unsafe_allow_html=True)
        for p in recent:
            st.markdown(f"<div class='site-card'><span style='color:#2dd4bf'>📷</span> <span style='color:#e2e8f0'> {p['caption'] or 'No caption'}</span> <span style='color:#64748b;font-size:12px'>· {p['job_id']} · {p['photo_date']}</span></div>", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════
# VARIATION
# ══════════════════════════════════════════════════════════════════════════
elif page == "variation":
    st.markdown("<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:6px'>⚠️ Log Variation</div>", unsafe_allow_html=True)
    st.markdown("<div style='color:#94a3b8;font-size:13px;margin-bottom:16px'>Found extra work on site? Log it here for office approval.</div>", unsafe_allow_html=True)

    all_jobs = local_fetch("SELECT job_id, client FROM jobs ORDER BY job_id")
    job_options = [f"{j['job_id']} — {j['client']}" for j in all_jobs] if all_jobs else ["No jobs"]
    job_ids = [j["job_id"] for j in all_jobs] if all_jobs else [""]
    var_idx = st.selectbox("Job", range(len(job_options)), format_func=lambda x: job_options[x])
    var_job = job_ids[var_idx] if job_ids else ""
    var_desc = st.text_area("What did you find?", placeholder="e.g. Found rotten fascia board on north face — approx 6m needs replacing", height=120)

    if st.button("Submit Variation", type="primary"):
        if var_desc.strip():
            local_execute("INSERT INTO mobile_variations (employee,job_id,description,submitted_at,status,synced) VALUES (?,?,?,?,?,0)",
                (user, var_job, var_desc.strip(), datetime.now().isoformat(), "Pending"))
            sync_to_supabase(user)
            st.success("✅ Variation submitted — office will review and approve.")
            st.balloons()
        else:
            st.error("Please describe what you found.")

    my_vars = local_fetch("SELECT job_id, description, status FROM mobile_variations WHERE employee=? ORDER BY id DESC LIMIT 5", (user,))
    if my_vars:
        st.markdown("<div style='font-size:12px;font-weight:700;color:#2dd4bf;text-transform:uppercase;margin:16px 0 8px'>My Variations</div>", unsafe_allow_html=True)
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
    st.markdown(f"<div style='font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:16px'>👤 {user}</div>", unsafe_allow_html=True)

    week_total = local_fetch("SELECT SUM(hours) AS h FROM labour_logs WHERE employee=? AND work_date >= date('now','-7 days')", (user,))
    week_h = float(week_total[0]["h"] or 0) if week_total and week_total[0]["h"] else 0

    st.markdown(f"""
    <div class='site-card' style='text-align:center;padding:24px'>
        <div style='font-size:52px;font-weight:900;color:#2dd4bf'>{today_hours}h</div>
        <div style='color:#64748b;font-size:14px'>today</div>
        <div style='height:1px;background:#2a3d4f;margin:16px 0'></div>
        <div style='font-size:28px;font-weight:700;color:#94a3b8'>{week_h:.1f}h</div>
        <div style='color:#64748b;font-size:13px'>this week (approved)</div>
    </div>""", unsafe_allow_html=True)

    st.markdown("<div style='color:#475569;font-size:12px;text-align:center;margin:8px 0 16px'>Hours are pending director approval before counting toward your timesheet.</div>", unsafe_allow_html=True)

    if st.button("🔄 Sync with Office", use_container_width=True):
        sync_from_supabase()
        sync_to_supabase(user)
        st.success("✅ Synced!")

    st.markdown("<div style='font-size:12px;font-weight:700;color:#2dd4bf;text-transform:uppercase;margin:20px 0 10px'>Change PIN</div>", unsafe_allow_html=True)
    new_pin = st.text_input("New PIN (4-6 digits)", type="password", max_chars=6)
    confirm_pin = st.text_input("Confirm PIN", type="password", max_chars=6)
    if st.button("Update PIN"):
        if new_pin and new_pin == confirm_pin and new_pin.isdigit():
            local_execute("UPDATE employees SET pin=? WHERE name=?", (new_pin, user))
            st.success("✅ PIN updated!")
        else:
            st.error("PINs must match and be digits only")

    st.divider()
    if st.button("Sign Out"):
        st.session_state.mobile_user = None
        st.session_state.mobile_page = "home"
        st.session_state.pin_input = ""
        st.rerun()
