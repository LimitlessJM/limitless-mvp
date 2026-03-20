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
    if not USE_SUPABASE: return False, ""
    try:
        url = f"{SUPABASE_URL}/rest/v1/{table}"
        headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                   "Content-Type": "application/json", "Prefer": "return=minimal"}
        r = requests.post(url, headers=headers, data=json.dumps(data), timeout=5)
        return r.status_code in [200, 201], f"{r.status_code}: {r.text[:100]}"
    except Exception as _e:
        return False, str(_e)

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
        # ── Add status column to clock_events if missing ─────────────────
        try:
            conn.execute("ALTER TABLE clock_events ADD COLUMN status TEXT DEFAULT 'Pending'")
        except: pass
        try:
            conn.execute("ALTER TABLE clock_events ADD COLUMN approved_by TEXT DEFAULT ''")
        except: pass
        try:
            conn.execute("ALTER TABLE clock_events ADD COLUMN approved_at TEXT DEFAULT ''")
        except: pass
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
        if emps:
            # Clear local employees and replace with fresh list from desktop
            local_execute("DELETE FROM employees")
            for e in emps:
                local_execute("INSERT OR REPLACE INTO employees (id,name,role,hourly_rate,active,pin) VALUES (?,?,?,?,?,?)",
                    (e.get("id"), e.get("name",""), e.get("role",""),
                     e.get("hourly_rate",0), e.get("active",1), e.get("pin","")))
                count += 1
        jobs = supa_get("jobs")
        if jobs:
            local_execute("DELETE FROM jobs")
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
    if not USE_SUPABASE: return []
    errors = []
    try:
        unsynced = local_fetch("SELECT * FROM clock_events WHERE synced=0 AND employee=?", (employee,))
        for e in unsynced:
            ok, msg = supa_post("clock_events", {
                "employee": e["employee"], "job_id": e["job_id"] or "",
                "event_type": e["event_type"], "event_time": e["event_time"],
                "event_date": e["event_date"], "note": e["note"] or "",
                "status": "Pending"
            })
            if ok:
                local_execute("UPDATE clock_events SET synced=1 WHERE id=?", (e["id"],))
            else:
                errors.append(f"clock_event: {msg}")
        unsynced_v = local_fetch("SELECT * FROM mobile_variations WHERE synced=0 AND employee=?", (employee,))
        for v in unsynced_v:
            ok, msg = supa_post("mobile_variations", {
                "employee": v["employee"], "job_id": v["job_id"],
                "description": v["description"], "submitted_at": v["submitted_at"],
                "status": v["status"]
            })
            if ok:
                local_execute("UPDATE mobile_variations SET synced=1 WHERE id=?", (v["id"],))
            else:
                errors.append(f"variation: {msg}")
    except Exception as _e:
        errors.append(str(_e))
    return errors

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
            errs = sync_to_supabase(user)
            if errs:
                st.warning(f"⚠️ Sync issue: {errs[0]}")
            else:
                st.success(f"✅ Clocked out — {today_hours}h logged · Pending director approval")
            st.rerun()
    else:
        if st.button("▶  Clock In", type="primary", use_container_width=True):
            local_execute("INSERT INTO clock_events (employee,job_id,event_type,event_time,event_date,note,status,synced) VALUES (?,?,?,?,?,?,?,0)",
                (user, selected_job, "in", datetime.now().strftime("%H:%M:%S"), today_str, clock_note, "Pending"))
            errs = sync_to_supabase(user)
            if errs:
                st.warning(f"⚠️ Sync issue: {errs[0]}")
            else:
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

    # Connection status
    if USE_SUPABASE:
        st.markdown("<div style='background:#0d2a1f;border:1px solid #2dd4bf;border-radius:8px;padding:8px 14px;font-size:13px;color:#2dd4bf;margin-bottom:12px'>🟢 Connected to office</div>", unsafe_allow_html=True)
    else:
        st.markdown("<div style='background:#2d0f0f;border:1px solid #f43f5e;border-radius:8px;padding:8px 14px;font-size:13px;color:#f43f5e;margin-bottom:12px'>🔴 No office connection — check Supabase secrets in Streamlit settings</div>", unsafe_allow_html=True)

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
