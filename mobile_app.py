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
@import url('https://fonts.googleapis.com/css2?family=Barlow+Semi+Condensed:wght@400;500;600;700;800;900&display=swap');
* { font-family: 'Barlow Semi Condensed', sans-serif !important; }
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

LOGO_B64 = ""  # Logo removed - using text wordmark

# ── Supabase ───────────────────────────────────────────────────────────────
import os as _os
SUPABASE_URL = _os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = _os.environ.get("SUPABASE_KEY", "")
try:
    if not SUPABASE_URL:
        SUPABASE_URL = st.secrets.get("SUPABASE_URL", "")
    if not SUPABASE_KEY:
        SUPABASE_KEY = st.secrets.get("SUPABASE_KEY", "")
except:
    pass

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
            hourly_rate REAL DEFAULT 0, note TEXT DEFAULT '',
            synced INTEGER DEFAULT 0)""")
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
        try:
            conn.execute("ALTER TABLE labour_logs ADD COLUMN synced INTEGER DEFAULT 0")
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
        # Pull approved labour_logs from desktop so Profile shows real hours
        approved_ll = supa_get("labour_logs")
        for ll in approved_ll:
            emp   = ll.get("employee","")
            wdate = ll.get("work_date","")
            jid   = ll.get("job_id","")
            hrs   = ll.get("hours",0)
            if not emp or not wdate: continue
            rows = local_fetch(
                "SELECT id FROM labour_logs WHERE employee=? AND work_date=? AND job_id=? AND hours=?",
                (emp, wdate, jid, hrs))
            if not rows:
                local_execute(
                    "INSERT INTO labour_logs (work_date,job_id,employee,hours,hourly_rate,note,synced) VALUES (?,?,?,?,?,?,1)",
                    (wdate, jid, emp, hrs, ll.get("hourly_rate",0), ll.get("note","") or ""))
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
        # ── Push labour_logs ───────────────────────────────────────────────
        unsynced_ll = local_fetch("SELECT * FROM labour_logs WHERE synced=0 AND employee=?", (employee,))
        for ll in unsynced_ll:
            ok, msg = supa_post("labour_logs", {
                "work_date":   ll["work_date"],
                "job_id":      ll["job_id"] or "",
                "employee":    ll["employee"],
                "hours":       ll["hours"],
                "hourly_rate": ll["hourly_rate"],
                "note":        ll["note"] or ""
            })
            if ok:
                local_execute("UPDATE labour_logs SET synced=1 WHERE id=?", (ll["id"],))
            else:
                errors.append(f"labour_log: {msg}")
        # ── Push job_photos ────────────────────────────────────────────────
        unsynced_ph = local_fetch("SELECT * FROM job_photos WHERE id NOT IN (SELECT COALESCE(id,0) FROM job_photos WHERE photo_data IS NULL) ORDER BY id")
        # Use a simpler unsynced flag approach — add synced col if missing
        try:
            import sqlite3 as _sq
            with _sq.connect(DB_PATH) as _c:
                _c.execute("ALTER TABLE job_photos ADD COLUMN synced INTEGER DEFAULT 0")
                _c.commit()
        except: pass
        unsynced_ph = local_fetch("SELECT * FROM job_photos WHERE synced=0")
        for ph in unsynced_ph:
            import base64 as _b64
            photo_b64 = _b64.b64encode(ph["photo_data"]).decode() if ph["photo_data"] else ""
            ok, msg = supa_post("job_photos", {
                "job_id":      ph["job_id"] or "",
                "photo_date":  ph["photo_date"] or "",
                "caption":     ph["caption"] or "",
                "photo_data":  photo_b64,
                "uploaded_by": ph["uploaded_by"] or ""
            })
            if ok:
                local_execute("UPDATE job_photos SET synced=1 WHERE id=?", (ph["id"],))
            else:
                errors.append(f"photo: {msg}")
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
        <div style="font-size:28px;font-weight:700;letter-spacing:.08em;color:#2dd4bf;font-family:'Barlow Semi Condensed',sans-serif">LIMITLESS</div><div style="font-size:11px;font-weight:600;letter-spacing:.2em;color:#64748b;font-family:'Barlow Semi Condensed',sans-serif">JOB MANAGEMENT</div>
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
    <div style="font-size:20px;font-weight:700;letter-spacing:.06em;color:#2dd4bf;font-family:'Barlow Semi Condensed',sans-serif">LIMITLESS</div>
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
            # ── Calculate hours and write labour_logs ──────────────────────
            emp = local_fetch("SELECT hourly_rate FROM employees WHERE name=?", (user,))
            rate = float(emp[0]["hourly_rate"]) if emp else 0.0
            hours_worked = get_today_hours(user)
            if hours_worked > 0:
                local_execute(
                    "INSERT INTO labour_logs (work_date, job_id, employee, hours, hourly_rate, note, synced) VALUES (?,?,?,?,?,?,0)",
                    (today_str, selected_job, user, hours_worked, rate, clock_note or ""))
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
            sync_from_supabase()   # pull latest approvals + assignments
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

    # ── Connection status + diagnostics ───────────────────────────────────
    if USE_SUPABASE:
        st.markdown("<div style='background:#0d2a1f;border:1px solid #2dd4bf;border-radius:8px;padding:8px 14px;font-size:13px;color:#2dd4bf;margin-bottom:12px'>🟢 Connected to Supabase</div>", unsafe_allow_html=True)
        with st.expander("🔍 Sync Diagnostics"):
            tables = ["employees","jobs","day_assignments","clock_events","mobile_variations","labour_logs","job_photos"]
            for t in tables:
                try:
                    rows = supa_get(t)
                    st.markdown(f"<span style='color:#2dd4bf'>✅ {t}</span> — {len(rows)} rows", unsafe_allow_html=True)
                except Exception as _te:
                    st.markdown(f"<span style='color:#f43f5e'>❌ {t} — {_te}</span>", unsafe_allow_html=True)
            st.divider()
            st.caption("Local unsynced:")
            ce_u = local_fetch("SELECT COUNT(*) as n FROM clock_events WHERE synced=0")
            ll_u = local_fetch("SELECT COUNT(*) as n FROM labour_logs WHERE synced=0")
            ph_u = local_fetch("SELECT COUNT(*) as n FROM job_photos WHERE synced=0")
            st.write(f"Clock events: {ce_u[0]['n']} | Labour logs: {ll_u[0]['n']} | Photos: {ph_u[0]['n']}")
            st.caption(f"URL: {SUPABASE_URL[:40] if SUPABASE_URL else 'NOT SET'}")
    else:
        st.markdown("<div style='background:#2d0f0f;border:1px solid #f43f5e;border-radius:8px;padding:8px 14px;font-size:13px;color:#f43f5e;margin-bottom:12px'>🔴 No Supabase connection</div>", unsafe_allow_html=True)
        st.warning(f"SUPABASE_URL set: {bool(SUPABASE_URL)} | SUPABASE_KEY set: {bool(SUPABASE_KEY)}")
        st.info("Add SUPABASE_URL and SUPABASE_KEY to your Streamlit secrets or environment variables.")

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
