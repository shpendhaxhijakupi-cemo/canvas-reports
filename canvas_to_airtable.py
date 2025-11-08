import os
import time
import requests
import traceback
import re
from typing import Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyairtable import Api

# ==============================
# Env / Config
# ==============================
BASE_URL = os.environ["CANVAS_API_URL"].rstrip("/")
CANVAS_ACCESS_TOKEN = os.environ["CANVAS_ACCESS_TOKEN"]

AIRTABLE_API_KEY = os.environ["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]

# Prefer table IDs if provided; else use names
AIRTABLE_DETAILED_TABLE_ID = os.environ.get("AIRTABLE_DETAILED_TABLE_ID")
AIRTABLE_SUMMARY_TABLE_ID  = os.environ.get("AIRTABLE_SUMMARY_TABLE_ID")
AIRTABLE_DETAILED_TABLE = os.environ.get("AIRTABLE_DETAILED_TABLE", "Phoenix Student Assignment Details")
AIRTABLE_SUMMARY_TABLE  = os.environ.get("AIRTABLE_SUMMARY_TABLE",  "Phoenix Christian Course Details")

# Run-time toggles
DEBUG = os.environ.get("DEBUG", "0") == "1"
LOG_SCHEMA = os.environ.get("LOG_SCHEMA", "0") == "1"
ALLOW_SELECT_FALLBACK = os.environ.get("ALLOW_SELECT_FALLBACK", "0") == "1"
SLEEP_BETWEEN_REQUESTS = float(os.environ.get("SLEEP_BETWEEN_REQUESTS", "0.0"))
SHOW_FETCH_ASSIGNMENTS = True
SHOW_FETCH_SUBMISSIONS = True
ENABLE_AIRTABLE_WRITE_PROBE = False

# === HARD-CODED: wipe tables first by default ===
WIPE_TABLES_FIRST = True
FAST_WIPE_WORKERS = int(os.environ.get("FAST_WIPE_WORKERS", "8"))   # parallel delete threads
FAST_WIPE_PAGE_SIZE = int(os.environ.get("FAST_WIPE_PAGE_SIZE", "100"))
AIRTABLE_RPS = float(os.environ.get("AIRTABLE_RPS", "10"))          # crude throttle (requests/sec)

# Partition/job label from workflow (e.g., "B2C_P1" / "B2C_P2" / "Phoenix" ...)
PARTNER_NAME = os.environ.get("PARTNER_NAME", "").strip()

# Student IDs from env (comma/space/newline separated)
STUDENT_USER_IDS_RAW = os.environ.get("STUDENT_USER_IDS", "")

# Allow split via env by partition name (B2C_P1 / B2C_P2) if STUDENT_USER_IDS not set directly
if not STUDENT_USER_IDS_RAW and PARTNER_NAME.upper().endswith("_P1"):
    STUDENT_USER_IDS_RAW = os.environ.get("STUDENT_IDS_B2C_P1", "").strip()
elif not STUDENT_USER_IDS_RAW and PARTNER_NAME.upper().endswith("_P2"):
    STUDENT_USER_IDS_RAW = os.environ.get("STUDENT_IDS_B2C_P2", "").strip()

# Skip these assignment titles (exact match, lowercased elsewhere)
SKIP_EXACT_TITLES = {"end of unit feedback", "quarterly feedback"}

# Exclude these courses (exact name or containing the word "Live")
EXCLUDED_COURSE_EXACT = {"canvas student orientation program"}
_EXCLUDED_LIVE_RE = re.compile(r"\blive\b", re.IGNORECASE)

def _is_excluded_course(name: Optional[str]) -> bool:
    if not name:
        return False
    n = name.strip().lower()
    if n in EXCLUDED_COURSE_EXACT:
        return True
    return bool(_EXCLUDED_LIVE_RE.search(name))

# ==============================
# Print helpers
# ==============================
def p(msg: str): print(msg, flush=True)
def dbg(msg: str):
    if DEBUG: p(f"[DEBUG] {msg}")

def _preview_ids(ids: List[str], n: int = 3) -> str:
    if not ids:
        return "[]"
    if len(ids) <= n * 2:
        return str(ids)
    return f"{ids[:n]} ... {ids[-n:]}"

# ==============================
# Airtable setup
# ==============================
api = Api(AIRTABLE_API_KEY)

def _resolve_table(base_id: str, table_id: Optional[str], table_name: str):
    if table_id:
        return api.table(base_id, table_id), f"(table_id={table_id})"
    return api.table(base_id, table_name), f"(table_name='{table_name}')"

tbl_detailed, detailed_selector = _resolve_table(AIRTABLE_BASE_ID, AIRTABLE_DETAILED_TABLE_ID, AIRTABLE_DETAILED_TABLE)
tbl_summary,  summary_selector  = _resolve_table(AIRTABLE_BASE_ID, AIRTABLE_SUMMARY_TABLE_ID,  AIRTABLE_SUMMARY_TABLE)

# ==============================
# Canvas helpers (PAT + masquerade)
# ==============================
def make_canvas_request(endpoint: str, params=None):
    if SLEEP_BETWEEN_REQUESTS:
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    url = endpoint if endpoint.startswith("http") else f"{BASE_URL}/{endpoint.lstrip('/')}"
    headers = {"Authorization": f"Bearer {CANVAS_ACCESS_TOKEN}"}
    resp = requests.get(url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    return resp

def get_user_profile_admin(user_id: str) -> dict:
    return make_canvas_request("users/self/profile", params={"as_user_id": user_id}).json()

def get_all_active_courses_admin(user_id: str, term_id=None) -> List[dict]:
    """
    Request includes term; Canvas returns student-personalized course info.
    We'll prefer 'original_name' to avoid student nicknames being used.
    """
    states = ["active", "invited_or_pending", "completed", "inactive"]
    seen = set()
    courses: List[dict] = []
    for es in states:
        endpoint = "users/self/courses"
        params = {"as_user_id": user_id, "enrollment_state": es, "per_page": 100, "include[]": "term"}
        while endpoint:
            resp = make_canvas_request(endpoint, params=params)
            data = resp.json()
            if term_id is not None:
                data = [c for c in data if c.get("enrollment_term_id") == term_id]
            for c in data:
                cid = c.get("id")
                if cid and cid not in seen:
                    seen.add(cid)
                    courses.append(c)
            # pagination
            links = resp.headers.get("Link", "")
            next_url = None
            for link in links.split(","):
                if 'rel="next"' in link:
                    next_url = link[link.find("<")+1:link.find(">")]
                    break
            if next_url:
                endpoint = next_url[len(BASE_URL)+1:] if next_url.startswith(BASE_URL) else next_url
                params = None
            else:
                endpoint = None
    return courses

def get_all_assignments(course_id: int, stats: dict) -> List[dict]:
    out = []
    endpoint = f"courses/{course_id}/assignments"
    while endpoint:
        resp = make_canvas_request(endpoint)
        data = resp.json()
        for a in data:
            if not a.get("published"):
                continue
            name_norm = (a.get("name") or "").strip().lower()
            if name_norm in SKIP_EXACT_TITLES:
                stats["skipped"] += 1
                continue
            if SHOW_FETCH_ASSIGNMENTS:
                p(f"[KEEP] assignment {a.get('id')} '{a.get('name')}' course {course_id}")
            out.append(a)
            stats["processed"] += 1
        links = resp.headers.get("Link", "")
        next_url = None
        for link in links.split(","):
            if 'rel="next"' in link:
                next_url = link[link.find("<")+1:link.find(">")]
                break
        endpoint = next_url[len(BASE_URL)+1:] if next_url else None
    return out

def get_submission(course_id: int, assignment_id: int, user_id: str) -> dict:
    try:
        if SHOW_FETCH_SUBMISSIONS:
            p(f"Fetching submission for assignment {assignment_id} (course {course_id}) user {user_id}")
        sub = make_canvas_request(
            f"courses/{course_id}/assignments/{assignment_id}/submissions/{user_id}"
        ).json()
        state = sub.get("workflow_state", "unsubmitted")
        grade = sub.get("grade", "N/A")
        if sub.get("excused", False):
            return {"submission_status": "excused", "grade": "Excused"}
        if state in ["graded", "submitted"] and grade not in [None, "-", ""]:
            return {"submission_status": "graded", "grade": grade}
        return {"submission_status": "unsubmitted", "grade": "N/A"}
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return {"submission_status": "unsubmitted", "grade": "N/A"}
        raise

# ==============================
# Airtable helpers (retry + schema)
# ==============================
def _chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i+size]

def _status_from_exc(e):
    try:
        resp = getattr(e, "response", None)
        if resp is not None and hasattr(resp, "status_code"):
            return int(resp.status_code)
    except Exception:
        pass
    return None

def _airtable_retry(fn, *args, **kwargs):
    delays = [0, 1, 2, 4]
    last_exc = None
    for d in delays:
        try:
            if d:
                time.sleep(d)
            return fn(*args, **kwargs)
        except Exception as e:
            status = _status_from_exc(e)
            if status in (429, 500, 502, 503, 504):
                p(f"[WARN] Airtable API {status}; retrying in {d}s...")
                last_exc = e
                continue
            p("[ERROR] Airtable call failed (no retry):")
            try:
                p(repr(e))
                resp = getattr(e, "response", None)
                if resp is not None:
                    p("Body: " + getattr(resp, "text", "")[:2000])
            except Exception:
                pass
            raise
    if last_exc:
        raise last_exc

def _airtable_preflight():
    p(f"[INFO] Airtable target: base={AIRTABLE_BASE_ID} detailed={detailed_selector} summary={summary_selector}")
    _airtable_retry(tbl_detailed.all, max_records=1)
    _airtable_retry(tbl_summary.all,  max_records=1)
    p("[INFO] Airtable preflight OK (can read both tables).")

def _airtable_write_probe():
    if not ENABLE_AIRTABLE_WRITE_PROBE:
        return
    p("[INFO] Airtable write probe: creating + deleting 1 record in the detailed table")
    tmp = _airtable_retry(tbl_detailed.batch_create, [{"Student Name": "__probe__"}])
    try:
        rec_id = tmp[0]["id"] if isinstance(tmp, list) and tmp and isinstance(tmp[0], dict) else None
        if rec_id:
            _airtable_retry(tbl_detailed.batch_delete, [rec_id])
        p("[INFO] Airtable write probe OK.]")
    except Exception:
        p("[WARN] Probe create succeeded but cleanup failed; continuing")

def _fetch_base_schema() -> Dict[str, dict]:
    url = f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    out = {}
    for t in data.get("tables", []):
        out[t["id"]] = t
        out[t["name"]] = t
    return out

def _single_select_options(table_def: dict):
    opts = {}
    for f in table_def.get("fields", []):
        if f.get("type") == "singleSelect":
            choices = [c.get("name") for c in (f.get("options", {}).get("choices") or []) if c.get("name")]
            opts[f["name"]] = choices
    return opts

def _validate_or_coerce_selects(rows: List[dict], table_def: dict, table_label: str):
    if not rows:
        return
    options_map = _single_select_options(table_def)
    if LOG_SCHEMA:
        p(f"[SCHEMA] {table_label} select options: {{k: options_map[k][:10] for k in options_map}}")
    if not options_map:
        return
    seen: Dict[str, set] = {fname: set() for fname in options_map.keys() if fname}
    for r in rows:
        for fname in options_map.keys():
            if fname in r and r[fname] not in (None, ""):
                seen[fname].add(r[fname])
    missing: Dict[str, List[str]] = {}
    for fname, values in seen.items():
        allowed = set(options_map.get(fname, []))
        unknown = [v for v in values if v not in allowed]
        if unknown:
            missing[fname] = sorted(set(unknown))
    if not missing:
        p(f"[INFO] {table_label}: single select values OK.")
        return
    if not ALLOW_SELECT_FALLBACK:
        p(f"[FATAL] {table_label}: missing single-select options detected:")
        for fname, vals in missing.items():
            p(f"       - Field '{fname}': add options → {vals[:20]}{' ...' if len(vals)>20 else ''}")
        p("       Fix in Airtable (add these options or change field to 'Single line text'),")
        p("       or set ALLOW_SELECT_FALLBACK=1 to coerce unknowns to an existing option.")
        raise SystemExit(1)
    p(f"[WARN] {table_label}: coercing unknown single-select values (ALLOW_SELECT_FALLBACK=1).")
    for fname, choices in options_map.items():
        if not choices:
            continue
        fallback = "Other" if "Other" in choices else choices[0]
        allowed = set(choices)
        for r in rows:
            if fname in r and r[fname] not in allowed:
                r[fname] = fallback

# Writable-field filtering
def _writable_fieldnames(table_def: dict) -> set:
    non_writable = {"formula", "rollup", "lookup", "createdTime", "lastModifiedTime", "autoNumber", "button"}
    writable = set()
    for f in table_def.get("fields", []):
        ftype = f.get("type")
        name = f.get("name")
        if not name:
            continue
        if ftype in non_writable:
            continue
        if ftype == "multipleRecordLinks":
            continue
        writable.add(name)
    return writable

def _filter_rows_to_writable(rows: List[dict], table_def: dict, label: str) -> List[dict]:
    if not table_def or not rows:
        return rows
    writable = _writable_fieldnames(table_def)
    trimmed = [{k: v for k, v in r.items() if k in writable} for r in rows]
    try:
        for orig, new in zip(rows, trimmed):
            if orig.keys() != new.keys():
                removed = [k for k in orig.keys() if k not in new]
                p(f"[INFO] {label}: dropping non-writable fields → {removed}")
                break
    except Exception:
        pass
    return trimmed

def _field_type(table_def: dict, field_name: str) -> Optional[str]:
    for f in table_def.get("fields", []):
        if f.get("name") == field_name:
            return f.get("type")
    return None

def _coerce_percentage_for_schema(rows: List[dict], table_def: dict, field_name: str):
    if not rows or not table_def:
        return
    ftype = _field_type(table_def, field_name)
    if not ftype:
        return
    if ftype == "singleLineText":
        for r in rows:
            v = r.get(field_name, None)
            if isinstance(v, (int, float)):
                r[field_name] = f"{v*100:.2f}%"

# ==============================
# FAST WIPE HELPERS (always used)
# ==============================
def _delete_chunk(table, ids: List[str]):
    _airtable_retry(table.batch_delete, ids)

def _collect_all_ids(table, label: str) -> List[str]:
    """Robustly collect IDs. Try .all() first, then fall back to iterate()."""
    ids: List[str] = []
    sample: List[str] = []

    # Primary: .all()
    try:
        records = _airtable_retry(table.all, page_size=FAST_WIPE_PAGE_SIZE)
        for r in records or []:
            rid = r.get("id")
            if rid:
                ids.append(rid)
                if len(sample) < 3:
                    sample.append(rid)
    except Exception:
        pass

    # Fallback: iterate()
    if not ids:
        try:
            for page in table.iterate(page_size=FAST_WIPE_PAGE_SIZE):
                if isinstance(page, dict):
                    rid = page.get("id")
                    if rid:
                        ids.append(rid)
                        if len(sample) < 3:
                            sample.append(rid)
                else:
                    for rec in page:
                        rid = rec.get("id") if isinstance(rec, dict) else None
                        if rid:
                            ids.append(rid)
                            if len(sample) < 3:
                                sample.append(rid)
        except Exception:
            p(f"[WARN] {label}: iterate() failed to list records")
            traceback.print_exc()

    if ids:
        p(f"[WIPE] {label}: found {len(ids)} record IDs (sample {sample})")
    else:
        p(f"[WIPE] {label}: no records returned by API. (Check table name/ID & token access.)")
    return ids

def wipe_table_fast(table, label: str):
    p(f"[WIPE] Collecting all record IDs from {label}…")
    ids = _collect_all_ids(table, label)
    total = len(ids)
    if not total:
        p(f"[WIPE] {label}: nothing to delete.")
        return
    p(f"[WIPE] {label}: deleting {total} records in parallel…")

    chunks: List[List[str]] = list(_chunks(ids, 10))  # 10 IDs per request
    max_workers = max(1, FAST_WIPE_WORKERS)
    per_second = max(1, int(AIRTABLE_RPS))
    submitted = 0
    done_reqs = 0

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = []
        for ch in chunks:
            futures.append(ex.submit(_delete_chunk, table, ch))
            submitted += 1
            if submitted % per_second == 0:
                time.sleep(1.0)  # crude throttle

        for fut in as_completed(futures):
            try:
                fut.result()
                done_reqs += 1
                if done_reqs % 50 == 0:
                    dbg(f"[WIPE] {label}: ~{done_reqs*10} rows deleted (requests: {done_reqs})")
            except Exception:
                p(f"[WARN] {label}: a delete batch failed; continuing")
                traceback.print_exc()

    p(f"[WIPE] {label}: wipe complete (requests: {len(chunks)}, rows≈{total}).")

# ==============================
# CRUD helpers (writes)
# ==============================
def airtable_insert_detailed(rows: List[dict]):
    p(f"[INFO] Inserting {len(rows)} detailed rows")
    if not rows:
        return
    for i, chunk in enumerate(_chunks(rows, 10), start=1):
        try:
            _airtable_retry(tbl_detailed.batch_create, chunk)
            dbg(f" detailed batch {i} ok ({len(chunk)})")
        except Exception:
            p(f"[ERROR] detailed batch {i} failed, first record preview:")
            try: p(str(chunk[0]))
            except Exception: pass
            traceback.print_exc()
            raise

def airtable_insert_summary(rows: List[dict]):
    p(f"[INFO] Inserting {len(rows)} summary rows")
    if not rows:
        return
    for i, chunk in enumerate(_chunks(rows, 10), start=1):
        try:
            _airtable_retry(tbl_summary.batch_create, chunk)
            dbg(f" summary batch {i} ok ({len(chunk)})")
        except Exception:
            p(f"[ERROR] summary batch {i} failed, first record preview:")
            try: p(str(chunk[0]))
            except Exception: pass
            traceback.print_exc()
            raise

# ==============================
# Main
# ==============================
def _parse_student_ids_from_env(raw: str) -> List[str]:
    if not raw:
        return []
    parts = [x.strip() for chunk in raw.replace("\n", ",").replace(" ", ",").split(",") for x in [chunk] if x.strip()]
    return parts

def main():
    p("=== START canvas_to_airtable ===")
    _airtable_preflight()
    _airtable_write_probe()

    user_ids = _parse_student_ids_from_env(STUDENT_USER_IDS_RAW)
    if user_ids:
        p(f"[INFO] PARTNER_NAME='{PARTNER_NAME}' → using {len(user_ids)} student IDs")
        try:
            p(f"[INFO] ID preview: {_preview_ids(user_ids)}")
        except Exception:
            pass
    else:
        p("[INFO] No STUDENT_USER_IDS provided; nothing to do.")
        return

    # ALWAYS WIPE FIRST (hard-coded) unless explicitly told to skip (for parallel partitions)
    SKIP_WIPE = os.environ.get("SKIP_WIPE", "0") == "1"
    if PARTNER_NAME.upper().endswith("_P2") or SKIP_WIPE:
        p("[WIPE] Skipped (either _P2 partition or SKIP_WIPE=1).")
    else:
        p("[WIPE] Wiping both Airtable tables before writing (hard-coded).")
        wipe_table_fast(tbl_detailed, "Detailed table")
        wipe_table_fast(tbl_summary,  "Summary table")

    stats = {"processed": 0, "skipped": 0}
    p("[INFO] Skipping global terms lookup; using course.term.name from Canvas.")

    students_data = {}

    for user_id in user_ids:
        try:
            profile = get_user_profile_admin(user_id)
            student_name = profile.get("name", f"User {user_id}")

            courses = get_all_active_courses_admin(user_id, term_id=None)
            seen_courses = set()
            all_data: Dict[Tuple[str, str], List[dict]] = {}

            for course in courses:
                cid = course["id"]
                if cid in seen_courses:
                    continue
                seen_courses.add(cid)

                # Prefer official course name; fall back to student's nickname only if needed
                course_name = course.get("original_name") or course.get("name") or f"Course {cid}"

                # Exclude configured courses
                if _is_excluded_course(course_name):
                    p(f"[SKIP COURSE] Excluding course '{course_name}' (id {cid})")
                    continue

                term_obj = course.get("term") or {}
                term_id = course.get("enrollment_term_id")
                term_name = term_obj.get("name") or (f"Term {term_id}" if term_id else "Unknown Term")

                assignments = get_all_assignments(cid, stats)
                all_data[(term_name, course_name)] = [
                    {
                        "assignment_name": a["name"],
                        "due_date": a.get("due_at"),
                        "Course Name": course_name,
                        "Term Name": term_name,
                        **get_submission(cid, a["id"], user_id),
                    }
                    for a in assignments
                ]

            students_data[student_name] = {"detailed_data": all_data}
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            p(f"[ERROR] User {user_id} failed with HTTP {status}. Ensure admin PAT with Masquerade.")
            continue
        except Exception as e:
            p(f"[ERROR] Processing user {user_id}: {e}")
            traceback.print_exc()
            continue

    # Flatten → Airtable rows
    detailed_rows: List[dict] = []
    summary_rows: List[dict] = []

    for student_name, data in students_data.items():
        for (term_name, course_name), assignments in data["detailed_data"].items():
            total = len(assignments)
            completed = sum(1 for a in assignments if a.get("submission_status") in ["graded", "excused"])
            unsubmitted = total - completed
            pct = (completed / total) if total > 0 else 0.0

            for a in assignments:
                detailed_rows.append({
                    "Student Name": student_name,
                    "Term Name": term_name,
                    "Course Name": course_name,
                    "Assignment Name": a.get("assignment_name", "N/A"),
                    "Due Date": a.get("due_date") or None,
                    "Submission Status": a.get("submission_status", "unsubmitted"),
                    "Grade": a.get("grade", "N/A"),
                })

            summary_rows.append({
                "Student Name": student_name,
                "Term Name": term_name,
                "Course Name": course_name,
                "Total Assignments": total,
                "Completed": completed,
                "Unsubmitted": unsubmitted,
                "Percentage Completed": pct,  # coerced to text if needed
            })

    p(f"[INFO] Built {len(detailed_rows)} detailed rows; {len(summary_rows)} summary rows.")
    p(f"[INFO] Students in run: {len(students_data)} → {list(students_data.keys())[:5]}{'...' if len(students_data)>5 else ''}")

    # ===== Schema check for select fields =====
    p("[STEP] Fetching base schema…")
    schema = _fetch_base_schema()
    detailed_def = schema.get(AIRTABLE_DETAILED_TABLE_ID or AIRTABLE_DETAILED_TABLE)
    summary_def  = schema.get(AIRTABLE_SUMMARY_TABLE_ID  or AIRTABLE_SUMMARY_TABLE)
    if detailed_def:
        _validate_or_coerce_selects(detailed_rows, detailed_def, "Detailed table")
    if summary_def:
        _validate_or_coerce_selects(summary_rows, summary_def, "Summary table")
    p("[STEP] Schema check finished.")

    # If "Percentage Completed" is single line text, format as "xx.xx%"
    if summary_def:
        _coerce_percentage_for_schema(summary_rows, summary_def, "Percentage Completed")

    # Drop non-writable fields
    if detailed_def:
        detailed_rows = _filter_rows_to_writable(detailed_rows, detailed_def, "Detailed table")
    if summary_def:
        summary_rows  = _filter_rows_to_writable(summary_rows,  summary_def,  "Summary table")

    # Write
    p("[STEP] Writing detailed rows…")
    airtable_insert_detailed(detailed_rows)
    p("[STEP] Writing summary rows…")
    airtable_insert_summary(summary_rows)

    p("\n=== Run Summary (All Terms) ===")
    p(f"Assignments processed: {stats['processed']}")
    p(f"Assignments skipped  : {stats['skipped']} (exact-title skips)")
    p("===============================")

if __name__ == "__main__":
    try:
        p("=== START canvas_to_airtable ===")
        main()
        p("=== END OF SCRIPT (success) ===")
    except SystemExit as e:
        p(f"=== END OF SCRIPT (SystemExit code {e.code}) ===")
        raise
    except KeyboardInterrupt:
        p("=== END OF SCRIPT (KeyboardInterrupt) ===")
        raise
    except Exception:
        p("\n[FATAL] Unhandled exception:")
        traceback.print_exc()
        p("=== END OF SCRIPT (error) ===")
        raise
