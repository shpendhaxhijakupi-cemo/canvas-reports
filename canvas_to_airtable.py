import os
import sys
import time
import requests
import traceback
from typing import Dict, List, Tuple
from pyairtable import Api

# ==============================
# Config from env
# ==============================
BASE_URL = os.environ["CANVAS_API_URL"].rstrip("/")
CANVAS_ACCESS_TOKEN = os.environ["CANVAS_ACCESS_TOKEN"]
CANVAS_ACCOUNT_ID = os.environ.get("CANVAS_ACCOUNT_ID", "1")
DEBUG = os.environ.get("DEBUG", "0") == "1"

AIRTABLE_API_KEY = os.environ["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]

# Prefer table IDs if provided; else use names
AIRTABLE_DETAILED_TABLE_ID = os.environ.get("AIRTABLE_DETAILED_TABLE_ID")
AIRTABLE_SUMMARY_TABLE_ID  = os.environ.get("AIRTABLE_SUMMARY_TABLE_ID")
AIRTABLE_DETAILED_TABLE = os.environ.get("AIRTABLE_DETAILED_TABLE", "Phoenix Student Assignment Details")
AIRTABLE_SUMMARY_TABLE  = os.environ.get("AIRTABLE_SUMMARY_TABLE",  "Phoenix Christian Course Details")

# Logging / pacing
SHOW_FETCH_ASSIGNMENTS = True
SHOW_FETCH_SUBMISSIONS = True
SLEEP_BETWEEN_REQUESTS = float(os.environ.get("SLEEP_BETWEEN_REQUESTS", "0.0"))

# Skip rules (exact-title)
SKIP_EXACT_TITLES = {"end of unit feedback", "quarterly feedback"}

# Schema handling for select fields
ALLOW_SELECT_FALLBACK = os.environ.get("ALLOW_SELECT_FALLBACK", "0") == "1"
LOG_SCHEMA = os.environ.get("LOG_SCHEMA", "0") == "1"

# Optional tiny write probe after preflight
ENABLE_AIRTABLE_WRITE_PROBE = False

# ==============================
# tiny print helper (always flush)
# ==============================
def p(msg: str):
    print(msg, flush=True)

def dbg(msg: str):
    if DEBUG:
        p(f"[DEBUG] {msg}")

# ==============================
# Airtable clients
# ==============================
api = Api(AIRTABLE_API_KEY)

def _resolve_table(base_id: str, table_id: str | None, table_name: str):
    if table_id:
        return api.table(base_id, table_id), f"(table_id={table_id})"
    return api.table(base_id, table_name), f"(table_name='{table_name}')"

tbl_detailed, detailed_selector = _resolve_table(
    AIRTABLE_BASE_ID, AIRTABLE_DETAILED_TABLE_ID, AIRTABLE_DETAILED_TABLE
)
tbl_summary,  summary_selector  = _resolve_table(
    AIRTABLE_BASE_ID, AIRTABLE_SUMMARY_TABLE_ID,  AIRTABLE_SUMMARY_TABLE
)

def is_skippable_assignment(a: dict) -> bool:
    name = (a.get("name") or "").strip().lower()
    return name in SKIP_EXACT_TITLES

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

def get_terms():
    try:
        data = make_canvas_request(f"accounts/{CANVAS_ACCOUNT_ID}/terms").json()
        return data.get("enrollment_terms", [])
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code in (401, 403):
            p("[WARN] No permission to read /accounts/*/terms; continuing without names.")
            return []
        raise

def get_terms_map():
    try:
        return {t["id"]: t["name"] for t in get_terms()}
    except Exception:
        return {}

def get_user_profile_admin(user_id: str) -> dict:
    return make_canvas_request("users/self/profile", params={"as_user_id": user_id}).json()

def get_all_active_courses_admin(user_id: str, term_id=None) -> List[dict]:
    states = ["active", "invited_or_pending", "completed", "inactive"]
    seen = set()
    all_courses: List[dict] = []
    for es in states:
        endpoint = "users/self/courses"
        params = {"as_user_id": user_id, "enrollment_state": es, "per_page": 100}
        while endpoint:
            resp = make_canvas_request(endpoint, params=params)
            data = resp.json()
            if term_id is not None:
                data = [c for c in data if c.get("enrollment_term_id") == term_id]
            for c in data:
                cid = c.get("id")
                if cid and cid not in seen:
                    seen.add(cid)
                    all_courses.append(c)
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
    return all_courses

def get_all_assignments(course_id: int, stats: dict) -> List[dict]:
    out = []
    endpoint = f"courses/{course_id}/assignments"
    while endpoint:
        resp = make_canvas_request(endpoint)
        data = resp.json()
        for a in data:
            if not a.get("published"):
                continue
            if is_skippable_assignment(a):
                stats["skipped"] += 1
                continue
            if SHOW_FETCH_ASSIGNMENTS:
                p(f"[KEEP] assignment {a.get('id')} '{a.get('name')}' course {course_id}")
            out.append(a)
            stats["processed"] += 1
        # pagination
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
        excused = sub.get("excused", False)
        if excused:
            return {"submission_status": "excused", "grade": "Excused"}
        if state in ["graded", "submitted"] and grade not in [None, "-", ""]:
            return {"submission_status": "graded", "grade": grade}
        return {"submission_status": "unsubmitted", "grade": "N/A"}
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return {"submission_status": "unsubmitted", "grade": "N/A"}
        raise

# ==============================
# Airtable helpers (retry + preflight + schema check)
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
        p("[INFO] Airtable write probe OK.")
    except Exception:
        p("[WARN] Probe create succeeded but cleanup failed; continuing")

def _fetch_base_schema() -> Dict[str, dict]:
    """Return {table_id: table_def} and {table_name: table_def} using Airtable schema API."""
    url = f"https://api.airtable.com/v0/meta/bases/{AIRTABLE_BASE_ID}/tables"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    resp = requests.get(url, headers=headers, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    out = {}
    for t in data.get("tables", []):
        out[t["id"]] = t
        out[t["name"]] = t  # allow lookup by name too
    return out

def _single_select_options(table_def: dict):
    """Return {field_name: [option names]} for singleSelect fields."""
    opts = {}
    for f in table_def.get("fields", []):
        if f.get("type") == "singleSelect":
            choices = [c.get("name") for c in (f.get("options", {}).get("choices") or []) if c.get("name")]
            opts[f["name"]] = choices
    return opts

def _validate_or_coerce_selects(rows: List[dict], table_def: dict, table_label: str):
    """Check singleSelect fields against options. Coerce if ALLOW_SELECT_FALLBACK else exit 1."""
    if not rows:
        return
    options_map = _single_select_options(table_def)  # name -> choices list
    if LOG_SCHEMA:
        p(f"[SCHEMA] {table_label} select options: { {k: options_map[k][:10] for k in options_map} }")
    if not options_map:
        return

    seen: Dict[str, set] = {fname: set() for fname in options_map.keys()}
    for r in rows:
        for fname in options_map.keys():
            if fname in r and r[fname] is not None and r[fname] != "":
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

    # Coerce unknowns to 'Other' if present else first option
    p(f"[WARN] {table_label}: coercing unknown single-select values (ALLOW_SELECT_FALLBACK=1).")
    for fname, choices in options_map.items():
        if not choices:
            continue
        fallback = "Other" if "Other" in choices else choices[0]
        allowed = set(choices)
        for r in rows:
            if fname in r and r[fname] not in allowed:
                r[fname] = fallback

# ===== NEW: writable-field filtering (skip formula/rollup/lookup/etc.) =====
def _writable_fieldnames(table_def: dict) -> set:
    """
    Return the set of field names that are writable via the API.
    Skip computed/system fields like formula, rollup, lookup, createdTime, lastModifiedTime, autoNumber, button.
    """
    non_writable = {"formula", "rollup", "lookup", "createdTime", "lastModifiedTime", "autoNumber", "button"}
    writable = set()
    for f in table_def.get("fields", []):
        ftype = f.get("type")
        name = f.get("name")
        if not name:
            continue
        if ftype in non_writable:
            continue
        # Link fields require record IDs; we aren't setting them, so skip
        if ftype == "multipleRecordLinks":
            continue
        writable.add(name)
    return writable

def _filter_rows_to_writable(rows: List[dict], table_def: dict, label: str) -> List[dict]:
    """Return copies of rows containing only writable fields for this table."""
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
# ===== END NEW =====

def delete_existing_for_students(student_names: List[str]):
    if not student_names:
        p("[INFO] No students in run; nothing to delete.")
        return

    def esc(n: str) -> str:
        return n.replace('"', r'\"')

    formula = "OR(" + ",".join([f'{{Student Name}} = "{esc(n)}"' for n in student_names]) + ")"
    p(f"[INFO] Deleting existing rows for {len(student_names)} student(s)")

    def collect_ids(table, formula: str) -> List[str]:
        ids: List[str] = []
        try:
            records = _airtable_retry(table.all, formula=formula) or []
            for r in records:
                if isinstance(r, dict) and r.get("id"):
                    ids.append(r["id"])
        except Exception:
            for chunk in table.iterate(formula=formula):
                if isinstance(chunk, dict) and chunk.get("id"):
                    ids.append(chunk["id"])
                elif isinstance(chunk, list):
                    for r in chunk:
                        if isinstance(r, dict) and r.get("id"):
                            ids.append(r["id"])
        return ids

    det_ids = collect_ids(tbl_detailed, formula)
    p(f"[INFO] Detailed: deleting {len(det_ids)} rows")
    for chunk in _chunks(det_ids, 50):
        _airtable_retry(tbl_detailed.batch_delete, chunk)

    sum_ids = collect_ids(tbl_summary, formula)
    p(f"[INFO] Summary: deleting {len(sum_ids)} rows")
    for chunk in _chunks(sum_ids, 50):
        _airtable_retry(tbl_summary.batch_delete, chunk)

def airtable_insert_detailed(rows: List[dict]):
    p(f"[INFO] Inserting {len(rows)} detailed rows")
    if not rows:
        return
    for i, chunk in enumerate(_chunks(rows, 10), start=1):
        try:
            _airtable_retry(tbl_detailed.batch_create, chunk)  # pyairtable 2.x
            dbg(f" detailed batch {i} ok ({len(chunk)})")
        except Exception:
            p(f"[ERROR] detailed batch {i} failed, first record preview:")
            try:
                p(str(chunk[0]))
            except Exception:
                pass
            traceback.print_exc()
            raise

def airtable_insert_summary(rows: List[dict]):
    p(f"[INFO] Inserting {len(rows)} summary rows")
    if not rows:
        return
    for i, chunk in enumerate(_chunks(rows, 10), start=1):
        try:
            _airtable_retry(tbl_summary.batch_create, chunk)  # pyairtable 2.x
            dbg(f" summary batch {i} ok ({len(chunk)})")
        except Exception:
            p(f"[ERROR] summary batch {i} failed, first record preview:")
            try:
                p(str(chunk[0]))
            except Exception:
                pass
            traceback.print_exc()
            raise

# ==============================
# Main
# ==============================
def main():
    p("=== START canvas_to_airtable ===")

    _airtable_preflight()
    _airtable_write_probe()

    stats = {"processed": 0, "skipped": 0}
    term_map = get_terms_map()

    if term_map:
        p("Terms discovered:")
        for tid, tname in term_map.items():
            p(f" - {tname} (ID: {tid})")
    else:
        p("[INFO] Proceeding without term names; will label as 'Term <id>'.")

    user_ids = input("Enter the student user IDs (comma-separated): ").strip().split(",")
    user_ids = [u.strip() for u in user_ids if u.strip()]

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

                course_name = course.get("name", f"Course {cid}")
                term_id = course.get("enrollment_term_id")
                term_name = term_map.get(term_id, f"Term {term_id}" if term_id else "Unknown Term")

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
            pct = (completed / total) if total > 0 else 0.0  # If you switch the Airtable field to 0-100, change to pct*100.

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
                "Percentage Completed": pct,  # If Airtable field is Formula/Rollup/Lookup, we will drop it below.
            })

    p(f"[INFO] Built {len(detailed_rows)} detailed rows; {len(summary_rows)} summary rows.")
    p(f"[INFO] Students in run: {len(students_data)} → {list(students_data.keys())[:5]}{'...' if len(students_data)>5 else ''}")

    # ===== Schema check for single-selects =====
    p("[STEP] Fetching base schema…")
    schema = _fetch_base_schema()
    detailed_def = schema.get(AIRTABLE_DETAILED_TABLE_ID or AIRTABLE_DETAILED_TABLE)
    summary_def  = schema.get(AIRTABLE_SUMMARY_TABLE_ID  or AIRTABLE_SUMMARY_TABLE)
    if detailed_def:
        _validate_or_coerce_selects(detailed_rows, detailed_def, "Detailed table")
    if summary_def:
        _validate_or_coerce_selects(summary_rows, summary_def, "Summary table")
    p("[STEP] Schema check finished.")

    # ===== NEW: Drop non-writable fields so API writes don't 422 on formulas/rollups/lookups =====
    if detailed_def:
        detailed_rows = _filter_rows_to_writable(detailed_rows, detailed_def, "Detailed table")
    if summary_def:
        summary_rows  = _filter_rows_to_writable(summary_rows,  summary_def,  "Summary table")

    # Idempotent write
    p("[STEP] Deleting any prior rows for these students…")
    delete_existing_for_students(list(students_data.keys()))
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
