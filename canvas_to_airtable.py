import os
import time
import requests
import traceback
from typing import Dict, List, Tuple
from pyairtable import Api

# ==============================
# Config from env (PAT method)
# ==============================
BASE_URL = os.environ["CANVAS_API_URL"].rstrip("/")
CANVAS_ACCESS_TOKEN = os.environ["CANVAS_ACCESS_TOKEN"]
CANVAS_ACCOUNT_ID = os.environ.get("CANVAS_ACCOUNT_ID", "1")  # optional; used if you can read terms
DEBUG = os.environ.get("DEBUG", "0") == "1"

AIRTABLE_API_KEY = os.environ["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID = os.environ["AIRTABLE_BASE_ID"]
AIRTABLE_DETAILED_TABLE = os.environ.get("AIRTABLE_DETAILED_TABLE", "Phoenix Student Assignment Details")
AIRTABLE_SUMMARY_TABLE  = os.environ.get("AIRTABLE_SUMMARY_TABLE",  "Phoenix Christian Course Details")

# Logging
SHOW_FETCH_ASSIGNMENTS = True
SHOW_FETCH_SUBMISSIONS = True
SLEEP_BETWEEN_REQUESTS = float(os.environ.get("SLEEP_BETWEEN_REQUESTS", "0.0"))

# Skip rules (EXACT titles only; case-insensitive handled in function)
SKIP_EXACT_TITLES = {"end of unit feedback", "quarterly feedback"}

# Airtable clients
api = Api(AIRTABLE_API_KEY)
tbl_detailed = api.table(AIRTABLE_BASE_ID, AIRTABLE_DETAILED_TABLE)
tbl_summary  = api.table(AIRTABLE_BASE_ID, AIRTABLE_SUMMARY_TABLE)

def dbg(msg: str):
    if DEBUG:
        print(f"[DEBUG] {msg}")

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

def get_terms() -> List[dict]:
    """Try to fetch terms; return [] if not permitted (401/403)."""
    try:
        data = make_canvas_request(f"accounts/{CANVAS_ACCOUNT_ID}/terms").json()
        return data.get("enrollment_terms", [])
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code in (401, 403):
            print("[WARN] No permission to read /accounts/*/terms; continuing without names.")
            return []
        raise

def get_terms_map() -> Dict[int, str]:
    try:
        return {t["id"]: t["name"] for t in get_terms()}
    except Exception:
        return {}

def get_user_profile_admin(user_id: str) -> dict:
    """Admin-friendly: use masquerade to read another user's profile."""
    return make_canvas_request("users/self/profile", params={"as_user_id": user_id}).json()

def get_all_active_courses_admin(user_id: str, term_id=None) -> List[dict]:
    """Admin-friendly: list a user's courses using masquerade (users/self/courses?as_user_id=...)."""
    states = ["active", "invited_or_pending", "completed", "inactive"]
    seen = set()
    all_courses: List[dict] = []
    for es in states:
        endpoint = "users/self/courses"
        params = {
            "as_user_id": user_id,
            "enrollment_state": es,
            "per_page": 100,
        }
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
                print(f"[KEEP] assignment {a.get('id')} '{a.get('name')}' course {course_id}")
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
            print(f"Fetching submission for assignment {assignment_id} (course {course_id}) user {user_id}")
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
# Airtable helpers (verbose + retry)
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
    """Retry wrapper for Airtable rate limits/5xx with small backoff."""
    delays = [0, 1, 2, 4]  # seconds
    last_exc = None
    for d in delays:
        try:
            if d:
                time.sleep(d)
            return fn(*args, **kwargs)
        except Exception as e:
            status = _status_from_exc(e)
            if status in (429, 500, 502, 503, 504):
                print(f"[WARN] Airtable API {status}; retrying in {d}s...")
                last_exc = e
                continue
            print("[ERROR] Airtable call failed (no retry):")
            try:
                print(repr(e))
                resp = getattr(e, "response", None)
                if resp is not None:
                    print("Body:", getattr(resp, "text", "")[:2000])
            except Exception:
                pass
            raise
    if last_exc:
        raise last_exc

def delete_existing_for_students(student_names: List[str]):
    """Delete existing rows for these students in both tables (idempotent runs)."""
    if not student_names:
        print("[INFO] No students in run; nothing to delete.")
        return

    def esc(n: str) -> str:
        return n.replace('"', r'\"')

    formula = "OR(" + ",".join([f'{{Student Name}} = "{esc(n)}"' for n in student_names]) + ")"
    print(f"[INFO] Deleting existing rows for {len(student_names)} student(s)")

    def collect_ids(table, formula: str) -> List[str]:
        ids: List[str] = []
        try:
            records = _airtable_retry(table.all, formula=formula) or []
            for r in records:
                if isinstance(r, dict) and r.get("id"):
                    ids.append(r["id"])
        except Exception:
            # Fallback: iterate() may yield pages (lists) or dicts
            for chunk in table.iterate(formula=formula):
                if isinstance(chunk, dict):
                    rid = chunk.get("id")
                    if rid:
                        ids.append(rid)
                elif isinstance(chunk, list):
                    for r in chunk:
                        if isinstance(r, dict) and r.get("id"):
                            ids.append(r["id"])
        return ids

    # Detailed table deletions
    det_ids = collect_ids(tbl_detailed, formula)
    print(f"[INFO] Detailed: deleting {len(det_ids)} rows")
    for chunk in _chunks(det_ids, 50):
        _airtable_retry(tbl_detailed.batch_delete, chunk)

    # Summary table deletions
    sum_ids = collect_ids(tbl_summary, formula)
    print(f"[INFO] Summary: deleting {len(sum_ids)} rows")
    for chunk in _chunks(sum_ids, 50):
        _airtable_retry(tbl_summary.batch_delete, chunk)

def airtable_insert_detailed(rows: List[dict]):
    print(f"[INFO] Inserting {len(rows)} detailed rows")
    if not rows:
        return
    for i, chunk in enumerate(_chunks(rows, 10), start=1):
        try:
            _airtable_retry(tbl_detailed.batch_create, [{"fields": r} for r in chunk])
            dbg(f" detailed batch {i} ok ({len(chunk)})")
        except Exception:
            print(f"[ERROR] detailed batch {i} failed, first record preview:")
            try:
                print(chunk[0])
            except Exception:
                pass
            traceback.print_exc()
            raise

def airtable_insert_summary(rows: List[dict]):
    print(f"[INFO] Inserting {len(rows)} summary rows")
    if not rows:
        return
    for i, chunk in enumerate(_chunks(rows, 10), start=1):
        try:
            _airtable_retry(tbl_summary.batch_create, [{"fields": r} for r in chunk])
            dbg(f" summary batch {i} ok ({len(chunk)})")
        except Exception:
            print(f"[ERROR] summary batch {i} failed, first record preview:")
            try:
                print(chunk[0])
            except Exception:
                pass
            traceback.print_exc()
            raise

# ==============================
# Main
# ==============================
def main():
    stats = {"processed": 0, "skipped": 0}
    term_map = get_terms_map()

    if term_map:
        print("Terms discovered:")
        for tid, tname in term_map.items():
            print(f" - {tname} (ID: {tid})")
    else:
        print("[INFO] Proceeding without term names; will label as 'Term <id>'.")

    user_ids = input("Enter the student user IDs (comma-separated): ").strip().split(",")
    user_ids = [u.strip() for u in user_ids if u.strip()]

    students_data = {}
    student_names_in_run: List[str] = []

    for user_id in user_ids:
        try:
            # Admin-safe profile via masquerade:
            profile = get_user_profile_admin(user_id)
            student_name = profile.get("name", f"User {user_id}")
            student_names_in_run.append(student_name)

            # Admin-safe courses via masquerade:
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
                        "due_date": a.get("due_at"),  # ISO 8601
                        **get_submission(cid, a["id"], user_id),
                    }
                    for a in assignments
                ]

            students_data[student_name] = {"detailed_data": all_data}
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else "?"
            print(f"[ERROR] User {user_id} failed with HTTP {status}. "
                  f"Ensure your PAT is for an Account Admin with 'Masquerade' permission.")
            continue
        except Exception as e:
            print(f"[ERROR] Processing user {user_id}: {e}")
            traceback.print_exc()
            continue

    # Transform to Airtable rows
    detailed_rows: List[dict] = []
    summary_rows: List[dict] = []

    for student_name, data in students_data.items():
        for (term_name, course_name), assignments in data["detailed_data"].items():
            total = len(assignments)
            completed = sum(1 for a in assignments if a.get("submission_status") in ["graded", "excused"])
            unsubmitted = total - completed
            pct = (completed / total) if total > 0 else 0.0  # 0..1 for Percent

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
                "Percentage Completed": pct,  # Airtable Percent expects 0..1
            })

    print(f"[INFO] Built {len(detailed_rows)} detailed rows; {len(summary_rows)} summary rows.")
    print(f"[INFO] Students in run: {len(students_data)} → {list(students_data.keys())[:5]}{'...' if len(students_data)>5 else ''}")

    # Idempotent write: clear existing rows for these students, then insert fresh rows
    delete_existing_for_students(list(students_data.keys()))
    airtable_insert_detailed(detailed_rows)
    airtable_insert_summary(summary_rows)

    print("\n=== Run Summary (All Terms) ===")
    print(f"Assignments processed: {stats['processed']}")
    print(f"Assignments skipped  : {stats['skipped']} (exact-title skips)")
    print("===============================")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("\n[FATAL] Unhandled exception:")
        traceback.print_exc()
        raise
