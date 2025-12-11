import os
import json
import re
import time
import hashlib
import logging
import random
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, urlencode
from urllib.request import Request, urlopen

from zoneinfo import ZoneInfo

import boto3
import botocore.exceptions

# ---------------------------------------------------
# Logging setup
# ---------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------
# Environment / configuration
# ---------------------------------------------------

GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GOOGLE_CX = os.getenv("GOOGLE_CX")

# New table for book leads
TABLE_NAME = os.getenv("TABLE_NAME", "book-leads-v1")

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

# Email / outreach configuration
SES_REGION = os.getenv("SES_REGION", "us-east-1")
FROM_EMAIL = os.getenv("FROM_EMAIL")  # must be verified in SES
REPORT_EMAIL = os.getenv("REPORT_EMAIL", FROM_EMAIL or "")
TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
TEST_RECIPIENT_EMAIL = os.getenv("TEST_RECIPIENT_EMAIL", REPORT_EMAIL or FROM_EMAIL or "")

# When real outreach should start
GO_LIVE_DATE_STR = os.getenv("GO_LIVE_DATE", "2026-01-06")

# Sending caps
DAILY_TOTAL_LIMIT = int(os.getenv("DAILY_TOTAL_LIMIT", "50"))
MAX_PER_DOMAIN_PER_DAY = int(os.getenv("MAX_PER_DOMAIN_PER_DAY", "3"))

# 3-step follow-up sequence (your choice 2B)
MAX_SEQUENCE_STEPS = int(os.getenv("MAX_SEQUENCE_STEPS", "3"))
INITIAL_FOLLOWUP_DAYS = int(os.getenv("INITIAL_FOLLOWUP_DAYS", "5"))
FOLLOWUP_INTERVAL_DAYS = int(os.getenv("FOLLOWUP_INTERVAL_DAYS", "7"))

# Email filters
ONLY_EDU_EMAILS = os.getenv("ONLY_EDU_EMAILS", "false").lower() == "true"

# Reply stats period
REPLY_REPORT_PERIOD = os.getenv("REPLY_REPORT_PERIOD", "weekly").lower()

# PDF download URL for the free resource
PDF_URL = os.getenv(
    "PDF_URL",
    "https://YOURDOMAIN.com/path/to/5-student-success-shifts.pdf",  # replace later
)

CAMPAIGN_LABEL = os.getenv("CAMPAIGN_LABEL", "BookAgents50")

EASTERN_TZ = ZoneInfo("US/Eastern")

try:
    GO_LIVE_DATE = datetime.strptime(GO_LIVE_DATE_STR, "%Y-%m-%d").date()
except ValueError:
    GO_LIVE_DATE = datetime(2026, 1, 6).date()

ses = boto3.client("ses", region_name=SES_REGION)

# ---------------------------------------------------
# 50 BOOK AGENTS – categories & queries
# ---------------------------------------------------

AGENTS = {
    # ----------------------------------------
    # CATEGORY 1 — Campus Bookstores & Retail (10)
    # ----------------------------------------
    "campus_bookstore_east_agent": {
        "search_queries": [
            '"campus bookstore" "bookstore manager" "university" "New York" site:.edu',
            '"campus bookstore" "manager" "Massachusetts" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Campus Bookstores – East Coast",
    },
    "campus_bookstore_midwest_agent": {
        "search_queries": [
            '"campus bookstore" "bookstore manager" "Illinois" site:.edu',
            '"campus bookstore" "manager" "Ohio" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Campus Bookstores – Midwest",
    },
    "campus_bookstore_west_agent": {
        "search_queries": [
            '"campus bookstore" "bookstore manager" "California" site:.edu',
            '"campus bookstore" "manager" "Washington" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Campus Bookstores – West",
    },
    "campus_bookstore_hbcu_agent": {
        "search_queries": [
            '"bookstore" "HBCU" "campus bookstore" site:.edu',
            '"campus bookstore" "historically black college" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "HBCU Campus Bookstores",
    },
    "campus_bookstore_cc_agent": {
        "search_queries": [
            '"community college bookstore" "campus bookstore" site:.edu',
            '"bookstore manager" "community college" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Community College Bookstores",
    },
    "campus_bookstore_ordering_agent": {
        "search_queries": [
            '"textbook ordering" "campus bookstore" site:.edu',
            '"bookstore" "course materials ordering" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Bookstore Ordering Departments",
    },
    "university_merch_buyer_agent": {
        "search_queries": [
            '"merchandise buyer" "university bookstore" site:.edu',
            '"university" "merchandise buyer" "campus store" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "University Merchandise Buyers",
    },
    "auxiliary_services_agent": {
        "search_queries": [
            '"auxiliary services" "bookstore" "textbooks" site:.edu',
            '"auxiliary enterprises" "campus store" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Auxiliary Services",
    },
    "campus_retail_partnership_agent": {
        "search_queries": [
            '"campus store" "retail partnerships" site:.edu',
            '"campus retail" "bookstore" "partner" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Campus Retail Partnerships",
    },
    "purchasing_office_books_agent": {
        "search_queries": [
            '"purchasing office" "books" "campus" site:.edu',
            '"procurement" "textbooks" "university" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Purchasing Office – Books & Merch",
    },

    # ----------------------------------------
    # CATEGORY 2 — Academic Success Departments (8)
    # ----------------------------------------
    "academic_success_center_agent": {
        "search_queries": [
            '"academic success center" "students" site:.edu',
            '"academic success center" "resources" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Academic Success Centers",
    },
    "learning_center_director_agent": {
        "search_queries": [
            '"learning center director" "students" site:.edu',
            '"learning center" "student success" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Learning Centers",
    },
    "tutoring_center_admin_agent": {
        "search_queries": [
            '"tutoring center" "coordinator" site:.edu',
            '"tutoring center director" "students" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Tutoring Centers",
    },
    "student_retention_coordinator_agent": {
        "search_queries": [
            '"student retention coordinator" site:.edu',
            '"director of student retention" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Student Retention Offices",
    },
    "first_year_persistence_agent": {
        "search_queries": [
            '"first-year persistence" "students" site:.edu',
            '"first year persistence program" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "First-Year Persistence Programs",
    },
    "academic_advising_director_agent": {
        "search_queries": [
            '"director of academic advising" site:.edu',
            '"academic advising center" "student success" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Academic Advising Directors",
    },
    "study_skills_program_agent": {
        "search_queries": [
            '"study skills program" "students" site:.edu',
            '"study skills workshop" "student success" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Study Skills Programs",
    },
    "si_coordinator_agent": {
        "search_queries": [
            '"supplemental instruction coordinator" site:.edu',
            '"SI coordinator" "supplemental instruction" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Supplemental Instruction (SI)",
    },

    # ----------------------------------------
    # CATEGORY 3 — TRIO & Support Programs (7)
    # ----------------------------------------
    "trio_sss_director_agent": {
        "search_queries": [
            '"TRIO Student Support Services" director site:.edu',
            '"TRIO SSS" "student support services" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "TRIO SSS Directors",
    },
    "trio_upward_bound_agent": {
        "search_queries": [
            '"TRIO Upward Bound" coordinator site:.edu',
            '"Upward Bound" "TRIO" "program director" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "TRIO Upward Bound",
    },
    "trio_eoc_director_agent": {
        "search_queries": [
            '"TRIO Educational Opportunity Center" director site:.edu',
            '"TRIO EOC" "program director" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "TRIO EOC Directors",
    },
    "trio_mcnair_agent": {
        "search_queries": [
            '"McNair Scholars Program" director site:.edu',
            '"McNair Scholars" "TRIO" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "TRIO McNair Scholars",
    },
    "gear_up_coordinator_agent": {
        "search_queries": [
            '"GEAR UP coordinator" site:.edu',
            '"GEAR UP program" "college readiness" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "GEAR UP Coordinators",
    },
    "heop_program_agent": {
        "search_queries": [
            '"HEOP program" director site:.edu',
            '"opportunity program" "HEOP" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "HEOP / Opportunity Programs",
    },
    "college_bridge_program_agent": {
        "search_queries": [
            '"college bridge program" "students" site:.edu',
            '"bridge to college" "summer bridge" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "College Bridge Programs",
    },

    # ----------------------------------------
    # CATEGORY 4 — Faculty & Classroom Use (10)
    # ----------------------------------------
    "college_success_faculty_agent": {
        "search_queries": [
            '"college success course" syllabus site:.edu',
            '"student success course" "required text" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "College Success Course Faculty",
    },
    "first_year_seminar_faculty_agent": {
        "search_queries": [
            '"first year seminar" syllabus site:.edu',
            '"FYS" "first year seminar" "course" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "First-Year Seminar Instructors",
    },
    "freshman_experience_prof_agent": {
        "search_queries": [
            '"freshman experience" course syllabus site:.edu',
            '"freshman experience seminar" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Freshman Experience Faculty",
    },
    "dev_english_instructor_agent": {
        "search_queries": [
            '"developmental English" syllabus site:.edu',
            '"developmental reading" "student success" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Developmental English Instructors",
    },
    "student_life_skills_faculty_agent": {
        "search_queries": [
            '"student life skills" course syllabus site:.edu',
            '"SLS course" "student life skills" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Student Life Skills Faculty",
    },
    "cc_success_course_director_agent": {
        "search_queries": [
            '"college success course director" site:.edu',
            '"student success" "course coordinator" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "CC Success Course Directors",
    },
    "leadership_course_professor_agent": {
        "search_queries": [
            '"student leadership course" syllabus site:.edu',
            '"leadership studies" course syllabus site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Leadership Course Professors",
    },
    "education_department_faculty_agent": {
        "search_queries": [
            '"education department" "student success" course site:.edu',
            '"education department" "first year" "course" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Education Department Faculty",
    },
    "student_development_sdv_faculty_agent": {
        "search_queries": [
            '"student development" "SDV" course syllabus site:.edu',
            '"SDV 100" "student development" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Student Development / SDV Faculty",
    },
    "learning_community_faculty_agent": {
        "search_queries": [
            '"learning community" "first year" syllabus site:.edu',
            '"learning communities" "student success" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Seminar / Learning Communities Faculty",
    },

    # ----------------------------------------
    # CATEGORY 5 — High School & Transition Programs (7)
    # ----------------------------------------
    "hs_college_career_readiness_agent": {
        "search_queries": [
            '"college and career readiness" director "high school"',
            '"college & career coordinator" "high school"',
        ],
        "max_results_per_query": 5,
        "segment": "HS College & Career Readiness",
    },
    "hs_guidance_counselor_agent": {
        "search_queries": [
            '"guidance counselor" "high school" email',
            '"school counseling office" "high school"',
        ],
        "max_results_per_query": 5,
        "segment": "High School Guidance Counselors",
    },
    "avid_coordinator_agent": {
        "search_queries": [
            '"AVID coordinator" "high school"',
            '"AVID program" "college readiness" "coordinator"',
        ],
        "max_results_per_query": 5,
        "segment": "AVID Coordinators",
    },
    "hs_leadership_advisor_agent": {
        "search_queries": [
            '"student council advisor" "high school"',
            '"leadership advisor" "ASB" "high school"',
        ],
        "max_results_per_query": 5,
        "segment": "High School Leadership Advisors",
    },
    "ub_high_school_partner_agent": {
        "search_queries": [
            '"Upward Bound" "high school partner"',
            '"Upward Bound" "target high school"',
        ],
        "max_results_per_query": 5,
        "segment": "Upward Bound HS Partners",
    },
    "freshman_academy_teacher_agent": {
        "search_queries": [
            '"freshman academy" "high school" "teacher"',
            '"ninth grade academy" "high school"',
        ],
        "max_results_per_query": 5,
        "segment": "Freshman Academy Teachers",
    },
    "cte_coordinator_agent": {
        "search_queries": [
            '"CTE coordinator" "high school"',
            '"career technical education coordinator" "school"',
        ],
        "max_results_per_query": 5,
        "segment": "CTE Coordinators",
    },

    # ----------------------------------------
    # CATEGORY 6 — Student Clubs & Organizations (5)
    # ----------------------------------------
    "student_business_club_agent": {
        "search_queries": [
            '"student business club" "advisor" site:.edu',
            '"business club" "student organization" advisor site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Student Business Clubs",
    },
    "student_entrepreneurship_club_agent": {
        "search_queries": [
            '"entrepreneurship club" "advisor" site:.edu',
            '"entrepreneurship society" "student organization" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Student Entrepreneurship Clubs",
    },
    "bsu_coordinator_agent": {
        "search_queries": [
            '"Black Student Union" advisor site:.edu',
            '"Black Student Union" "faculty advisor" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Black Student Union Coordinators",
    },
    "men_of_color_group_agent": {
        "search_queries": [
            '"men of color" "student group" advisor site:.edu',
            '"men of color initiative" "student organization" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Men of Color Student Groups",
    },
    "peer_mentor_program_agent": {
        "search_queries": [
            '"peer mentor program" coordinator site:.edu',
            '"peer mentoring program" "student success" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Peer Mentor Programs",
    },

    # ----------------------------------------
    # CATEGORY 7 — Libraries & Resource Centers (3)
    # ----------------------------------------
    "library_acquisitions_agent": {
        "search_queries": [
            '"library acquisitions" "librarian" site:.edu',
            '"acquisitions librarian" "college" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "Library Acquisitions",
    },
    "cc_library_director_agent": {
        "search_queries": [
            '"library director" "community college"',
            '"community college library" "director"',
        ],
        "max_results_per_query": 5,
        "segment": "Community College Library Directors",
    },
    "university_library_resource_agent": {
        "search_queries": [
            '"student success" "library guide" site:.edu',
            '"library" "student success resources" site:.edu',
        ],
        "max_results_per_query": 5,
        "segment": "University Library Resource Specialists",
    },
}

# ---------------------------------------------------
# Helper functions (scraping) – NO external requests
# ---------------------------------------------------

def http_get_text(url: str, params: dict | None = None, headers: dict | None = None, timeout: int = 15) -> str | None:
    """Generic HTTP GET returning decoded text using urllib."""
    try:
        if params:
            qs = urlencode(params)
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}{qs}"

        req = Request(url, headers=headers or {})
        with urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="ignore")
    except Exception as e:
        logger.warning(f"Error fetching {url}: {e}")
        return None


def google_search(query: str, num: int = 5):
    """Call Google Custom Search and return items list."""
    logger.info(f"Searching Google: {query}")

    params = {
        "key": GOOGLE_API_KEY,
        "cx": GOOGLE_CX,
        "q": query,
        "num": num,
    }

    base_url = "https://www.googleapis.com/customsearch/v1"
    text = http_get_text(base_url, params=params, headers=None, timeout=15)
    if not text:
        return []

    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding Google JSON for query={query}: {e}")
        return []

    return data.get("items", [])


def fetch_html(url: str) -> str | None:
    """Fetch raw HTML for a URL using urllib."""
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BookAgents/1.0; +https://example.com)"
    }
    return http_get_text(url, headers=headers, timeout=15)


def find_email_in_text(text: str) -> str | None:
    """Very simple email finder from raw text."""
    if not text:
        return None

    pattern = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
    matches = re.findall(pattern, text)
    if not matches:
        return None

    return matches[0]


def extract_domain(url: str) -> str:
    parsed = urlparse(url)
    return parsed.netloc


def google_search_for_domain_email(domain: str) -> str | None:
    """Fallback search on domain to find an email."""
    fallback_query = f'site:{domain} "email" "contact"'
    logger.info(f"Fallback search on domain: {domain} with query: {fallback_query}")
    try:
        items = google_search(fallback_query, num=3)
    except Exception as e:
        logger.warning(f"Error in fallback domain search for {domain}: {e}")
        return None

    for item in items:
        link = item.get("link")
        if not link:
            continue

        html = fetch_html(link)
        if not html:
            continue

        email = find_email_in_text(html)
        if email:
            return email

    return None


def make_id(url: str, agent_name: str) -> str:
    h = hashlib.sha256()
    h.update((url + "|" + agent_name).encode("utf-8"))
    return h.hexdigest()


def item_exists(item_id: str) -> bool:
    try:
        resp = table.get_item(Key={"id": item_id})
        return "Item" in resp
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error checking existence in DynamoDB for id={item_id}: {e}")
        return False


def save_to_dynamodb(item: dict, agent_name: str) -> bool:
    try:
        table.put_item(Item=item)
        return True
    except botocore.exceptions.ClientError as e:
        logger.error(f"[{agent_name}] Error saving to DynamoDB: {e}")
        return False


def run_agent(agent_name: str, context: dict | None = None) -> dict:
    """
    Generic runner for all 50 book agents.

    - De-duplicates by id
    - Only saves items that have a non-empty contact_email
    """
    if agent_name not in AGENTS:
        raise ValueError(f"Unknown agent: {agent_name}")

    cfg = AGENTS[agent_name]
    total_saved = 0

    logger.info(f"[{agent_name}] Starting run. Segment: {cfg.get('segment', '')}")

    for q in cfg["search_queries"]:
        try:
            items = google_search(q, num=cfg["max_results_per_query"])
        except Exception as e:
            logger.error(f"[{agent_name}] Error during Google search: {e}")
            continue

        for result in items:
            url = result.get("link")
            title = result.get("title")

            if not url:
                continue

            logger.info(f"[{agent_name}] Processing URL: {url}")

            item_id = make_id(url, agent_name)

            if item_exists(item_id):
                logger.info(
                    f"[{agent_name}] Skipping duplicate URL (already in table): {url}"
                )
                continue

            html = fetch_html(url)
            email = find_email_in_text(html) if html else None

            if not email:
                domain = extract_domain(url)
                logger.info(
                    f"[{agent_name}] No email on main page. Fallback search on domain: {domain}"
                )
                email = google_search_for_domain_email(domain)

            if not email:
                logger.info(
                    f"[{agent_name}] No email found even after fallback. Skipping URL: {url}"
                )
                continue

            segment = cfg.get("segment", agent_name.replace("_agent", ""))

            item = {
                "id": item_id,
                "url": url,
                "title": title or "",
                "contact_email": email,
                "source": agent_name,
                "segment": segment,
                "campaign": CAMPAIGN_LABEL,
                "scraped_at": int(time.time()),
            }

            logger.info(f"[{agent_name}] Saving item to DynamoDB: {url}")
            if save_to_dynamodb(item, agent_name):
                total_saved += 1
            else:
                logger.error(
                    f"[{agent_name}] Error processing result: failed to save item for URL {url}"
                )

    logger.info(f"[{agent_name}] Finished. Saved {total_saved} items.")
    return {
        "message": f"{agent_name} ran successfully. Saved {total_saved} items.",
        "saved": total_saved,
        "source": agent_name,
    }


def make_response(body: dict, status_code: int = 200) -> dict:
    return {
        "statusCode": status_code,
        "body": json.dumps(body),
    }

# ---------------------------------------------------
# Single scraper handler (Option 1B)
# ---------------------------------------------------

def book_scraper_handler(event, context):
    """
    EventBridge passes in:
      {"agent_name": "campus_bookstore_east_agent"}

    If agent_name is missing, this will run ALL 50 agents (manual use only).
    """
    agent_name = None
    if isinstance(event, dict):
        agent_name = event.get("agent_name") or event.get("agent")

    if agent_name:
        logger.info(f"[book_scraper_handler] Running single agent: {agent_name}")
        body = run_agent(agent_name, event)
        return make_response(body)

    # Fallback: run all 50 agents (for manual trigger/testing)
    logger.info("[book_scraper_handler] No agent_name provided, running ALL agents.")
    total_saved = 0
    per_agent = {}
    for name in AGENTS.keys():
        res = run_agent(name, event)
        total_saved += res.get("saved", 0)
        per_agent[name] = res.get("saved", 0)

    return make_response(
        {
            "message": "All book agents ran.",
            "total_saved": total_saved,
            "by_agent": per_agent,
        }
    )

# ---------------------------------------------------
# Outreach helpers (email sequence, weekdays, holidays)
# ---------------------------------------------------

def now_eastern() -> datetime:
    return datetime.now(tz=EASTERN_TZ)


def is_weekday(date_obj) -> bool:
    return date_obj.weekday() < 5  # Monday=0, Sunday=6


def nth_weekday(year: int, month: int, weekday: int, n: int):
    dt = datetime(year, month, 1, tzinfo=EASTERN_TZ)
    while dt.weekday() != weekday:
        dt += timedelta(days=1)
    dt += timedelta(days=7 * (n - 1))
    return dt.date()


def last_weekday(year: int, month: int, weekday: int):
    if month == 12:
        dt = datetime(year + 1, 1, 1, tzinfo=EASTERN_TZ) - timedelta(days=1)
    else:
        dt = datetime(year, month + 1, 1, tzinfo=EASTERN_TZ) - timedelta(days=1)
    while dt.weekday() != weekday:
        dt -= timedelta(days=1)
    return dt.date()


def observed_date(fixed_date):
    if fixed_date.weekday() == 5:  # Saturday
        return fixed_date - timedelta(days=1)
    if fixed_date.weekday() == 6:  # Sunday
        return fixed_date + timedelta(days=1)
    return fixed_date


def is_us_federal_holiday(date_obj) -> bool:
    year = date_obj.year
    holidays = set()

    new_year = observed_date(datetime(year, 1, 1, tzinfo=EASTERN_TZ).date())
    holidays.add(new_year)

    holidays.add(nth_weekday(year, 1, 0, 3))  # MLK Day
    holidays.add(nth_weekday(year, 2, 0, 3))  # Presidents' Day
    holidays.add(last_weekday(year, 5, 0))    # Memorial Day

    juneteenth = observed_date(datetime(year, 6, 19, tzinfo=EASTERN_TZ).date())
    holidays.add(juneteenth)

    independence = observed_date(datetime(year, 7, 4, tzinfo=EASTERN_TZ).date())
    holidays.add(independence)

    holidays.add(nth_weekday(year, 9, 0, 1))  # Labor Day
    holidays.add(nth_weekday(year, 10, 0, 2)) # Columbus / Indigenous Peoples' Day

    veterans = observed_date(datetime(year, 11, 11, tzinfo=EASTERN_TZ).date())
    holidays.add(veterans)

    holidays.add(nth_weekday(year, 11, 3, 4)) # Thanksgiving

    christmas = observed_date(datetime(year, 12, 25, tzinfo=EASTERN_TZ).date())
    holidays.add(christmas)

    return date_obj in holidays


def scan_all_items():
    items = []
    resp = table.scan()
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))
    return items


def parse_timestamp(value):
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=EASTERN_TZ)
        if isinstance(value, str):
            v = value
            if v.endswith("Z"):
                v = v[:-1] + "+00:00"
            return datetime.fromisoformat(v).astimezone(EASTERN_TZ)
    except Exception:
        return None
    return None


def send_ses_email(to_email: str, subject: str, body: str) -> bool:
    if not FROM_EMAIL:
        logger.error("FROM_EMAIL is not configured.")
        return False
    try:
        ses.send_email(
            Source=FROM_EMAIL,
            Destination={"ToAddresses": [to_email]},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {"Text": {"Data": body, "Charset": "UTF-8"}},
            },
        )
        logger.info(f"Sent SES email to {to_email} with subject '{subject}'")
        return True
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error sending SES email to {to_email}: {e}")
        return False

# ---------------------------------------------------
# 3-step book outreach email sequence (Option 2B)
# ---------------------------------------------------

def compose_book_outreach_email(step_index: int):
    """
    step_index:
      0 = first email
      1 = 2nd (follow-up)
      2+ = 3rd (last)
    """
    if step_index <= 0:
        subject = "Free student success resource for your students"
        body = (
            "Good morning,\n\n"
            "I hope you’re doing well.\n\n"
            "I wanted to share a short, free resource I created called\n"
            "\"5 Student Success Shifts: Simple Mindset Moves that Help Students Stay, Succeed, and Graduate.\"\n\n"
            "Many colleges, TRIO programs, and high schools use it as a quick PDF to support\n"
            "first-year students, bridge programs, and student success courses.\n\n"
            f"You can download it here: {PDF_URL}\n\n"
            "If you ever look for short, student-friendly books or resources to support\n"
            "college success, retention, or leadership, I’d be happy to help.\n\n"
            "Warm regards,\n"
            "Tawan Perry\n"
        )
    elif step_index == 1:
        subject = "Checking in about the student success PDF"
        body = (
            "Hello,\n\n"
            "I’m just checking in to see if you had a chance to look at the\n"
            "\"5 Student Success Shifts\" PDF I shared.\n\n"
            "It’s designed so you can easily:\n"
            "- Share it in first-year seminars or college success courses\n"
            "- Use it with TRIO/GEAR UP/bridge programs\n"
            "- Offer it as a quick pre-read before a workshop or orientation\n\n"
            f"Here’s the link again: {PDF_URL}\n\n"
            "If you’d like sample chapters or info on using one of my books as a\n"
            "course or program text, I’d be glad to send that along.\n\n"
            "Best,\n"
            "Tawan\n"
        )
    else:
        subject = "Last quick note about a free resource for your students"
        body = (
            "Good afternoon,\n\n"
            "I promise this is my last quick note about the student success PDF.\n\n"
            "If now isn’t a good time, no worries at all. I just wanted to make sure\n"
            "you had the link in case it could support your students:\n\n"
            f"{PDF_URL}\n\n"
            "Whether you serve first-year students, TRIO participants, or high school\n"
            "college readiness programs, I’m always glad to share practical resources\n"
            "and discuss options for course- or program-wide book use.\n\n"
            "Thank you for the work you do for students.\n\n"
            "Warmly,\n"
            "Tawan\n"
        )

    return subject, body


def update_sequence_metadata(item_id: str, item: dict, new_step: int, sent_at_ts: int,
                             subject: str, body: str):
    update_expr_parts = [
        "sequence_step = :step",
        "last_email_sent_at = :last",
        "last_email_subject = :subj",
        "last_email_body = :body",
    ]
    expr_values = {
        ":step": new_step,
        ":last": sent_at_ts,
        ":subj": subject,
        ":body": body,
    }

    if "first_email_sent_at" not in item:
        update_expr_parts.append("first_email_sent_at = :first")
        expr_values[":first"] = sent_at_ts

    if new_step >= MAX_SEQUENCE_STEPS:
        update_expr_parts.append("sequence_completed = :completed")
        expr_values[":completed"] = True

    update_expr = "SET " + ", ".join(update_expr_parts)

    try:
        table.update_item(
            Key={"id": item_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values,
        )
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error updating sequence metadata for id={item_id}: {e}")


def build_daily_summary(today, sent_total, sent_by_step, sent_by_domain, details):
    lines = []
    lines.append(f"Date: {today.isoformat()} (US/Eastern)")
    lines.append("")
    lines.append(f"Campaign: {CAMPAIGN_LABEL}")
    lines.append(f"Total outreach emails sent: {sent_total}")
    lines.append("")

    if sent_by_step:
        lines.append("By sequence step:")
        for step, count in sorted(sent_by_step.items()):
            label = f"Email #{step}"
            lines.append(f"  {label}: {count}")
        lines.append("")

    if sent_by_domain:
        lines.append("By domain (top 15):")
        for domain, count in sorted(
            sent_by_domain.items(), key=lambda kv: kv[1], reverse=True
        )[:15]:
            lines.append(f"  {domain}: {count}")
        lines.append("")

    if details:
        lines.append("Sample of emails sent today (up to 10):")
        for d in details[:10]:
            lines.append(
                f"- {d['email']} | step {d['step']} | segment={d.get('segment','')} | {d['title'][:120]}"
            )
        lines.append("")

    return "\n".join(lines)

# ---------------------------------------------------
# Book daily outreach handler (3-step sequence)
# ---------------------------------------------------

def book_daily_outreach_handler(event, context):
    """
    Trigger at ~8:30 AM Eastern, Mon–Fri.

    - Uses the 3-step book/PDF sequence
    - Sends up to DAILY_TOTAL_LIMIT emails (default 50)
    - Respects MAX_PER_DOMAIN_PER_DAY
    """
    now = now_eastern()
    today = now.date()

    logger.info(f"[book_daily_outreach_handler] Invoked at {now.isoformat()}")

    if not is_weekday(today):
        logger.info("Today is weekend. Skipping outreach.")
        return make_response({"message": "Weekend. No outreach sent."})

    if is_us_federal_holiday(today):
        logger.info("Today is a US federal holiday. Skipping outreach.")
        return make_response({"message": "US federal holiday. No outreach sent."})

    # Test mode: one sample email only to you
    if TEST_MODE:
        logger.info("TEST_MODE enabled. Sending sample email to test recipient only.")
        if not TEST_RECIPIENT_EMAIL:
            logger.error("TEST_RECIPIENT_EMAIL / REPORT_EMAIL not configured.")
            return make_response(
                {"message": "TEST_MODE but no test recipient configured."}, 500
            )

        subject, body = compose_book_outreach_email(step_index=0)
        ok = send_ses_email(TEST_RECIPIENT_EMAIL, subject, body)
        status = "sent" if ok else "failed"
        return make_response(
            {
                "message": f"TEST_MODE: {status} sample email.",
                "to": TEST_RECIPIENT_EMAIL,
            }
        )

    if today < GO_LIVE_DATE:
        logger.info(
            f"Today ({today}) is before GO_LIVE_DATE ({GO_LIVE_DATE}). Skipping outreach."
        )
        return make_response(
            {"message": "Before GO_LIVE_DATE. No outreach sent yet."}
        )

    items = scan_all_items()
    logger.info(f"Scanned table, found {len(items)} items.")

    sent_total = 0
    sent_by_domain: dict[str, int] = {}
    sent_by_step: dict[int, int] = {}
    details = []

    for item in items:
        if sent_total >= DAILY_TOTAL_LIMIT:
            break

        email = item.get("contact_email")
        if not email:
            continue

        email = email.strip()
        if "@" not in email:
            continue

        domain = email.split("@")[-1].lower()

        if ONLY_EDU_EMAILS and not domain.endswith(".edu"):
            # You may want to set ONLY_EDU_EMAILS=false so HS / orgs are included
            continue

        if sent_by_domain.get(domain, 0) >= MAX_PER_DOMAIN_PER_DAY:
            continue

        if item.get("do_not_contact") or item.get("stop_sequence"):
            continue
        if item.get("sequence_completed"):
            continue
        if item.get("bounce_detected"):
            continue

        seq_step = int(item.get("sequence_step", 0))

        if seq_step >= MAX_SEQUENCE_STEPS:
            continue

        first_sent = parse_timestamp(item.get("first_email_sent_at"))
        if first_sent:
            days_since_first = (today - first_sent.date()).days
            if days_since_first > 30:
                # 30-day rolloff
                continue

        # Decide if due today
        if seq_step == 0:
            due = True
        else:
            last_sent = parse_timestamp(item.get("last_email_sent_at"))
            if not last_sent:
                continue
            days_since_last = (today - last_sent.date()).days
            if seq_step == 1:
                due = days_since_last >= INITIAL_FOLLOWUP_DAYS
            else:
                due = days_since_last >= FOLLOWUP_INTERVAL_DAYS

        if not due:
            continue

        if sent_total >= DAILY_TOTAL_LIMIT:
            break
        if sent_by_domain.get(domain, 0) >= MAX_PER_DOMAIN_PER_DAY:
            continue

        subject, body = compose_book_outreach_email(seq_step)

        # small delay
        time.sleep(random.uniform(1, 3))

        if send_ses_email(email, subject, body):
            sent_total += 1
            sent_by_domain[domain] = sent_by_domain.get(domain, 0) + 1

            step_number = seq_step + 1
            sent_by_step[step_number] = sent_by_step.get(step_number, 0) + 1

            now_ts = int(now.timestamp())
            update_sequence_metadata(item["id"], item, seq_step + 1, now_ts, subject, body)

            details.append(
                {
                    "id": item["id"],
                    "email": email,
                    "domain": domain,
                    "step": step_number,
                    "title": item.get("title", ""),
                    "segment": item.get("segment", ""),
                }
            )

    summary_text = build_daily_summary(today, sent_total, sent_by_step, sent_by_domain, details)

    if REPORT_EMAIL:
        send_ses_email(
            REPORT_EMAIL,
            f"Daily Book Outreach Summary – {today.isoformat()}",
            summary_text,
        )

    return make_response(
        {
            "message": "Daily book outreach completed.",
            "sent_total": sent_total,
            "by_step": sent_by_step,
            "by_domain": sent_by_domain,
        }
    )

# ---------------------------------------------------
# Reply stats report handler (weekly / monthly)
# ---------------------------------------------------

def book_reply_stats_report_handler(event, context):
    """
    Generate a weekly or monthly stats report based on
    manually_replied = true and manually_replied_at timestamps.
    """
    now = now_eastern()
    today = now.date()

    if REPLY_REPORT_PERIOD == "monthly":
        window_days = 30
    else:
        window_days = 7

    start_date = today - timedelta(days=window_days)

    logger.info(
        f"[book_reply_stats_report_handler] Generating {REPLY_REPORT_PERIOD} report "
        f"for replies between {start_date} and {today}"
    )

    items = scan_all_items()
    total_replies = 0
    replies_by_source: dict[str, int] = {}
    replies_by_step: dict[int, int] = {}
    fast_replies = 0  # <= 2 days from first email

    for item in items:
        if not item.get("manually_replied"):
            continue

        replied_at = parse_timestamp(item.get("manually_replied_at"))
        if not replied_at:
            continue

        replied_date = replied_at.date()
        if not (start_date <= replied_date <= today):
            continue

        total_replies += 1

        source = item.get("source", "unknown")
        replies_by_source[source] = replies_by_source.get(source, 0) + 1

        step = int(item.get("sequence_step", 0))
        replies_by_step[step] = replies_by_step.get(step, 0) + 1

        first_sent = parse_timestamp(item.get("first_email_sent_at"))
        if first_sent:
            days_to_reply = (replied_date - first_sent.date()).days
            if days_to_reply <= 2:
                fast_replies += 1

    lines = []
    lines.append(
        f"Reply stats report ({REPLY_REPORT_PERIOD}) – {today.isoformat()} (US/Eastern)"
    )
    lines.append("")
    lines.append(f"Campaign: {CAMPAIGN_LABEL}")
    lines.append(f"Window: last {window_days} days ({start_date} to {today})")
    lines.append("")
    lines.append(f"Total replies recorded: {total_replies}")
    lines.append(f"Fast replies (<= 2 days from first email): {fast_replies}")
    lines.append("")

    if replies_by_source:
        lines.append("Replies by agent/source (top 15):")
        for src, count in sorted(
            replies_by_source.items(), key=lambda kv: kv[1], reverse=True
        )[:15]:
            lines.append(f"  {src}: {count}")
        lines.append("")

    if replies_by_step:
        lines.append("Replies by sequence email number:")
        for step, count in sorted(replies_by_step.items()):
            label = f"Email #{step}" if step > 0 else "Email #0 (pre-sequence?)"
            lines.append(f"  {label}: {count}")
        lines.append("")

    report_body = "\n".join(lines)

    if REPORT_EMAIL:
        send_ses_email(
            REPORT_EMAIL,
            f"Book Outreach Reply Stats – {today.isoformat()}",
            report_body,
        )

    return make_response(
        {
            "message": "Reply stats report generated.",
            "total_replies": total_replies,
            "window_days": window_days,
        }
    )
