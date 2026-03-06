#!/usr/bin/env python3
"""
📊 Maya Monitor v4 — ניטור דיווחי חברות בבורסת ת"א
=====================================================
Endpoints that WORK:
  - company/reports?companyId=X → דיווחים (Subject, PubDate)
  - report/company?companyId=X → same
  - company/financereports?companyId=X → דוחות כספיים

Gemini (free) + Claude (financial reports only)
"""

import asyncio, json, logging, os, re, signal, sys, hashlib
from datetime import datetime, timezone, timedelta

import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from telegram import Bot
from telegram.error import TelegramError, RetryAfter

load_dotenv()

# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
STATE_FILE = os.getenv("STATE_FILE", "/data/bot_state.json")

SCAN_MINUTES = 15
MAYA_API = "https://mayaapi.tase.co.il/api"
GEMINI_API = "https://generativelanguage.googleapis.com/v1beta"
CLAUDE_API = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL = "claude-sonnet-4-20250514"
GEMINI_MODEL = "gemini-2.0-flash"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "X-Maya-With": "allow",
    "Origin": "https://maya.tase.co.il",
    "Referer": "https://maya.tase.co.il/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
}

# ═══ RESOLVED Maya companyIds (verified by scan) ═══
MAYA_IDS = {
    "שיח מדיקל": "249",
    "טלסיס": "354",
    "צרפתי": "425",
    "גאון קבוצה": "454",
    "גן שמואל": "532",
    "ג'נריישן קפיטל": "551",
    "טכנ גילוי אש גז": "553",
    "אורד": "592",
    "עלבד": "625",
    "פלרם": "644",
    "פוליגון": "745",
    "לכיש": "826",
    "נקסט ויז'ן": "1092",
    "קווליטאו": "1093",
    "נאוויטס פטר": "1119",
    "ארן": "1122",
    "פיסיבי טכנ": "1178",
    "אלספק": "1194",
    "אסטיגי": "1213",
    "אימאג'סט": "1539",
    "הייפר גלובל": "1719",
    "גילת טלקום": "1796",
    "תאת טכנו": "2110",
    "קמטק": "2174",
    "תיגבור קבוצה": "1105",
    # רגא שרותים — not found in Maya scan
}

# Reports to SKIP (boring, routine — no AI, no notification)
SKIP_KW = [
    "החזקות בעלי עניין",
    "שינוי החזקות",
    "החזקות עניין",
    "הודעה על שיעבוד",
    "דוח חודשי",
    "הודעה בדבר כינוס אסיפה",
    "מינוי רואה חשבון",
    "אישור דוחות כספיים",
    "פרוטוקול אסיפה",
    "הזמנה לאסיפה",
    "תוצאות אסיפה",
    "צו עיכוב",
    "עיכוב הליכים",
    "הודעה על הקצאה",
    "דוח הצעת מדף",
    "נושאי משרה ליום",
    "הודעה בהתאם לתקנה",
    "עדכון תשקיף",
    "מכתב התחייבות",
]

def should_skip(title):
    """Skip boring/routine reports."""
    if not title:
        return True
    return any(kw in title for kw in SKIP_KW)

def is_interesting(title):
    """Reports that deserve full AI analysis."""
    INTERESTING_KW = [
        "עסקה", "הסכם", "רכישה", "מיזוג", "מכירה",
        "תביעה", "הנפקה", "הצעה", "אירוע", "מהותי",
        "התקשרות", "הזמנה", "פיתוח", "אישור", "שיתוף פעולה",
        "רווח", "הפסד", "גידול", "ירידה", "הכנסות",
        "חלוקת דיבידנד", "הקצאה", "תוצאות",
        "פרויקט", "זכייה", "חוזה", "ייצוא", "ייבוא",
        "אזהרת רווח", "profit warning",
    ]
    t = title if title else ""
    return any(kw in t for kw in INTERESTING_KW) or is_financial(title)

FINANCIAL_KW = [
    "דוח כספי", "דוחות כספיים", "תוצאות כספיות", "רבעון",
    "דוח תקופתי", "דוח שנתי", "מאזן", "רווח והפסד",
    "תזרים מזומנים", "דוח רבעוני", "Q1", "Q2", "Q3", "Q4",
    "quarterly", "annual", "financial",
]

# ═══════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════

logging.basicConfig(level=logging.INFO, format="%(asctime)s │ %(levelname)-7s │ %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("maya")
for q in ["httpx", "telegram", "apscheduler"]:
    logging.getLogger(q).setLevel(logging.WARNING)


# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

def now_s(): return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
def now_u(): return datetime.now(timezone.utc)
def is_financial(title): return any(k in (title or "") for k in FINANCIAL_KW)
def rhash(r):
    return hashlib.md5(f"{r.get('id','')}{r.get('title','')[:50]}{r.get('date','')}".encode()).hexdigest()[:12]


# ═══════════════════════════════════════════════════════════
# MAYA — FETCH REPORTS (working endpoints!)
# ═══════════════════════════════════════════════════════════

async def fetch_reports(session, company_name, company_id):
    """Fetch דיווחים for a company. Uses company/reports endpoint (verified working)."""
    reports = []

    # Primary: company/reports — returns dict with "Reports" list
    try:
        url = f"{MAYA_API}/company/reports?companyId={company_id}"
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=15)) as r:
            if r.status == 200:
                data = await r.json()
                items = []
                if isinstance(data, dict):
                    items = data.get("Reports", data.get("Items", []))
                elif isinstance(data, list):
                    items = data

                for item in (items if isinstance(items, list) else []):
                    rp = parse_report(item, company_name, company_id)
                    if rp:
                        reports.append(rp)
            else:
                logger.debug(f"{company_name}: company/reports HTTP {r.status}")
    except Exception as e:
        logger.debug(f"{company_name} reports: {e}")

    # Fallback: report/company
    if not reports:
        try:
            url = f"{MAYA_API}/report/company?companyId={company_id}"
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    data = await r.json()
                    items = data.get("Reports", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
                    for item in items:
                        rp = parse_report(item, company_name, company_id)
                        if rp:
                            reports.append(rp)
        except Exception as e:
            logger.debug(f"{company_name} report/company: {e}")

    return reports


def parse_report(item, company_name, company_id):
    """Parse a report item from Maya API."""
    if not item or not isinstance(item, dict):
        return None

    title = item.get("Subject") or item.get("Title") or item.get("title") or ""
    if not title.strip():
        return None

    date_str = item.get("PubDate") or item.get("PublishDate") or item.get("Date") or ""
    report_id = str(item.get("RptCode") or item.get("RptCd") or item.get("Id") or item.get("id") or "")
    form_type = item.get("searchArrange") or item.get("FormType") or item.get("type") or ""
    comment = item.get("Comment") or ""
    has_eng = item.get("HasEngRpt", False)

    url = ""
    if report_id:
        url = f"https://maya.tase.co.il/reports/details/{report_id}"

    return {
        "id": report_id,
        "title": title.strip(),
        "date": date_str,
        "form_type": str(form_type),
        "comment": comment,
        "company": company_name,
        "company_id": company_id,
        "url": url,
        "has_english": has_eng,
    }


async def fetch_report_content(session, report):
    """Get report content — handles HTM, PDF links inside HTM, and direct PDFs."""
    report_id = report.get("id", "")
    if not report_id or not report_id.isdigit():
        return ""

    rpt_num = int(report_id)
    range_start = (rpt_num // 1000) * 1000 + 1
    range_end = range_start + 999
    content = ""

    # Strategy 1: HTM file (may contain text OR a PDF link)
    htm_url = f"https://mayafiles.tase.co.il/rhtm/{range_start}-{range_end}/H{report_id}.htm"
    try:
        async with session.get(htm_url, timeout=aiohttp.ClientTimeout(total=15),
                               headers={"User-Agent": "Mozilla/5.0 Chrome/121.0.0.0"}) as r:
            if r.status == 200:
                html = await r.text()

                # Check if HTM contains PDF links
                pdf_links = re.findall(r'href=["\']([^"\']*\.pdf[^"\']*)["\']', html, re.IGNORECASE)
                # Also check for rpdf links
                pdf_links += re.findall(r'(https?://mayafiles\.tase\.co\.il/rpdf/[^"\'<>\s]+)', html)
                # Also look for relative PDF paths
                pdf_links += [f"https://mayafiles.tase.co.il{m}" for m in re.findall(r'(/rpdf/[^"\'<>\s]+\.pdf)', html)]

                if pdf_links:
                    # Download and parse the PDF
                    pdf_url = pdf_links[0]
                    if not pdf_url.startswith("http"):
                        pdf_url = f"https://mayafiles.tase.co.il{pdf_url}"
                    logger.info(f"  📎 Found PDF link in HTM: {pdf_url}")
                    pdf_text = await fetch_pdf_text(session, pdf_url)
                    if pdf_text:
                        content = pdf_text
                        logger.info(f"  ✅ PDF content: {len(content)} chars")

                # If no PDF or PDF failed, extract text from HTM itself
                if not content:
                    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
                    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
                    text = re.sub(r'<[^>]+>', ' ', text)
                    text = re.sub(r'&nbsp;', ' ', text)
                    text = re.sub(r'&[a-z]+;', '', text)
                    text = re.sub(r'\s+', ' ', text).strip()
                    if len(text) > 50:
                        content = text
                        logger.info(f"  ✅ HTM text content: {len(content)} chars")
    except Exception as e:
        logger.debug(f"  HTM fetch: {e}")

    # Strategy 2: Try direct PDF from archive
    if not content:
        for suffix in ["-00.pdf", "-01.pdf"]:
            pdf_url = f"https://mayafiles.tase.co.il/rpdf/{range_start}-{range_end}/P{report_id}{suffix}"
            pdf_text = await fetch_pdf_text(session, pdf_url)
            if pdf_text:
                content = pdf_text
                logger.info(f"  ✅ Direct PDF: {len(content)} chars from {pdf_url}")
                break

    # Strategy 3: maya.tase.co.il/api/v1/reports/{id}
    if not content:
        try:
            url = f"https://maya.tase.co.il/api/v1/reports/{report_id}"
            async with session.get(url, headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 Chrome/121.0.0.0",
                "Referer": "https://maya.tase.co.il/",
            }, timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, dict):
                        parts = []
                        for field in ["title", "body", "content", "text", "description", "comment"]:
                            val = data.get(field, "")
                            if val and len(str(val)) > 10:
                                parts.append(str(val))
                        if parts:
                            content = "\n".join(parts)
                            logger.info(f"  ✅ API v1: {len(content)} chars")
        except Exception as e:
            logger.debug(f"  API v1: {e}")

    return content[:6000]


async def fetch_pdf_text(session, pdf_url):
    """Download a PDF and extract text from it."""
    try:
        async with session.get(pdf_url, timeout=aiohttp.ClientTimeout(total=20),
                               headers={"User-Agent": "Mozilla/5.0 Chrome/121.0.0.0"}) as r:
            if r.status != 200:
                return ""
            ct = r.content_type or ""
            if "pdf" not in ct and "octet" not in ct:
                return ""
            pdf_bytes = await r.read()
            if len(pdf_bytes) < 100:
                return ""

            # Parse PDF using PyPDF2
            import io
            try:
                from PyPDF2 import PdfReader
                reader = PdfReader(io.BytesIO(pdf_bytes))
                text_parts = []
                for page in reader.pages[:30]:  # Max 30 pages
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                text = "\n".join(text_parts)
                if text and len(text) > 50:
                    return text[:6000]
            except ImportError:
                logger.error("PyPDF2 not installed!")
            except Exception as e:
                logger.debug(f"  PDF parse error: {e}")
    except Exception as e:
        logger.debug(f"  PDF download error: {e}")
    return ""


# ═══════════════════════════════════════════════════════════
# COMPANY PROFILES — read annual reports to know the company
# ═══════════════════════════════════════════════════════════

async def build_company_profiles(session):
    """Read latest financial reports for each company and build profiles."""
    profiles = {}
    logger.info("🏗️ Building company profiles from financial reports...")

    for name, cid in MAYA_IDS.items():
        try:
            # Get all reports
            reports = await fetch_reports(session, name, cid)

            # Find financial reports (דוח כספי, דוח תקופתי, דוח שנתי)
            financial = [r for r in reports if is_financial(r.get("title", ""))]
            if not financial:
                logger.info(f"  {name}: no financial reports found, trying latest 3")
                financial = reports[:3]

            # Read the content of the 2 most recent financial reports
            contents = []
            for rp in financial[:2]:
                content = await fetch_report_content(session, rp)
                if content and len(content) > 100:
                    contents.append(f"[{rp.get('title','')} — {rp.get('date','')[:10]}]\n{content[:3000]}")
                await asyncio.sleep(0.3)

            if not contents:
                logger.info(f"  {name}: no content available for profile")
                continue

            all_content = "\n\n───\n\n".join(contents)

            # Use Gemini (free) to build the profile
            if GEMINI_API_KEY:
                profile = await build_profile_gemini(session, name, all_content)
            elif ANTHROPIC_API_KEY:
                profile = await build_profile_claude(session, name, all_content)
            else:
                profile = None

            if profile:
                profiles[name] = profile
                logger.info(f"  ✅ {name}: profile built ({len(profile)} chars)")
            else:
                logger.warning(f"  ❌ {name}: profile build failed")

            await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"  {name} profile error: {e}")

    logger.info(f"Profiles built: {len(profiles)}/{len(MAYA_IDS)}")
    return profiles


async def build_profile_gemini(session, company_name, report_content):
    """Use Gemini (free) to summarize company from its financial reports."""
    prompt = f"""קרא את הדוחות הכספיים הבאים של חברת {company_name} ובנה פרופיל חברה תמציתי.

══ דוחות ══
{report_content[:5000]}

══ הנחיות ══
כתוב פרופיל חברה שאנליסט יכול להשתמש בו כרקע בזמן ניתוח דיווחים חדשים.
כלול:
1. מה החברה עושה (תחום פעילות, מוצרים/שירותים)
2. מצב כספי אחרון (הכנסות, רווח/הפסד, מגמה)
3. נתונים מרכזיים (מספר עובדים, שווקים, לקוחות עיקריים)
4. מגמות ואתגרים עיקריים
5. אירועים מהותיים אחרונים

כתוב בעברית, תמציתי, 200-400 מילים. טקסט רגיל, לא JSON."""

    try:
        url = f"{GEMINI_API}/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        async with session.post(url, json={"contents": [{"parts": [{"text": prompt}]}]},
                                timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status != 200:
                logger.debug(f"Gemini profile {r.status}")
                return None
            data = await r.json()
            candidates = data.get("candidates", [])
            if candidates:
                return candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
    except Exception as e:
        logger.debug(f"Gemini profile: {e}")
    return None


async def build_profile_claude(session, company_name, report_content):
    """Fallback: use Claude to build profile."""
    prompt = f"""קרא את הדוחות של {company_name} ובנה פרופיל חברה תמציתי (200-400 מילים).
כלול: תחום פעילות, מצב כספי, מגמות, אתגרים, אירועים מהותיים.

{report_content[:4000]}"""

    try:
        async with session.post(CLAUDE_API,
            json={"model": CLAUDE_MODEL, "max_tokens": 1000,
                  "system": "בנה פרופיל חברה תמציתי בעברית. טקסט רגיל.",
                  "messages": [{"role": "user", "content": prompt}]},
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status != 200:
                return None
            c = (await r.json()).get("content", [])
            return c[0]["text"] if c else None
    except Exception as e:
        logger.debug(f"Claude profile: {e}")
    return None


# ═══════════════════════════════════════════════════════════
# AI ANALYSIS
# ═══════════════════════════════════════════════════════════

async def gemini_analyze(session, report, content, other_reports=None, company_profile=None):
    if not GEMINI_API_KEY: return None

    other_ctx = ""
    if other_reports:
        other_ctx = "\n\nדיווחים אחרונים נוספים (כבר פורסמו!):\n" + "\n".join([f"  • {r}" for r in other_reports])

    profile_ctx = ""
    if company_profile:
        profile_ctx = f"\n\nפרופיל החברה:\n{company_profile}"

    prompt = f"""אתה אנליסט בורסאי מומחה. נתח דיווח מהבורסה.

חברה: {report['company']}
כותרת: {report['title']}
סוג: {report.get('form_type','?')}
תאריך: {report.get('date','?')}
{profile_ctx}

{f"תוכן:{chr(10)}{content[:3000]}" if content else "(אין תוכן)"}
{other_ctx}

חשוב:
- תן תוכן הדיווח + ניתוח משמעות ברמה גבוהה
- השתמש בפרופיל החברה כדי לתת הקשר נכון
- אל תציע לצפות לדיווח שכבר פורסם

JSON בלבד (בלי backticks):
{{
    "content_summary": "מה כתוב בדיווח — תוכן עובדתי מפורט. מספרים, שמות, תאריכים, עובדות (4-6 משפטים)",
    "analysis": "ניתוח משמעות ברמה גבוהה — מה זה אומר לחברה, למשקיעים, מה ההשלכות. זו הפרשנות שלך (4-6 משפטים)",
    "impact": "השפעה צפויה על מחיר המניה + הסבר (2-3 משפטים)",
    "key_numbers": "מספרים מרכזיים מהדיווח אם יש",
    "action": "המלצה למשקיע (1-2 משפטים)",
    "importance": "גבוהה/בינונית/נמוכה",
    "direction": "עלייה/ירידה/יציבות"
}}"""
    try:
        url = f"{GEMINI_API}/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        async with session.post(url, json={"contents": [{"parts": [{"text": prompt}]}]},
                                timeout=aiohttp.ClientTimeout(total=45)) as r:
            if r.status != 200:
                err = await r.text()
                logger.error(f"Gemini HTTP {r.status}: {err[:300]}")
                return None
            data = await r.json()
            candidates = data.get("candidates", [])
            if not candidates:
                logger.error(f"Gemini: no candidates. Response: {json.dumps(data, ensure_ascii=False)[:300]}")
                return None
            text = candidates[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            if not text:
                logger.error(f"Gemini: empty text. Candidate: {json.dumps(candidates[0], ensure_ascii=False)[:300]}")
                return None
            logger.info(f"  Gemini raw response: {text[:150]}...")
            result = pj(text)
            if not result:
                logger.error(f"Gemini: JSON parse failed. Text: {text[:300]}")
            return result
    except Exception as e:
        logger.error(f"Gemini error: {e}"); return None


async def claude_analyze(session, report, content, other_reports=None, company_profile=None):
    if not ANTHROPIC_API_KEY: return None

    other_ctx = ""
    if other_reports:
        other_ctx = "\n\n══ דיווחים אחרונים נוספים של אותה חברה (כבר פורסמו!) ══\n" + "\n".join([f"  • {r}" for r in other_reports])

    profile_ctx = ""
    if company_profile:
        profile_ctx = f"\n\n══ פרופיל החברה (מבוסס על דוחות כספיים אחרונים) ══\n{company_profile}"

    prompt = f"""[{now_s()}]

אתה אנליסט השקעות בכיר בבית השקעות מוביל בישראל, עם 15+ שנות ניסיון בשוק ההון הישראלי.
אתה מנתח דיווח שפורסם במערכת מאי"ה.

══ פרטי הדיווח ══
חברה: {report['company']}
כותרת: {report['title']}
סוג: {report.get('form_type','?')}
תאריך: {report.get('date','?')}
{profile_ctx}

══ תוכן הדיווח ══
{content[:4500] if content else "(אין תוכן מלא זמין)"}
{other_ctx}

══ הנחיות קריטיות ══
{"⚠️ אין תוכן מלא. אל תמציא נתונים! תן הערכה קצרה על סמך הכותרת והפרופיל." if not content else "יש לך את התוכן המלא. נתח אותו לעומק."}
⚠️ שים לב לרשימת הדיווחים האחרונים! אל תכתוב "יש לצפות לדיווח" אם הוא כבר פורסם.
⚠️ השתמש בפרופיל החברה כדי לתת ניתוח בהקשר הנכון — אתה מכיר את החברה.

══ הנחיות ══
נתח כאנליסט מקצועי ברמה הגבוהה ביותר:

1. תוכן הדיווח: תמצת את העובדות — מה בדיוק מדווח, מספרים, שמות, תאריכים, סכומים
2. ניתוח עומק: מה המשמעות האמיתית? למה זה חשוב? מה הרקע? מה ה-context?
   - נתח את ההשלכות על מצב החברה, המאזן, תזרים המזומנים
   - האם זה חד-פעמי או מגמה?
   - איך זה משתלב עם דיווחים קודמים או מגמות בענף?
3. השפעה על המניה: תן הערכה מנומקת — לא "חיובי" סתמי, אלא הסבר מעמיק
4. הסתכלות קדימה: מה לצפות בהמשך? מתי נדע יותר? מה הטריגר הבא?
5. המלצה: מה אנליסט היה אומר ללקוח שמחזיק את המניה?

דבר כאנליסט שמגיש briefing לפורטפוליו מנג'ר. היה ספציפי, ישיר, מקצועי.
אל תהיה גנרי — כל משפט צריך להוסיף ערך.

JSON בלבד:
{{
    "content_summary": "מה בדיוק כתוב בדיווח — תמצית עובדתית חדה עם כל הפרטים המהותיים (5-8 משפטים)",
    "deep_analysis": "ניתוח עומק ברמת אנליסט בכיר: משמעות, הקשר, השלכות, מגמות. זו הפרשנות המקצועית שלך (6-10 משפטים)",
    "market_impact": "השפעה צפויה על מחיר המניה — כיוון, עוצמה, טווח זמן, ומדוע (3-5 משפטים)",
    "forward_look": "מה לצפות בהמשך — אירועים, טריגרים, תאריכים חשובים (2-4 משפטים)",
    "key_numbers": "כל המספרים המהותיים מהדיווח (סכומים, אחוזים, תאריכים)",
    "bottom_line": "שורה תחתונה: מה אנליסט אומר ללקוח ב-2 משפטים",
    "importance": "גבוהה/בינונית/נמוכה",
    "direction": "עלייה/ירידה/יציבות",
    "confidence": "גבוהה/בינונית/נמוכה"
}}"""
    try:
        async with session.post(CLAUDE_API,
            json={"model": CLAUDE_MODEL, "max_tokens": 3000,
                  "system": "אתה אנליסט השקעות בכיר עם מומחיות בשוק ההון הישראלי. אתה מנתח דיווחי חברות ממערכת מאי\"ה. הניתוח שלך ברמה של בית השקעות מוביל. עברית מקצועית. JSON בלבד ללא backticks.",
                  "messages": [{"role": "user", "content": prompt}]},
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status != 200:
                err = await r.text()
                logger.error(f"Claude HTTP {r.status}: {err[:300]}")
                return None
            c = (await r.json()).get("content", [])
            return pj(c[0]["text"]) if c else None
    except Exception as e:
        logger.error(f"Claude: {e}"); return None


def pj(text):
    if not text: return {}
    try:
        c = text.strip()
        if c.startswith("```"): c = re.sub(r"```json?|```", "", c).strip()
        return json.loads(c)
    except:
        return {}


# ═══════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════

class State:
    def __init__(self):
        self.seen = []
        self.scan_count = 0
        self.gemini_today = 0
        self.claude_today = 0
        self.day = ""
        self.profiles = {}  # company_name → profile text
        self.profiles_date = ""  # when profiles were last built
        self._load()

    def _load(self):
        if os.path.exists(STATE_FILE):
            try:
                s = json.load(open(STATE_FILE))
                self.seen = s.get("seen", [])
                self.scan_count = s.get("scan_count", 0)
                self.gemini_today = s.get("gemini_today", 0)
                self.claude_today = s.get("claude_today", 0)
                self.day = s.get("day", "")
                self.profiles = s.get("profiles", {})
                self.profiles_date = s.get("profiles_date", "")
            except: pass

    def save(self):
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            json.dump({"seen": self.seen[-1000:], "scan_count": self.scan_count,
                       "gemini_today": self.gemini_today, "claude_today": self.claude_today,
                       "day": self.day, "profiles": self.profiles,
                       "profiles_date": self.profiles_date}, open(STATE_FILE, "w"))
        except Exception as e:
            logger.error(f"Save: {e}")

    def is_new(self, h): return h not in self.seen
    def mark(self, h): self.seen.append(h)
    def tick(self, engine):
        d = now_u().strftime("%Y-%m-%d")
        if d != self.day: self.gemini_today = 0; self.claude_today = 0; self.day = d
        if engine == "gemini": self.gemini_today += 1
        elif engine == "claude": self.claude_today += 1

    def needs_profiles(self):
        """Rebuild profiles weekly or if empty."""
        if not self.profiles:
            return True
        if not self.profiles_date:
            return True
        try:
            last = datetime.fromisoformat(self.profiles_date)
            return now_u() - last > timedelta(days=7)
        except:
            return True


# ═══════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════

class TG:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat = TELEGRAM_CHAT_ID

    async def send(self, text):
        try:
            for i in range(0, len(text), 4000):
                await self.bot.send_message(chat_id=self.chat, text=text[i:i+4000], disable_web_page_preview=True)
                if i + 4000 < len(text): await asyncio.sleep(0.5)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after); await self.send(text)
        except TelegramError as e:
            logger.error(f"TG: {e}")

    async def startup(self, n):
        await self.send(
            "📊 Maya Monitor v4\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🏢 חברות: {n}\n"
            f"⏰ כל {SCAN_MINUTES} דקות\n"
            f"💎 Gemini: {'✅' if GEMINI_API_KEY else '❌'}\n"
            f"🧠 Claude: {'✅' if ANTHROPIC_API_KEY else '❌'}\n\n"
            "דיווח חדש → ניתוח AI → התראה\n━━━━━━━━━━━━━━━━━━━━━━")

    async def report_alert(self, rp, ai, engine):
        imp = ai.get("importance", "—")
        ie = {"גבוהה": "🔴", "בינונית": "🟡", "נמוכה": "🟢"}.get(imp, "⚪")
        et = "🧠 Claude" if engine == "claude" else "💎 Gemini"
        d = ai.get("direction", "")
        de = "📈" if "עלייה" in d else "📉" if "ירידה" in d else "➡️"

        msg = (f"📊 דיווח חדש {ie}\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
               f"🏢 {rp['company']}\n📋 {rp['title']}\n"
               f"📅 {rp.get('date', '—')}\n"
               f"🔗 {rp.get('url', '—')}\n"
               f"{et}\n\n")

        # Content: what the report says
        if ai.get("content_summary"):
            msg += f"📄 תוכן הדיווח:\n{ai['content_summary']}\n\n"

        if ai.get("key_numbers"):
            msg += f"🔢 מספרים מרכזיים:\n{ai['key_numbers']}\n\n"

        # Deep analysis (Claude)
        if ai.get("deep_analysis"):
            msg += f"🧠 ניתוח אנליסט:\n{ai['deep_analysis']}\n\n"
        elif ai.get("analysis"):
            msg += f"🧠 ניתוח:\n{ai['analysis']}\n\n"

        # Market impact
        if ai.get("market_impact"):
            msg += f"{de} השפעה על המניה:\n{ai['market_impact']}\n\n"
        elif ai.get("impact"):
            msg += f"{de} השפעה:\n{ai['impact']}\n\n"

        # Forward look
        if ai.get("forward_look"):
            msg += f"🔮 מבט קדימה:\n{ai['forward_look']}\n\n"

        # Bottom line
        if ai.get("bottom_line"):
            msg += f"💡 שורה תחתונה:\n{ai['bottom_line']}\n\n"
        elif ai.get("action"):
            msg += f"💡 המלצה:\n{ai['action']}\n\n"

        conf = ai.get("confidence", "")
        if conf:
            msg += f"🎯 ביטחון: {conf}\n\n"

        msg += "━━━━━━━━━━━━━━━━━━━━━━"
        await self.send(msg)

    async def raw_alert(self, rp):
        await self.send(
            f"📊 דיווח חדש\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🏢 {rp['company']}\n📋 {rp['title']}\n"
            f"📅 {rp.get('date', '—')}\n🔗 {rp.get('url', '—')}\n━━━━━━━━━━━━━━━━━━━━━━")

    async def status(self, sc, g, c, new_c, total_reports):
        lst = "\n".join([f"  • {n}" for n in sorted(MAYA_IDS.keys())])
        await self.send(
            f"📊 סטטוס — {now_s()}\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"סריקות: {sc} | Gemini: {g} | Claude: {c}\n"
            f"דיווחים בסריקה: {new_c} | סה\"כ נמצאו: {total_reports}\n\n"
            f"חברות ({len(MAYA_IDS)}):\n{lst}\n━━━━━━━━━━━━━━━━━━━━━━")


# ═══════════════════════════════════════════════════════════
# MAIN SCAN
# ═══════════════════════════════════════════════════════════

state = None
tg = None

async def scan():
    global state, tg
    state.scan_count += 1
    is_first = len(state.seen) == 0
    demo_sent = False
    logger.info(f"═══ Scan #{state.scan_count} {'(FIRST — 1 demo + mark rest)' if is_first else ''} ═══")
    new_c = 0
    total_reports = 0

    try:
        async with aiohttp.ClientSession() as s:
            for company_name, company_id in MAYA_IDS.items():
                try:
                    reports = await fetch_reports(s, company_name, company_id)
                    total_reports += len(reports)
                    if reports:
                        logger.info(f"  {company_name}: {len(reports)} reports (latest: {reports[0].get('title','')[:40]})")

                    for rp in reports:
                        h = rhash(rp)
                        if not state.is_new(h):
                            continue

                        state.mark(h)

                        # Skip boring reports entirely
                        if should_skip(rp["title"]):
                            logger.debug(f"  SKIP: {rp['title'][:50]}")
                            continue

                        # First scan: send 1 demo, skip rest
                        if is_first:
                            if not demo_sent and is_interesting(rp["title"]):
                                demo_sent = True
                                logger.info(f"📋 DEMO: {company_name} — {rp['title']} (id={rp.get('id','')})")
                                # Fall through to AI analysis below
                            else:
                                continue

                        new_c += 1
                        logger.info(f"📋 NEW: {company_name} — {rp['title']} (id={rp.get('id','')})")

                        # Only run AI on interesting reports
                        if is_interesting(rp["title"]):
                            # Fetch content
                            content = await fetch_report_content(s, rp)
                            logger.info(f"  Content: {len(content)} chars")

                            # Build context: other recent reports from same company
                            other_reports = [r["title"] + " (" + r.get("date","")[:10] + ")"
                                             for r in reports if r.get("id") != rp.get("id")][:10]

                            # Get company profile
                            profile = state.profiles.get(company_name, "")

                            # Demo always uses Claude for best quality
                            if is_first and ANTHROPIC_API_KEY:
                                logger.info(f"  DEMO mode — using Claude for analyst-level analysis")
                                ai = await claude_analyze(s, rp, content, other_reports, profile)
                                state.tick("claude")
                                engine = "claude"
                            else:
                                # Normal: try Gemini first (free), Claude as fallback
                                ai = None
                                engine = "gemini"

                                if GEMINI_API_KEY:
                                    ai = await gemini_analyze(s, rp, content, other_reports, profile)
                                    state.tick("gemini")

                                if not ai and ANTHROPIC_API_KEY:
                                    logger.info(f"  Gemini failed, trying Claude...")
                                    ai = await claude_analyze(s, rp, content, other_reports, profile)
                                    state.tick("claude")
                                    engine = "claude"

                            if ai:
                                logger.info(f"  AI OK ({engine}): {list(ai.keys())}")
                                await tg.report_alert(rp, ai, engine)
                            else:
                                logger.warning(f"  AI FAILED — sending raw alert")
                                await tg.raw_alert(rp)
                        else:
                            # Not interesting enough for AI, just notify
                            await tg.raw_alert(rp)

                        await asyncio.sleep(1)

                except Exception as e:
                    logger.debug(f"{company_name}: {e}")

                await asyncio.sleep(0.3)

            # Status every ~4h
            if state.scan_count % 16 == 0:
                await tg.status(state.scan_count, state.gemini_today, state.claude_today, new_c, total_reports)

            # First scan summary
            if is_first:
                marked = len(state.seen)
                await tg.send(
                    f"✅ סריקה ראשונה הושלמה\n\n"
                    f"📊 נמצאו {total_reports} דיווחים קיימים\n"
                    f"📝 סומנו {marked} כ'נקראו'\n\n"
                    f"מעכשיו — רק דיווחים חדשים יישלחו 🔔")

            state.save()
            logger.info(f"Done — {total_reports} total, {len(state.seen)} marked, {new_c} new sent")

    except Exception as e:
        logger.exception(f"Scan: {e}")
        try: await tg.send(f"⚠️ {str(e)[:300]}")
        except: pass


async def main():
    global state, tg

    # Reset options
    if os.getenv("RESET_STATE") == "1":
        if os.path.exists(STATE_FILE):
            # Keep profiles, reset everything else
            try:
                old = json.load(open(STATE_FILE))
                saved_profiles = old.get("profiles", {})
                saved_profiles_date = old.get("profiles_date", "")
            except:
                saved_profiles, saved_profiles_date = {}, ""
            os.remove(STATE_FILE)
            logger.info("🔄 State reset (profiles preserved)")
        else:
            saved_profiles, saved_profiles_date = {}, ""
    else:
        saved_profiles, saved_profiles_date = None, None

    if os.getenv("RESET_PROFILES") == "1":
        saved_profiles, saved_profiles_date = {}, ""
        logger.info("🔄 Profiles reset — will rebuild")

    state, tg = State(), TG()

    # Restore preserved profiles after reset
    if saved_profiles is not None:
        state.profiles = saved_profiles
        state.profiles_date = saved_profiles_date
        state.save()

    state, tg = State(), TG()
    logger.info(f"📊 Starting Maya Monitor v4 — {len(MAYA_IDS)} companies, {len(state.seen)} seen")
    logger.info(f"  GEMINI_API_KEY: {'SET (' + GEMINI_API_KEY[:8] + '...)' if GEMINI_API_KEY else 'MISSING'}")
    logger.info(f"  ANTHROPIC_API_KEY: {'SET (' + ANTHROPIC_API_KEY[:8] + '...)' if ANTHROPIC_API_KEY else 'MISSING'}")
    logger.info(f"  Profiles: {len(state.profiles)}/{len(MAYA_IDS)}")

    # Build company profiles if needed (weekly refresh)
    if state.needs_profiles():
        logger.info("🏗️ Building/refreshing company profiles...")
        await tg.send("🏗️ בונה פרופילי חברות מדוחות כספיים... (פעם בשבוע, 2-3 דקות)")
        async with aiohttp.ClientSession() as s:
            profiles = await build_company_profiles(s)
            state.profiles.update(profiles)
            state.profiles_date = now_u().isoformat()
            state.save()
        await tg.send(f"✅ פרופילים נבנו: {len(state.profiles)}/{len(MAYA_IDS)} חברות")
        logger.info(f"Profiles ready: {len(state.profiles)}")
    else:
        logger.info(f"Using cached profiles ({len(state.profiles)} companies, built {state.profiles_date[:10]})")

    await tg.startup(len(MAYA_IDS))

    if "--once" in sys.argv:
        await scan(); return

    sched = AsyncIOScheduler()
    sched.add_job(scan, IntervalTrigger(minutes=SCAN_MINUTES),
                  id="scan", max_instances=1, misfire_grace_time=120)
    sched.start()
    await scan()

    stop = asyncio.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    await stop.wait()
    sched.shutdown(wait=False)


if __name__ == "__main__":
    asyncio.run(main())
