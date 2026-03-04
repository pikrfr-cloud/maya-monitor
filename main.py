#!/usr/bin/env python3
"""
📊 Maya Monitor — ניטור דיווחי חברות בבורסת ת"א
==================================================
סורק את מערכת מאי"ה לדיווחים חדשים של חברות נבחרות.
ניתוח ב-Gemini (חינם) + Claude (לדוחות כספיים).
התראות לטלגרם בעברית.
"""

import asyncio, json, logging, os, re, signal, sys, hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional

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

SCAN_MINUTES = 15  # Check Maya every 15 min
MAYA_API = "https://mayaapi.tase.co.il/api"
GEMINI_API = "https://generativelanguage.googleapis.com/v1beta"
CLAUDE_API = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL = "claude-sonnet-4-20250514"
GEMINI_MODEL = "gemini-2.0-flash"

# Companies to track: name → security_id
COMPANIES = {
    "שיח מדיקל": "249011",
    "טלסיס": "354019",
    "צרפתי": "425017",
    "גאון קבוצה": "454017",
    "גן שמואל": "532010",
    "אסטיגי": "550012",
    "עלבד": "625012",
    "פלרם": "644013",
    "פוליגון": "745018",
    "לכיש": "826016",
    "גילת טלקום": "1080597",
    "תאת טכנו": "1082726",
    "קווליטאו": "1083955",
    "ארן": "1085265",
    "אלספק": "1090364",
    "פיסיבי טכנ": "1091685",
    "קמטק": "1095264",
    "אורד": "1104496",
    "תיגבור קבוצה": "1105022",
    "נאוויטס פטר": "1141969",
    "ג'נריישן קפיטל": "1156926",
    "טכנ גילוי אש גז": "1165307",
    "נקסט ויז'ן": "1176593",
    "אימאג'סט": "1183813",
    "הייפר גלובל": "1184985",
    "רגא שרותים": "1226885",
}

# Reverse map: security_id → name
ID_TO_NAME = {v: k for k, v in COMPANIES.items()}

# Report types that are "financial" → use Claude for deep analysis
FINANCIAL_KEYWORDS = [
    "דוח כספי", "דוחות כספיים", "תוצאות כספיות", "רבעון",
    "דוח תקופתי", "דוח שנתי", "מאזן", "רווח והפסד",
    "תזרים מזומנים", "דוח רבעוני", "Q1", "Q2", "Q3", "Q4",
    "annual report", "financial statement", "quarterly",
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

def is_financial_report(title: str, form_type: str = "") -> bool:
    text = f"{title} {form_type}".lower()
    return any(kw.lower() in text for kw in FINANCIAL_KEYWORDS)

def report_hash(report: dict) -> str:
    """Create unique hash for a report to track seen reports."""
    key = f"{report.get('id', '')}{report.get('title', '')}{report.get('date', '')}"
    return hashlib.md5(key.encode()).hexdigest()[:12]

MAYA_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://maya.tase.co.il",
    "Referer": "https://maya.tase.co.il/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
}


# ═══════════════════════════════════════════════════════════
# MAYA DATA FETCHING
# ═══════════════════════════════════════════════════════════

async def fetch_company_reports(session: aiohttp.ClientSession, security_id: str) -> list[dict]:
    """Fetch recent reports from Maya for a company."""
    reports = []

    # Strategy 1: Maya API - company reports (try with security_id as company_id)
    for endpoint in [
        f"{MAYA_API}/company/financereports?companyId={security_id}",
        f"{MAYA_API}/company/alldetails?companyId={security_id}",
    ]:
        try:
            async with session.get(endpoint, headers=MAYA_HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, list):
                        for item in data:
                            reports.append(parse_maya_report(item, security_id))
                    elif isinstance(data, dict):
                        # alldetails returns nested data
                        for key in ["CompanyReportsList", "Reports", "reports", "Items"]:
                            if key in data and isinstance(data[key], list):
                                for item in data[key]:
                                    reports.append(parse_maya_report(item, security_id))
                    break  # Success, no need to try other endpoints
        except Exception as e:
            logger.debug(f"Maya API {endpoint}: {e}")

    # Strategy 2: Maya report page scraping (HTML)
    if not reports:
        try:
            url = f"https://maya.tase.co.il/reports/company/{security_id}"
            async with session.get(url, headers=MAYA_HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200:
                    html = await r.text()
                    reports = parse_maya_html(html, security_id)
        except Exception as e:
            logger.debug(f"Maya HTML: {e}")

    # Strategy 3: TASE services API
    if not reports:
        try:
            padded = security_id.zfill(9)
            url = f"https://servicesm.tase.co.il/taseinterfaces/api/CompanyReports/?companyId={padded}"
            async with session.get(url, headers=MAYA_HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, list):
                        for item in data:
                            reports.append(parse_maya_report(item, security_id))
        except Exception as e:
            logger.debug(f"TASE services: {e}")

    # Strategy 4: Direct Maya filings RSS/search
    if not reports:
        try:
            company_name = ID_TO_NAME.get(security_id, "")
            if company_name:
                search_url = f"{MAYA_API}/report/search"
                payload = {
                    "CompanyName": company_name,
                    "DateFrom": (now_u() - timedelta(days=7)).strftime("%Y-%m-%dT00:00:00"),
                    "DateTo": now_u().strftime("%Y-%m-%dT23:59:59"),
                    "Page": 1,
                    "PageSize": 10,
                }
                async with session.post(search_url, json=payload, headers=MAYA_HEADERS,
                                        timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status == 200:
                        data = await r.json()
                        items = data if isinstance(data, list) else data.get("Reports", data.get("Items", []))
                        for item in (items if isinstance(items, list) else []):
                            reports.append(parse_maya_report(item, security_id))
        except Exception as e:
            logger.debug(f"Maya search: {e}")

    # Filter out empty/invalid
    reports = [r for r in reports if r and r.get("title")]
    return reports


def parse_maya_report(item: dict, security_id: str) -> dict:
    """Normalize a report item from any Maya API format."""
    if not item or not isinstance(item, dict):
        return {}

    # Try various field names Maya uses
    title = (item.get("Title") or item.get("title") or
             item.get("Subject") or item.get("subject") or
             item.get("Header") or item.get("header") or
             item.get("Description") or "")

    date_str = (item.get("PublishDate") or item.get("publishDate") or
                item.get("Date") or item.get("date") or
                item.get("ReleaseDate") or item.get("releaseDate") or "")

    report_id = str(item.get("Id") or item.get("id") or
                    item.get("ReportId") or item.get("reportId") or "")

    form_type = (item.get("FormType") or item.get("formType") or
                 item.get("Type") or item.get("type") or
                 item.get("Category") or item.get("category") or "")

    # Build URL to the actual report
    report_url = ""
    if report_id:
        report_url = f"https://maya.tase.co.il/reports/details/{report_id}"
    url_field = item.get("Url") or item.get("url") or item.get("Link") or ""
    if url_field:
        report_url = url_field

    # PDF link
    pdf_url = ""
    if item.get("PdfUrl") or item.get("pdfUrl"):
        pdf_url = item.get("PdfUrl") or item.get("pdfUrl")
    elif report_id:
        # Common Maya PDF pattern
        pdf_url = f"https://mayafiles.tase.co.il/rpdf/{report_id}.pdf"

    return {
        "id": report_id,
        "title": title.strip(),
        "date": date_str,
        "form_type": form_type,
        "security_id": security_id,
        "company": ID_TO_NAME.get(security_id, security_id),
        "url": report_url,
        "pdf_url": pdf_url,
        "raw": {k: str(v)[:200] for k, v in item.items() if v} if item else {},
    }


def parse_maya_html(html: str, security_id: str) -> list[dict]:
    """Extract reports from Maya HTML page (fallback)."""
    reports = []
    # Maya SPA might not render, but try to extract any JSON data embedded
    json_match = re.findall(r'window\.__INITIAL_STATE__\s*=\s*({.*?});', html, re.DOTALL)
    if json_match:
        try:
            data = json.loads(json_match[0])
            for key in ["reports", "Reports", "items", "Items"]:
                if key in data and isinstance(data[key], list):
                    for item in data[key]:
                        reports.append(parse_maya_report(item, security_id))
        except Exception:
            pass
    return reports


async def fetch_report_content(session: aiohttp.ClientSession, report: dict) -> str:
    """Try to fetch the actual report text content."""
    content = ""

    # Try the report details page
    if report.get("url"):
        try:
            async with session.get(report["url"], headers=MAYA_HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200:
                    html = await r.text()
                    # Strip HTML tags for text content
                    text = re.sub(r'<[^>]+>', ' ', html)
                    text = re.sub(r'\s+', ' ', text).strip()
                    if len(text) > 100:
                        content = text[:5000]
        except Exception:
            pass

    # Try report details API
    if not content and report.get("id"):
        try:
            url = f"{MAYA_API}/report/details/{report['id']}"
            async with session.get(url, headers=MAYA_HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=20)) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, dict):
                        body = (data.get("Body") or data.get("body") or
                                data.get("Content") or data.get("content") or
                                data.get("Text") or data.get("text") or "")
                        if body:
                            text = re.sub(r'<[^>]+>', ' ', str(body))
                            content = re.sub(r'\s+', ' ', text).strip()[:5000]
        except Exception:
            pass

    return content


# ═══════════════════════════════════════════════════════════
# AI ANALYSIS
# ═══════════════════════════════════════════════════════════

async def gemini_analyze(session: aiohttp.ClientSession, report: dict, content: str) -> Optional[dict]:
    """Free Gemini analysis for routine reports."""
    if not GEMINI_API_KEY:
        return None

    company = report.get("company", "")
    title = report.get("title", "")

    prompt = f"""אתה אנליסט בורסאי מומחה. נתח את הדיווח הבא מהבורסה לניירות ערך בתל אביב.

חברה: {company}
כותרת הדיווח: {title}
סוג: {report.get('form_type', 'לא ידוע')}
תאריך: {report.get('date', 'לא ידוע')}

תוכן הדיווח:
{content[:3000] if content else "(אין תוכן זמין — נתח על סמך הכותרת בלבד)"}

ענה ב-JSON בלבד (בלי backticks):
{{
    "summary": "סיכום תמציתי של הדיווח (3-5 משפטים)",
    "impact": "השפעה צפויה על מחיר המניה: חיובי/שלילי/ניטרלי + הסבר (2 משפטים)",
    "key_numbers": "מספרים מרכזיים מהדיווח אם יש (הכנסות, רווח, הפסד וכו')",
    "action": "מה כדאי למשקיע לעשות (משפט אחד)",
    "importance": "גבוהה" או "בינונית" או "נמוכה",
    "tags": ["תג1", "תג2"]
}}"""

    try:
        url = f"{GEMINI_API}/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        payload = {"contents": [{"parts": [{"text": prompt}]}]}
        async with session.post(url, json=payload,
                                timeout=aiohttp.ClientTimeout(total=45)) as r:
            if r.status != 200:
                err = await r.text()
                logger.error(f"Gemini {r.status}: {err[:200]}")
                return None
            data = await r.json()
            text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
            return parse_json(text)
    except Exception as e:
        logger.error(f"Gemini: {e}")
    return None


async def claude_deep_analyze(session: aiohttp.ClientSession, report: dict, content: str) -> Optional[dict]:
    """Claude deep analysis for financial reports."""
    if not ANTHROPIC_API_KEY:
        return None

    company = report.get("company", "")
    title = report.get("title", "")

    prompt = f"""[{now_s()}]

אתה אנליסט פיננסי בכיר. בצע ניתוח מעמיק של הדוח הכספי הבא.

חברה: {company}
כותרת: {title}
סוג: {report.get('form_type', 'לא ידוע')}
תאריך: {report.get('date', 'לא ידוע')}

תוכן הדוח:
{content[:4000] if content else "(אין תוכן זמין — נתח על סמך הכותרת)"}

בצע ניתוח מעמיק. ענה ב-JSON:
{{
    "summary": "סיכום מנהלים (4-6 משפטים)",
    "financials": {{
        "revenue": "הכנסות (אם זמין)",
        "profit_loss": "רווח/הפסד (אם זמין)",
        "margins": "שיעורי רווחיות (אם זמין)",
        "cash_flow": "תזרים מזומנים (אם זמין)",
        "debt": "חוב (אם זמין)",
        "comparison": "השוואה לתקופה קודמת (אם זמין)"
    }},
    "strengths": ["חוזקה 1", "חוזקה 2"],
    "risks": ["סיכון 1", "סיכון 2"],
    "impact": "השפעה צפויה על המניה + הסבר מפורט (3-4 משפטים)",
    "target_direction": "עלייה/ירידה/יציבות",
    "action": "המלצה למשקיע (2-3 משפטים)",
    "importance": "גבוהה/בינונית/נמוכה",
    "confidence": "גבוהה/בינונית/נמוכה"
}}"""

    try:
        async with session.post(CLAUDE_API,
            json={"model": CLAUDE_MODEL, "max_tokens": 2500,
                  "system": "אתה אנליסט פיננסי בכיר. עברית בלבד. JSON בלבד ללא backticks.",
                  "messages": [{"role": "user", "content": prompt}]},
            headers={"x-api-key": ANTHROPIC_API_KEY, "anthropic-version": "2023-06-01",
                     "content-type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status != 200:
                logger.error(f"Claude {r.status}: {(await r.text())[:200]}")
                return None
            c = (await r.json()).get("content", [])
            text = c[0]["text"] if c and c[0].get("type") == "text" else ""
            return parse_json(text)
    except Exception as e:
        logger.error(f"Claude: {e}")
    return None


def parse_json(text):
    if not text: return {}
    try:
        c = text.strip()
        if c.startswith("```"): c = re.sub(r"```json?|```", "", c).strip()
        return json.loads(c)
    except: return {}


# ═══════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════

class State:
    def __init__(self):
        self.seen_reports = []  # List of report hashes
        self.scan_count = 0
        self.gemini_today = 0
        self.claude_today = 0
        self.day = ""
        self.working_ids = {}   # security_id → True/False (API works with this ID)
        self._load()

    def _load(self):
        if os.path.exists(STATE_FILE):
            try:
                s = json.load(open(STATE_FILE))
                self.seen_reports = s.get("seen_reports", [])
                self.scan_count = s.get("scan_count", 0)
                self.gemini_today = s.get("gemini_today", 0)
                self.claude_today = s.get("claude_today", 0)
                self.day = s.get("day", "")
                self.working_ids = s.get("working_ids", {})
                logger.info(f"State: scan#{self.scan_count}, seen {len(self.seen_reports)} reports")
            except Exception as e:
                logger.warning(f"State: {e}")

    def save(self):
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            json.dump({
                "seen_reports": self.seen_reports[-500:],
                "scan_count": self.scan_count,
                "gemini_today": self.gemini_today,
                "claude_today": self.claude_today,
                "day": self.day,
                "working_ids": self.working_ids,
            }, open(STATE_FILE, "w"))
        except Exception as e:
            logger.error(f"Save: {e}")

    def is_new(self, h: str) -> bool:
        return h not in self.seen_reports

    def mark_seen(self, h: str):
        self.seen_reports.append(h)

    def tick_ai(self, engine: str):
        d = now_u().strftime("%Y-%m-%d")
        if d != self.day:
            self.gemini_today = 0
            self.claude_today = 0
            self.day = d
        if engine == "gemini":
            self.gemini_today += 1
        elif engine == "claude":
            self.claude_today += 1


# ═══════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════

class TG:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat = TELEGRAM_CHAT_ID

    async def send(self, text):
        try:
            if len(text) > 4000:
                for i in range(0, len(text), 4000):
                    await self.bot.send_message(chat_id=self.chat, text=text[i:i+4000],
                                                disable_web_page_preview=True)
                    await asyncio.sleep(0.5)
                return
            await self.bot.send_message(chat_id=self.chat, text=text, disable_web_page_preview=True)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after); await self.send(text)
        except TelegramError as e:
            logger.error(f"TG: {e}")

    async def startup(self, n_companies):
        await self.send(
            "📊 Maya Monitor — ניטור דיווחי חברות\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🏢 חברות במעקב: {n_companies}\n"
            f"⏰ סריקה: כל {SCAN_MINUTES} דקות\n"
            f"🧠 Gemini: {'✅' if GEMINI_API_KEY else '❌'} (חינם)\n"
            f"🧠 Claude: {'✅' if ANTHROPIC_API_KEY else '❌'} (לדוחות כספיים)\n\n"
            "דיווח חדש → ניתוח AI → התראה לטלגרם\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        )

    async def report_alert(self, report: dict, ai: dict, engine: str):
        company = report.get("company", "—")
        title = report.get("title", "—")
        importance = ai.get("importance", "—")
        imp_emoji = {"גבוהה": "🔴", "בינונית": "🟡", "נמוכה": "🟢"}.get(importance, "⚪")
        engine_tag = "🧠 Claude" if engine == "claude" else "💎 Gemini"
        is_financial = is_financial_report(title, report.get("form_type", ""))

        msg = (
            f"📊 דיווח חדש {imp_emoji}\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🏢 {company}\n"
            f"📋 {title}\n"
            f"📅 {report.get('date', '—')}\n"
            f"📁 {report.get('form_type', '—')}\n"
            f"{engine_tag}\n\n"
        )

        if ai.get("summary"):
            msg += f"📝 סיכום:\n{ai['summary']}\n\n"

        if ai.get("impact"):
            direction = ai.get("target_direction", "")
            dir_emoji = "📈" if "עלייה" in direction else "📉" if "ירידה" in direction else "➡️"
            msg += f"{dir_emoji} השפעה על המניה:\n{ai['impact']}\n\n"

        if ai.get("key_numbers"):
            msg += f"🔢 מספרים מרכזיים:\n{ai['key_numbers']}\n\n"

        # Claude deep analysis extras
        if engine == "claude":
            fin = ai.get("financials", {})
            if fin:
                fin_lines = []
                for k, v in fin.items():
                    if v and v != "אין מידע":
                        labels = {"revenue": "הכנסות", "profit_loss": "רווח/הפסד",
                                  "margins": "שולי רווח", "cash_flow": "תזרים",
                                  "debt": "חוב", "comparison": "השוואה"}
                        fin_lines.append(f"  • {labels.get(k, k)}: {v}")
                if fin_lines:
                    msg += "💰 נתונים פיננסיים:\n" + "\n".join(fin_lines) + "\n\n"

            strengths = ai.get("strengths", [])
            if strengths:
                msg += "💪 חוזקות:\n" + "\n".join([f"  ✅ {s}" for s in strengths]) + "\n\n"

            risks = ai.get("risks", [])
            if risks:
                msg += "⚠️ סיכונים:\n" + "\n".join([f"  🔸 {r}" for r in risks]) + "\n\n"

        if ai.get("action"):
            msg += f"💡 המלצה:\n{ai['action']}\n\n"

        if report.get("url"):
            msg += f"🔗 {report['url']}\n"

        msg += "━━━━━━━━━━━━━━━━━━━━━━"
        await self.send(msg)

    async def status(self, sc, gem, cla, new_count):
        companies_list = "\n".join([f"  • {name}" for name in sorted(COMPANIES.keys())])
        await self.send(
            f"📊 סטטוס — {now_s()}\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"סריקות: {sc}\n"
            f"Gemini היום: {gem}\n"
            f"Claude היום: {cla}\n"
            f"דיווחים חדשים בסריקה אחרונה: {new_count}\n\n"
            f"חברות במעקב:\n{companies_list}\n"
            "━━━━━━━━━━━━━━━━━━━━━━"
        )


# ═══════════════════════════════════════════════════════════
# MAIN SCAN
# ═══════════════════════════════════════════════════════════

state = None
tg = None

async def scan():
    global state, tg
    state.scan_count += 1
    logger.info(f"═══ Maya Scan #{state.scan_count} ═══")
    new_count = 0

    try:
        async with aiohttp.ClientSession() as s:
            for company_name, sec_id in COMPANIES.items():
                try:
                    reports = await fetch_company_reports(s, sec_id)

                    for report in reports:
                        h = report_hash(report)
                        if not state.is_new(h):
                            continue

                        # New report found!
                        state.mark_seen(h)
                        new_count += 1
                        title = report.get("title", "")
                        logger.info(f"📋 NEW: {company_name} — {title}")

                        # Fetch report content
                        content = await fetch_report_content(s, report)

                        # Decide: Gemini (routine) or Claude (financial)
                        use_claude = is_financial_report(title, report.get("form_type", ""))
                        ai = None

                        if use_claude and ANTHROPIC_API_KEY:
                            logger.info(f"  🧠 Claude deep analysis...")
                            ai = await claude_deep_analyze(s, report, content)
                            state.tick_ai("claude")
                            engine = "claude"
                        else:
                            logger.info(f"  💎 Gemini analysis...")
                            ai = await gemini_analyze(s, report, content)
                            state.tick_ai("gemini")
                            engine = "gemini"

                        if ai:
                            await tg.report_alert(report, ai, engine)
                        else:
                            # Fallback: send raw alert without AI
                            await tg.send(
                                f"📊 דיווח חדש\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
                                f"🏢 {company_name}\n"
                                f"📋 {title}\n"
                                f"📅 {report.get('date', '—')}\n"
                                f"🔗 {report.get('url', '—')}\n"
                                "━━━━━━━━━━━━━━━━━━━━━━"
                            )

                        await asyncio.sleep(1)  # Rate limit

                except Exception as e:
                    logger.debug(f"{company_name}: {e}")

                # Small delay between companies
                await asyncio.sleep(0.3)

            # Status every ~4h (16 scans)
            if state.scan_count % 16 == 0:
                await tg.status(state.scan_count, state.gemini_today, state.claude_today, new_count)

            state.save()
            logger.info(f"Scan done — {new_count} new reports, "
                       f"Gemini: {state.gemini_today}, Claude: {state.claude_today}")

    except Exception as e:
        logger.exception(f"Scan error: {e}")
        try:
            await tg.send(f"⚠️ שגיאה: {str(e)[:300]}")
        except:
            pass


# ═══════════════════════════════════════════════════════════
# ENTRY
# ═══════════════════════════════════════════════════════════

async def main():
    global state, tg
    state, tg = State(), TG()
    logger.info("📊 Starting Maya Monitor...")

    await tg.startup(len(COMPANIES))

    if "--once" in sys.argv:
        await scan()
        return

    sched = AsyncIOScheduler()
    sched.add_job(scan, IntervalTrigger(minutes=SCAN_MINUTES),
                  id="scan", max_instances=1, misfire_grace_time=120)
    sched.start()

    # First scan
    await scan()

    logger.info(f"Running — scan every {SCAN_MINUTES} min")
    stop = asyncio.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    await stop.wait()
    sched.shutdown(wait=False)


if __name__ == "__main__":
    asyncio.run(main())
