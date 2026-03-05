#!/usr/bin/env python3
"""
📊 Maya Monitor — ניטור דיווחי חברות בבורסת ת"א
==================================================
שתי שיטות מקבילות למשיכת דיווחים:
  1. רזולוציה של companyId מ-Maya → API ישיר
  2. RSS feed → סינון לפי שם חברה (גיבוי)

Gemini (חינם) + Claude (לדוחות כספיים)
"""

import asyncio, json, logging, os, re, signal, sys, hashlib
from datetime import datetime, timezone, timedelta
from typing import Optional

import aiohttp
import feedparser
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

# Companies: name → security_id
# (the bot will resolve Maya companyId automatically)
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

# All company name variants for matching
COMPANY_NAMES = set(COMPANIES.keys())
SECURITY_IDS = set(COMPANIES.values())

FINANCIAL_KW = [
    "דוח כספי", "דוחות כספיים", "תוצאות כספיות", "רבעון",
    "דוח תקופתי", "דוח שנתי", "מאזן", "רווח והפסד",
    "תזרים מזומנים", "דוח רבעוני", "Q1", "Q2", "Q3", "Q4",
    "quarterly", "annual report", "financial",
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

def is_financial(title, form_type=""):
    text = f"{title} {form_type}".lower()
    return any(k.lower() in text for k in FINANCIAL_KW)

def rhash(r):
    key = f"{r.get('id','')}{r.get('title','')}{r.get('date','')}"
    return hashlib.md5(key.encode()).hexdigest()[:12]

def match_company(text):
    """Check if text mentions one of our tracked companies."""
    for name in COMPANY_NAMES:
        # Try exact match and partial match
        if name in text:
            return name
        # Try matching without suffixes like בע"מ
        short = name.split()[0] if len(name.split()) > 1 else name
        if short in text and len(short) >= 3:
            return name
    return None

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Origin": "https://maya.tase.co.il",
    "Referer": "https://maya.tase.co.il/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Accept-Language": "he-IL,he;q=0.9,en;q=0.8",
}

# ═══════════════════════════════════════════════════════════
# MAYA DATA — MULTIPLE STRATEGIES
# ═══════════════════════════════════════════════════════════

async def resolve_company_ids(session):
    """Try to find Maya companyId for each company."""
    resolved = {}

    for name, sec_id in COMPANIES.items():
        # Strategy 1: Search API
        try:
            url = f"{MAYA_API}/company/search"
            async with session.get(url, params={"name": name}, headers=HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    data = await r.json()
                    if isinstance(data, list) and data:
                        cid = str(data[0].get("CompanyId") or data[0].get("Id") or "")
                        if cid:
                            resolved[name] = cid
                            logger.info(f"  ✅ {name} → companyId {cid}")
                            continue
        except Exception:
            pass

        # Strategy 2: Try common patterns
        # Some companies: companyId = first digits of securityId
        for try_id in [sec_id[:3], sec_id[:4], sec_id]:
            try:
                url = f"{MAYA_API}/company/details?companyId={try_id}"
                async with session.get(url, headers=HEADERS,
                                       timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status == 200:
                        data = await r.json()
                        if data and isinstance(data, dict):
                            cname = data.get("CompanyName") or data.get("CompanyLongName") or ""
                            if name.split()[0] in cname:
                                resolved[name] = try_id
                                logger.info(f"  ✅ {name} → companyId {try_id} (pattern)")
                                break
            except Exception:
                pass

        if name not in resolved:
            logger.warning(f"  ❌ {name} — companyId non résolu")

        await asyncio.sleep(0.5)

    return resolved


async def fetch_reports_by_company(session, company_id, company_name):
    """Fetch reports for a specific company using Maya API."""
    reports = []

    for endpoint in [
        f"{MAYA_API}/company/financereports?companyId={company_id}",
        f"{MAYA_API}/report/details/company/{company_id}",
    ]:
        try:
            async with session.get(endpoint, headers=HEADERS,
                                   timeout=aiohttp.ClientTimeout(total=15)) as r:
                if r.status == 200:
                    data = await r.json()
                    items = data if isinstance(data, list) else []
                    if isinstance(data, dict):
                        for k in ["Reports", "Items", "CompanyReportsList", "reports"]:
                            if k in data and isinstance(data[k], list):
                                items = data[k]; break
                    for item in items[:20]:
                        rp = normalize_report(item, company_name)
                        if rp.get("title"):
                            reports.append(rp)
                    if reports:
                        break
        except Exception as e:
            logger.debug(f"API {endpoint}: {e}")

    return reports


async def fetch_latest_reports(session):
    """Fetch ALL recent reports from Maya and filter by our companies."""
    reports = []

    # Strategy: search reports by date range
    try:
        today = now_u().strftime("%Y-%m-%dT00:00:00")
        yesterday = (now_u() - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00")
        now = now_u().strftime("%Y-%m-%dT23:59:59")

        for from_date in [today, yesterday]:
            url = f"{MAYA_API}/report/search"
            payload = {
                "DateFrom": from_date,
                "DateTo": now,
                "Page": 1,
                "PageSize": 200,
            }
            try:
                async with session.post(url, json=payload, headers=HEADERS,
                                        timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status == 200:
                        data = await r.json()
                        items = data if isinstance(data, list) else data.get("Reports", data.get("Items", []))
                        if isinstance(items, list):
                            for item in items:
                                company = match_report_to_company(item)
                                if company:
                                    rp = normalize_report(item, company)
                                    if rp.get("title"):
                                        reports.append(rp)
                        if reports:
                            break
            except Exception as e:
                logger.debug(f"Reports search: {e}")

    except Exception as e:
        logger.error(f"fetch_latest: {e}")

    # Strategy 2: Try fetching by date directly
    if not reports:
        for date_str in [now_u().strftime("%Y-%m-%d"), (now_u() - timedelta(days=1)).strftime("%Y-%m-%d")]:
            try:
                url = f"{MAYA_API}/report/bydate?date={date_str}"
                async with session.get(url, headers=HEADERS,
                                       timeout=aiohttp.ClientTimeout(total=20)) as r:
                    if r.status == 200:
                        data = await r.json()
                        items = data if isinstance(data, list) else data.get("Reports", data.get("Items", []))
                        if isinstance(items, list):
                            for item in items:
                                company = match_report_to_company(item)
                                if company:
                                    rp = normalize_report(item, company)
                                    if rp.get("title"):
                                        reports.append(rp)
            except Exception as e:
                logger.debug(f"Reports by date: {e}")

    # Strategy 3: RSS feeds from TASE
    if not reports:
        for rss_url in [
            "https://maya.tase.co.il/rss",
            "https://maya.tase.co.il/reports/rss",
        ]:
            try:
                async with session.get(rss_url, headers=HEADERS,
                                       timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status == 200:
                        text = await r.text()
                        feed = feedparser.parse(text)
                        for entry in feed.entries:
                            t = entry.get("title", "")
                            company = match_company(t)
                            if company:
                                reports.append({
                                    "id": entry.get("id", ""),
                                    "title": t,
                                    "date": entry.get("published", ""),
                                    "company": company,
                                    "url": entry.get("link", ""),
                                    "form_type": "",
                                })
            except Exception as e:
                logger.debug(f"RSS: {e}")

    return reports


def match_report_to_company(item):
    """Check if a report item belongs to one of our tracked companies."""
    text = ""
    for field in ["CompanyName", "companyName", "Company", "company",
                   "Subject", "Title", "title", "Header"]:
        val = item.get(field, "")
        if val:
            text += f" {val}"

    # Check securityId
    for field in ["SecurityId", "securityId", "SecurityID"]:
        sid = str(item.get(field, ""))
        if sid in SECURITY_IDS:
            # Find company name by security id
            for name, sec in COMPANIES.items():
                if sec == sid:
                    return name

    return match_company(text)


def normalize_report(item, company_name):
    if not item or not isinstance(item, dict):
        return {}

    title = ""
    for f in ["Title", "title", "Subject", "subject", "Header", "header", "Description"]:
        if item.get(f):
            title = str(item[f]).strip(); break

    date_str = ""
    for f in ["PublishDate", "publishDate", "Date", "date", "ReleaseDate", "releaseDate"]:
        if item.get(f):
            date_str = str(item[f]); break

    rid = ""
    for f in ["Id", "id", "ReportId", "reportId"]:
        if item.get(f):
            rid = str(item[f]); break

    form_type = ""
    for f in ["FormType", "formType", "Type", "type", "Category", "category"]:
        if item.get(f):
            form_type = str(item[f]); break

    url = ""
    if rid:
        url = f"https://maya.tase.co.il/reports/details/{rid}"
    for f in ["Url", "url", "Link"]:
        if item.get(f):
            url = str(item[f]); break

    return {
        "id": rid,
        "title": title,
        "date": date_str,
        "form_type": form_type,
        "company": company_name,
        "url": url,
    }


async def fetch_content(session, report):
    """Try to fetch report content."""
    if not report.get("id"):
        return ""
    try:
        url = f"{MAYA_API}/report/details/{report['id']}"
        async with session.get(url, headers=HEADERS,
                               timeout=aiohttp.ClientTimeout(total=20)) as r:
            if r.status == 200:
                data = await r.json()
                if isinstance(data, dict):
                    for f in ["Body", "body", "Content", "content", "Text", "text", "HtmlBody"]:
                        if data.get(f):
                            text = re.sub(r'<[^>]+>', ' ', str(data[f]))
                            return re.sub(r'\s+', ' ', text).strip()[:5000]
    except Exception:
        pass
    return ""


# ═══════════════════════════════════════════════════════════
# AI ANALYSIS
# ═══════════════════════════════════════════════════════════

async def gemini_analyze(session, report, content):
    if not GEMINI_API_KEY: return None
    prompt = f"""אתה אנליסט בורסאי. נתח דיווח מהבורסה.

חברה: {report.get('company','')}
כותרת: {report.get('title','')}
סוג: {report.get('form_type','?')}
תאריך: {report.get('date','?')}

תוכן:
{content[:3000] if content else "(אין תוכן — נתח על סמך הכותרת בלבד)"}

ענה JSON בלבד (בלי backticks):
{{
    "summary": "סיכום (3-5 משפטים)",
    "impact": "השפעה על המניה: חיובי/שלילי/ניטרלי + הסבר (2 משפטים)",
    "key_numbers": "מספרים מרכזיים אם יש",
    "action": "המלצה למשקיע (משפט)",
    "importance": "גבוהה/בינונית/נמוכה",
    "target_direction": "עלייה/ירידה/יציבות"
}}"""
    try:
        url = f"{GEMINI_API}/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
        async with session.post(url, json={"contents":[{"parts":[{"text":prompt}]}]},
                                timeout=aiohttp.ClientTimeout(total=45)) as r:
            if r.status != 200:
                logger.error(f"Gemini {r.status}: {(await r.text())[:200]}")
                return None
            data = await r.json()
            text = data.get("candidates",[{}])[0].get("content",{}).get("parts",[{}])[0].get("text","")
            return pj(text)
    except Exception as e:
        logger.error(f"Gemini: {e}"); return None


async def claude_analyze(session, report, content):
    if not ANTHROPIC_API_KEY: return None
    prompt = f"""[{now_s()}]
אתה אנליסט פיננסי בכיר. נתח דוח כספי.

חברה: {report.get('company','')}
כותרת: {report.get('title','')}
סוג: {report.get('form_type','?')}

תוכן:
{content[:4000] if content else "(אין תוכן — נתח על סמך הכותרת)"}

JSON בלבד:
{{
    "summary": "סיכום מנהלים (4-6 משפטים)",
    "financials": {{"revenue":"הכנסות","profit_loss":"רווח/הפסד","margins":"שולי רווח","cash_flow":"תזרים","comparison":"השוואה לתקופה קודמת"}},
    "strengths": ["חוזקה 1","חוזקה 2"],
    "risks": ["סיכון 1","סיכון 2"],
    "impact": "השפעה על המניה (3-4 משפטים)",
    "target_direction": "עלייה/ירידה/יציבות",
    "action": "המלצה (2-3 משפטים)",
    "importance": "גבוהה/בינונית/נמוכה",
    "confidence": "גבוהה/בינונית/נמוכה"
}}"""
    try:
        async with session.post(CLAUDE_API,
            json={"model":CLAUDE_MODEL,"max_tokens":2500,
                  "system":"אנליסט פיננסי בכיר. עברית. JSON בלי backticks.",
                  "messages":[{"role":"user","content":prompt}]},
            headers={"x-api-key":ANTHROPIC_API_KEY,"anthropic-version":"2023-06-01",
                     "content-type":"application/json"},
            timeout=aiohttp.ClientTimeout(total=60)) as r:
            if r.status != 200:
                logger.error(f"Claude {r.status}"); return None
            c = (await r.json()).get("content",[])
            return pj(c[0]["text"]) if c else None
    except Exception as e:
        logger.error(f"Claude: {e}"); return None


def pj(text):
    if not text: return {}
    try:
        c = text.strip()
        if c.startswith("```"): c = re.sub(r"```json?|```","",c).strip()
        return json.loads(c)
    except: return {}


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
        self.company_ids = {}  # name → maya companyId
        self._load()

    def _load(self):
        if os.path.exists(STATE_FILE):
            try:
                s = json.load(open(STATE_FILE))
                self.seen = s.get("seen",[])
                self.scan_count = s.get("scan_count",0)
                self.gemini_today = s.get("gemini_today",0)
                self.claude_today = s.get("claude_today",0)
                self.day = s.get("day","")
                self.company_ids = s.get("company_ids",{})
                logger.info(f"State: scan#{self.scan_count}, {len(self.company_ids)} IDs resolved")
            except: pass

    def save(self):
        try:
            os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
            json.dump({"seen":self.seen[-500:],"scan_count":self.scan_count,
                "gemini_today":self.gemini_today,"claude_today":self.claude_today,
                "day":self.day,"company_ids":self.company_ids},
                open(STATE_FILE,"w"))
        except Exception as e: logger.error(f"Save: {e}")

    def is_new(self, h): return h not in self.seen
    def mark(self, h): self.seen.append(h)

    def tick(self, engine):
        d = now_u().strftime("%Y-%m-%d")
        if d != self.day: self.gemini_today=0; self.claude_today=0; self.day=d
        if engine=="gemini": self.gemini_today+=1
        elif engine=="claude": self.claude_today+=1


# ═══════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════

class TG:
    def __init__(self):
        self.bot = Bot(token=TELEGRAM_BOT_TOKEN)
        self.chat = TELEGRAM_CHAT_ID

    async def send(self, text):
        try:
            if len(text)>4000:
                for i in range(0,len(text),4000):
                    await self.bot.send_message(chat_id=self.chat,text=text[i:i+4000],disable_web_page_preview=True)
                    await asyncio.sleep(0.5)
                return
            await self.bot.send_message(chat_id=self.chat,text=text,disable_web_page_preview=True)
        except RetryAfter as e:
            await asyncio.sleep(e.retry_after); await self.send(text)
        except TelegramError as e: logger.error(f"TG: {e}")

    async def startup(self, n, resolved):
        await self.send(
            "📊 Maya Monitor\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🏢 חברות: {n}\n"
            f"🔗 חברות עם Maya ID: {resolved}/{n}\n"
            f"⏰ כל {SCAN_MINUTES} דקות\n"
            f"💎 Gemini: {'✅' if GEMINI_API_KEY else '❌'}\n"
            f"🧠 Claude: {'✅' if ANTHROPIC_API_KEY else '❌'}\n\n"
            "דיווח חדש → ניתוח AI → התראה\n━━━━━━━━━━━━━━━━━━━━━━")

    async def report(self, rp, ai, engine):
        imp = ai.get("importance","—")
        ie = {"גבוהה":"🔴","בינונית":"🟡","נמוכה":"🟢"}.get(imp,"⚪")
        et = "🧠 Claude" if engine=="claude" else "💎 Gemini"
        d = ai.get("target_direction","")
        de = "📈" if "עלייה" in d else "📉" if "ירידה" in d else "➡️"

        msg = (f"📊 דיווח חדש {ie}\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
               f"🏢 {rp.get('company','—')}\n📋 {rp.get('title','—')}\n"
               f"📅 {rp.get('date','—')}\n{et}\n\n")

        if ai.get("summary"):
            msg += f"📝 סיכום:\n{ai['summary']}\n\n"
        if ai.get("impact"):
            msg += f"{de} השפעה:\n{ai['impact']}\n\n"
        if ai.get("key_numbers"):
            msg += f"🔢 מספרים: {ai['key_numbers']}\n\n"

        if engine=="claude":
            fin = ai.get("financials",{})
            fl = []
            labels = {"revenue":"הכנסות","profit_loss":"רווח/הפסד","margins":"שולי רווח",
                      "cash_flow":"תזרים","comparison":"השוואה"}
            for k,v in fin.items():
                if v and "אין" not in str(v): fl.append(f"  • {labels.get(k,k)}: {v}")
            if fl: msg += "💰 נתונים:\n" + "\n".join(fl) + "\n\n"
            for tag,items,emoji in [("strengths","💪 חוזקות","✅"),("risks","⚠️ סיכונים","🔸")]:
                lst = ai.get(tag,[])
                if lst: msg += f"{items}:\n" + "\n".join([f"  {emoji} {x}" for x in lst]) + "\n\n"

        if ai.get("action"):
            msg += f"💡 המלצה: {ai['action']}\n\n"
        if rp.get("url"):
            msg += f"🔗 {rp['url']}\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━"
        await self.send(msg)

    async def raw_report(self, rp):
        await self.send(
            f"📊 דיווח חדש\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🏢 {rp.get('company','—')}\n📋 {rp.get('title','—')}\n"
            f"📅 {rp.get('date','—')}\n🔗 {rp.get('url','—')}\n"
            "━━━━━━━━━━━━━━━━━━━━━━")

    async def status(self, sc, gem, cla, new_c):
        lst = "\n".join([f"  • {n}" for n in sorted(COMPANIES.keys())])
        await self.send(
            f"📊 סטטוס — {now_s()}\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"סריקות: {sc} | Gemini: {gem} | Claude: {cla}\n"
            f"דיווחים בסריקה: {new_c}\n\n{lst}\n━━━━━━━━━━━━━━━━━━━━━━")


# ═══════════════════════════════════════════════════════════
# MAIN SCAN
# ═══════════════════════════════════════════════════════════

state = None
tg = None

async def scan():
    global state, tg
    state.scan_count += 1
    logger.info(f"═══ Scan #{state.scan_count} ═══")
    new_c = 0

    try:
        async with aiohttp.ClientSession() as s:
            all_reports = []

            # Method 1: Fetch by company ID (if resolved)
            for name, cid in state.company_ids.items():
                try:
                    reps = await fetch_reports_by_company(s, cid, name)
                    all_reports.extend(reps)
                except Exception as e:
                    logger.debug(f"{name}: {e}")
                await asyncio.sleep(0.3)

            # Method 2: Fetch ALL recent reports and filter
            try:
                latest = await fetch_latest_reports(s)
                all_reports.extend(latest)
                logger.info(f"Latest reports: {len(latest)} matching our companies")
            except Exception as e:
                logger.debug(f"Latest: {e}")

            # Deduplicate
            seen_hashes = set()
            unique = []
            for rp in all_reports:
                h = rhash(rp)
                if h not in seen_hashes:
                    seen_hashes.add(h)
                    unique.append(rp)

            logger.info(f"Total unique reports: {len(unique)}")

            # Process new reports
            for rp in unique:
                h = rhash(rp)
                if not state.is_new(h):
                    continue

                state.mark(h)
                new_c += 1
                logger.info(f"📋 NEW: {rp.get('company','')} — {rp.get('title','')}")

                content = await fetch_content(s, rp)

                use_claude = is_financial(rp.get("title",""), rp.get("form_type",""))
                ai = None

                if use_claude and ANTHROPIC_API_KEY:
                    logger.info(f"  🧠 Claude")
                    ai = await claude_analyze(s, rp, content)
                    state.tick("claude")
                    engine = "claude"
                else:
                    logger.info(f"  💎 Gemini")
                    ai = await gemini_analyze(s, rp, content)
                    state.tick("gemini")
                    engine = "gemini"

                if ai:
                    await tg.report(rp, ai, engine)
                else:
                    await tg.raw_report(rp)

                await asyncio.sleep(1)

            if state.scan_count % 16 == 0:
                await tg.status(state.scan_count, state.gemini_today, state.claude_today, new_c)

            state.save()
            logger.info(f"Done — {new_c} new, Gemini: {state.gemini_today}, Claude: {state.claude_today}")

    except Exception as e:
        logger.exception(f"Scan: {e}")
        try: await tg.send(f"⚠️ {str(e)[:300]}")
        except: pass


async def main():
    global state, tg
    state, tg = State(), TG()
    logger.info("📊 Starting Maya Monitor...")

    # Resolve company IDs (only if not already cached)
    if len(state.company_ids) < len(COMPANIES) // 2:
        logger.info("Resolving company IDs from Maya...")
        async with aiohttp.ClientSession() as s:
            resolved = await resolve_company_ids(s)
            state.company_ids.update(resolved)
            state.save()
        logger.info(f"Resolved {len(state.company_ids)}/{len(COMPANIES)} companies")
    else:
        logger.info(f"Using cached IDs: {len(state.company_ids)}/{len(COMPANIES)}")

    await tg.startup(len(COMPANIES), len(state.company_ids))

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
