#!/usr/bin/env python3
"""Scrape Bellhaven communities and build an idempotent, human-reviewed CRM queue.

The pipeline never writes to the CRM. It only reads the website and the CRM and
produces proposals in data/proposals.json; app.py applies a proposal after a
reviewer approves it.
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import ssl
import sys
import time
from html.parser import HTMLParser
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin, urlparse
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
BASE = os.getenv("WEBSITE_BASE", "https://analyst-assessment-production.up.railway.app").rstrip("/")
API = os.getenv("CRM_API_BASE", BASE + "/api/v1").rstrip("/")
TOKEN = os.getenv("CRM_API_TOKEN", "")
PARENT_NAME = "Bellhaven Senior Living (Parent Account)"
PARENT_ID = os.getenv("BELLHAVEN_PARENT_ID", "")

# Match thresholds. Scores are not capped; the UI displays min(score, 100).
STRONG = 100      # auto-proposable correction
MIN_GAP = 25      # strong match must beat the next distinct candidate by this much
CANDIDATE = 30    # below this, a CRM account is not considered a candidate
NOT_LISTED_MARKER = "Not found in current Bellhaven community directory"
MAX_ACCOUNT_PAGES = 200
MAX_LISTING_PAGES = 50


class PipelineError(RuntimeError):
    pass


# --------------------------------------------------------------------------- IO

def load_env_file():
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip("'\""))


def configure():
    """Re-read settings after .env is loaded. Used by both pipeline and app."""
    global TOKEN, API, BASE, PARENT_ID
    load_env_file()
    BASE = os.getenv("WEBSITE_BASE", BASE).rstrip("/")
    API = os.getenv("CRM_API_BASE", BASE + "/api/v1").rstrip("/")
    TOKEN = os.getenv("CRM_API_TOKEN", TOKEN)
    PARENT_ID = os.getenv("BELLHAVEN_PARENT_ID", PARENT_ID)


def _ssl_context():
    cafile = os.getenv("SSL_CERT_FILE")
    return ssl.create_default_context(cafile=cafile) if cafile else ssl.create_default_context()


def request(url: str, *, api: bool = False, method: str = "GET", payload=None, retries: int = 3):
    """HTTP helper. GETs are retried on 429/5xx/network errors; writes are never retried."""
    headers = {"User-Agent": "Bellhaven-Reconciliation/0.2", "Accept": "application/json, text/html"}
    if api:
        if not TOKEN:
            raise PipelineError("Set CRM_API_TOKEN in your environment or .env file.")
        headers["Authorization"] = f"Bearer {TOKEN}"
    body = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode()
    attempts = retries if method == "GET" else 1
    for attempt in range(1, attempts + 1):
        req = Request(url, data=body, headers=headers, method=method)
        try:
            ctx = _ssl_context() if url.startswith("https") else None
            with urlopen(req, timeout=30, context=ctx) as resp:
                raw = resp.read().decode("utf-8", "replace")
                ctype = resp.headers.get("Content-Type", "")
        except HTTPError as e:
            if method == "GET" and attempt < attempts and (e.code == 429 or e.code >= 500):
                time.sleep(2 ** attempt)
                continue
            raise
        except URLError:
            if method == "GET" and attempt < attempts:
                time.sleep(2 ** attempt)
                continue
            raise
        if api or "json" in ctype:
            if not raw.strip():
                return {}
            try:
                return json.loads(raw)
            except ValueError:
                raise PipelineError(f"Expected JSON from {method} {url}; got: {raw[:200]!r}")
        return raw
    raise PipelineError(f"Request failed: {method} {url}")


def unwrap(obj):
    """APIs often wrap a record as {"data": {...}}. Return the record itself."""
    if isinstance(obj, dict) and isinstance(obj.get("data"), dict):
        return obj["data"]
    return obj


def record_id(obj):
    obj = unwrap(obj) or {}
    return obj.get("account_id") or obj.get("id") if isinstance(obj, dict) else None


def read_json(name, default):
    p = DATA / name
    return json.loads(p.read_text()) if p.exists() else default


def write_json(name, value):
    """Atomic write: a crash mid-write can never leave a truncated file."""
    DATA.mkdir(exist_ok=True)
    tmp = DATA / f".{name}.tmp"
    tmp.write_text(json.dumps(value, indent=2, ensure_ascii=False) + "\n")
    os.replace(tmp, DATA / name)


@contextlib.contextmanager
def data_lock(timeout: float = 60):
    """Cross-process lock shared by pipeline.py and app.py around proposals.json."""
    DATA.mkdir(exist_ok=True)
    path = DATA / ".lock"
    fh = open(path, "a+")
    try:
        try:
            import fcntl
        except ImportError:  # Windows: best effort, no lock
            yield
            return
        deadline = time.time() + timeout
        while True:
            try:
                fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.time() > deadline:
                    raise PipelineError("Another pipeline/app write holds data/.lock; try again shortly.")
                time.sleep(0.2)
        try:
            yield
        finally:
            fcntl.flock(fh, fcntl.LOCK_UN)
    finally:
        fh.close()


def append_log(name, entry):
    DATA.mkdir(exist_ok=True)
    with open(DATA / name, "a") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def now():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ---------------------------------------------------------------------- scraping

class ListingParser(HTMLParser):
    """Collects every link to a community detail page, wherever it sits on the page."""

    def __init__(self, base, page_url):
        super().__init__()
        self.base = base
        self.page_url = page_url
        self.link = None
        self.text = []
        self.items = []
        self.all_text = []

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        href = dict(attrs).get("href") or ""
        path = community_path(href, self.page_url, self.base)
        if path:
            self.link = path
            self.text = []

    def handle_data(self, data):
        self.all_text.append(data)
        if self.link:
            self.text.append(data.strip())

    def handle_endtag(self, tag):
        if tag == "a" and self.link:
            self.items.append((self.link, " ".join(t for t in self.text if t)))
            self.link = None


def community_path(href, page_url, base):
    """Return '/communities/<slug>' for detail links (relative or absolute), else None."""
    if not href:
        return None
    full = urlparse(urljoin(page_url, href))
    if full.netloc and full.netloc != urlparse(base).netloc:
        return None
    m = re.fullmatch(r"/communities/([^/?#]+)/?", full.path)
    return f"/communities/{m.group(1)}" if m else None


class DetailParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.in_h1 = False
        self.h1s = []
        self.dt = False
        self.dd = False
        self.key = ""
        self.fields = {}
        self.val = []

    def handle_starttag(self, tag, attrs):
        if tag == "h1":
            self.in_h1 = True
            self.h1s.append("")
        elif tag == "dt":
            self.dt = True
            self.val = []
        elif tag == "dd":
            self.dd = True
            self.val = []
        elif self.dd and tag in ("br", "p", "div", "li"):
            self.val.append("\n")
        elif self.dd and tag == "span":
            self.val.append(" ")

    def handle_data(self, data):
        if self.in_h1:
            self.h1s[-1] += data
        if self.dt or self.dd:
            self.val.append(data)

    def handle_endtag(self, tag):
        if tag == "h1":
            self.in_h1 = False
        elif tag == "dt":
            self.key = " ".join("".join(self.val).split()).lower().rstrip(":")
            self.dt = False
        elif tag == "dd":
            raw = "".join(self.val)
            lines = [" ".join(line.split()) for line in raw.splitlines()]
            self.fields[self.key] = "\n".join(l for l in lines if l).strip()
            self.dd = False
        elif self.dd and tag in ("span", "li"):
            self.val.append(" " if tag == "span" else "\n")

    @property
    def name(self):
        names = [" ".join(h.split()) for h in self.h1s if h.strip()]
        return names[0] if names else ""


CITY_STATE_ZIP = re.compile(r"^\s*(.+?),\s*([A-Za-z]{2})\.?\s+(\d{5})(?:-\d{4})?\s*$")


def address_parts(s):
    """Street is everything before the 'City, ST ZIP' line (suite lines are kept)."""
    lines = [x.strip().rstrip(",") for x in (s or "").splitlines() if x.strip()]
    if len(lines) == 1 and "," in lines[0]:
        # Single-line form: "123 Main St, Austin, TX 78701"
        parts = [p.strip() for p in lines[0].split(",")]
        if len(parts) >= 3:
            lines = [", ".join(parts[:-2]), ", ".join(parts[-2:])]
    if len(lines) >= 2:
        m = CITY_STATE_ZIP.match(lines[-1])
        if m:
            return {"street": ", ".join(lines[:-1]), "city": m.group(1).strip(),
                    "state": m.group(2).upper(), "zip": m.group(3)}
    return {"street": "", "city": "", "state": "", "zip": ""}


def field(fields, *names):
    for n in names:
        for k, v in fields.items():
            if k == n or k.startswith(n):
                return v
    return ""


def split_offerings(text):
    parts = re.split(r"\s*(?:\n|,|;|\||•|·|/)\s*|\s+and\s+", text or "")
    return [p.strip() for p in parts if p.strip()]


def scrape():
    report = {"listing_pages": 0, "links_found": 0, "parsed": 0, "skipped": [], "claimed_count": None}
    all_links, seen = [], set()
    for page in range(1, MAX_LISTING_PAGES + 1):
        url = BASE + "/communities" + (f"?page={page}" if page > 1 else "")
        p = ListingParser(BASE, url)
        p.feed(request(url))
        if page == 1:
            m = re.search(r"\b(\d{1,4})\s+(?:senior\s+living\s+)?communities\b", " ".join(p.all_text), re.I)
            report["claimed_count"] = int(m.group(1)) if m else None
        found = [x for x in p.items if x[0] not in seen]
        if not found:
            break
        report["listing_pages"] = page
        seen.update(x[0] for x in found)
        all_links.extend(found)
    report["links_found"] = len(all_links)

    locations = []
    for path, _teaser in all_links:
        p = DetailParser()
        p.feed(request(BASE + path))
        address = address_parts(field(p.fields, "address", "location"))
        reason = None
        if not p.name:
            reason = "no <h1> facility name"
        elif not address["street"] or not address["zip"]:
            reason = f"address not parsed: {field(p.fields, 'address', 'location')!r}"
        if reason:
            report["skipped"].append({"url": BASE + path, "reason": reason})
            continue
        care = field(p.fields, "care offerings", "care offered", "care types", "services", "care")
        locations.append({
            "source_url": BASE + path, "slug": path.rsplit("/", 1)[-1], "name": p.name, **address,
            "care_offerings": split_offerings(care), "care_type": care.replace("\n", ", "),
        })
        time.sleep(0.05)
    report["parsed"] = len(locations)
    if report["claimed_count"] and report["claimed_count"] != report["links_found"]:
        report["warning"] = (f"Directory says {report['claimed_count']} communities; "
                             f"found {report['links_found']} detail links, parsed {report['parsed']}.")
    if not locations:
        raise PipelineError("No facility detail pages were parsed; refusing to replace existing data.")
    write_json("locations.json", locations)
    write_json("scrape_report.json", report)
    return locations, report


# ------------------------------------------------------------------------- CRM

def fetch_accounts():
    """Read every CRM account. Fails rather than silently returning a partial list."""
    rows, ids, total = [], set(), None
    for page in range(1, MAX_ACCOUNT_PAGES + 1):
        result = request(API + "/accounts?" + urlencode({"page": page, "page_size": 100}), api=True)
        if isinstance(result, list):
            batch, page_total = result, None
        else:
            batch = result.get("data") or result.get("accounts") or result.get("results") or []
            page_total = result.get("total") or result.get("count") or (result.get("meta") or {}).get("total")
        if page_total is not None:
            total = int(page_total)
        new = [r for r in batch if record_id(r) not in ids]
        if batch and not new:
            raise PipelineError(f"Page {page} repeated earlier accounts; the API may ignore the page parameter.")
        for r in new:
            r.setdefault("account_id", record_id(r))
            ids.add(r["account_id"])
        rows.extend(new)
        if not batch or (total is not None and len(rows) >= total):
            break
    else:
        raise PipelineError(f"More than {MAX_ACCOUNT_PAGES} pages of accounts; raise MAX_ACCOUNT_PAGES.")
    if not rows:
        raise PipelineError("CRM returned no accounts; refusing to generate proposals.")
    if total is None:
        raise PipelineError("CRM response has no total/count; cannot confirm every account was read.")
    if len(rows) != total:
        raise PipelineError(f"Read {len(rows)} unique accounts but CRM reports {total}.")
    return rows


def get_account(account_id, accounts_cache=None):
    """Current state of one account. Falls back to the list endpoint if GET by id is unsupported."""
    try:
        return unwrap(request(f"{API}/accounts/{account_id}", api=True))
    except HTTPError as e:
        if e.code not in (404, 405):
            raise
    for acc in accounts_cache if accounts_cache is not None else fetch_accounts():
        if acc.get("account_id") == account_id:
            return acc
    return None


# -------------------------------------------------------------------- matching

ABBR = {
    "street": "st", "road": "rd", "avenue": "ave", "drive": "dr", "lane": "ln", "boulevard": "blvd",
    "court": "ct", "highway": "hwy", "parkway": "pkwy", "place": "pl", "terrace": "ter", "circle": "cir",
    "trail": "trl", "way": "way", "west": "w", "east": "e", "north": "n", "south": "s",
    "northwest": "nw", "northeast": "ne", "southwest": "sw", "southeast": "se", "saint": "st",
    "suite": "ste", "apartment": "apt", "building": "bldg",
}


def norm(s):
    s = (s or "").lower().replace("&", " and ")
    s = re.sub(r"[.,#]", " ", s)
    s = " ".join(ABBR.get(w, w) for w in s.split())
    return re.sub(r"[^a-z0-9]", "", s)


def norm_zip(z):
    digits = re.sub(r"\D", "", str(z if z is not None else ""))
    if not digits:
        return ""
    if len(digits) in (3, 4):   # integer ZIP that lost its leading zeros
        digits = digits.zfill(5)
    return digits[:5]


STATES = dict(zip(
    "alabama alaska arizona arkansas california colorado connecticut delaware florida georgia hawaii idaho illinois "
    "indiana iowa kansas kentucky louisiana maine maryland massachusetts michigan minnesota mississippi missouri "
    "montana nebraska nevada newhampshire newjersey newmexico newyork northcarolina northdakota ohio oklahoma oregon "
    "pennsylvania rhodeisland southcarolina southdakota tennessee texas utah vermont virginia washington westvirginia "
    "wisconsin wyoming districtofcolumbia".split(),
    "AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR "
    "PA RI SC SD TN TX UT VT VA WA WV WI WY DC".split()))


def norm_state(s):
    s = re.sub(r"[^a-z]", "", (s or "").lower())
    return STATES.get(s, s.upper())


def street_parts(street):
    """('1100', 'mainst', 'ste200') — house number, street name, unit."""
    s = (street or "").lower()
    unit = ""
    m = re.search(r"(?:,|\s)\s*(?:suite|ste|unit|apt|#|bldg|building)\.?\s*([\w-]+)\s*$", s)
    if m:
        unit = m.group(1)
        s = s[:m.start()]
    m = re.match(r"\s*(\d+[a-z]?)\b[\s,]*(.*)", s)
    number, rest = (m.group(1), m.group(2)) if m else ("", s)
    return number, norm(rest), unit


def match_score(loc, acc):
    """Additive evidence with penalties for conflicts. Not capped."""
    score, reasons = 0, []
    lz, az = norm_zip(loc.get("zip")), norm_zip(acc.get("billing_zip"))
    if lz and az:
        if lz == az:
            score += 40; reasons.append("ZIP matches")
        else:
            score -= 40; reasons.append(f"ZIP conflicts ({lz} vs {az})")
    ls, as_ = norm_state(loc.get("state")), norm_state(acc.get("billing_state"))
    if ls and as_:
        if ls == as_:
            score += 10; reasons.append("state matches")
        else:
            score -= 50; reasons.append(f"state conflicts ({ls} vs {as_})")
    if loc.get("city") and norm(loc["city"]) == norm(acc.get("billing_city")):
        score += 10; reasons.append("city matches")

    ln_, lname, _ = street_parts(loc.get("street"))
    an_, aname, _ = street_parts(acc.get("billing_street"))
    if ln_ and an_ and ln_ != an_:
        score -= 40; reasons.append(f"house number conflicts ({ln_} vs {an_})")
    elif lname and aname:
        if lname == aname and ln_ == an_:
            score += 60; reasons.append("street address matches")
        elif ln_ == an_ and ln_ and (lname in aname or aname in lname):
            score += 40; reasons.append("same house number; street name nearly matches")

    fn, an = norm(loc.get("name")), norm(acc.get("name"))
    if fn and fn == an:
        score += 40; reasons.append("facility name matches exactly")
    elif fn and an and min(len(fn), len(an)) >= 6 and (fn in an or an in fn):
        score += 15; reasons.append("facility name overlaps")
    return score, reasons


def to_number(v):
    """Parse revenue/AR. Returns None when missing or unparseable (never assumes zero)."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        return float(v)
    try:
        return float(re.sub(r"[$,\s]", "", str(v)))
    except ValueError:
        return None


def billing(acc):
    rev, ar = to_number(acc.get("lifetime_revenue")), to_number(acc.get("outstanding_ar"))
    return {"lifetime_revenue": acc.get("lifetime_revenue"), "outstanding_ar": acc.get("outstanding_ar"),
            "known": rev is not None and ar is not None,
            "chow_required": rev is not None and ar is not None and rev > 0 and ar > 0}


def same_identity(a, b):
    return (norm(a.get("name")) == norm(b.get("name")) and norm(a.get("billing_street")) == norm(b.get("billing_street"))
            and norm_zip(a.get("billing_zip")) == norm_zip(b.get("billing_zip")))


def append_note(existing, line):
    existing = (existing or "").strip()
    if line.lower() in existing.lower():
        return existing
    return f"{existing}\n{line}".strip()


SNAPSHOT_FIELDS = ("name", "parent_id", "status", "note", "billing_street", "billing_city", "billing_state",
                   "billing_zip", "lifetime_revenue", "outstanding_ar", "chow_current_account", "duplicate_of_account")


def snapshot(acc):
    return {k: acc.get(k) for k in SNAPSHOT_FIELDS} if acc else None


def build_proposals(locations, accounts, parent_id, locked_ids=frozenset()):
    """Pure function: website + CRM in, proposals out. locked_ids are accounts mid-write; they are skipped."""
    parent_id = parent_id or next((a["account_id"] for a in accounts if a.get("name") == PARENT_NAME), "")
    if not parent_id:
        raise PipelineError("Could not identify Bellhaven parent account.")
    by_id = {a["account_id"]: a for a in accounts}
    parents = {a.get("parent_id") for a in accounts if a.get("parent_id")}
    chow_targets = {a.get("chow_current_account") for a in accounts if a.get("chow_current_account")}

    def eligible(acc):
        return not (acc["account_id"] == parent_id or acc["account_id"] in parents
                    or acc.get("chow_current_account") or acc.get("duplicate_of_account")
                    or acc["account_id"] in locked_ids)

    pool = [a for a in accounts if eligible(a)]
    proposals, matched = [], set(locked_ids)
    matched.update(a["account_id"] for a in accounts if a.get("chow_current_account") or a.get("duplicate_of_account"))

    locked_accounts = [by_id[i] for i in locked_ids if i in by_id]
    for loc in locations:
        # A write for this facility is half-finished; leave it alone until the reviewer completes it.
        if any(match_score(loc, a)[0] >= STRONG for a in locked_accounts):
            continue
        cands = []
        for acc in pool:
            score, reasons = match_score(loc, acc)
            if score >= CANDIDATE:
                cands.append((score, acc, reasons))
        cands.sort(key=lambda x: (x[0], x[1].get("parent_id") == parent_id, str(x[1]["account_id"])), reverse=True)

        # Collapse exact duplicates of the top candidate into one group, so they don't
        # create a false tie and so the location can still get its correction.
        group, rest = [], []
        if cands:
            top = cands[0][1]
            for c in cands:
                (group if c is cands[0] or same_identity(c[1], top) else rest).append(c)
        best = group[0] if group else None
        second = rest[0] if rest else None

        dup_block = None
        if len(group) > 1:
            survivor, dup_props, dup_block = pick_survivor([g[1] for g in group], parent_id, chow_targets)
            if dup_block is None:
                best = next(g for g in group if g[1] is survivor)
            for loser in dup_props:
                matched.add(loser["account_id"])
                proposals.append(duplicate_proposal(loc, loser, survivor))
            for g in group:
                matched.add(g[1]["account_id"])

        strong = best and best[0] >= STRONG and (not second or best[0] - second[0] >= MIN_GAP)
        if best and dup_block:
            matched.add(best[1]["account_id"])
            proposals.append(review_proposal(loc, best, cands, dup_block))
        elif strong:
            score, acc, reasons = best
            matched.add(acc["account_id"])
            p = correction_proposal(loc, acc, score, reasons, parent_id)
            if p:
                proposals.append(p)
        elif best:
            matched.add(best[1]["account_id"])
            why = ("Top two candidates are too close to call." if second and best[0] >= STRONG
                   else "Evidence is not strong enough for an automatic correction.")
            proposals.append(review_proposal(loc, best, cands, why))
        else:
            payload = {"name": loc["name"], "parent_id": parent_id, "billing_street": loc["street"],
                       "billing_city": loc["city"], "billing_state": loc["state"], "billing_zip": loc["zip"],
                       "care_type": loc["care_type"], "status": "Active",
                       "note": "Created from Bellhaven public community listing after review."}
            evidence = {"website": loc, "crm": None, "match_score": 0, "match_reasons": [],
                        "other_candidates": []}
            proposals.append(make_proposal("new_location", "create", None, loc, {"create_account": payload}, evidence))

    for acc in accounts:
        if acc.get("parent_id") != parent_id or acc["account_id"] in matched:
            continue
        if acc.get("status") == "Needs Review" and NOT_LISTED_MARKER.lower() in (acc.get("note") or "").lower():
            continue
        line = f"{NOT_LISTED_MARKER}; verify ownership/listing before changing account."
        changes = {"status": "Needs Review", "note": append_note(acc.get("note"), line)}
        evidence = {"website": None, "crm": acc, "match_score": 0,
                    "match_reasons": ["No current directory listing matched this Bellhaven CRM account."],
                    "billing_review": billing(acc)}
        loc = {"name": acc.get("name", ""), "slug": f"crm-{acc['account_id']}"}
        proposals.append(make_proposal("not_listed", "update", acc, loc, changes, evidence))
    return proposals, parent_id


def pick_survivor(group, parent_id, chow_targets):
    """Return (survivor, losers, block_reason). Never retires an account carrying revenue+AR."""
    def rank(a):  # Bellhaven-linked first, then a CHOW target, then the oldest (lowest) id
        return (a.get("parent_id") != parent_id, a["account_id"] not in chow_targets, str(a["account_id"]))
    ordered = sorted(group, key=rank)
    survivor, losers = ordered[0], ordered[1:]
    for loser in losers:
        b = billing(loser)
        if b["chow_required"]:
            return survivor, [], (f"Possible duplicate {loser['account_id']} has revenue history and outstanding AR; "
                                  "it may be a pre-CHOW account. Resolve manually instead of marking it inactive.")
        if not b["known"]:
            return survivor, [], (f"Possible duplicate {loser['account_id']} is missing revenue/AR values; "
                                  "check billing before marking it inactive.")
    return survivor, losers, None


def duplicate_proposal(loc, loser, survivor):
    line = f"Duplicate facility record; surviving account {survivor['account_id']} matches the Bellhaven listing at the same address."
    changes = {"duplicate_of_account": survivor["account_id"], "status": "Inactive",
               "note": append_note(loser.get("note"), line)}
    evidence = {"website": loc, "crm": loser, "match_score": 100,
                "match_reasons": ["Same facility name, normalized street, and ZIP as another CRM account."],
                "duplicate_survivor": snapshot(survivor), "billing_review": billing(loser)}
    return make_proposal("duplicate", "update", loser, loc, changes, evidence)


def review_proposal(loc, best, cands, why):
    score, acc, reasons = best
    evidence = {"website": loc, "crm": acc, "match_score": score, "match_reasons": reasons, "review_reason": why,
                "billing_review": billing(acc),
                "other_candidates": [{"account_id": a["account_id"], "name": a.get("name"), "score": s,
                                      "parent_name": a.get("parent_name")} for s, a, _ in cands if a is not acc][:3]}
    return make_proposal("needs_review", "none", acc, loc, {}, evidence)


def correction_proposal(loc, acc, score, reasons, parent_id):
    evidence = {"website": loc, "crm": acc, "match_score": score, "match_reasons": reasons, "billing_review": billing(acc)}
    if acc.get("parent_id") != parent_id:
        b = billing(acc)
        if not b["known"]:
            evidence["review_reason"] = ("Parent change needed, but lifetime_revenue/outstanding_ar is missing or not numeric, "
                                         "so the CHOW rule cannot be applied safely.")
            return make_proposal("needs_review", "none", acc, loc, {}, evidence)
        if b["chow_required"]:
            new = {"name": loc["name"], "parent_id": parent_id, "billing_street": loc["street"],
                   "billing_city": loc["city"], "billing_state": loc["state"], "billing_zip": loc["zip"],
                   "care_type": loc["care_type"], "status": "Active",
                   "note": f"Created under change-of-ownership billing SOP; prior account {acc['account_id']} preserved."}
            return make_proposal("chow", "chow", acc, loc, {"create_account": new, "link_old_account": acc["account_id"]}, evidence)
        changes = {"parent_id": parent_id}
        if acc.get("name") != loc["name"]:
            changes["name"] = loc["name"]
        return make_proposal("correct_match", "update", acc, loc, changes, evidence)
    if acc.get("name") != loc["name"]:
        return make_proposal("correct_match", "update", acc, loc, {"name": loc["name"]}, evidence)
    return None


def make_proposal(kind, action, account, loc, changes, evidence):
    key = f"{kind}|{(account or {}).get('account_id', 'new')}|{loc.get('slug') or norm(loc.get('name', ''))}"
    # Fingerprint only what the decision depends on, so unrelated CRM edits don't reset decisions.
    fp_basis = {"changes": changes, "crm": snapshot(account),
                "website": {k: loc.get(k) for k in ("name", "street", "city", "state", "zip")}}
    precondition = {k: (account or {}).get(k) for k in ("name", "parent_id", "status", "chow_current_account",
                                                         "duplicate_of_account")} if account else None
    if evidence.get("crm"):
        evidence = {**evidence, "crm": snapshot(evidence["crm"]) | {"account_id": account["account_id"],
                                                                     "parent_name": account.get("parent_name"),
                                                                     "care_type": account.get("care_type")}}
    return {"proposal_id": hashlib.sha256(key.encode()).hexdigest()[:16], "key": key, "kind": kind, "action": action,
            "account_id": (account or {}).get("account_id"), "title": loc.get("name") or (account or {}).get("name", "Review account"),
            "changes": changes, "precondition": precondition, "evidence": evidence,
            "fingerprint": hashlib.sha256(json.dumps(fp_basis, sort_keys=True, default=str).encode()).hexdigest(),
            "decision": "pending", "stage": None, "created_at": now()}


# ---------------------------------------------------------------- verification

def verify(p, by_id):
    """Check an approved proposal against the CRM as it is now. Returns (ok, detail)."""
    res = p.get("result") or {}
    if p["action"] == "none":
        return True, "No CRM change was requested."
    if p["action"] == "create":
        nid = res.get("created_account_id")
        return (nid in by_id, f"Created account {nid} {'found' if nid in by_id else 'NOT found'} in CRM.")
    if p["action"] == "chow":
        nid = res.get("created_account_id")
        old = by_id.get(p["account_id"]) or {}
        ok = nid in by_id and old.get("chow_current_account") == nid
        return ok, f"New account {nid} {'exists' if nid in by_id else 'missing'}; old account link is {old.get('chow_current_account')!r}."
    acc = by_id.get(p["account_id"])
    if not acc:
        return False, f"Account {p['account_id']} no longer in CRM."
    diffs = [k for k, v in p["changes"].items() if (acc.get(k) or "") != (v or "")]
    return (not diffs, "All changed fields match the CRM." if not diffs else f"CRM differs on: {', '.join(diffs)}")


# ------------------------------------------------------------------------- run

def merge(old, proposals, accounts):
    """Carry decisions across reruns. In-flight writes are kept untouched."""
    by_id = {a["account_id"]: a for a in accounts}
    old_by_id = {p["proposal_id"]: p for p in old}
    merged, seen = [], set()
    for p in old:  # writes that were started but not finished stay exactly as they are
        if p.get("stage") in ("create_in_flight", "link_pending"):
            merged.append(p); seen.add(p["proposal_id"])
    for p in proposals:
        if p["proposal_id"] in seen:
            continue
        prev = old_by_id.get(p["proposal_id"])
        if prev and prev.get("decision") in ("approved", "rejected") and prev.get("fingerprint") == p["fingerprint"]:
            for k in ("decision", "decided_at", "result", "stage"):
                p[k] = prev.get(k)
        merged.append(p); seen.add(p["proposal_id"])
    for p in old:  # history: decided proposals that no longer regenerate (usually because they took effect)
        if p["proposal_id"] not in seen and p.get("decision") in ("approved", "rejected"):
            merged.append(p); seen.add(p["proposal_id"])
    for p in merged:
        if p.get("decision") == "approved":
            ok, detail = verify(p, by_id)
            p["verified"], p["verified_detail"], p["verified_at"] = ok, detail, now()
    return merged


def run():
    configure()
    locations, report = scrape()
    accounts = fetch_accounts()
    with data_lock():
        old = read_json("proposals.json", [])
        locked = {p["account_id"] for p in old if p.get("stage") in ("create_in_flight", "link_pending") and p.get("account_id")}
        locked |= {(p.get("result") or {}).get("created_account_id") for p in old if p.get("stage") == "link_pending"}
        locked.discard(None)
        proposals, parent_id = build_proposals(locations, accounts, PARENT_ID, frozenset(locked))
        merged = merge(old, proposals, accounts)
        write_json("accounts.json", accounts)
        write_json("proposals.json", merged)
        write_json("meta.json", {"parent_id": parent_id, "scraped_locations": len(locations), "crm_accounts": len(accounts),
                                 "scrape_report": report, "generated_at": now()})
    counts = {}
    for p in merged:
        k = f"{p['kind']}:{p.get('decision')}"
        counts[k] = counts.get(k, 0) + 1
    unverified = [p["proposal_id"] for p in merged if p.get("decision") == "approved" and not p.get("verified")]
    print(json.dumps({"locations": len(locations), "accounts": len(accounts), "proposals": counts,
                      "scrape_warning": report.get("warning"), "skipped_pages": report["skipped"],
                      "unverified_approved": unverified}, indent=2))


if __name__ == "__main__":
    try:
        run()
    except (PipelineError, HTTPError, URLError, ValueError, OSError) as e:
        print(f"pipeline: {e}", file=sys.stderr)
        sys.exit(1)
