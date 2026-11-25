#!/usr/bin/env python3
"""Local evidence-first review queue. CRM writes only happen after a reviewer approves a proposal."""
import html
import json
import secrets
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs

import pipeline
from pipeline import (PipelineError, append_log, data_lock, get_account, norm, norm_zip, now, read_json,
                      record_id, request, unwrap, verify, write_json)

HOST, PORT = "127.0.0.1", 8000
CSRF = secrets.token_urlsafe(24)   # new per server start; forms must echo it


class Drift(PipelineError):
    pass


def safe(v):
    return html.escape(str(v if v is not None else ""))


# ---------------------------------------------------------------- CRM writes

def check_precondition(p):
    """Refuse to write if the account changed since the proposal's evidence was captured."""
    if not p.get("precondition"):
        return
    current = get_account(p["account_id"])
    if current is None:
        raise Drift(f"Account {p['account_id']} no longer exists. Rerun the pipeline.")
    diffs = [k for k, v in p["precondition"].items() if (current.get(k) or "") != (v or "")]
    if diffs:
        raise Drift(f"Account {p['account_id']} changed since this proposal was generated ({', '.join(diffs)}). "
                    "Rerun the pipeline to refresh the evidence.")


def find_existing(payload):
    """An account that already matches a create payload (name, street, ZIP, parent)."""
    for acc in pipeline.fetch_accounts():
        if (norm(acc.get("name")) == norm(payload["name"]) and norm(acc.get("billing_street")) == norm(payload["billing_street"])
                and norm_zip(acc.get("billing_zip")) == norm_zip(payload["billing_zip"])
                and acc.get("parent_id") == payload["parent_id"]):
            return acc
    return None


def create_account(p, persist, next_stage=None):
    """Create once. The proposal is marked in-flight before the POST, so a retry searches before re-posting."""
    payload = p["changes"]["create_account"]
    nid, response = None, None
    if p.get("stage") == "create_in_flight":
        existing = find_existing(payload)
        if existing:
            nid, response = existing["account_id"], {"adopted_existing": True}
    if not nid:
        p["stage"] = "create_in_flight"
        persist()
        try:
            response = unwrap(request(pipeline.API + "/accounts", api=True, method="POST", payload=payload))
        except HTTPError as e:
            if 400 <= e.code < 500 and e.code not in (408, 409, 429):
                p["stage"] = None   # the CRM rejected it; nothing was created
                persist()
            raise
        nid = record_id(response)
        if not nid:
            existing = find_existing(payload)
            if not existing:
                raise PipelineError("Create response had no account id and no matching account was found. "
                                    "Check the CRM before approving again; a retry will search first.")
            nid = existing["account_id"]
    p["result"] = {"created_account_id": nid, "create_response": response}
    p["stage"] = next_stage
    persist()
    return nid


def apply(p, persist):
    action = p["action"]
    if action in ("none", "review"):
        # Review-only proposals record a local decision; an empty CRM PATCH is invalid.
        return {"result": "no CRM change"}
    if action == "update":
        check_precondition(p)
        resp = request(f"{pipeline.API}/accounts/{p['account_id']}", api=True, method="PATCH", payload=p["changes"])
        return {"response": unwrap(resp)}
    if action == "create":
        create_account(p, persist)
        return p["result"]
    if action == "chow":
        if p.get("stage") != "link_pending":
            check_precondition(p)
            create_account(p, persist, next_stage="link_pending")
        nid = p["result"]["created_account_id"]
        resp = request(f"{pipeline.API}/accounts/{p['account_id']}", api=True, method="PATCH",
                       payload={"chow_current_account": nid})
        p["stage"] = None
        return {**p["result"], "link_response": unwrap(resp)}
    raise PipelineError(f"Unknown action {action!r}")


# ------------------------------------------------------------------------- UI

def describe(p):
    ch = p.get("changes", {})
    out = []
    if p["action"] == "chow":
        c = ch["create_account"]
        out.append(f"Create new account “{c['name']}” under Bellhaven, then set chow_current_account on "
                   f"{p['account_id']} to the new id. The old account, its parent and billing stay as they are.")
    elif "create_account" in ch:
        c = ch["create_account"]
        out.append(f"Create account “{c['name']}” under Bellhaven at {c['billing_street']}, {c['billing_city']}, "
                   f"{c['billing_state']} {c['billing_zip']}.")
    if ch.get("parent_id"):
        out.append("Move parent to Bellhaven.")
    if ch.get("name"):
        out.append(f"Rename to “{ch['name']}”.")
    if ch.get("duplicate_of_account"):
        out.append(f"Mark as duplicate of {ch['duplicate_of_account']}.")
    if ch.get("status"):
        out.append(f"Set status to {ch['status']}.")
    if ch.get("note"):
        out.append("Add note (existing note is kept).")
    return " ".join(out) or "No CRM change. Marking this reviewed only records your decision."


def approve_label(p):
    if p["action"] == "none":
        return "Mark reviewed (no CRM change)"
    if p.get("stage") == "link_pending":
        return "Retry link only"
    if p.get("stage") == "create_in_flight":
        return "Check CRM and finish create"
    return {"chow": "Approve: create and link", "create": "Approve: create account"}.get(p["action"], "Approve change")


def money(v):
    return "missing" if v is None or v == "" else f"${safe(v)}"


def card(p):
    e = p.get("evidence", {})
    crm, site = e.get("crm"), e.get("website")
    blocks = []
    if site:
        blocks.append(f"<div><b>Website</b><br>{safe(site.get('name'))}<br>{safe(site.get('street'))}, {safe(site.get('city'))}, "
                      f"{safe(site.get('state'))} {safe(site.get('zip'))}<br>Care: {safe(site.get('care_type'))}<br>"
                      f"<a target='_blank' rel='noreferrer' href='{safe(site.get('source_url'))}'>Open source listing</a></div>")
    if crm:
        b = e.get("billing_review") or {}
        blocks.append(f"<div><b>CRM</b><br>{safe(crm.get('name'))}<br>{safe(crm.get('billing_street'))}, {safe(crm.get('billing_city'))}, "
                      f"{safe(crm.get('billing_state'))} {safe(crm.get('billing_zip'))}<br>Parent: {safe(crm.get('parent_name') or crm.get('parent_id'))}"
                      f"<br>Status: {safe(crm.get('status'))}<br>Lifetime revenue: {money(b.get('lifetime_revenue', crm.get('lifetime_revenue')))} · "
                      f"Outstanding AR: {money(b.get('outstanding_ar', crm.get('outstanding_ar')))}<br>Account ID: {safe(crm.get('account_id'))}</div>")
    else:
        blocks.append("<div><b>CRM</b><br>No account matched with enough confidence.</div>")
    if e.get("duplicate_survivor"):
        s = e["duplicate_survivor"]
        blocks.append(f"<div><b>Surviving account</b><br>{safe(s.get('name'))}<br>{safe(s.get('billing_street'))} {safe(s.get('billing_zip'))}"
                      f"<br>Parent: {safe(s.get('parent_id'))}</div>")
    if e.get("other_candidates"):
        blocks.append("<div><b>Other candidates</b><br>" + "<br>".join(
            f"{safe(c.get('name'))} · {safe(c.get('account_id'))} (score {safe(c.get('score'))})" for c in e["other_candidates"]) + "</div>")
    if e.get("match_reasons"):
        blocks.append("<div><b>Match evidence</b><br>" + "<br>".join(safe(x) for x in e["match_reasons"]) + "</div>")
    alert = ""
    if e.get("review_reason"):
        alert += f"<p class='alert'>{safe(e['review_reason'])}</p>"
    if p.get("stage") == "link_pending":
        alert += (f"<p class='alert'>New account {safe((p.get('result') or {}).get('created_account_id'))} was created, "
                  "but the old account is not linked yet. Approving retries the link only.</p>")
    if p.get("stage") == "create_in_flight":
        alert += ("<p class='alert'>A create request was sent but its outcome is unknown. Approving searches the CRM "
                  "for the account first and creates it only if it is not there.</p>")
    score = e.get("match_score", 0) or 0
    return (f"<article class='card'><div class='eyebrow'>{safe(p['kind'].replace('_', ' '))}"
            f"<span>Match score {safe(min(score, 100))}</span></div><h2>{safe(p['title'])}</h2>{alert}"
            f"<div class='evidence'>{''.join(blocks)}</div><p class='change'>{safe(describe(p))}</p>"
            f"<form method='post' action='/decision'><input type='hidden' name='csrf' value='{CSRF}'>"
            f"<input type='hidden' name='id' value='{safe(p['proposal_id'])}'>"
            f"<button name='decision' value='approved'>{safe(approve_label(p))}</button>"
            + ("" if p.get("stage") else "<button class='secondary' name='decision' value='rejected'>Reject</button>")
            + "</form></article>")


def history_rows(items):
    rows = []
    for p in sorted((x for x in items if x.get("decision") in ("approved", "rejected")),
                    key=lambda x: x.get("decided_at") or "", reverse=True):
        if p["decision"] == "rejected":
            v = "—"
        elif p.get("verified") is None:
            v = "Not checked yet (rerun the pipeline)"
        else:
            v = ("✓ " if p["verified"] else "✗ ") + (p.get("verified_detail") or "")
        rows.append(f"<tr><td>{safe(p.get('decided_at'))}</td><td>{safe(p['kind'].replace('_', ' '))}</td><td>{safe(p['title'])}</td>"
                    f"<td>{safe(p.get('account_id') or (p.get('result') or {}).get('created_account_id'))}</td>"
                    f"<td>{safe(p['decision'])}</td><td>{safe(v)}</td></tr>")
    if not rows:
        return "<p class='muted'>No decisions yet.</p>"
    return ("<table><thead><tr><th>Decided</th><th>Type</th><th>Facility</th><th>Account</th><th>Decision</th>"
            "<th>CRM check</th></tr></thead><tbody>" + "".join(rows) + "</tbody></table>")


CSS = """:root{--ink:#17211d;--muted:#64716b;--line:#e2e9e5;--green:#17664a;--pale:#f2f7f4;--gold:#e9b949}*{box-sizing:border-box}
body{margin:0;background:#f7f9f8;color:var(--ink);font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
header{background:#123e31;color:white;padding:24px max(calc((100% - 1100px)/2),24px)}header small{color:#cfe1d7;text-transform:uppercase;letter-spacing:.12em;font-weight:700}
h1{margin:4px 0 0;font-size:28px}main{max-width:1100px;margin:30px auto;padding:0 20px}.summary{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:22px}
.pill{padding:12px 18px;background:white;border:1px solid var(--line);border-radius:10px;color:var(--muted)}.pill b{color:var(--ink);font-size:20px;margin-right:6px}
.notice,.alert{background:#fff8e5;border:1px solid #f2dfab;border-radius:10px;padding:12px 16px;margin:12px 0}
.card{background:white;border:1px solid var(--line);border-radius:14px;padding:22px;margin:16px 0;box-shadow:0 2px 8px #102f2010}
.eyebrow{color:var(--green);text-transform:uppercase;font-size:11px;font-weight:800;letter-spacing:.09em}.eyebrow span{float:right;color:var(--muted)}
h2{font-size:21px;margin:5px 0 15px}.evidence{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px}
.evidence>div{background:var(--pale);border-radius:9px;padding:13px;font-size:13px;overflow-wrap:anywhere}a{color:var(--green)}
.change{background:#fafbf9;border-left:3px solid var(--gold);padding:10px 13px;margin:16px 0;font-weight:600;font-size:13px}
button{border:0;background:var(--green);color:white;padding:10px 15px;border-radius:7px;font-weight:700;cursor:pointer;margin-right:8px}
button.secondary{background:white;color:#485650;border:1px solid #cbd5cf}.empty{background:white;border:1px solid var(--line);padding:28px;border-radius:12px;color:var(--muted)}
table{width:100%;border-collapse:collapse;background:white;font-size:13px}th,td{text-align:left;padding:8px 10px;border-bottom:1px solid var(--line);vertical-align:top}
.muted,footer{color:var(--muted);font-size:12px}footer{padding:24px 0}"""


def render():
    items = read_json("proposals.json", [])
    meta = read_json("meta.json", {})
    counts = {s: sum(p.get("decision") == s for p in items) for s in ("pending", "approved", "rejected")}
    unverified = sum(1 for p in items if p.get("decision") == "approved" and p.get("verified") is False)
    pending = [p for p in items if p.get("decision") == "pending"]
    pending.sort(key=lambda p: (not p.get("stage"), p["kind"] == "needs_review", p["title"]))
    cards = "".join(card(p) for p in pending) or "<div class='empty'>Nothing awaiting review. Run <code>python3 pipeline.py</code> to refresh the queue.</div>"
    report = meta.get("scrape_report") or {}
    warn = ""
    if report.get("warning") or report.get("skipped"):
        skipped = "".join(f"<li>{safe(s['url'])}: {safe(s['reason'])}</li>" for s in report.get("skipped", []))
        warn = f"<div class='notice'><b>Scrape check.</b> {safe(report.get('warning') or '')}<ul>{skipped}</ul></div>"
    pills = "".join(f"<div class='pill'><b>{safe(v)}</b> {safe(k)}</div>" for k, v in [
        ("awaiting review", counts["pending"]), ("approved", counts["approved"]), ("rejected", counts["rejected"]),
        ("approved but not confirmed in CRM", unverified), ("website locations", meta.get("scraped_locations", 0)),
        ("CRM accounts", meta.get("crm_accounts", 0))])
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Bellhaven Review</title><style>{CSS}</style></head><body><header><small>Clipboard Health · Analyst Assessment</small>
<h1>Bellhaven ownership review</h1></header><main><div class="summary">{pills}</div>
<div class="notice"><b>Every CRM write needs your approval.</b> Check the source listing and billing figures before approving.
A CHOW proposal creates the new Bellhaven account first, then links the preserved account to it.
Each write re-reads the account first and stops if it changed since the queue was built.</div>{warn}
{cards}<h2 style="margin-top:40px">Decision history</h2>{history_rows(items)}
<footer>Queue generated {safe(meta.get('generated_at', ''))}. Each pipeline refresh re-checks approved changes against the CRM
(the “CRM check” column). Every decision is also appended to data/decisions.jsonl.</footer></main></body></html>"""


def message_page(title, text, code):
    return code, (f"<!doctype html><meta charset='utf-8'><title>{safe(title)}</title><style>{CSS}</style><main>"
                  f"<div class='card'><h2>{safe(title)}</h2><p>{safe(text)}</p><p><a href='/'>Back to the queue</a></p></div></main>")


def decide(pid, decision):
    with data_lock():
        items = read_json("proposals.json", [])
        p = next((x for x in items if x["proposal_id"] == pid), None)
        if not p or p.get("decision") != "pending" or decision not in ("approved", "rejected"):
            return message_page("Already decided", "This proposal is missing or was already decided.", 409)
        if decision == "rejected" and p.get("stage"):
            return message_page("Write in progress", "This proposal has a partly finished CRM write; finish it first.", 409)

        def persist():
            write_json("proposals.json", items)

        log = {"at": now(), "proposal_id": pid, "kind": p["kind"], "action": p["action"], "account_id": p.get("account_id"),
               "decision": decision, "changes": p.get("changes")}
        if decision == "approved":
            try:
                p["result"] = apply(p, persist)
            except Drift as e:
                append_log("decisions.jsonl", {**log, "outcome": "blocked", "error": str(e)})
                persist()
                return message_page("Nothing was written", str(e), 409)
            except (PipelineError, HTTPError, URLError, OSError, ValueError, KeyError) as e:
                detail = f"HTTP {e.code}: {e.read().decode('utf-8', 'replace')[:300]}" if isinstance(e, HTTPError) else str(e)
                append_log("decisions.jsonl", {**log, "outcome": "failed", "stage": p.get("stage"), "error": detail})
                persist()
                return message_page("CRM write failed", detail, 502)
        p["decision"], p["decided_at"] = decision, now()
        if decision == "approved" and p["action"] in ("update", "create", "chow"):
            try:
                acc_ids = [p.get("account_id"), (p.get("result") or {}).get("created_account_id")]
                by_id = {i: get_account(i) for i in acc_ids if i}
                ok, detail = verify(p, {k: v for k, v in by_id.items() if v})
                p["verified"], p["verified_detail"], p["verified_at"] = ok, detail, now()
            except Exception as e:  # the write succeeded; a failed read-back is reported, not fatal
                p["verified"], p["verified_detail"] = None, f"Read-back failed: {e}"
        persist()
        append_log("decisions.jsonl", {**log, "outcome": "ok", "result": p.get("result"), "verified": p.get("verified")})
    return None


class Handler(BaseHTTPRequestHandler):
    def _send(self, code, body, ctype="text/html; charset=utf-8"):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        self.wfile.write(body.encode())

    def do_GET(self):
        if self.path != "/":
            self.send_error(404)
            return
        self._send(200, render())

    def do_POST(self):
        if self.path != "/decision":
            self.send_error(404)
            return
        if self.headers.get("Host") not in (f"{HOST}:{PORT}", f"localhost:{PORT}"):
            self.send_error(403, "Unexpected Host header")
            return
        length = min(int(self.headers.get("Content-Length", 0) or 0), 10_000)
        form = parse_qs(self.rfile.read(length).decode())
        if not secrets.compare_digest(form.get("csrf", [""])[0], CSRF):
            self._send(*message_page("Form expired", "The app was restarted. Reload the queue and try again.", 403))
            return
        try:
            outcome = decide(form.get("id", [""])[0], form.get("decision", [""])[0])
        except PipelineError as e:   # e.g. data lock held by a running pipeline
            outcome = message_page("Try again", str(e), 503)
        if outcome:
            code, body = outcome
            self._send(code, body)
            return
        self.send_response(303)
        self.send_header("Location", "/")
        self.end_headers()

    def log_message(self, fmt, *args):
        print("review:", fmt % args)


if __name__ == "__main__":
    pipeline.configure()
    print(f"Review app: http://{HOST}:{PORT}")
    HTTPServer((HOST, PORT), Handler).serve_forever()
