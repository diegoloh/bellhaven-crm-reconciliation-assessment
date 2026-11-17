# Bellhaven CRM reconciliation

This project collects Bellhaven's public community listings, compares them with the assessment CRM, and puts proposed changes in a local review queue. The pipeline makes no CRM writes. The app sends a change only after a reviewer approves it.

## Run it

Requires Python 3.10 or later. No packages need to be installed.

1. Copy `.env.example` to `.env` and add the CRM token from your assessment workspace. Keep `.env` private; Git ignores it.
2. Run `python3 pipeline.py` to scrape the site, read the CRM and refresh the queue.
3. Run `python3 app.py` to start the review app.
4. Open `http://127.0.0.1:8000`. Review each proposal's website and CRM evidence before deciding.

Approving a proposal sends the CRM request shown on its card. Rejecting one records the decision and changes nothing. Pipeline data is stored in the local, Git-ignored `data/` directory.

## What gets checked

**Scraping.** The pipeline follows every link to a `/communities/<slug>` page, whether it's relative or absolute and wherever it sits on the listing page. It reads the name, address, city, state, ZIP, care offerings and source URL from each detail page. Addresses with a suite line are kept. Any page that can't be parsed is listed with a reason in `data/scrape_report.json` and at the top of the app, rather than being dropped silently. If the count shown on the directory page differs from the number of links found, the pipeline warns about it.

**Reading the CRM.** Every account page is read. The run stops with an error if the response has no total, if a page repeats earlier accounts, or if the number of unique accounts doesn't match the total. That way a partial account list can never produce proposals.

**Matching.** Each listing is scored against each CRM account:

| Evidence | Points |
|---|---|
| ZIP matches / conflicts | +40 / −40 |
| State matches / conflicts | +10 / −50 |
| City matches | +10 |
| Same house number and street | +60 |
| Same house number, street name nearly matches | +40 |
| House number conflicts | −40 |
| Facility name matches / overlaps | +40 / +15 |

ZIPs are compared on the first five digits, with leading zeros restored. States are compared as two-letter codes, so full state names work too. A correction is proposed only when the top score is at least 100 and at least 25 points ahead of the next different account. Everything else that scores 30 or more goes to review with the reason shown. A listing with no candidate is proposed as a new account. The Bellhaven parent account, other parent accounts, and accounts already marked as a duplicate or CHOW-linked are never candidates.

**CHOW rule.** When the matched account needs a parent change:

- If lifetime revenue and outstanding AR are both positive, the proposal creates a new account under Bellhaven, then sets `chow_current_account` on the old account to the new ID. The old account's parent and billing aren't touched.
- If either value is zero, the existing account is re-parented.
- If either value is missing or not a number, the item goes to review. Missing data is never treated as zero.

**Duplicates.** A duplicate is proposed only when the facility name, normalized street and ZIP match. The Bellhaven-linked account is preferred as the survivor, followed by a CHOW target and then the oldest ID. The other record is proposed for `duplicate_of_account` and `Inactive`, with an explanatory note. If it has positive revenue and AR, or either billing value is missing, it goes to review instead because it may be a pre-CHOW account.

**Not listed.** A Bellhaven account that no listing matched is proposed as `Needs Review`. Absence from the directory isn't treated as proof of closure. The explanation is added to the account's existing note, not written over it.

## Safety on approval and rerun

- **Checks before writing.** The app re-reads the account first. If its name, parent, status or link fields changed since the queue was built, nothing is written and you're asked to refresh.
- **Creates happen once.** A proposal is marked in-flight before the create request is sent. If the outcome is unclear (a timeout, or a response without an ID), the next approval searches the CRM for the account and only creates it if it isn't there.
- **CHOW is resumable.** The new account's ID is saved before the link step. If the link fails, the card offers "Retry link only", and the pipeline leaves both accounts alone until the link is done.
- **Writes are confirmed.** After each write, and on every pipeline refresh, approved proposals are checked against the current CRM. The result appears in the app's decision history.
- **Decisions survive reruns.** A decision is kept as long as the fields it depends on haven't changed. Every decision, failure and blocked write is appended to `data/decisions.jsonl`.
- **No lost writes.** `pipeline.py` and `app.py` share a lock on `data/`, and files are written atomically, so a refresh can't overwrite decisions made at the same moment.
- **Retries only for reads.** GET requests are retried on network errors, 429 and 5xx. Write requests are never retried automatically.

The pipeline uses deterministic rules and makes no LLM calls.

## Daily run

This cron entry refreshes proposals every day at 12:17 UTC. It never approves or writes CRM changes:

```cron
CRON_TZ=UTC
17 12 * * * cd /path/to/bellhaven-reconciliation && mkdir -p data && /usr/bin/env python3 pipeline.py >> data/pipeline.log 2>&1
```

`CRON_TZ` makes cron use UTC instead of the machine's local time. `mkdir -p data` is needed because the log folder doesn't exist in a fresh clone. On macOS, check that `python3` on cron's PATH is 3.10 or later. If a refresh starts while you're approving something in the app, it waits for the lock.

For a hosted schedule, store the token as a secret and keep the job proposal-only. CRM changes should still require a reviewer.

## Assessment snapshot

The directory landing page says there are 35 communities, but its pages link to 34 detail records. This run used those 34 records. The initial CRM snapshot had 121 accounts. Seventeen high-confidence decisions were applied, and the refreshed CRM had 125 accounts. Six ambiguous matches remained for review.

Those decisions were made with an earlier version of the app, before the locking and decision log described above. Its local queue history was lost during a refresh. I refreshed the CRM account list and verified the new Batavia account in the read-only CRM browser. I have not run this revised version of the pipeline against the CRM, so its new safeguards still need a live check.

## Before submitting

Record the short walkthrough requested in the assessment, add a public repository or shared archive link, enter the actual time spent, and submit the form. Don't show the token or `.env` file in the recording. The form has not been submitted.
