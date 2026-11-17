# Writeup draft

I scraped the Bellhaven directory and its linked community pages for each facility's name, address, city, state, ZIP code, care offerings, and source URL. I matched those records to the CRM with a points system. Exact street and ZIP agreement count most, while a conflicting ZIP, state, or house number counts against a match. A correction is proposed only when one account clearly leads. Close calls and conflicting evidence go to review; a listing with no credible match is proposed as a new account. Bellhaven accounts missing from the directory are flagged for review, not treated as closed.

I used the local app to approve 17 high-confidence changes:

- Nine name or parent corrections. The two direct parent moves had either zero outstanding AR or zero lifetime revenue.
- Two new locations.
- One duplicate, marked inactive and linked to its surviving account. No account was deleted.
- Three accounts set to “Needs Review” because they were not in the current directory. Their notes were empty before the update.
- Two CHOW cases.

For both CHOW cases, the account had revenue history and outstanding AR. I left the old account and parent intact, created a new account under Bellhaven, and linked the old record to it.

The directory landing page says there are 35 communities, but its paginated pages link to 34 detail records. I used those 34 pages. The revised pipeline reports a count mismatch and lists any detail page it cannot parse.

The revised code also adds checks before writing, a resumable CHOW link step, protection against duplicate creates, a decision log, and a shared lock between the app and pipeline. These safeguards have not yet been run against the CRM. The 17 changes above were applied with an earlier version. Afterward, I refreshed the CRM account list and verified the new Batavia account in the read-only CRM browser. I have not confirmed that the revised matcher reaches the same decisions.

The matching rules are deterministic and make no LLM calls. I used an AI assistant to help draft and review the code, then checked the recommendations against the exercise instructions and CRM evidence. The app shows the website and CRM records behind each proposal.

The README includes a daily cron entry that refreshes proposals only. It does not approve or write CRM changes. Before submitting, I still need to record the requested demo, add a repository or archive link, enter the actual time spent, and submit the assessment form.
