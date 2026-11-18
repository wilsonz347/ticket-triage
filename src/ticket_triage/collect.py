"""Fetch Apache JIRA tickets and store them in a local SQLite database.

The first run fetches every ticket created since START_DATE. Later runs fetch
only tickets updated since the last successful run, minus OVERLAP.

Each page is saved twice: as compressed raw JSON (a backup for reprocessing)
and as rows in the tickets and changes tables. Tickets are replaced by key
(like KAFKA-13575) and their history is rewritten, so re-running is safe.
The last-run time is stored only after every project succeeds, so a failed
run is redone in full next time.

Usage: python -m ticket_triage.collect
"""

import gzip
import http.client
import json
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

SEARCH_URL = "https://issues.apache.org/jira/rest/api/2/search"
PROJECTS = ["KAFKA", "SPARK", "HADOOP", "FLINK", "CASSANDRA"]
START_DATE = "2022-01-01"

# The database file. Raw pages are saved in a "raw" folder next to it.
DB_PATH = Path("data") / "tickets.db"

# Only ask for the fields we use. Otherwise JIRA sends about 120 fields per ticket.
FIELDS = [
    "project",
    "issuetype",
    "priority",
    "status",
    "resolution",
    "components",
    "reporter",
    "summary",
    "description",
    "created",
    "updated",
    "resolutiondate",
]

# 100 tickets per page keeps each response under about 1 MB, edit history included.
PAGE_SIZE = 100

# JIRA reads the dates in our search in its own time zone, which we don't know.
# Going back one extra day covers any time zone difference. Tickets fetched
# twice are simply replaced, so the overlap costs a little time and nothing else.
OVERLAP = timedelta(days=1)

# Wait between requests, to be gentle with Apache's server.
PAUSE_SECONDS = 1

# How many times to try a request before giving up.
MAX_ATTEMPTS = 3


def open_database(db_path=DB_PATH):
    """Open the SQLite database, creating the file and its tables if they don't exist yet.

    Tables:
    - tickets: one row per ticket, with its current values.
    - changes: one row per edited field, for example priority went from Major to Minor.
    - collect_state: small facts about collection, such as when the last run started.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS tickets (
            key         TEXT PRIMARY KEY,
            project     TEXT NOT NULL,
            issue_type  TEXT,
            priority    TEXT,
            status      TEXT,
            resolution  TEXT,
            components  TEXT,   -- JSON list of names, for example ["core", "clients"]
            reporter    TEXT,
            summary     TEXT,
            description TEXT,
            created     TEXT NOT NULL,
            updated     TEXT NOT NULL,
            resolved    TEXT
        );

        CREATE TABLE IF NOT EXISTS changes (
            ticket_key  TEXT NOT NULL REFERENCES tickets (key),
            changed_at  TEXT NOT NULL,
            field       TEXT NOT NULL,
            from_value  TEXT,
            to_value    TEXT
        );

        CREATE INDEX IF NOT EXISTS changes_by_ticket ON changes (ticket_key);

        CREATE TABLE IF NOT EXISTS collect_state (
            name  TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )
    return connection


def read_last_run(connection):
    """Return when the last successful run started, or None if there has never been one."""
    row = connection.execute(
        "SELECT value FROM collect_state WHERE name = 'last_run'"
    ).fetchone()
    if row is None:
        return None
    return datetime.fromisoformat(row[0])


def write_last_run(connection, run_started_at):
    """Record when this run started. Call this only after every project was saved."""
    with connection:
        connection.execute(
            "INSERT OR REPLACE INTO collect_state (name, value) VALUES ('last_run', ?)",
            (run_started_at.isoformat(),),
        )


def build_query(project, since):
    """Build the JIRA search (JQL) for one project.

    since: only tickets updated at or after this time. None means no limit,
    which is what the first run (the backfill) uses.
    """
    query = f'project = {project} AND created >= "{START_DATE}"'

    if since is not None:
        since_text = since.strftime("%Y-%m-%d %H:%M")
        query = query + f' AND updated >= "{since_text}"'

    # Sort by creation time, because it never changes. If we sorted by "updated",
    # a ticket edited while we are paging would move to the end, every ticket
    # after it would shift back one place, and we could skip one.
    # The key breaks ties between tickets created at the same moment.
    # shortcut: a ticket deleted or moved to another project while we page can
    # still shift results and skip one. It's rare; revisit if counts look off.
    query = query + " ORDER BY created ASC, key ASC"
    return query


def fetch_page(query, start_at):
    """Ask JIRA for one page of tickets, starting at position `start_at` in the results.

    Returns JIRA's answer as a dictionary. The parts we use:
    - "issues": the tickets on this page, each with its edit history ("changelog")
    - "total": how many tickets match the search in all

    If the request fails, it waits and tries again, up to MAX_ATTEMPTS times.
    """
    parameters = {
        "jql": query,
        "startAt": start_at,
        "maxResults": PAGE_SIZE,
        "fields": ",".join(FIELDS),
        "expand": "changelog",
    }
    url = SEARCH_URL + "?" + urllib.parse.urlencode(parameters)
    request = urllib.request.Request(url, headers={"Accept": "application/json"})

    attempt = 1
    while True:
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            # A 4xx error means our request is wrong (for example a bad search),
            # so trying again won't help. The exception is 429, "too many requests".
            if error.code < 500 and error.code != 429:
                raise
            problem = error
        except (OSError, http.client.HTTPException, ValueError) as error:
            # The connection failed, timed out, or the answer was cut off.
            problem = error

        if attempt == MAX_ATTEMPTS:
            raise problem
        wait_seconds = 10 * attempt
        print(f"  Request failed ({problem}). Trying again in {wait_seconds} seconds.")
        time.sleep(wait_seconds)
        attempt = attempt + 1


def name_or_none(value):
    """Return value["name"], or None if the value is missing.

    JIRA sends fields like priority and resolution as {"name": "Major", ...},
    or as null when they are empty.
    """
    if value is None:
        return None
    return value["name"]


def parse_ticket(raw):
    """Turn one ticket from JIRA's nested format into a flat dictionary, one value per column."""
    fields = raw["fields"]

    component_names = []
    for component in fields["components"]:
        component_names.append(component["name"])

    return {
        "key": raw["key"],
        "project": fields["project"]["key"],
        "issue_type": name_or_none(fields.get("issuetype")),
        "priority": name_or_none(fields.get("priority")),
        "status": name_or_none(fields.get("status")),
        "resolution": name_or_none(fields.get("resolution")),
        "components": json.dumps(component_names),
        "reporter": name_or_none(fields.get("reporter")),
        "summary": fields.get("summary"),
        "description": fields.get("description"),
        "created": fields["created"],
        "updated": fields["updated"],
        "resolved": fields.get("resolutiondate"),
    }


def parse_changes(raw):
    """List every field edit in a ticket's history, one dictionary per edited field.

    JIRA groups edits made at the same moment into one history entry, so one
    entry can hold several edited fields. We split them into one row each.
    """
    key = raw["key"]
    changelog = raw["changelog"]
    histories = changelog["histories"]

    # shortcut: we only warn when JIRA leaves out part of a long history.
    # If this warning shows up, fetch those tickets' full history separately.
    if changelog["total"] > len(histories):
        print(
            f"  Warning: {key} has {changelog['total']} history entries, "
            f"but JIRA sent only {len(histories)}. Its history is incomplete."
        )

    changes = []
    for history in histories:
        for item in history["items"]:
            changes.append(
                {
                    "ticket_key": key,
                    "changed_at": history["created"],
                    "field": item["field"],
                    "from_value": item.get("fromString"),
                    "to_value": item.get("toString"),
                }
            )
    return changes


def save_tickets(connection, raw_tickets):
    """Save tickets and their edit history to the database.

    Safe to repeat with the same tickets:
    - A ticket that is already stored is replaced, because its key is the primary key.
    - Its stored edit history is deleted and written again, so edits are never doubled.

    The whole page is saved as one transaction: either every ticket is saved, or none is.
    """
    # "with connection" commits at the end, or undoes everything if an error happens.
    with connection:
        for raw in raw_tickets:
            ticket = parse_ticket(raw)
            connection.execute(
                """
                INSERT OR REPLACE INTO tickets (
                    key, project, issue_type, priority, status, resolution, components,
                    reporter, summary, description, created, updated, resolved
                ) VALUES (
                    :key, :project, :issue_type, :priority, :status, :resolution, :components,
                    :reporter, :summary, :description, :created, :updated, :resolved
                )
                """,
                ticket,
            )

            connection.execute("DELETE FROM changes WHERE ticket_key = ?", (ticket["key"],))
            for change in parse_changes(raw):
                connection.execute(
                    """
                    INSERT INTO changes (ticket_key, changed_at, field, from_value, to_value)
                    VALUES (:ticket_key, :changed_at, :field, :from_value, :to_value)
                    """,
                    change,
                )


def save_raw_page(page, file_path):
    """Save one page exactly as JIRA sent it, as compressed JSON."""
    with gzip.open(file_path, "wt", encoding="utf-8") as file:
        json.dump(page, file)


def collect_project(connection, project, since, raw_folder):
    """Fetch every matching ticket for one project and save it, one page at a time.

    Returns how many tickets were saved.
    """
    query = build_query(project, since)
    start_at = 0
    saved = 0

    while True:
        page = fetch_page(query, start_at)
        tickets = page["issues"]
        if len(tickets) == 0:
            break

        save_raw_page(page, raw_folder / f"{project}_{start_at:06d}.json.gz")
        save_tickets(connection, tickets)

        saved = saved + len(tickets)
        print(f"  {project}: {saved} of {page['total']} tickets saved")

        start_at = start_at + len(tickets)
        if start_at >= page["total"]:
            break
        time.sleep(PAUSE_SECONDS)

    return saved


def run(projects=PROJECTS, db_path=DB_PATH):
    """Run one collection: fetch new and changed tickets for every project and save them."""
    run_started_at = datetime.now(timezone.utc)
    connection = open_database(db_path)

    last_run = read_last_run(connection)
    if last_run is None:
        print(f"No previous run found. Fetching all tickets created since {START_DATE}.")
        since = None
    else:
        since = last_run - OVERLAP
        print(f"Last run started at {last_run}. Fetching tickets updated since {since}.")

    raw_folder = Path(db_path).parent / "raw" / run_started_at.strftime("%Y-%m-%dT%H%M%SZ")
    raw_folder.mkdir(parents=True, exist_ok=True)

    total = 0
    try:
        for project in projects:
            print(f"{project}:")
            total = total + collect_project(connection, project, since, raw_folder)

        # We only get here if every project above was saved without an error.
        write_last_run(connection, run_started_at)
    finally:
        connection.close()
    print(f"Done. {total} tickets saved.")


if __name__ == "__main__":
    run()
