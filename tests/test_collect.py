from datetime import datetime, timezone

import pytest

from ticket_triage import collect


def make_ticket(key="KAFKA-1", priority="Major", history=None):
    """A small ticket in the same shape JIRA sends."""
    if history is None:
        history = [
            {
                "created": "2022-01-02T10:00:00.000+0000",
                "items": [{"field": "priority", "fromString": "Minor", "toString": "Major"}],
            }
        ]
    return {
        "key": key,
        "fields": {
            "project": {"key": "KAFKA"},
            "issuetype": {"name": "Bug"},
            "priority": {"name": priority},
            "status": {"name": "Open"},
            "resolution": None,
            "components": [{"name": "core"}],
            "reporter": {"name": "someone"},
            "summary": "Something breaks",
            "description": "Details",
            "created": "2022-01-01T09:00:00.000+0000",
            "updated": "2022-01-02T10:00:00.000+0000",
            "resolutiondate": None,
        },
        "changelog": {"total": len(history), "histories": history},
    }


def count_rows(connection, table):
    return connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def test_saving_the_same_tickets_twice_changes_nothing(tmp_path):
    connection = collect.open_database(tmp_path / "tickets.db")
    tickets = [make_ticket("KAFKA-1"), make_ticket("KAFKA-2")]

    collect.save_tickets(connection, tickets)
    collect.save_tickets(connection, tickets)

    assert count_rows(connection, "tickets") == 2
    assert count_rows(connection, "changes") == 2


def test_a_ticket_fetched_again_replaces_the_old_copy(tmp_path):
    connection = collect.open_database(tmp_path / "tickets.db")
    collect.save_tickets(connection, [make_ticket(priority="Major")])

    newer_history = [
        {
            "created": "2022-01-02T10:00:00.000+0000",
            "items": [{"field": "priority", "fromString": "Minor", "toString": "Major"}],
        },
        {
            "created": "2022-01-05T10:00:00.000+0000",
            "items": [{"field": "priority", "fromString": "Major", "toString": "Blocker"}],
        },
    ]
    collect.save_tickets(connection, [make_ticket(priority="Blocker", history=newer_history)])

    priority = connection.execute("SELECT priority FROM tickets WHERE key = 'KAFKA-1'").fetchone()[0]
    assert priority == "Blocker"
    assert count_rows(connection, "changes") == 2


def test_last_run_is_empty_at_first_then_remembered(tmp_path):
    connection = collect.open_database(tmp_path / "tickets.db")
    assert collect.read_last_run(connection) is None

    started = datetime(2026, 11, 18, 6, 0, tzinfo=timezone.utc)
    collect.write_last_run(connection, started)

    assert collect.read_last_run(connection) == started


def test_query_without_since_fetches_everything_since_start_date():
    query = collect.build_query("KAFKA", None)
    assert query == 'project = KAFKA AND created >= "2022-01-01" ORDER BY created ASC, key ASC'


def test_query_with_since_only_fetches_updated_tickets():
    since = datetime(2026, 11, 17, 6, 30, tzinfo=timezone.utc)
    query = collect.build_query("KAFKA", since)
    assert 'AND updated >= "2026-11-17 06:30"' in query


def test_paging_fetches_every_page_and_saves_every_ticket(tmp_path, monkeypatch):
    # Fake JIRA with 5 tickets, sent as pages of 2, 2 and 1.
    pages = {
        0: [make_ticket("KAFKA-1"), make_ticket("KAFKA-2")],
        2: [make_ticket("KAFKA-3"), make_ticket("KAFKA-4")],
        4: [make_ticket("KAFKA-5")],
    }
    asked_for = []

    def fake_fetch_page(query, start_at):
        asked_for.append(start_at)
        return {"issues": pages[start_at], "total": 5}

    monkeypatch.setattr(collect, "fetch_page", fake_fetch_page)
    monkeypatch.setattr(collect, "PAUSE_SECONDS", 0)
    connection = collect.open_database(tmp_path / "tickets.db")
    raw_folder = tmp_path / "raw"
    raw_folder.mkdir()

    saved = collect.collect_project(connection, "KAFKA", None, raw_folder)

    assert saved == 5
    assert asked_for == [0, 2, 4]
    assert count_rows(connection, "tickets") == 5


def test_last_run_is_not_recorded_when_a_project_fails(tmp_path, monkeypatch):
    def fake_fetch_page(query, start_at):
        if "project = SPARK" in query:
            raise ConnectionError("JIRA is down")
        return {"issues": [make_ticket("KAFKA-1")], "total": 1}

    monkeypatch.setattr(collect, "fetch_page", fake_fetch_page)
    db_path = tmp_path / "tickets.db"

    with pytest.raises(ConnectionError):
        collect.run(projects=["KAFKA", "SPARK"], db_path=db_path)

    connection = collect.open_database(db_path)
    assert collect.read_last_run(connection) is None
    assert count_rows(connection, "tickets") == 1
