"""Live-Postgres tests for gmail-sync's spam label events (cortex-kvyn).

The fake connection in test_spam_label_events.py matches statement text, so it
passes an inverted SPAM predicate or a broken parsed-check. These drive
_process_label_event against real tables and the real queue, through the real
cortex-utils enqueue.

Skipped unless CORTEX_TEST_DSN points at a throwaway Postgres; CI starts one.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import psycopg2
import pytest
from cortex_utils.queue.schema import queue_ddl

from postmark.services.gmail_sync import GmailSync

DSN = os.environ.get("CORTEX_TEST_DSN")

pytestmark = pytest.mark.skipif(
    not DSN, reason="set CORTEX_TEST_DSN to a throwaway Postgres to run these"
)

SCHEMA = """
CREATE TABLE emails_raw (gmail_id TEXT PRIMARY KEY, label_ids TEXT[]);
CREATE TABLE emails_parsed (gmail_id TEXT PRIMARY KEY);
INSERT INTO emails_raw VALUES ('inbox', '{INBOX}'), ('spam', '{SPAM,UNREAD}');
INSERT INTO emails_parsed VALUES ('inbox'), ('spam');
"""


@pytest.fixture
def conn() -> Iterator[Any]:
    c = psycopg2.connect(DSN)
    c.autocommit = True
    with c.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS t_pm_spam CASCADE")
        cur.execute("CREATE SCHEMA t_pm_spam")
        cur.execute("SET search_path = t_pm_spam")
        cur.execute(queue_ddl())
        cur.execute(
            "CREATE TABLE queue_today PARTITION OF queue "
            "FOR VALUES FROM (CURRENT_DATE) TO (CURRENT_DATE + 1)"
        )
        cur.execute(SCHEMA)
    c.autocommit = False
    try:
        yield c
    finally:
        c.rollback()
        c.autocommit = True
        with c.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS t_pm_spam CASCADE")
        c.close()


def _sync(conn: Any) -> GmailSync:
    sync = GmailSync.__new__(GmailSync)
    sync.config = MagicMock(actions_queue_enabled=False)
    sync._detect_learning_opportunities = MagicMock()  # type: ignore[method-assign]

    @contextmanager
    def get_conn() -> Iterator[Any]:
        yield conn

    sync._get_conn = get_conn  # type: ignore[method-assign]
    return sync


def _jobs(conn: Any) -> list[tuple[str, str]]:
    with conn.cursor() as cur:
        cur.execute("SELECT queue_name, payload->>'gmail_id' FROM queue ORDER BY id")
        return cur.fetchall()


def _labels(conn: Any, gmail_id: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT label_ids FROM emails_raw WHERE gmail_id = %s", (gmail_id,))
        return sorted(cur.fetchone()[0])


def test_into_spam_records_labels_and_enqueues_labeling(conn: Any) -> None:
    _sync(conn)._process_label_event("inbox", ["SPAM", "UNREAD"], "added")
    assert _labels(conn, "inbox") == ["INBOX", "SPAM", "UNREAD"]
    assert _jobs(conn) == [("labeling", "inbox")]


def test_a_job_already_processing_does_not_swallow_the_transition(conn: Any) -> None:
    # That job may have read the labels from before SPAM was added.
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO queue (queue_name, payload, status)"
            " VALUES ('labeling', '{\"gmail_id\": \"inbox\"}', 'processing')"
        )
    conn.commit()
    _sync(conn)._process_label_event("inbox", ["SPAM"], "added")
    assert _jobs(conn) == [("labeling", "inbox"), ("labeling", "inbox")]


def test_a_failed_label_event_rolls_back_its_job(conn: Any) -> None:
    sync = _sync(conn)
    sync._detect_learning_opportunities = MagicMock(  # type: ignore[method-assign]
        side_effect=psycopg2.OperationalError("boom")
    )
    with pytest.raises(psycopg2.OperationalError):
        sync._process_label_event("inbox", ["SPAM"], "added")
    assert _labels(conn, "inbox") == ["INBOX"]
    assert _jobs(conn) == []


def test_out_of_spam_enqueues_triage(conn: Any) -> None:
    _sync(conn)._process_label_event("spam", ["SPAM"], "removed")
    assert _labels(conn, "spam") == ["UNREAD"]
    assert _jobs(conn) == [("triage", "spam")]


def test_out_of_spam_before_parse_enqueues_nothing(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute("DELETE FROM emails_parsed WHERE gmail_id = 'spam'")
    conn.commit()
    _sync(conn)._process_label_event("spam", ["SPAM"], "removed")
    assert _jobs(conn) == []


def test_is_spam_reads_current_labels(conn: Any) -> None:
    sync = _sync(conn)
    assert sync._is_spam(conn, "spam") is True
    assert sync._is_spam(conn, "inbox") is False
    assert sync._is_spam(conn, "missing") is False
