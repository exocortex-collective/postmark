"""Gmail moving mail in and out of SPAM (cortex-kvyn)."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

import pytest

import postmark.services.gmail_sync as gs
from postmark.services.gmail_sync import GmailSync


class FakeConn:
    """Answers the two lookups the spam paths make; records everything else."""

    def __init__(self, parsed: bool = True, spam: bool = False) -> None:
        self.parsed = parsed
        self.spam = spam
        self.committed = False
        self._row: tuple | None = None

    def cursor(self) -> FakeConn:
        return self

    def __enter__(self) -> FakeConn:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def execute(self, sql: str, params: Any = None) -> None:
        if "FROM emails_parsed" in sql:
            self._row = (1,) if self.parsed else None
        elif "'SPAM' = ANY(label_ids)" in sql:
            self._row = (self.spam,)
        else:
            self._row = None

    def fetchone(self) -> tuple | None:
        return self._row

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        pass


@pytest.fixture
def enqueued(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, dict, dict]]:
    calls: list[tuple[str, dict, dict]] = []
    monkeypatch.setattr(
        gs,
        "enqueue",
        lambda conn, queue, payload, **kw: calls.append((queue, payload, kw)),
    )
    return calls


def make_sync(conn: FakeConn) -> GmailSync:
    sync = GmailSync.__new__(GmailSync)
    sync.config = MagicMock(actions_queue_enabled=False)
    sync._detect_learning_opportunities = MagicMock()  # type: ignore[method-assign]

    @contextmanager
    def get_conn():
        yield conn

    sync._get_conn = get_conn  # type: ignore[method-assign]
    return sync


def test_moved_into_spam_enqueues_labeling(enqueued) -> None:
    conn = FakeConn()
    make_sync(conn)._process_label_event("g1", ["SPAM"], "added")
    assert enqueued == [("labeling", {"gmail_id": "g1"}, {"commit": False})]
    assert conn.committed


def test_moved_out_of_spam_enqueues_triage(enqueued) -> None:
    make_sync(FakeConn(parsed=True))._process_label_event("g1", ["SPAM"], "removed")
    assert [(q, p) for q, p, _ in enqueued] == [("triage", {"gmail_id": "g1"})]


def test_moved_out_of_spam_before_parse_leaves_it_to_the_parse_worker(
    enqueued,
) -> None:
    make_sync(FakeConn(parsed=False))._process_label_event("g1", ["SPAM"], "removed")
    assert enqueued == []


def test_other_label_events_enqueue_nothing(enqueued) -> None:
    make_sync(FakeConn())._process_label_event("g1", ["INBOX", "UNREAD"], "removed")
    assert enqueued == []


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    rec = MagicMock(return_value=True)
    monkeypatch.setattr(gs, "active_label_prefix", lambda conn: "Cortex")
    monkeypatch.setattr(gs, "active_default_label", lambda conn: "Uncategorized")
    monkeypatch.setattr(
        gs, "expected_cortex_labels", lambda conn, gmail_id, prefix=None: {"Cortex/A"}
    )
    monkeypatch.setattr(gs, "record_learning_opportunity", rec)
    return rec


def _learning_sync() -> GmailSync:
    sync = GmailSync.__new__(GmailSync)
    sync._get_label_name = lambda lid: {"L_A": "Cortex/A"}.get(lid)  # type: ignore[method-assign]
    sync._get_sender_addr = lambda conn, gmail_id: "scam@example.com"  # type: ignore[method-assign]
    return sync


def test_label_stripped_from_spam_is_not_a_teaching_signal(recorder) -> None:
    _learning_sync()._detect_learning_opportunities(
        FakeConn(spam=True), "g1", ["L_A"], "removed"
    )
    recorder.assert_not_called()


def test_label_removed_from_non_spam_is_still_a_teaching_signal(recorder) -> None:
    _learning_sync()._detect_learning_opportunities(
        FakeConn(spam=False), "g1", ["L_A"], "removed"
    )
    recorder.assert_called_once()
