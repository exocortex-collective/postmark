"""Tests for inline learning-opportunity detection in gmail_sync (cortex-uo9b.1)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import postmark.services.gmail_sync as gs
from postmark.services.gmail_sync import GmailSync


def make_sync(
    label_map: dict[str, str], sender: str | None = "bob@example.com"
) -> GmailSync:
    """Build a bare GmailSync with the label/sender lookups stubbed."""
    sync = GmailSync.__new__(GmailSync)
    sync._get_label_name = lambda lid: label_map.get(lid)  # type: ignore[method-assign]
    sync._get_sender_addr = lambda conn, gmail_id: sender  # type: ignore[method-assign]
    return sync


@pytest.fixture
def patched(monkeypatch: pytest.MonkeyPatch):
    """Patch the cortex-utils helpers used by the detector; return (recorder, set_expected)."""
    rec = MagicMock(return_value=True)
    monkeypatch.setattr(gs, "active_label_prefix", lambda conn: "Cortex")
    monkeypatch.setattr(gs, "active_default_label", lambda conn: "Uncategorized")
    monkeypatch.setattr(gs, "record_learning_opportunity", rec)

    def set_expected(value: set[str] | None) -> None:
        monkeypatch.setattr(
            gs, "expected_cortex_labels", lambda conn, gmail_id, prefix=None: value
        )

    return rec, set_expected


def test_added_divergent_records(patched) -> None:
    rec, set_expected = patched
    set_expected({"Cortex/A"})
    sync = make_sync({"L_B": "Cortex/B"})
    sync._detect_learning_opportunities(MagicMock(), "g1", ["L_B"], "added")
    rec.assert_called_once()
    kwargs = rec.call_args.kwargs
    assert kwargs["label"] == "Cortex/B"
    assert kwargs["direction"] == gs.ADD
    assert kwargs["sender"] == "bob@example.com"
    assert kwargs["expected_labels"] == {"Cortex/A"}


def test_added_aligned_machine_label_ignored(patched) -> None:
    # Machine-applied label matches the classification -> aligns -> no signal.
    rec, set_expected = patched
    set_expected({"Cortex/A"})
    sync = make_sync({"L_A": "Cortex/A"})
    sync._detect_learning_opportunities(MagicMock(), "g1", ["L_A"], "added")
    rec.assert_not_called()


def test_not_triaged_is_noise(patched) -> None:
    rec, set_expected = patched
    set_expected(None)  # no classification yet
    sync = make_sync({"L_B": "Cortex/B"})
    sync._detect_learning_opportunities(MagicMock(), "g1", ["L_B"], "added")
    rec.assert_not_called()


def test_removed_previously_assigned_records(patched) -> None:
    rec, set_expected = patched
    set_expected({"Cortex/A"})
    sync = make_sync({"L_A": "Cortex/A"})
    sync._detect_learning_opportunities(MagicMock(), "g1", ["L_A"], "removed")
    rec.assert_called_once()
    assert rec.call_args.kwargs["direction"] == gs.REMOVE


def test_removed_not_assigned_ignored(patched) -> None:
    rec, set_expected = patched
    set_expected({"Cortex/A"})
    sync = make_sync({"L_B": "Cortex/B"})
    sync._detect_learning_opportunities(MagicMock(), "g1", ["L_B"], "removed")
    rec.assert_not_called()


def test_removed_default_label_ignored(patched) -> None:
    # Removing the default label (Cortex/Uncategorized) is automatic
    # reconciliation when a real label is applied, not a teaching signal —
    # so it must not generate a "stop labeling as Uncategorized" proposal.
    rec, set_expected = patched
    set_expected({"Cortex/Uncategorized"})
    sync = make_sync({"L_U": "Cortex/Uncategorized"})
    sync._detect_learning_opportunities(MagicMock(), "g1", ["L_U"], "removed")
    rec.assert_not_called()


def test_removed_default_and_real_label_records_only_real(patched) -> None:
    # A single event removing both the default label and a real one: the filter
    # is surgical — it drops only the default, still recording the real remove.
    rec, set_expected = patched
    set_expected({"Cortex/Uncategorized", "Cortex/A"})
    sync = make_sync({"L_U": "Cortex/Uncategorized", "L_A": "Cortex/A"})
    sync._detect_learning_opportunities(MagicMock(), "g1", ["L_U", "L_A"], "removed")
    rec.assert_called_once()
    assert rec.call_args.kwargs["label"] == "Cortex/A"


def test_removed_default_label_empty_prefix_ignored(
    patched, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Empty prefix (manage all): default_full is the bare default name, and its
    # removal is still skipped — exercises the `else default` branch.
    rec, set_expected = patched
    monkeypatch.setattr(gs, "active_label_prefix", lambda conn: "")
    set_expected({"Uncategorized"})
    sync = make_sync({"L_U": "Uncategorized"})
    sync._detect_learning_opportunities(MagicMock(), "g1", ["L_U"], "removed")
    rec.assert_not_called()


def test_non_managed_label_ignored(patched) -> None:
    # A human labelling with a non-Cortex label is not a teaching signal.
    rec, set_expected = patched
    set_expected({"Cortex/A"})
    sync = make_sync({"L_P": "Personal/X"})
    sync._detect_learning_opportunities(MagicMock(), "g1", ["L_P"], "added")
    rec.assert_not_called()


def test_unknown_label_id_skipped(patched) -> None:
    rec, set_expected = patched
    set_expected({"Cortex/A"})
    sync = make_sync({})  # _get_label_name returns None
    sync._detect_learning_opportunities(MagicMock(), "g1", ["L_missing"], "added")
    rec.assert_not_called()


def test_get_sender_addr_returns_from_addr() -> None:
    sync = GmailSync.__new__(GmailSync)
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = ("bob@example.com",)
    conn.cursor.return_value.__enter__.return_value = cur
    assert sync._get_sender_addr(conn, "g1") == "bob@example.com"


def test_get_sender_addr_none_when_absent() -> None:
    sync = GmailSync.__new__(GmailSync)
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = None
    conn.cursor.return_value.__enter__.return_value = cur
    assert sync._get_sender_addr(conn, "g1") is None
