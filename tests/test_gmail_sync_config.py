"""Tests for GmailSyncConfig env parsing (cortex-s8sd: actions-queue flag)."""

import pytest

from postmark.services.gmail_sync import GmailSyncConfig

_BASE_ENV = {
    "GCP_PROJECT_ID": "cortex-gmail",
    "PUBSUB_SUBSCRIPTION": "sub",
    "PUBSUB_TOPIC": "topic",
    "GMAIL_TOKEN_PATH": "/tmp/token.json",
    "DUCKDB_API_URL": "http://duckdb:8081",
    "POSTGRES_HOST": "db",
    "POSTGRES_DB": "cortex",
    "POSTGRES_USER": "cortex",
    "POSTGRES_PASSWORD": "secret",
}


def _set_env(monkeypatch: pytest.MonkeyPatch, **overrides: str) -> None:
    for key, value in {**_BASE_ENV, **overrides}.items():
        monkeypatch.setenv(key, value)


def test_actions_queue_disabled_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ACTIONS_QUEUE_ENABLED", raising=False)
    _set_env(monkeypatch)
    assert GmailSyncConfig.from_env().actions_queue_enabled is False


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "Yes"])
def test_actions_queue_enabled_truthy(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    _set_env(monkeypatch, ACTIONS_QUEUE_ENABLED=value)
    assert GmailSyncConfig.from_env().actions_queue_enabled is True


@pytest.mark.parametrize("value", ["", "0", "false", "no", "off"])
def test_actions_queue_enabled_falsy(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    _set_env(monkeypatch, ACTIONS_QUEUE_ENABLED=value)
    assert GmailSyncConfig.from_env().actions_queue_enabled is False
