"""Every service must ensure the shared queue schema on boot.

cortex-rgmk: the exponential-backoff feature was merged months before it first
RAN in production. Its migration existed, was correct, and was a manual CLI
step the deploy flow never invoked -- so two workers crash-looped on
`column "next_attempt_at" does not exist`.

Nothing was wrong with the migration. Nothing called it. That is the failure
this asserts against, so it checks the call is REACHED rather than that the
import is present: an import satisfies a grep and prevents nothing.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _fake_conn() -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchone.return_value = (0,)
    cur.fetchall.return_value = []
    conn.cursor.return_value.__enter__.return_value = cur
    return conn


@pytest.mark.parametrize(
    ("module", "cls", "method"),
    [
        ("postmark.services.parse_worker", "ParseWorker", "_ensure_schema"),
        ("postmark.services.attachment_worker", "AttachmentWorker", "_ensure_schema"),
        ("postmark.services.gmail_sync", "GmailSync", "_ensure_postgres_schema"),
    ],
)
def test_boot_ensures_the_shared_queue_schema(
    module: str, cls: str, method: str
) -> None:
    import importlib

    mod = importlib.import_module(module)
    assert hasattr(mod, cls), f"{module}.{cls} moved; this test needs updating"

    conn = _fake_conn()
    service = object.__new__(getattr(mod, cls))
    service._get_pg_conn = lambda: conn  # type: ignore[attr-defined]

    with patch.object(mod, "ensure_queue_schema") as ensure:
        bound = getattr(getattr(mod, cls), method)
        try:
            # gmail_sync's hook takes the connection; the workers fetch their own.
            (
                bound(service, conn)
                if method == "_ensure_postgres_schema"
                else bound(service)
            )
        except Exception:
            # The service's own DDL runs against a mock and may not complete.
            # What matters is whether the queue schema was ensured first.
            pass

    assert ensure.called, (
        f"{cls}.{method} does not ensure the queue schema -- this is exactly "
        "the shape of cortex-rgmk: a migration nothing calls"
    )
    assert ensure.call_args[0][0] is conn, "ensured on some other connection"
