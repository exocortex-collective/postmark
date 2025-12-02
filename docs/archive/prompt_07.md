**Context**
- Dedup logic complete.
- Need lightweight threading.

**Task**
Add `postmark/email/threading.py` with `detect_thread(email, resolver)`:
1. Try `In‑Reply‑To` / `References`.
2. Fallback: normalised subject equality (strip “Re: ”, “Fwd: ”).
3. Fallback: emails within 7‑day window same subject.

`resolver(message_id) -> Optional[Email]` is an injected callback to look up existing messages.

**Tests**
- Fixture set of three reply emails forms same thread id.
- Edge case: same subject after 30 days → new thread.

Write the code and the tests.
