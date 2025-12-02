**Context**
- Storage layers done.
- Need to convert raw EML into structured objects.

**Task**
1. Add `postmark/email/parser.py`:
   - `Email` dataclass (uuid, message_id, date, subject, from_addr, body_text, body_html, attachments).
   - `parse_eml(path: Path) -> Email`.
2. Implement `normalise_body(text: str) -> str`:
   - Strip HTML tags.
   - Standardise whitespace & line endings.
   - Remove quoted replies (simple `"> "` heuristic).

**Tests**
- Provide fixture EMLs (plain & HTML).
- Assert parser fills all mandatory fields.
- Assert `normalise_body` removes `> quoted` lines.
- Run tests before comitting

Write the code and the tests.
