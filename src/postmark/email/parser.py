import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from email.parser import BytesParser
from email.policy import default
from pathlib import Path

from bs4 import BeautifulSoup


@dataclass
class Email:
    uuid: str
    message_id: str | None
    date: datetime | None
    subject: str
    from_addr: str
    body_text: str
    body_html: str
    attachments: list[str] = field(default_factory=list)
    in_reply_to: str | None = None
    references: list[str] = field(default_factory=list)


def parse_eml(path: Path) -> Email:
    with path.open("rb") as f:
        msg = BytesParser().parse(f)
    # Basic fields
    eml_uuid = str(uuid.uuid4())
    message_id = msg["Message-Id"]
    date_str = msg["Date"]
    subject = msg["Subject"] or ""
    from_addr = msg["From"] or ""

    # Convert date string → datetime (simplest approach)
    # (You might need dateutil.parser.parse or time.strptime)
    # For brevity, below is a naive approach:
    from email.utils import parsedate_to_datetime

    parsed_dt = None
    if date_str:
        parsed_dt = parsedate_to_datetime(date_str)
    dt = parsed_dt

    # Extract body text & HTML
    body_text = []
    body_html = []
    attachments = []
    for part in msg.walk():
        ctype = part.get_content_type()
        if ctype in ["text/plain", "text/html"]:
            payload = part.get_payload(decode=True)
            if isinstance(payload, bytes):
                text_part = payload.decode(
                    part.get_content_charset() or "utf-8", errors="replace"
                )
            else:
                text_part = str(payload or "")
            if ctype == "text/plain":
                body_text.append(text_part)
            else:
                body_html.append(text_part)
        elif part.get_filename():
            # It's an attachment
            attachments.append(part.get_filename() or "unknown.bin")

    return Email(
        uuid=eml_uuid,
        message_id=message_id,
        date=dt,
        subject=subject,
        from_addr=from_addr,
        body_text="\n".join(body_text),
        body_html="\n".join(body_html),
        attachments=attachments,
    )


def normalise_body(text: str) -> str:
    soup = BeautifulSoup(text, "html.parser")
    text_no_html = soup.get_text()
    # Standardise whitespace & line endings
    lines = [ln.strip() for ln in text_no_html.splitlines()]
    # Remove lines starting with '> '
    lines = [ln for ln in lines if not ln.startswith("> ")]
    return "\n".join(line for line in lines if line)
