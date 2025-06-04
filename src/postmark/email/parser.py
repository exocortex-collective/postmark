from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from email.parser import BytesParser
from email.policy import default
import re
import uuid


@dataclass
class Email:
    uuid: str
    message_id: str | None
    date: datetime
    subject: str
    from_addr: str
    body_text: str
    body_html: str
    attachments: list[str] = field(default_factory=list)


def parse_eml(path: Path) -> Email:
    with path.open("rb") as f:
        msg = BytesParser(policy=default).parse(f)
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
    dt = parsedate_to_datetime(date_str) if date_str else datetime.utcnow()

    # Extract body text & HTML
    body_text = []
    body_html = []
    attachments = []
    for part in msg.walk():
        ctype = part.get_content_type()
        if ctype in ["text/plain", "text/html"]:
            payload = part.get_payload(decode=True) or b""
            text_part = payload.decode(part.get_content_charset() or "utf-8", errors="replace")
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
    # Remove HTML tags (super simplistic)
    text_no_html = re.sub(r"<[^>]*>", "", text)
    # Standardise whitespace & line endings
    lines = [ln.strip() for ln in text_no_html.splitlines()]
    # Remove lines starting with '> '
    lines = [ln for ln in lines if not ln.startswith("> ")]
    return "\n".join(line for line in lines if line)
