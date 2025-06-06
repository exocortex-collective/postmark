import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from email.message import EmailMessage, Message
from email.parser import BytesParser
from email.policy import default
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup


@dataclass
class Attachment:
    name: str
    mime_type: str
    payload: bytes
    size: int


@dataclass
class Email:
    uuid: str
    message_id: str | None
    date: datetime | None
    subject: str
    from_addr: str
    body_text: str
    body_html: str
    raw_msg: bytes
    in_reply_to: str | None = None
    attachments: list[Attachment] = field(default_factory=list)
    references: list[str] = field(default_factory=list)
    to_addrs: list[str] = field(default_factory=list)
    cc_addrs: list[str] = field(default_factory=list)
    bcc_addrs: list[str] = field(default_factory=list)


def parse_email_date(date_str: str | None) -> Optional[datetime]:
    """Parse a date string to datetime, or return None if invalid."""
    if not date_str:
        return None
    from email.utils import parsedate_to_datetime

    try:
        return parsedate_to_datetime(date_str)
    except (TypeError, ValueError):
        return None


def strip_non_text_attachments(msg: Message) -> EmailMessage:
    """Return a copy of msg with only text/plain and text/html parts (and no other attachments)."""
    if not msg.is_multipart():
        ctype = msg.get_content_type()
        if ctype in ("text/plain", "text/html"):
            em = EmailMessage()
            payload = msg.get_payload(decode=True)
            if isinstance(payload, bytes):
                charset = msg.get_content_charset() or "utf-8"
                em.set_content(payload.decode(charset, errors="replace"))
            else:
                em.set_content(str(payload or ""))
            return em
        new_msg = EmailMessage()
        new_msg.set_content("")
        return new_msg
    new_msg = EmailMessage()
    for k, v in msg.items():
        new_msg[k] = v
    parts = msg.iter_parts() if hasattr(msg, "iter_parts") else msg.walk()
    for part in parts:
        ctype = part.get_content_type()
        if ctype in ("text/plain", "text/html"):
            if not isinstance(part, EmailMessage):
                em = EmailMessage()
                for k, v in part.items():
                    em[k] = v
                payload = part.get_payload(decode=True)
                if isinstance(payload, bytes):
                    charset = part.get_content_charset() or "utf-8"
                    em.set_content(payload.decode(charset, errors="replace"))
                else:
                    em.set_content(str(payload or ""))
                new_msg.attach(em)
            else:
                new_msg.attach(part)
    return new_msg


def parse_email(path: Path) -> Email:
    with path.open("rb") as f:
        msg = BytesParser().parse(f)
    # Basic fields
    message_id = msg["Message-Id"]
    date_str = msg["Date"]
    subject = str(msg["Subject"] or "")
    from_addr = str(msg["From"] or "")

    from email.utils import getaddresses

    dt = parse_email_date(date_str)

    dt_str = dt.isoformat() if dt is not None else ""
    eml_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{message_id or ''}-{dt_str}"))

    # Extract To, Cc, Bcc addresses
    to_list: list[str] = msg.get_all("To", [])
    cc_list: list[str] = msg.get_all("Cc", [])
    bcc_list: list[str] = msg.get_all("Bcc", [])

    to_addrs = [str(a) for _, a in getaddresses(to_list)]
    cc_addrs = [str(a) for _, a in getaddresses(cc_list)]
    bcc_addrs = [str(a) for _, a in getaddresses(bcc_list)]

    # Extract In-Reply-To header
    in_reply_to = str(msg["In-Reply-To"])

    # Extract body text & HTML
    body_text = []
    body_html = []
    attachments: list[Attachment] = []
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
            payload = part.get_payload(decode=True)
            if isinstance(payload, bytes):
                pass
            elif isinstance(payload, str):
                payload = payload.encode("utf-8", errors="replace")
            else:
                payload = b""
            name = part.get_filename() or "unknown.bin"
            mime_type = part.get_content_type()
            size = len(payload)
            attachments.append(
                Attachment(
                    name=name,
                    mime_type=mime_type,
                    payload=payload,
                    size=size,
                )
            )

    # Strip non-text attachments for raw_msg
    msg_no_attach = strip_non_text_attachments(msg)
    return Email(
        uuid=eml_uuid,
        message_id=message_id,
        date=dt,
        subject=subject,
        from_addr=from_addr,
        body_text="\n".join(body_text),
        body_html="\n".join(body_html),
        attachments=attachments,
        to_addrs=to_addrs,
        cc_addrs=cc_addrs,
        bcc_addrs=bcc_addrs,
        in_reply_to=in_reply_to,
        raw_msg=msg_no_attach.as_bytes(),
    )


def normalise_body(text: str) -> str:
    soup = BeautifulSoup(text, "html.parser")
    text_no_html = soup.get_text()
    # Standardise whitespace & line endings
    lines = [ln.strip() for ln in text_no_html.splitlines()]
    # Remove lines starting with '> '
    lines = [ln for ln in lines if not ln.startswith("> ")]
    return "\n".join(line for line in lines if line)
