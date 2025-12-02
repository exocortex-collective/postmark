import datetime
from typing import Optional

from postmark.archive.threading import detect_thread
from postmark.email.parser import Email


def fake_resolver(key: str) -> Optional[Email]:
    """
    Fake callback that can return hard-coded Emails.
    Suppose references are stored in a dict,
    or 'subject:' lookups in a second dict.
    """
    if key == "<parent.1@example.com>":
        # Return a parent Email with some thread_id
        return Email(
            uuid="parent-uuid",
            message_id="<parent.1@example.com>",
            date=datetime.datetime(2024, 1, 1),
            subject="Hello World",
            from_addr="someone@example.com",
            body_text="Parent body",
            body_html="",
            raw_msg=b"",
            # You might store thread_id on an Email
            # or store it in a separate data structure
        )
    if key.startswith("subject:hello world|"):
        # E.g. key == "subject:hello world|2024-02-08T00:00:00|2024-02-22T00:00:00"
        try:
            _, subrange = key.split("|", 1)
            start_str, end_str = subrange.split("|")
            start_dt = datetime.datetime.fromisoformat(start_str)
            end_dt = datetime.datetime.fromisoformat(end_str)
        except ValueError:
            return None  # Parsing error => no match

        # The sibling is dated 2024-01-05
        sibling_email = Email(
            uuid="sibling-uuid",
            message_id="<sibling@example.com>",
            date=datetime.datetime(2024, 1, 5),
            subject="Re: Hello World",
            from_addr="other@example.com",
            body_text="Sibling body",
            body_html="",
            raw_msg=b"",
        )
        # Return sibling only if it's within [start_dt, end_dt]
        date = sibling_email.date
        if date is not None and start_dt <= date <= end_dt:
            return sibling_email
        return None
    # No match
    return None


def test_detect_thread_in_reply_to() -> None:
    child_email = Email(
        uuid="child-uuid",
        message_id="<child@example.com>",
        date=datetime.datetime(2024, 1, 2),
        subject="Re: Hello World",
        from_addr="someone@example.com",
        body_text="Child body",
        body_html="",
        raw_msg=b"",
    )
    # Set a fake in_reply_to
    setattr(child_email, "in_reply_to", "<parent.1@example.com>")
    thread_id = detect_thread(child_email, fake_resolver)
    assert thread_id == "parent-uuid", "Should return parent's thread or parent's .uuid"


def test_detect_thread_fallback_subject() -> None:
    fallback_email = Email(
        uuid="fallback-uuid",
        message_id="<fallback@example.com>",
        date=datetime.datetime(2024, 1, 5),
        subject="Re: Hello World",
        from_addr="someone@example.com",
        body_text="Testing fallback with same subject",
        body_html="",
        raw_msg=b"",
    )
    # No references set, so it hits subject fallback
    thread_id = detect_thread(fallback_email, fake_resolver)
    assert thread_id == "sibling-uuid", "Should match sibling's .uuid by subject"


def test_detect_thread_new() -> None:
    # 30 days difference => new thread
    new_email = Email(
        uuid="new-uuid",
        message_id="<new@example.com>",
        date=datetime.datetime(2024, 2, 15),
        subject="Hello World",
        from_addr="someone@example.com",
        body_text="Old subject line, but too far in time",
        body_html="",
        raw_msg=b"",
    )
    # This time difference is out of the 7-day window, so no subject-based match
    thread_id = detect_thread(new_email, fake_resolver)
    assert thread_id != "sibling-uuid", "Should be a new thread"
    assert thread_id != "parent-uuid", "Should not match parent's thread"
