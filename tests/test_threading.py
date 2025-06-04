import datetime

from postmark.email.parser import Email
from postmark.email.threading import detect_thread


def fake_resolver(key: str):
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
            # You might store thread_id on an Email
            # or store it in a separate data structure
        )
    if key.startswith("subject:hello world|"):
        # Pretend we find a match
        return Email(
            uuid="sibling-uuid",
            message_id="<sibling@example.com>",
            date=datetime.datetime(2024, 1, 5),
            subject="Re: Hello World",
            from_addr="other@example.com",
            body_text="Sibling body",
            body_html="",
        )
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
    )
    # This time difference is out of the 7-day window, so no subject-based match
    thread_id = detect_thread(new_email, fake_resolver)
    assert thread_id != "sibling-uuid", "Should be a new thread"
    assert thread_id != "parent-uuid", "Should not match parent's thread"
