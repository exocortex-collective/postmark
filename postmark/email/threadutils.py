import uuid
from datetime import timedelta
from typing import Callable, Optional

from postmark.email.parser import Email


def detect_thread(email: Email, resolver: Callable[[str], Optional[Email]]) -> str:
    """
    1) If email.in_reply_to or email.references, try each (last to first).
    2) If any are known, return parent's thread_id.
    3) Otherwise, fallback to subject-based matching (strip "Re: ", "Fwd: ") within 7 days.
    4) If that fails, generate a new thread ID.
    """
    # Step 1: Attempt in_reply_to / references
    # (Assuming `email.in_reply_to` is a single str, `email.references` a list of str)
    all_refs = []
    if email.in_reply_to:
        all_refs.append(email.in_reply_to)
    if hasattr(email, "references") and email.references:
        all_refs.extend(reversed(email.references))

    for ref in all_refs:
        parent = resolver(ref)
        if parent:
            return getattr(parent, "thread_id", str(uuid.uuid4()))

    # Step 2: Fallback to subject-based approach
    # Simple normalisation: remove "Re: " and "Fwd: " ignoring case
    def strip_prefixes(subj: str) -> str:
        s = subj.strip()
        for prefix in ["re:", "fwd:"]:
            prefix_lower = prefix.lower()
            while s.lower().startswith(prefix_lower):
                s = s[len(prefix) :].strip()
        return s

    norm_subj = strip_prefixes(email.subject or "")
    # Let's define a simpler approach: ask "resolver" about potential 'subject-based' parents.
    # We'll assume it can do so if we pass in a synthetic key with a prefix, e.g. "subject:<normalized>|<date range>"
    # This is a hack to avoid more complicated param passing:
    if email.date:
        start_date = email.date - timedelta(days=7)
        end_date = email.date + timedelta(days=7)
        subject_key = f"subject:{norm_subj.lower()}|{start_date.isoformat()}|{end_date.isoformat()}"
        subject_parent = resolver(subject_key)
        if subject_parent:
            return getattr(subject_parent, "thread_id", str(uuid.uuid4()))

    # Step 3: Otherwise, generate new thread id
    return str(uuid.uuid4())
