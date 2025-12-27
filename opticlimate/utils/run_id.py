# opticlimate/utils/run_id.py

from __future__ import annotations

import re
import unicodedata


# Windows + POSIX filename forbidden characters. (We also remove path separators.)
_INVALID_CHARS_RE = re.compile(r"[\\/:*?\"<>|]+")

# After cleaning, only allow lowercase ascii letters/digits plus "_" and "-".
_NON_ALNUM_RE = re.compile(r"[^a-z0-9_-]+")

# Collapse repeated separators like "---" or "__".
_REPEAT_SEP_RE = re.compile(r"[-_]{2,}")


def sanitize_run_id(raw: str, *, max_len: int = 80) -> str:
    """Sanitize a user-provided run id into a filesystem-safe slug.

    Rules:
      - Unicode normalize (NFKD) then best-effort ASCII
      - lowercase
      - whitespace -> "-"
      - strip forbidden filename characters (\\ / : * ? " < > |)
      - remove any other non [a-z0-9_-]
      - collapse repeated separators
      - trim leading/trailing separators
      - enforce max length (default 80)

    Returns:
      Sanitized slug, or "" if the input cannot be sanitized into anything.
    """

    if raw is None:
        return ""
    if not isinstance(raw, str):
        raw = str(raw)

    s = raw.strip()
    if not s:
        return ""

    # Normalize unicode and strip accents.
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")

    s = s.lower()
    s = re.sub(r"\s+", "-", s)
    s = _INVALID_CHARS_RE.sub("", s)
    s = _NON_ALNUM_RE.sub("-", s)
    s = _REPEAT_SEP_RE.sub(lambda m: m.group(0)[0], s)
    s = s.strip("-_")

    if max_len and len(s) > max_len:
        s = s[:max_len].rstrip("-_")

    return s
