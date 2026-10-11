"""Shared output boundaries for untrusted driver data."""
import json
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


def sensitive_key(key):
    name = re.sub(r"[^a-z0-9]", "", str(key).lower())
    return (name in {"cookie", "cookies", "authorization", "proxyauthorization", "auth", "apikey", "key", "password", "passwd", "session", "sessionid", "sig", "signature"}
            or name.endswith(("token", "secret", "password", "signature")))


def sanitize_url(raw_url, *, media=False):
    if not raw_url:
        return ""
    try:
        parts = urlsplit(str(raw_url).strip())
        if parts.scheme and parts.scheme.lower() not in ("http", "https"):
            return ""
        host = parts.netloc.rsplit("@", 1)[-1]
        query = urlencode([(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
                           if not sensitive_key(k)]) if media else ""
        return urlunsplit((parts.scheme, host, parts.path, query, ""))
    except ValueError:
        return ""


def public_config(value):
    if isinstance(value, dict):
        return {str(k): "[REDACTED]" if sensitive_key(k) else public_config(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [public_config(v) for v in value]
    if isinstance(value, str):
        if value.strip().lower().startswith(("http://", "https://")):
            value = sanitize_url(value, media=True)
        return redact_error(value)
    if value is None or isinstance(value, (int, float, bool)):
        return value
    return "[UNSERIALIZABLE]"


_ASSIGNMENT = re.compile(r"""(?<![\w-])(["']?[A-Za-z][A-Za-z0-9_-]*["']?)[ \t]*[:=][ \t]*""")
_URL = re.compile(r"""https?://[^\s"'<>]+""", re.IGNORECASE)


def _redact_assignments(text):
    # Scan quoted values once; malformed/truncated stderr is also sensitive.
    chunks, cursor = [], 0
    for match in _ASSIGNMENT.finditer(text):
        if match.start() < cursor or not sensitive_key(match[1].strip("\"'")):
            continue
        end = match.end()
        if end < len(text) and text[end] in "\"'":
            quote = text[end]
            end += 1
            while end < len(text):
                char = text[end]
                end += 1
                if char == "\\" and end < len(text):
                    end += 1
                elif char == quote:
                    break
        else:
            while end < len(text) and text[end] not in "\r\n":
                end += 1
        chunks.extend((text[cursor:match.end()], '"[REDACTED]"'))
        cursor = end
    chunks.append(text[cursor:])
    return "".join(chunks)


def redact_error(message):
    text = str(message)
    try:
        parsed = json.loads(text)
        if isinstance(parsed, (dict, list)):
            def clean(value):
                if isinstance(value, dict):
                    return {k: "[REDACTED]" if sensitive_key(k) else clean(v) for k, v in value.items()}
                if isinstance(value, list):
                    return [clean(v) for v in value]
                return value
            text = json.dumps(clean(parsed), ensure_ascii=False)
    except (ValueError, TypeError, RecursionError):
        pass
    text = _URL.sub(lambda m: sanitize_url(m[0], media=True), text)
    text = _redact_assignments(text)
    text = re.sub(r"""(?i)\b(?:Bearer|Basic)\s+[^\s"'<>]+""", "[REDACTED_AUTH]", text)
    text = re.sub(r"""[A-Za-z]:[\\/][^\r\n"']+""", "[REDACTED_PATH]", text)
    return re.sub(r"""/(?:home|Users)/[^\r\n"']+""", "[REDACTED_PATH]", text)


def safe_component(value):
    if not isinstance(value, str) or not value or value in (".", ".."):
        raise ValueError("A nonempty identifier is required")
    if re.search(r'[\\/<>:"|?*\x00-\x1f\x7f]', value) or value.endswith((".", " ")):
        raise ValueError("Identifier contains unsafe filesystem characters")
    if len(value.encode("utf-8")) > 180:
        raise ValueError("Identifier exceeds the portable filename limit")
    return value
