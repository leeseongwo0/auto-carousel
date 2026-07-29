"""Versioned outbound-URL and material-content identities."""

from __future__ import annotations

import posixpath
import re
import unicodedata
from hashlib import sha256
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit, urlunsplit

from .collectors.base import SourceObservation, UrlCandidate

IDENTITY_VERSION = "outbound_url_v1"
_TRACKING_KEYS = frozenset({"fbclid", "gclid", "dclid", "msclkid", "mc_cid", "mc_eid", "ref", "ref_"})
_TELEGRAM_HOSTS = frozenset({"t.me", "telegram.me", "telegram.dog"})
_URL_RE = re.compile(r"https?://[^\s<>()\[\]{}]+", re.IGNORECASE)


def _clean_percent(value: str, safe: str) -> str:
    return quote(unquote(value), safe=safe)


def normalize_outbound_url(value: str) -> str | None:
    """Return canonical eligible HTTP(S) URL without fetching or redirecting."""
    try:
        parsed = urlsplit(value.strip())
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
            return None
        host = parsed.hostname.encode("idna").decode("ascii").lower()
        if host in _TELEGRAM_HOSTS or host.endswith(".t.me") or host.endswith(".telegram.me"):
            return None
        port = parsed.port
    except (UnicodeError, ValueError):
        return None
    netloc = (
        host if port is None or (parsed.scheme.lower(), port) in {("http", 80), ("https", 443)} else f"{host}:{port}"
    )
    path = _clean_percent(parsed.path or "/", "/:@!$&'()*+,;=-._~")
    normalized_path = posixpath.normpath(path)
    if path.endswith("/") and not normalized_path.endswith("/"):
        normalized_path += "/"
    if not normalized_path.startswith("/"):
        normalized_path = "/" + normalized_path
    query = [
        (_clean_percent(key, "-._~"), _clean_percent(item, "-._~"))
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if key.casefold() not in _TRACKING_KEYS and not key.casefold().startswith("utm_")
    ]
    query.sort()
    return urlunsplit((parsed.scheme.lower(), netloc, normalized_path, urlencode(query), ""))


def select_outbound_url(observation: SourceObservation) -> tuple[str | None, str | None]:
    """Select preview, explicit entity, then bare URL deterministically."""
    candidates = list(observation.urls)
    candidates.extend(UrlCandidate(value, "bare") for value in _URL_RE.findall(observation.text))
    order = {"preview": 0, "entity": 1, "bare": 2}
    normalized = [
        (order.get(candidate.source, 3), index, normalize_outbound_url(candidate.url), candidate.source)
        for index, candidate in enumerate(candidates)
    ]
    eligible = [item for item in normalized if item[2] is not None]
    if not eligible:
        return None, None
    _, _, url, source = min(eligible, key=lambda item: (item[0], item[1], item[2] or ""))
    return url, source


def normalized_material(observation: SourceObservation) -> str:
    text = unicodedata.normalize("NFC", observation.text.replace("\r\n", "\n").replace("\r", "\n"))
    media = "\x1e".join(f"{item.kind}:{item.identity or ''}:{item.caption or ''}" for item in observation.media)
    return f"{text}\x1f{media}"


def material_identity(observation: SourceObservation) -> str:
    return f"material_v1:{sha256(normalized_material(observation).encode()).hexdigest()}"


def story_identity(observation: SourceObservation) -> str:
    url, _ = select_outbound_url(observation)
    if url:
        return f"{IDENTITY_VERSION}:{sha256(url.encode()).hexdigest()}"
    material = normalized_material(observation)
    if material == "\x1f":
        source_key = f"{observation.channel_id}\x1f{observation.external_post_id}"
        return f"empty_v1:{sha256(source_key.encode()).hexdigest()}"
    return material_identity(observation)


def content_identity(observation: SourceObservation) -> str:
    """Version identity changes for a material edit, even when story URL remains."""
    story = story_identity(observation)
    material = material_identity(observation)
    return f"content_v1:{sha256(f'{story}\x1f{material}'.encode()).hexdigest()}"
