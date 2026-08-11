"""Deterministic URL selection and bounded, direct article enrichment for V2."""

from __future__ import annotations

import hashlib
import importlib.metadata
import ipaddress
import json
import re
import socket
import ssl
import time
import unicodedata
import zlib
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from html.parser import HTMLParser
from typing import Protocol
from urllib.parse import urljoin, urlsplit, urlunsplit

import dns.exception
import dns.rdatatype
import dns.resolver
import tldextract

from .v2_observability import (
    ImmediateAlert,
    MetricName,
    NoopObservabilitySink,
    ObservabilitySink,
    event,
)

MAX_URL_BYTES = 4_096
MAX_REDIRECTS = 3
MAX_WIRE_BYTES = 2 * 1024 * 1024
MAX_HTML_BYTES = 4 * 1024 * 1024
MAX_TEXT_BYTES = 1024 * 1024
MAX_HEADER_BYTES = 64 * 1024
MAX_HEADER_FIELDS = 100
MAX_HEADER_LINE = 8 * 1024
MAX_DNS_WIRE_BYTES = 64 * 1024
MAX_DNS_RECORDS = 16
MAX_DNS_ADDRESSES = 8
MAX_CNAME_DEPTH = 5
DNS_DEADLINE_SECONDS = 2.0
CONNECT_DEADLINE_SECONDS = 3.0
TOTAL_DEADLINE_SECONDS = 10.0
EXTRACTOR_VERSION = "article_html_v1"

_PRIVATE_HARNESS_REASONS = frozenset(
    {
        "blocked_host",
        "dns_cname_depth",
        "dns_cname_loop",
        "dns_unrelated_alias",
        "dns_unrelated_answer",
        "peer_mismatch",
        "unsafe_dns",
    }
)


class _ReadableBinaryStream(Protocol):
    def read(self, size: int = -1) -> bytes: ...

    def readline(self, size: int = -1) -> bytes: ...

    def close(self) -> None: ...


class _DeadlineReader:
    def __init__(
        self,
        connection: socket.socket,
        deadline: float,
        monotonic: Callable[[], float],
    ) -> None:
        self._connection = connection
        self._deadline = deadline
        self._monotonic = monotonic
        self._buffer = bytearray()

    def _receive(self, size: int) -> bytes:
        remaining = self._deadline - self._monotonic()
        if remaining <= 0:
            raise TimeoutError("fetch_deadline")
        self._connection.settimeout(remaining)
        return self._connection.recv(max(1, size))

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = MAX_WIRE_BYTES + 1
        if self._buffer:
            chunk = bytes(self._buffer[:size])
            del self._buffer[: len(chunk)]
            return chunk
        return self._receive(size)

    def readline(self, size: int = -1) -> bytes:
        limit = MAX_HEADER_LINE + 1 if size < 0 else size
        while True:
            newline = self._buffer.find(b"\n")
            if newline >= 0:
                end = min(newline + 1, limit)
                line = bytes(self._buffer[:end])
                del self._buffer[:end]
                return line
            if len(self._buffer) >= limit:
                line = bytes(self._buffer[:limit])
                del self._buffer[:limit]
                return line
            chunk = self._receive(min(4_096, limit - len(self._buffer)))
            if not chunk:
                line = bytes(self._buffer)
                self._buffer.clear()
                return line
            self._buffer.extend(chunk)

    def close(self) -> None:
        self._buffer.clear()


NORMALIZER_VERSION = "article_body_v1"
_PSL_VERSION = importlib.metadata.version("tldextract")
_PSL = tldextract.TLDExtract(
    suffix_list_urls=(),
    include_psl_private_domains=True,
)


class ArticleResult(StrEnum):
    SUCCESS = "success"
    UNSAFE_URL = "unsafe_url"
    PERMANENT_FAILURE = "permanent_failure"
    TRANSIENT_FAILURE = "transient_failure"


class SourceDateEvidence(StrEnum):
    JSON_LD = "json_ld_date_published"
    OPEN_GRAPH = "open_graph_published_time"
    LABELED_TIME = "publication_labeled_time"


class CanonicalSource(StrEnum):
    REQUESTED = "requested"
    FINAL = "final"
    HTML = "html"


@dataclass(frozen=True, slots=True)
class UrlReference:
    url: str
    source: str = "bare"
    offset: int = 0


@dataclass(frozen=True, slots=True)
class SelectedUrl:
    requested_url: str
    canonical_url: str
    source: str
    offset: int


@dataclass(frozen=True, slots=True)
class ArticleSnapshot:
    result: ArticleResult
    requested_url: str
    final_url: str | None = None
    canonical_url: str | None = None
    canonical_source: CanonicalSource | None = None
    title: str | None = None
    body: str | None = None
    body_hash: str | None = None
    material_count: int = 0
    source_date: datetime | None = None
    source_date_evidence: SourceDateEvidence | None = None
    source_date_conflict: bool = False
    provenance: Mapping[str, str | int | bool | None] | None = None

    @property
    def meaningful_body(self) -> bool:
        return self.material_count >= 80

    @property
    def body_identity(self) -> str | None:
        return self.body_hash if self.material_count >= 200 else None


class DnsResolver(Protocol):
    def resolve(self, hostname: str) -> Sequence[str]: ...


class SocketFactory(Protocol):
    def __call__(self, address: tuple[str, int], timeout: float) -> socket.socket: ...


class UnsafeUrlError(ValueError):
    pass


@dataclass(slots=True)
class _ParserBudget:
    events: int = 0
    output_bytes: int = 0

    def event(self, *, attributes: int = 0) -> None:
        self.events += 1
        if self.events > 200_000 or attributes > 128:
            raise ValueError("parser_limit")

    def output(self, value: str) -> None:
        encoded = len(value.encode("utf-8"))
        if encoded > 256 * 1024:
            raise ValueError("parser_limit")
        self.output_bytes += encoded
        if self.output_bytes > MAX_TEXT_BYTES:
            raise ValueError("text_limit")


class _BodyParser(HTMLParser):
    _CAPTURE_TAGS = frozenset({"article", "main"})
    _IGNORED_TAGS = frozenset(
        {
            "aside",
            "button",
            "dialog",
            "footer",
            "form",
            "header",
            "nav",
            "noscript",
            "script",
            "style",
            "svg",
            "template",
        }
    )
    _BLOCK_TAGS = frozenset({"p", "div", "article", "main", "section", "br", "li", "h1", "h2", "h3"})
    _VOID_TAGS = frozenset(
        {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }
    )

    def __init__(self, budget: _ParserBudget) -> None:
        super().__init__(convert_charrefs=True)
        self.title: list[str] = []
        self.parts: list[str] = []
        self._budget = budget
        self._stack: list[tuple[str, bool, bool]] = []
        self._capture_depth = 0
        self._ignore_depth = 0
        self._in_title = False

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        self._budget.event(attributes=len(attrs))
        tag = tag.casefold()
        values = {key.casefold(): (value or "") for key, value in attrs}
        capture = tag in self._CAPTURE_TAGS or (values.get("itemprop", "").casefold() == "articlebody")
        ignored = tag in self._IGNORED_TAGS
        if capture:
            self._capture_depth += 1
        if ignored:
            self._ignore_depth += 1
        if tag == "title":
            self._in_title = True
        if tag in self._BLOCK_TAGS and self._capture_depth and not self._ignore_depth:
            self.parts.append("\n\n")
        if tag not in self._VOID_TAGS:
            self._stack.append((tag, capture, ignored))
            if len(self._stack) > 128:
                raise ValueError("parser_limit")

    def handle_startendtag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        self.handle_starttag(tag, attrs)
        if tag.casefold() not in self._VOID_TAGS:
            self.handle_endtag(tag)

    def handle_endtag(self, tag: str) -> None:
        self._budget.event()
        tag = tag.casefold()
        while self._stack:
            stacked_tag, capture, ignored = self._stack.pop()
            if capture:
                self._capture_depth -= 1
            if ignored:
                self._ignore_depth -= 1
            if stacked_tag == "title":
                self._in_title = False
            if stacked_tag == tag:
                break

    def handle_data(self, data: str) -> None:
        self._budget.event()
        if self._in_title:
            self._budget.output(data)
            self.title.append(data)
        elif self._capture_depth and not self._ignore_depth:
            self._budget.output(data)
            self.parts.append(data)


class _MetadataParser(HTMLParser):
    def __init__(self, budget: _ParserBudget) -> None:
        super().__init__(convert_charrefs=True)
        self.canonical_urls: list[str] = []
        self.date_values: list[tuple[SourceDateEvidence, str]] = []
        self._json_ld_depth = 0
        self._json_ld_parts: list[str] = []
        self._json_ld_bytes = 0
        self._budget = budget

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        self._budget.event(attributes=len(attrs))
        tag = tag.casefold()
        values = {key.casefold(): value for key, value in attrs if value is not None}
        if tag == "link":
            relations = {part.casefold() for part in values.get("rel", "").split()}
            href = values.get("href")
            if "canonical" in relations and href:
                self.canonical_urls.append(href)
        elif tag == "meta":
            label = (values.get("property") or values.get("name") or "").casefold()
            content = values.get("content")
            if label == "article:published_time" and content:
                self.date_values.append((SourceDateEvidence.OPEN_GRAPH, content))
        elif tag == "time":
            labels = " ".join(values.get(key, "") for key in ("class", "itemprop", "data-label")).casefold()
            raw = values.get("datetime")
            modified = re.search(
                r"(?:^|[\s:_-])(?:modified|updated|lastmod)(?:$|[\s:_-])",
                labels,
            )
            published = re.search(
                r"(?:^|[\s:_-])(?:datepublished|published(?:at|time|date)?|publicationdate)(?:$|[\s:_-])",
                labels,
            )
            if raw and published and not modified:
                self.date_values.append((SourceDateEvidence.LABELED_TIME, raw))
        elif tag == "script" and values.get("type", "").split(";", 1)[0].strip().casefold() == "application/ld+json":
            self._json_ld_depth += 1

    def handle_endtag(self, tag: str) -> None:
        self._budget.event()
        if tag.casefold() != "script" or not self._json_ld_depth:
            return
        self._json_ld_depth -= 1
        if self._json_ld_depth:
            return
        raw = "".join(self._json_ld_parts)
        self._json_ld_parts.clear()
        self._json_ld_bytes = 0
        try:
            value = json.loads(raw)
        except (json.JSONDecodeError, RecursionError):
            return
        self._collect_json_dates(value)

    def handle_data(self, data: str) -> None:
        self._budget.event()
        if not self._json_ld_depth:
            return
        self._budget.output(data)
        encoded = data.encode("utf-8")
        self._json_ld_bytes += len(encoded)
        if self._json_ld_bytes > 256 * 1024:
            raise ValueError("parser_limit")
        self._json_ld_parts.append(data)

    def _collect_json_dates(self, value: object) -> None:
        stack = [value]
        examined = 0
        while stack:
            current = stack.pop()
            examined += 1
            if examined > 10_000:
                raise ValueError("parser_limit")
            if isinstance(current, dict):
                raw = current.get("datePublished")
                if isinstance(raw, str):
                    self.date_values.append((SourceDateEvidence.JSON_LD, raw))
                stack.extend(current.values())
            elif isinstance(current, list):
                stack.extend(current)


def canonicalize_url(url: str) -> str:
    """Return the frozen V1 URL key or raise ``UnsafeUrlError``."""
    raw = url.strip()
    if len(raw.encode("utf-8")) > MAX_URL_BYTES or any(ord(char) < 32 or ord(char) == 127 for char in raw):
        raise UnsafeUrlError("invalid_url")
    parts = urlsplit(raw)
    if parts.scheme.casefold() not in {"http", "https"} or not parts.netloc or parts.username or parts.password:
        raise UnsafeUrlError("invalid_url")
    try:
        host = parts.hostname.encode("idna").decode("ascii").casefold().rstrip(".") if parts.hostname else ""
        port = parts.port
    except (UnicodeError, ValueError) as error:
        raise UnsafeUrlError("invalid_url") from error
    if not host or (port is not None and port not in {80, 443}):
        raise UnsafeUrlError("invalid_url")
    if host in {"t.me", "telegram.me", "telegram.org", "localhost"} or host.endswith(".telegram.org"):
        raise UnsafeUrlError("blocked_host")
    scheme = parts.scheme.casefold()
    netloc = host if port is None or (scheme, port) in {("http", 80), ("https", 443)} else f"{host}:{port}"
    path = _normalize_path(parts.path or "/")
    query = (
        sorted(
            component
            for component in (_normalize_query_component(raw_component) for raw_component in parts.query.split("&"))
            if component is not None
        )
        if parts.query
        else []
    )
    return urlunsplit((scheme, netloc, path, "&".join(query), ""))


_QUERY_SAFE = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~!$'()*+,;:@/?=")


def _normalize_query_component(component: str) -> str | None:
    has_equals = "=" in component
    raw_key, raw_value = component.split("=", 1) if has_equals else (component, "")
    key = _normalize_query_octets(raw_key)
    if _tracking_key(key):
        return None
    value = _normalize_query_octets(raw_value)
    return f"{key}={value}" if has_equals else key


def _normalize_query_octets(value: str) -> str:
    output: list[str] = []
    index = 0
    unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
    while index < len(value):
        character = value[index]
        if character == "%":
            if index + 2 >= len(value) or not re.fullmatch(
                r"[0-9A-Fa-f]{2}",
                value[index + 1 : index + 3],
            ):
                raise UnsafeUrlError("invalid_url")
            byte = int(value[index + 1 : index + 3], 16)
            decoded = chr(byte)
            output.append(decoded if decoded in unreserved else f"%{byte:02X}")
            index += 3
            continue
        if character in _QUERY_SAFE:
            output.append(character)
        elif ord(character) < 128:
            output.append(f"%{ord(character):02X}")
        else:
            output.extend(f"%{byte:02X}" for byte in character.encode("utf-8"))
        index += 1
    return "".join(output)


def _normalize_path(path: str) -> str:
    """Normalize literal dot segments and unreserved escapes without changing resource separators."""
    normalized: list[str] = []
    for segment in path.split("/"):
        segment = re.sub(
            r"%([0-9A-Fa-f]{2})",
            lambda match: (
                chr(int(match.group(1), 16))
                if chr(int(match.group(1), 16)) in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
                else "%" + match.group(1).upper()
            ),
            segment,
        )
        if segment == ".":
            continue
        if segment == "..":
            if normalized and normalized[-1] not in {"", ".."}:
                normalized.pop()
            continue
        normalized.append(segment)
    normalized_path = "/".join(normalized)
    return normalized_path or "/"


def _tracking_key(key: str) -> bool:
    return key.casefold().startswith("utm_") or key.casefold() in {"fbclid", "gclid", "mc_cid", "mc_eid"}


def select_article_urls(urls: Iterable[UrlReference], *, limit: int = 8) -> tuple[SelectedUrl, ...]:
    """Validate, canonicalize, deduplicate, and order Telegram URL occurrences."""
    rank = {"preview": 0, "entity": 1, "bare": 2}
    selected: dict[str, SelectedUrl] = {}
    for reference in urls:
        try:
            canonical = canonicalize_url(reference.url)
        except UnsafeUrlError:
            continue
        item = SelectedUrl(reference.url.strip(), canonical, reference.source, reference.offset)
        old = selected.get(canonical)
        if old is None or (rank.get(item.source, 3), item.offset, item.canonical_url.encode()) < (
            rank.get(old.source, 3),
            old.offset,
            old.canonical_url.encode(),
        ):
            selected[canonical] = item
    return tuple(
        sorted(
            selected.values(), key=lambda item: (rank.get(item.source, 3), item.offset, item.canonical_url.encode())
        )[:limit]
    )


def normalize_article_body(body: str) -> str:
    body = unicodedata.normalize("NFC", body).replace("\r\n", "\n").replace("\r", "\n")
    paragraphs = [re.sub(r"[\t\f\v ]+", " ", paragraph).strip() for paragraph in re.split(r"\n\s*\n+", body)]
    return "\n\n".join(part for part in paragraphs if part)


def _identity_body(body: str, title: str | None = None) -> str:
    normalized = normalize_article_body(body)
    normalized_title = normalize_article_body(title or "")
    if normalized_title:
        if normalized == normalized_title:
            normalized = ""
        elif normalized.startswith(normalized_title):
            boundary = normalized[len(normalized_title) : len(normalized_title) + 1]
            if not boundary or boundary.isspace() or boundary in ":—–-|":
                normalized = normalized[len(normalized_title) :].lstrip(" \n:—–-|")
    return re.sub(
        r"^\s*title\s+",
        " ",
        normalized,
        flags=re.I,
    )


def material_character_count(
    body: str,
    *,
    title: str | None = None,
) -> int:
    """Count the same title-excluded normalized body used for exact identity."""
    identity_body = _identity_body(body, title)
    cleaned = re.sub(
        r"https?://\S+|[@#][\w-]+",
        " ",
        identity_body,
        flags=re.I,
    )
    return sum(unicodedata.category(char)[0] in {"L", "N"} for char in cleaned)


def body_identity(
    body: str,
    *,
    title: str | None = None,
) -> str | None:
    normalized = _identity_body(body, title)
    if material_character_count(normalized) < 200:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def validate_dns_answers(answers: Iterable[str]) -> str:
    """Reject any unsafe/mixed answer set before selecting its stable public peer."""
    addresses: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    for count, answer in enumerate(answers, start=1):
        if count > 16:
            raise UnsafeUrlError("dns_rr_limit")
        if "%" in answer:
            raise UnsafeUrlError("unsafe_dns")
        try:
            address = ipaddress.ip_address(answer)
        except ValueError as error:
            raise UnsafeUrlError("invalid_dns") from error
        if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped is not None:
            raise UnsafeUrlError("unsafe_dns")
        if not address.is_global:
            raise UnsafeUrlError("unsafe_dns")
        if address not in addresses:
            addresses.append(address)
        if len(addresses) > 8:
            raise UnsafeUrlError("dns_answer_limit")
    if not addresses:
        raise UnsafeUrlError("dns_empty")
    return str(min(addresses, key=lambda address: address.packed))


def select_source_date(
    values: Iterable[tuple[SourceDateEvidence, datetime]], telegram_date: datetime
) -> tuple[datetime | None, SourceDateEvidence | None, bool]:
    eligible = [
        (kind, value.astimezone(UTC))
        for kind, value in values
        if value.tzinfo
        and value.year >= 2000
        and value.astimezone(UTC) <= telegram_date.astimezone(UTC) + timedelta(minutes=5)
    ]
    if not eligible:
        return None, None, False
    order = {SourceDateEvidence.JSON_LD: 0, SourceDateEvidence.OPEN_GRAPH: 1, SourceDateEvidence.LABELED_TIME: 2}
    best = min(order[kind] for kind, _ in eligible)
    same = [value for kind, value in eligible if order[kind] == best]
    if max(same) - min(same) > timedelta(minutes=5):
        return None, None, True
    kind = next(kind for kind, _ in eligible if order[kind] == best)
    return min(same), kind, False


def extract_source_dates(
    document: str,
) -> tuple[tuple[SourceDateEvidence, datetime], ...]:
    """Extract offset-bearing publication declarations by parsed attribute."""
    parser = _MetadataParser(_ParserBudget())
    parser.feed(document)
    parser.close()
    return _parsed_source_dates(parser.date_values)


def _parsed_source_dates(
    values: Iterable[tuple[SourceDateEvidence, str]],
) -> tuple[tuple[SourceDateEvidence, datetime], ...]:
    found: list[tuple[SourceDateEvidence, datetime]] = []
    for evidence, raw in values:
        try:
            value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            continue
        if value.tzinfo is not None:
            found.append((evidence, value))
    return tuple(found)


class SafeArticleTransport:
    """No-pool direct transport with a fresh validated peer for every hop."""

    def __init__(
        self,
        *,
        resolver: DnsResolver | None = None,
        socket_factory: SocketFactory | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        blocked_hosts: Sequence[str] = (),
        observability: ObservabilitySink | None = None,
    ) -> None:
        self._resolver = resolver or _DnsPythonResolver(monotonic)
        self._socket_factory = socket_factory or socket.create_connection
        self._monotonic = monotonic
        self._observability = observability if observability is not None else NoopObservabilitySink()
        normalized_hosts: set[str] = set()
        for value in blocked_hosts:
            try:
                normalized = str(value).strip().rstrip(".").encode("idna").decode("ascii").casefold()
            except UnicodeError as exc:
                raise ValueError("invalid blocked article host") from exc
            if not normalized or "/" in normalized or ":" in normalized:
                raise ValueError("invalid blocked article host")
            normalized_hosts.add(normalized)
        self._blocked_hosts = frozenset(normalized_hosts)

    def fetch(
        self,
        requested_url: str,
        *,
        telegram_date: datetime,
    ) -> ArticleSnapshot:
        deadline = self._monotonic() + TOTAL_DEADLINE_SECONDS
        current = requested_url
        redirect_chain: list[str] = []
        peer_kind = "unresolved"

        def failed(
            result: ArticleResult,
            reason: str,
        ) -> ArticleSnapshot:
            return ArticleSnapshot(
                result,
                requested_url,
                final_url=current,
                provenance=_failure_provenance(
                    requested_url,
                    current,
                    redirect_chain,
                    peer_kind=peer_kind,
                    result=result,
                    reason=reason,
                ),
            )

        try:
            current = canonicalize_url(requested_url)
            initial_host = urlsplit(current).hostname
            if initial_host is None or self._host_is_blocked(initial_host):
                raise UnsafeUrlError("blocked_host")
            redirect_chain = [current]
            peer_kind = "unresolved"
            while True:
                peer_kind = "unresolved"
                status, headers, payload, peer_kind = self._request(
                    current,
                    deadline,
                )
                if status in {301, 302, 303, 307, 308}:
                    location = headers.get("location")
                    if not location or len(redirect_chain) > MAX_REDIRECTS:
                        return failed(
                            ArticleResult.PERMANENT_FAILURE,
                            "redirect_rejected",
                        )
                    current = canonicalize_url(urljoin(current, location))
                    peer_kind = "unresolved"
                    redirect_host = urlsplit(current).hostname
                    if redirect_host is None or self._host_is_blocked(redirect_host):
                        raise UnsafeUrlError("blocked_host")
                    redirect_chain.append(current)
                    continue
                if status == 429 or status >= 500:
                    return failed(
                        ArticleResult.TRANSIENT_FAILURE,
                        "http_transient",
                    )
                if status in {404, 410}:
                    return failed(
                        ArticleResult.PERMANENT_FAILURE,
                        "http_missing",
                    )
                mime = headers.get("content-type", "").split(";", 1)[0].strip().casefold()
                if mime not in {"text/html", "application/xhtml+xml"} or b"<" not in payload[:512]:
                    return failed(
                        ArticleResult.PERMANENT_FAILURE,
                        "unsupported_mime",
                    )
                document = payload.decode("utf-8", "strict")
                parser_budget = _ParserBudget()
                body_parser = _BodyParser(parser_budget)
                metadata_parser = _MetadataParser(parser_budget)
                for start in range(0, len(document), 64 * 1024):
                    self._remaining(deadline)
                    chunk = document[start : start + 64 * 1024]
                    body_parser.feed(chunk)
                    metadata_parser.feed(chunk)
                body_parser.close()
                metadata_parser.close()
                self._remaining(deadline)
                body = normalize_article_body("".join(body_parser.parts))
                if len(body.encode("utf-8")) > MAX_TEXT_BYTES:
                    return failed(
                        ArticleResult.PERMANENT_FAILURE,
                        "text_limit",
                    )
                title = normalize_article_body("".join(body_parser.title)) or None
                accepted, source = _accepted_canonical_candidates(
                    metadata_parser.canonical_urls,
                    current,
                )
                source_date, evidence, conflict = select_source_date(
                    _parsed_source_dates(metadata_parser.date_values),
                    telegram_date,
                )
                body_hash = body_identity(body, title=title)
                material_count = material_character_count(
                    body,
                    title=title,
                )
                provenance = _provenance(
                    requested_url,
                    current,
                    accepted,
                    len(redirect_chain) - 1,
                    peer_kind,
                    status,
                    mime,
                    canonical_source=source,
                    redirect_chain=redirect_chain,
                    body_hash=body_hash,
                    material_count=material_count,
                    source_date_evidence=evidence,
                    source_date_conflict=conflict,
                    result=ArticleResult.SUCCESS,
                )
                self._remaining(deadline)
                return ArticleSnapshot(
                    ArticleResult.SUCCESS,
                    requested_url,
                    current,
                    accepted,
                    source,
                    title,
                    body,
                    body_hash,
                    material_count,
                    source_date,
                    evidence,
                    conflict,
                    provenance,
                )
        except UnsafeUrlError as error:
            if str(error) in _PRIVATE_HARNESS_REASONS:
                self._observability.emit(
                    event(
                        MetricName.ALERT,
                        labels={
                            "alert": ImmediateAlert.PRIVATE_HARNESS_HIT,
                        },
                    )
                )
            return failed(
                ArticleResult.UNSAFE_URL,
                "unsafe_url",
            )
        except TimeoutError:
            return failed(
                ArticleResult.TRANSIENT_FAILURE,
                "timeout",
            )
        except (OSError, ssl.SSLError, dns.exception.DNSException):
            return failed(
                ArticleResult.TRANSIENT_FAILURE,
                "network",
            )
        except (UnicodeError, ValueError, zlib.error):
            return failed(
                ArticleResult.PERMANENT_FAILURE,
                "parse",
            )

    def _host_is_blocked(self, hostname: str) -> bool:
        normalized = hostname.casefold().rstrip(".")
        return any(normalized == blocked or normalized.endswith("." + blocked) for blocked in self._blocked_hosts)

    def _remaining(self, deadline: float) -> float:
        remaining = deadline - self._monotonic()
        if remaining <= 0:
            raise TimeoutError("fetch_deadline")
        return remaining

    def _request(
        self,
        url: str,
        deadline: float,
    ) -> tuple[int, dict[str, str], bytes, str]:
        parsed = urlsplit(url)
        host = parsed.hostname
        assert host is not None
        before_dns = self._monotonic()
        answers = self._resolver.resolve(host)
        if self._monotonic() - before_dns > DNS_DEADLINE_SECONDS:
            raise TimeoutError("dns_deadline")
        peer = validate_dns_answers(answers)
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        raw = self._socket_factory(
            (peer, port),
            min(
                CONNECT_DEADLINE_SECONDS,
                self._remaining(deadline),
            ),
        )
        connection: socket.socket = raw
        stream: _ReadableBinaryStream | None = None
        try:
            raw.settimeout(self._remaining(deadline))
            if parsed.scheme == "https":
                context = ssl.create_default_context()
                context.minimum_version = ssl.TLSVersion.TLSv1_2
                context.set_alpn_protocols(["http/1.1"])
                connection = context.wrap_socket(
                    raw,
                    server_hostname=host,
                )
                if connection.selected_alpn_protocol() not in {
                    None,
                    "http/1.1",
                }:
                    raise UnsafeUrlError("alpn")
            connection.settimeout(self._remaining(deadline))
            actual_peer = str(ipaddress.ip_address(connection.getpeername()[0]))
            if actual_peer != peer:
                raise UnsafeUrlError("peer_mismatch")
            target = urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
            request = (
                f"GET {target} HTTP/1.1\r\n"
                f"Host: {host}\r\n"
                "User-Agent: newsbot-v2/1\r\n"
                "Accept: text/html,application/xhtml+xml\r\n"
                "Accept-Encoding: gzip, deflate, identity\r\n"
                "Connection: close\r\n\r\n"
            ).encode("ascii")
            connection.settimeout(self._remaining(deadline))
            connection.sendall(request)
            stream = _DeadlineReader(
                connection,
                deadline,
                self._monotonic,
            )
            status, headers = _read_response_headers(
                stream,
                connection,
                deadline,
                self._monotonic,
            )
            payload = _read_response_body(
                stream,
                connection,
                headers,
                deadline,
                self._monotonic,
            )
            return status, headers, payload, "public"
        finally:
            if stream is not None:
                stream.close()
            connection.close()
            if connection is not raw:
                raw.close()


class _DnsPythonResolver:
    def __init__(
        self,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._monotonic = monotonic
        self._resolver = dns.resolver.Resolver(configure=True)
        self._resolver.cache = None
        self._resolver.search = []

    def resolve(self, hostname: str) -> Sequence[str]:
        deadline = self._monotonic() + DNS_DEADLINE_SECONDS
        records: set[tuple[str, int, str]] = set()
        aliases: dict[str, str] = {}
        address_records: dict[str, set[str]] = {}
        wire_bytes = 0
        for rdtype in ("A", "AAAA"):
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise TimeoutError("dns_deadline")
            try:
                answer = self._resolver.resolve(
                    hostname,
                    rdtype,
                    search=False,
                    lifetime=remaining,
                    raise_on_no_answer=False,
                )
            except dns.resolver.NXDOMAIN as error:
                raise UnsafeUrlError("dns_empty") from error
            except dns.exception.Timeout as error:
                raise TimeoutError("dns_deadline") from error
            except dns.resolver.NoNameservers as error:
                raise OSError("dns_unavailable") from error
            response = answer.response
            encoded = response.to_wire()
            wire_bytes += len(encoded)
            if wire_bytes > MAX_DNS_WIRE_BYTES:
                raise UnsafeUrlError("dns_wire_limit")
            for rrset in response.answer:
                if rrset.rdtype == dns.rdatatype.DNAME:
                    raise UnsafeUrlError("unsupported_dns_alias")
                for item in rrset:
                    key = (
                        rrset.name.to_text().casefold(),
                        int(rrset.rdtype),
                        item.to_text().casefold(),
                    )
                    if key in records:
                        continue
                    records.add(key)
                    if len(records) > MAX_DNS_RECORDS:
                        raise UnsafeUrlError("dns_rr_limit")
                    if rrset.rdtype == dns.rdatatype.CNAME:
                        target = item.target.to_text().casefold()
                        existing_target = aliases.get(key[0])
                        if existing_target is not None and existing_target != target:
                            raise UnsafeUrlError("dns_cname_conflict")
                        aliases[key[0]] = target
                    elif rrset.rdtype in {
                        dns.rdatatype.A,
                        dns.rdatatype.AAAA,
                    }:
                        address_records.setdefault(
                            key[0],
                            set(),
                        ).add(item.address)
                        if sum(len(values) for values in address_records.values()) > MAX_DNS_ADDRESSES:
                            raise UnsafeUrlError("dns_answer_limit")
        root = hostname.casefold().rstrip(".") + "."
        current = root
        visited_aliases: set[str] = set()
        for _ in range(MAX_CNAME_DEPTH):
            target = aliases.get(current)
            if target is None:
                break
            if current in visited_aliases or target in visited_aliases:
                raise UnsafeUrlError("dns_cname_loop")
            visited_aliases.add(current)
            current = target
        else:
            if current in aliases:
                raise UnsafeUrlError("dns_cname_depth")
        if set(aliases) != visited_aliases:
            raise UnsafeUrlError("dns_unrelated_alias")
        if set(address_records) - {current}:
            raise UnsafeUrlError("dns_unrelated_answer")
        addresses = address_records.get(current, set())
        if not addresses:
            raise UnsafeUrlError("dns_empty")
        return tuple(sorted(addresses))


class _StreamingDecoder:
    def __init__(self, encoding: str) -> None:
        normalized = encoding.strip().casefold()
        if normalized in {"", "identity"}:
            self._decompressor = None
        elif normalized == "gzip":
            self._decompressor = zlib.decompressobj(zlib.MAX_WBITS | 16)
        elif normalized == "deflate":
            self._decompressor = zlib.decompressobj()
        else:
            raise ValueError("unsupported_content_encoding")
        self.wire_bytes = 0
        self.decoded_bytes = 0
        self.parts: list[bytes] = []

    def feed(self, data: bytes) -> None:
        self.wire_bytes += len(data)
        if self.wire_bytes > MAX_WIRE_BYTES:
            raise ValueError("wire_limit")
        if self._decompressor is None:
            decoded = data
        else:
            decoded = self._decompressor.decompress(
                data,
                MAX_HTML_BYTES - self.decoded_bytes + 1,
            )
            if self._decompressor.unconsumed_tail:
                raise ValueError("decoded_limit")
        self._append(decoded)

    def finish(self) -> bytes:
        if self._decompressor is not None:
            decoded = self._decompressor.flush(MAX_HTML_BYTES - self.decoded_bytes + 1)
            self._append(decoded)
            if not self._decompressor.eof:
                raise ValueError("truncated_compressed_body")
        return b"".join(self.parts)

    def _append(self, decoded: bytes) -> None:
        self.decoded_bytes += len(decoded)
        if self.decoded_bytes > MAX_HTML_BYTES:
            raise ValueError("decoded_limit")
        if self.wire_bytes and self.decoded_bytes > self.wire_bytes * 20:
            raise ValueError("compression_ratio")
        if decoded:
            self.parts.append(decoded)


def _read_response_headers(
    stream: _ReadableBinaryStream,
    connection: socket.socket,
    deadline: float,
    monotonic: Callable[[], float],
) -> tuple[int, dict[str, str]]:
    total = 0

    def read_line() -> bytes:
        nonlocal total
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("fetch_deadline")
        connection.settimeout(remaining)
        line = stream.readline(MAX_HEADER_LINE + 1)
        total += len(line)
        if not line or len(line) > MAX_HEADER_LINE or total > MAX_HEADER_BYTES:
            raise ValueError("header_limit")
        return line

    status_line = read_line()
    try:
        version, status_text, _reason = status_line.decode("iso-8859-1").rstrip("\r\n").split(" ", 2)
        status = int(status_text)
    except (UnicodeError, ValueError) as error:
        raise ValueError("invalid_status_line") from error
    if version not in {"HTTP/1.0", "HTTP/1.1"} or not (100 <= status <= 599):
        raise ValueError("invalid_status_line")
    headers: dict[str, str] = {}
    field_count = 0
    while True:
        line = read_line()
        if line in {b"\r\n", b"\n"}:
            break
        if line[:1] in {b" ", b"\t"} or b":" not in line:
            raise ValueError("invalid_header")
        field_count += 1
        if field_count > MAX_HEADER_FIELDS:
            raise ValueError("header_limit")
        raw_name, raw_value = line.split(b":", 1)
        try:
            name = raw_name.decode("ascii").strip().casefold()
            value = raw_value.decode("iso-8859-1").strip()
        except UnicodeError as error:
            raise ValueError("invalid_header") from error
        if not name or any(char not in "!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyz" for char in name):
            raise ValueError("invalid_header")
        if name == "content-length" and name in headers:
            if headers[name] != value:
                raise ValueError("conflicting_content_length")
            continue
        headers[name] = f"{headers[name]}, {value}" if name in headers else value
    return status, headers


def _read_response_body(
    stream: _ReadableBinaryStream,
    connection: socket.socket,
    headers: Mapping[str, str],
    deadline: float,
    monotonic: Callable[[], float],
) -> bytes:
    decoder = _StreamingDecoder(headers.get("content-encoding", "identity"))
    transfer = headers.get("transfer-encoding", "").casefold()
    if transfer and transfer != "chunked":
        raise ValueError("unsupported_transfer_encoding")
    if transfer == "chunked":
        while True:
            line = _read_body_line(
                stream,
                connection,
                deadline,
                monotonic,
            )
            try:
                chunk_size = int(
                    line.split(b";", 1)[0].strip(),
                    16,
                )
            except ValueError as error:
                raise ValueError("invalid_chunk") from error
            if chunk_size < 0:
                raise ValueError("invalid_chunk")
            if chunk_size == 0:
                _read_trailers(
                    stream,
                    connection,
                    deadline,
                    monotonic,
                )
                break
            _read_exact_into(
                stream,
                connection,
                decoder,
                chunk_size,
                deadline,
                monotonic,
            )
            if _read_body_line(
                stream,
                connection,
                deadline,
                monotonic,
            ) not in {b"\r\n", b"\n"}:
                raise ValueError("invalid_chunk")
    else:
        content_length = headers.get("content-length")
        if content_length is not None:
            try:
                expected = int(content_length)
            except ValueError as error:
                raise ValueError("invalid_content_length") from error
            if expected < 0 or expected > MAX_WIRE_BYTES:
                raise ValueError("wire_limit")
            _read_exact_into(
                stream,
                connection,
                decoder,
                expected,
                deadline,
                monotonic,
            )
        else:
            while True:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("fetch_deadline")
                connection.settimeout(remaining)
                chunk = stream.read(
                    min(
                        64 * 1024,
                        MAX_WIRE_BYTES - decoder.wire_bytes + 1,
                    )
                )
                if not chunk:
                    break
                decoder.feed(chunk)
    return decoder.finish()


def _read_exact_into(
    stream: _ReadableBinaryStream,
    connection: socket.socket,
    decoder: _StreamingDecoder,
    size: int,
    deadline: float,
    monotonic: Callable[[], float],
) -> None:
    remaining_bytes = size
    while remaining_bytes:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("fetch_deadline")
        connection.settimeout(remaining)
        chunk = stream.read(min(64 * 1024, remaining_bytes))
        if not chunk:
            raise OSError("truncated_response")
        decoder.feed(chunk)
        remaining_bytes -= len(chunk)


def _read_body_line(
    stream: _ReadableBinaryStream,
    connection: socket.socket,
    deadline: float,
    monotonic: Callable[[], float],
) -> bytes:
    remaining = deadline - monotonic()
    if remaining <= 0:
        raise TimeoutError("fetch_deadline")
    connection.settimeout(remaining)
    line = stream.readline(MAX_HEADER_LINE + 1)
    if not line or len(line) > MAX_HEADER_LINE:
        raise ValueError("chunk_line_limit")
    return line


def _read_trailers(
    stream: _ReadableBinaryStream,
    connection: socket.socket,
    deadline: float,
    monotonic: Callable[[], float],
) -> None:
    total = 0
    count = 0
    while True:
        line = _read_body_line(
            stream,
            connection,
            deadline,
            monotonic,
        )
        total += len(line)
        if total > MAX_HEADER_BYTES:
            raise ValueError("trailer_limit")
        if line in {b"\r\n", b"\n"}:
            return
        count += 1
        if count > MAX_HEADER_FIELDS or b":" not in line:
            raise ValueError("trailer_limit")


def _accepted_html_canonical(
    document: str,
    final_url: str,
) -> tuple[str, CanonicalSource]:
    parser = _MetadataParser(_ParserBudget())
    parser.feed(document)
    parser.close()
    return _accepted_canonical_candidates(
        parser.canonical_urls,
        final_url,
    )


def _accepted_canonical_candidates(
    candidates: Iterable[str],
    final_url: str,
) -> tuple[str, CanonicalSource]:
    final_host = urlsplit(final_url).hostname
    assert final_host is not None
    final_domain = _registrable_domain(final_host)
    for raw in candidates:
        try:
            candidate = canonicalize_url(urljoin(final_url, raw))
        except UnsafeUrlError:
            continue
        candidate_host = urlsplit(candidate).hostname
        if candidate_host is not None and _registrable_domain(candidate_host) == final_domain:
            return candidate, CanonicalSource.HTML
    return final_url, CanonicalSource.FINAL


def _registrable_domain(hostname: str) -> str:
    normalized = hostname.casefold().rstrip(".")
    result = _PSL(normalized)
    registrable = result.top_domain_under_public_suffix.casefold()
    return registrable or normalized


def _failure_provenance(
    requested_url: str,
    final_url: str,
    redirect_chain: Sequence[str],
    *,
    peer_kind: str,
    result: ArticleResult,
    reason: str,
) -> Mapping[str, str | int | bool | None]:
    def digest(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8", "replace")).hexdigest()

    chain = tuple(redirect_chain)
    return {
        "requested_url_hash": digest(requested_url),
        "final_url_hash": digest(final_url),
        "canonical_url_hash": None,
        "canonical_source": None,
        "redirect_count": max(0, len(chain) - 1),
        "redirect_chain_digest": digest("\0".join(chain)),
        "dns": ("bounded_validated" if peer_kind == "public" else "unresolved"),
        "peer": peer_kind,
        "registrable_domain_hash": None,
        "psl_version": _PSL_VERSION,
        "fetched_at": datetime.now(UTC).isoformat(),
        "status_class": None,
        "mime": None,
        "extractor_version": EXTRACTOR_VERSION,
        "normalizer_version": NORMALIZER_VERSION,
        "body_hash": None,
        "material_count": 0,
        "source_date_evidence": None,
        "source_date_conflict": False,
        "result": result.value,
        "failure_reason": reason,
    }


def _provenance(
    requested_url: str,
    final_url: str,
    canonical_url: str,
    redirects: int,
    peer_kind: str,
    status: int,
    mime: str,
    *,
    canonical_source: CanonicalSource | None = None,
    redirect_chain: Sequence[str] = (),
    body_hash: str | None = None,
    material_count: int = 0,
    source_date_evidence: SourceDateEvidence | None = None,
    source_date_conflict: bool = False,
    result: ArticleResult = ArticleResult.SUCCESS,
) -> Mapping[str, str | int | bool | None]:
    def digest(value: str) -> str:
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    final_host = urlsplit(final_url).hostname
    if final_host is None:
        raise UnsafeUrlError("invalid_url")
    chain = tuple(redirect_chain) or (
        requested_url,
        final_url,
    )
    return {
        "requested_url_hash": digest(requested_url),
        "final_url_hash": digest(final_url),
        "canonical_url_hash": digest(canonical_url),
        "canonical_source": (canonical_source.value if canonical_source is not None else CanonicalSource.FINAL.value),
        "redirect_count": redirects,
        "redirect_chain_digest": digest("\0".join(chain)),
        "dns": "bounded_validated",
        "peer": peer_kind,
        "registrable_domain_hash": digest(_registrable_domain(final_host)),
        "psl_version": _PSL_VERSION,
        "fetched_at": datetime.now(UTC).isoformat(),
        "status_class": f"{status // 100}xx",
        "mime": "html" if mime == "text/html" else "xhtml",
        "extractor_version": EXTRACTOR_VERSION,
        "normalizer_version": NORMALIZER_VERSION,
        "body_hash": body_hash,
        "material_count": material_count,
        "source_date_evidence": (source_date_evidence.value if source_date_evidence is not None else None),
        "source_date_conflict": source_date_conflict,
        "result": result.value,
    }
