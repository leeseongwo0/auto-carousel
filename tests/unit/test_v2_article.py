import json
from datetime import UTC, datetime, timedelta

import dns.rdatatype
import pytest

from newsbot.v2_article import (
    ArticleResult,
    CanonicalSource,
    SafeArticleTransport,
    SourceDateEvidence,
    UnsafeUrlError,
    UrlReference,
    _accepted_html_canonical,
    _DnsPythonResolver,
    _provenance,
    body_identity,
    canonicalize_url,
    extract_source_dates,
    material_character_count,
    select_article_urls,
    select_source_date,
    validate_dns_answers,
)
from newsbot.v2_observability import InMemoryObservabilitySink


def test_canonical_url_normalizes_only_frozen_components() -> None:
    assert (
        canonicalize_url(" HTTPS://BÜCHER.example:443/a/./b/../c?z=2&utm_source=x&a=1#fragment ")
        == "https://xn--bcher-kva.example/a/c?a=1&z=2"
    )
    assert canonicalize_url("https://example.com/A/") == "https://example.com/A/"
    assert canonicalize_url("https://example.com/a%2Fb//c/") == "https://example.com/a%2Fb//c/"
    assert canonicalize_url("https://example.com/a%7eb") == "https://example.com/a~b"


def test_query_canonicalization_preserves_distinct_octets_and_forms() -> None:
    plus = canonicalize_url("https://example.com/a?q=a+b")
    percent_space = canonicalize_url("https://example.com/a?q=a%20b")
    bare = canonicalize_url("https://example.com/a?flag")
    blank = canonicalize_url("https://example.com/a?flag=")
    assert len({plus, percent_space, bare, blank}) == 4
    assert canonicalize_url("https://example.com./a?z=%7e&utm%5Fsource=x") == "https://example.com/a?z=~"
    with pytest.raises(UnsafeUrlError):
        canonicalize_url("https://example.com/a?q=%zz")


@pytest.mark.parametrize(
    "url", ["ftp://example.com/a", "https://user@example.com/a", "https://example.com:444/a", "https://t.me/a"]
)
def test_unsafe_urls_are_rejected(url: str) -> None:
    with pytest.raises(UnsafeUrlError):
        canonicalize_url(url)


def test_selection_is_source_offset_and_canonical_byte_ordered() -> None:
    selected = select_article_urls(
        (
            UrlReference("https://example.com/z", "bare", 1),
            UrlReference("https://example.com/a?utm_source=x", "entity", 20),
            UrlReference("https://example.com/a", "preview", 4),
            UrlReference("https://example.com/b", "entity", 2),
        )
    )
    assert [(item.source, item.canonical_url) for item in selected] == [
        ("preview", "https://example.com/a"),
        ("entity", "https://example.com/b"),
        ("bare", "https://example.com/z"),
    ]


def test_body_identity_requires_exact_meaningful_body_threshold() -> None:
    assert material_character_count("title #tag https://example.com " + "a" * 79) == 79
    assert body_identity("a" * 199) is None
    assert body_identity("a" * 200) is not None


def test_body_hash_and_material_count_share_title_excluded_identity() -> None:
    title = "Repeated article title"
    body = f"{title}\n\n" + "a" * 200
    assert material_character_count(body, title=title) == 200
    assert body_identity(body, title=title) == body_identity("a" * 200)


def test_meaningful_body_gate_is_exactly_eighty_letters_or_numbers() -> None:
    assert material_character_count("a" * 79) == 79
    assert material_character_count("a" * 80) == 80


def test_private_resolver_is_rejected_before_any_socket_or_proxy_use() -> None:
    class PrivateResolver:
        def resolve(self, hostname: str) -> tuple[str, ...]:
            return ("127.0.0.1",)

    def forbidden_socket(address: tuple[str, int], timeout: float) -> object:
        raise AssertionError("unsafe DNS must not connect")

    result = SafeArticleTransport(resolver=PrivateResolver(), socket_factory=forbidden_socket).fetch(
        "https://publisher.example/story", telegram_date=datetime(2026, 8, 9, tzinfo=UTC)
    )
    assert result.result.value == "unsafe_url"


@pytest.mark.parametrize("answers", [("127.0.0.1",), ("8.8.8.8", "10.0.0.1"), ("::1",), ("203.0.113.1",)])
def test_dns_rejects_private_mixed_and_documentation_answers(answers: tuple[str, ...]) -> None:
    with pytest.raises(UnsafeUrlError):
        validate_dns_answers(answers)


def test_dns_peer_is_lowest_packed_public_address() -> None:
    assert validate_dns_answers(("8.8.8.8", "1.1.1.1")) == "1.1.1.1"
    with pytest.raises(UnsafeUrlError):
        validate_dns_answers(tuple(f"8.8.8.{index}" for index in range(1, 18)))
    with pytest.raises(UnsafeUrlError):
        validate_dns_answers(("fe80::1%en0",))


def test_html_canonical_requires_the_same_host_and_provenance_is_redacted() -> None:
    final = "https://publisher.example/story?secret=value"
    canonical, source = _accepted_html_canonical('<link rel="canonical" href="https://attacker.example/story">', final)
    assert (canonical, source) == (final, CanonicalSource.FINAL)
    provenance = _provenance(final, final, final, 0, "public", 200, "text/html")
    assert "secret=value" not in str(provenance)
    assert "publisher.example" not in str(provenance)


def test_private_psl_tenants_cannot_cross_canonicalize() -> None:
    final = "https://tenant-a.github.io/story"
    canonical, source = _accepted_html_canonical(
        '<link rel="canonical" href="https://tenant-b.github.io/story">',
        final,
    )
    assert (canonical, source) == (final, CanonicalSource.FINAL)


def test_modified_and_generic_time_labels_are_not_publication_dates() -> None:
    values = extract_source_dates(
        '<time itemprop="dateModified" datetime="2026-08-08T00:00:00+00:00"></time>'
        '<time class="updated date" datetime="2026-08-08T01:00:00+00:00"></time>'
        '<time itemprop="datePublished" datetime="2026-08-08T02:00:00+00:00"></time>'
    )
    assert values == (
        (
            SourceDateEvidence.LABELED_TIME,
            datetime(2026, 8, 8, 2, tzinfo=UTC),
        ),
    )


def test_private_redirect_resolver_cannot_reach_socket() -> None:
    class RedirectResolver:
        def resolve(self, hostname: str) -> tuple[str, ...]:
            return ("10.0.0.2",)

    def socket_never_called(address: tuple[str, int], timeout: float) -> object:
        raise AssertionError("redirect target DNS is validated before connect")

    result = SafeArticleTransport(resolver=RedirectResolver(), socket_factory=socket_never_called).fetch(
        "https://redirect.example/private", telegram_date=datetime(2026, 8, 9, tzinfo=UTC)
    )
    assert result.result.value == "unsafe_url"


def test_dns_wall_budget_prevents_connect_after_slow_resolution() -> None:
    ticks = iter((0.0, 0.0, 2.1))

    class PublicResolver:
        def resolve(self, hostname: str) -> tuple[str, ...]:
            return ("8.8.8.8",)

    def socket_never_called(address: tuple[str, int], timeout: float) -> object:
        raise AssertionError("expired DNS budget must not connect")

    result = SafeArticleTransport(
        resolver=PublicResolver(), socket_factory=socket_never_called, monotonic=lambda: next(ticks)
    ).fetch("http://publisher.example/story", telegram_date=datetime(2026, 8, 9, tzinfo=UTC))
    assert result.result.value == "transient_failure"


def test_private_harness_rejection_emits_only_bounded_alert() -> None:
    sink = InMemoryObservabilitySink()
    result = SafeArticleTransport(
        blocked_hosts=("publisher.example",),
        observability=sink,
    ).fetch(
        "https://publisher.example/private?token=secret",
        telegram_date=datetime(2026, 8, 9, tzinfo=UTC),
    )

    assert result.result is ArticleResult.UNSAFE_URL
    assert [item.as_dict()["labels"] for item in sink.events] == [{"alert": "private_harness_hit"}]
    assert "publisher.example" not in json.dumps(
        [item.as_dict() for item in sink.events],
        sort_keys=True,
    )


def test_source_date_precedence_and_same_precedence_conflict() -> None:
    telegram = datetime(2026, 8, 9, tzinfo=UTC)
    value, evidence, conflict = select_source_date(
        (
            (SourceDateEvidence.OPEN_GRAPH, telegram - timedelta(days=1)),
            (SourceDateEvidence.JSON_LD, telegram - timedelta(days=2)),
        ),
        telegram,
    )
    assert (value, evidence, conflict) == (telegram - timedelta(days=2), SourceDateEvidence.JSON_LD, False)
    assert select_source_date(
        (
            (SourceDateEvidence.JSON_LD, telegram - timedelta(days=1)),
            (SourceDateEvidence.JSON_LD, telegram - timedelta(days=1, minutes=-6)),
        ),
        telegram,
    )[2]


class _DnsName:
    def __init__(self, value: str) -> None:
        self.value = value

    def to_text(self) -> str:
        return self.value


class _DnsAddress:
    def __init__(self, value: str) -> None:
        self.address = value

    def to_text(self) -> str:
        return self.address


class _DnsAlias:
    def __init__(self, value: str) -> None:
        self.target = _DnsName(value)

    def to_text(self) -> str:
        return self.target.to_text()


class _DnsRrset(list):
    def __init__(
        self,
        owner: str,
        rdtype: int,
        *items: object,
    ) -> None:
        super().__init__(items)
        self.name = _DnsName(owner)
        self.rdtype = rdtype


class _DnsResponse:
    def __init__(self, answer: list[_DnsRrset]) -> None:
        self.answer = answer

    def to_wire(self) -> bytes:
        return b"dns-proof"


class _DnsAnswer:
    def __init__(self, response: _DnsResponse) -> None:
        self.response = response


class _ResolverFixture:
    def __init__(self, rrsets: list[_DnsRrset]) -> None:
        self.rrsets = rrsets

    def resolve(self, _hostname, rdtype, **_kwargs):
        return _DnsAnswer(_DnsResponse(self.rrsets if rdtype == "A" else []))


def _dns_resolver(rrsets: list[_DnsRrset]) -> _DnsPythonResolver:
    resolver = _DnsPythonResolver()
    resolver._resolver = _ResolverFixture(rrsets)
    return resolver


def test_dns_cname_chain_accepts_only_the_reachable_terminal_owner() -> None:
    valid = _dns_resolver(
        [
            _DnsRrset(
                "publisher.example.",
                dns.rdatatype.CNAME,
                _DnsAlias("edge.example."),
            ),
            _DnsRrset(
                "edge.example.",
                dns.rdatatype.CNAME,
                _DnsAlias("final.example."),
            ),
            _DnsRrset(
                "final.example.",
                dns.rdatatype.A,
                _DnsAddress("1.1.1.1"),
            ),
        ]
    )
    assert valid.resolve("publisher.example") == ("1.1.1.1",)

    unrelated = _dns_resolver(
        [
            _DnsRrset(
                "publisher.example.",
                dns.rdatatype.CNAME,
                _DnsAlias("final.example."),
            ),
            _DnsRrset(
                "final.example.",
                dns.rdatatype.A,
                _DnsAddress("1.1.1.1"),
            ),
            _DnsRrset(
                "unrelated.example.",
                dns.rdatatype.A,
                _DnsAddress("8.8.8.8"),
            ),
        ]
    )
    with pytest.raises(
        UnsafeUrlError,
        match="dns_unrelated_answer",
    ):
        unrelated.resolve("publisher.example")


def test_dns_cname_chain_enforces_depth_and_loop_bounds() -> None:
    chain = [
        _DnsRrset(
            f"alias-{index}.example.",
            dns.rdatatype.CNAME,
            _DnsAlias(f"alias-{index + 1}.example."),
        )
        for index in range(6)
    ]
    with pytest.raises(UnsafeUrlError, match="dns_cname_depth"):
        _dns_resolver(chain).resolve("alias-0.example")

    loop = _dns_resolver(
        [
            _DnsRrset(
                "publisher.example.",
                dns.rdatatype.CNAME,
                _DnsAlias("edge.example."),
            ),
            _DnsRrset(
                "edge.example.",
                dns.rdatatype.CNAME,
                _DnsAlias("publisher.example."),
            ),
        ]
    )
    with pytest.raises(UnsafeUrlError, match="dns_cname_loop"):
        loop.resolve("publisher.example")
