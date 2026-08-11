from __future__ import annotations

import gzip
import io
import os
import socket
import ssl
import threading
from datetime import UTC, datetime, timedelta

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from newsbot.v2_article import ArticleResult, SafeArticleTransport

TELEGRAM_DATE = datetime(2026, 8, 9, tzinfo=UTC)
PUBLIC_IP = "1.1.1.1"


class StaticResolver:
    def __init__(self, *answers: tuple[str, ...]) -> None:
        self.answers = list(answers or ((PUBLIC_IP,),))
        self.hosts: list[str] = []

    def resolve(self, hostname: str) -> tuple[str, ...]:
        self.hosts.append(hostname)
        return self.answers.pop(0)


class FakeSocket:
    def __init__(self, response: bytes, *, peer: str = PUBLIC_IP) -> None:
        self.response = io.BytesIO(response)
        self.peer = peer
        self.timeouts: list[float] = []
        self.request = b""
        self.closed = False

    def settimeout(self, timeout: float) -> None:
        self.timeouts.append(timeout)

    def getpeername(self) -> tuple[str, int]:
        return self.peer, 443

    def sendall(self, data: bytes) -> None:
        self.request += data

    def recv(self, size: int) -> bytes:
        return self.response.read(size)

    def close(self) -> None:
        self.closed = True


class SocketSequence:
    def __init__(self, *sockets: FakeSocket) -> None:
        self.sockets = list(sockets)
        self.calls: list[tuple[tuple[str, int], float]] = []

    def __call__(self, address: tuple[str, int], timeout: float) -> FakeSocket:
        self.calls.append((address, timeout))
        return self.sockets.pop(0)


def response(
    body: bytes,
    *,
    status: int = 200,
    headers: tuple[tuple[str, str], ...] = (),
) -> bytes:
    fields = [
        ("Content-Type", "text/html"),
        ("Content-Length", str(len(body))),
        *headers,
    ]
    head = [f"HTTP/1.1 {status} Test"]
    head.extend(f"{name}: {value}" for name, value in fields)
    return ("\r\n".join(head) + "\r\n\r\n").encode("ascii") + body


def test_direct_transport_ignores_proxy_and_parses_order_independent_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("NETRC", "/tmp/hostile-netrc")
    body = (
        b"<html><head>"
        b'<meta content="2026-08-08T01:00:00+00:00" '
        b'property="article:published_time">'
        b'<link href="https://news.publisher.com/story" rel="canonical">'
        b"</head><body><main><p>" + b"Evidence " * 40 + b"</p></main></body></html>"
    )
    fake = FakeSocket(response(body))
    sockets = SocketSequence(fake)
    result = SafeArticleTransport(
        resolver=StaticResolver(),
        socket_factory=sockets,
    ).fetch("http://publisher.com/story?utm_source=x", telegram_date=TELEGRAM_DATE)

    assert result.result is ArticleResult.SUCCESS
    assert result.canonical_url == "https://news.publisher.com/story"
    assert result.source_date == datetime(2026, 8, 8, 1, tzinfo=UTC)
    assert sockets.calls[0][0] == (PUBLIC_IP, 80)
    assert sockets.calls[0][1] <= 3.0
    request = fake.request.decode("ascii").casefold()
    assert "host: publisher.com\r\n" in request
    assert "proxy" not in request
    assert "authorization" not in request
    assert "cookie" not in request
    assert "referer" not in request
    assert "forwarded" not in request
    assert os.environ["HTTPS_PROXY"] not in request


def test_each_redirect_is_resolved_and_private_rebinding_never_connects() -> None:
    redirect = FakeSocket(
        response(
            b"",
            status=302,
            headers=(("Location", "https://target.example/private"),),
        )
    )
    sockets = SocketSequence(redirect)
    resolver = StaticResolver((PUBLIC_IP,), ("10.0.0.5",))
    result = SafeArticleTransport(
        resolver=resolver,
        socket_factory=sockets,
    ).fetch("http://source.example/start", telegram_date=TELEGRAM_DATE)

    assert result.result is ArticleResult.UNSAFE_URL
    assert resolver.hosts == ["source.example", "target.example"]
    assert len(sockets.calls) == 1
    assert result.provenance["dns"] == "unresolved"
    assert result.provenance["peer"] == "unresolved"


def test_deployment_blocked_hosts_apply_before_dns_and_on_redirects() -> None:
    initial_resolver = StaticResolver()
    initial_sockets = SocketSequence()
    initial = SafeArticleTransport(
        resolver=initial_resolver,
        socket_factory=initial_sockets,
        blocked_hosts=("internal.publisher.example",),
    ).fetch(
        "https://internal.publisher.example/story",
        telegram_date=TELEGRAM_DATE,
    )
    assert initial.result is ArticleResult.UNSAFE_URL
    assert initial_resolver.hosts == []
    assert initial_sockets.calls == []
    assert initial.provenance["dns"] == "unresolved"
    assert initial.provenance["peer"] == "unresolved"

    redirect = FakeSocket(
        response(
            b"",
            status=302,
            headers=(
                (
                    "Location",
                    "https://child.internal.publisher.example/private",
                ),
            ),
        )
    )
    redirect_resolver = StaticResolver((PUBLIC_IP,))
    redirected = SafeArticleTransport(
        resolver=redirect_resolver,
        socket_factory=SocketSequence(redirect),
        blocked_hosts=("internal.publisher.example",),
    ).fetch(
        "http://source.example/start",
        telegram_date=TELEGRAM_DATE,
    )
    assert redirected.result is ArticleResult.UNSAFE_URL
    assert redirect_resolver.hosts == ["source.example"]
    assert redirected.provenance["dns"] == "unresolved"
    assert redirected.provenance["peer"] == "unresolved"


def test_peer_mismatch_blocks_request_bytes() -> None:
    fake = FakeSocket(response(b"<html></html>"), peer="8.8.8.8")
    result = SafeArticleTransport(
        resolver=StaticResolver(),
        socket_factory=SocketSequence(fake),
    ).fetch("http://publisher.example/story", telegram_date=TELEGRAM_DATE)

    assert result.result is ArticleResult.UNSAFE_URL
    assert fake.request == b""


def test_header_line_and_streaming_compression_ratio_fail_closed() -> None:
    oversized_header = b"HTTP/1.1 200 OK\r\nX-Fill: " + b"a" * (8 * 1024) + b"\r\n\r\n"
    header_result = SafeArticleTransport(
        resolver=StaticResolver(),
        socket_factory=SocketSequence(FakeSocket(oversized_header)),
    ).fetch("http://publisher.example/header", telegram_date=TELEGRAM_DATE)
    assert header_result.result is ArticleResult.PERMANENT_FAILURE

    encoded = gzip.compress(b"<html><body>" + b"a" * 100_000 + b"</body></html>")
    compressed = response(
        encoded,
        headers=(("Content-Encoding", "gzip"),),
    )
    ratio_result = SafeArticleTransport(
        resolver=StaticResolver(),
        socket_factory=SocketSequence(FakeSocket(compressed)),
    ).fetch("http://publisher.example/ratio", telegram_date=TELEGRAM_DATE)
    assert ratio_result.result is ArticleResult.PERMANENT_FAILURE


def test_absolute_deadline_stops_slow_drip_header() -> None:
    class SlowSocket(FakeSocket):
        def recv(self, size: int) -> bytes:
            return super().recv(1)

    class Clock:
        def __init__(self) -> None:
            self.value = -0.25

        def __call__(self) -> float:
            self.value += 0.25
            return self.value

    slow = SlowSocket(response(b"<html><body><main><p>evidence</p></main></body></html>"))
    result = SafeArticleTransport(
        resolver=StaticResolver(),
        socket_factory=SocketSequence(slow),
        monotonic=Clock(),
    ).fetch(
        "http://publisher.example/slow",
        telegram_date=TELEGRAM_DATE,
    )
    assert result.result is ArticleResult.TRANSIENT_FAILURE


def test_page_chrome_cannot_satisfy_article_body_gate() -> None:
    body = (
        b"<html><body><nav>"
        + b"Navigation advertisement cookie menu " * 100
        + b"</nav><main><p>short evidence</p></main><footer>"
        + b"Footer links " * 100
        + b"</footer></body></html>"
    )
    result = SafeArticleTransport(
        resolver=StaticResolver(),
        socket_factory=SocketSequence(FakeSocket(response(body))),
    ).fetch(
        "http://publisher.example/chrome",
        telegram_date=TELEGRAM_DATE,
    )
    assert result.result is ArticleResult.SUCCESS
    assert result.body == "short evidence"
    assert result.material_count < 80


def test_tls_uses_original_host_for_sni_and_closes_wrapped_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = b"<html><body><main><p>" + b"Evidence " * 30 + b"</p></main></body></html>"
    raw = FakeSocket(response(body))
    wrapped = FakeSocket(response(body))
    seen: dict[str, object] = {}

    class Context:
        minimum_version = ssl.TLSVersion.TLSv1_2

        def set_alpn_protocols(self, protocols: list[str]) -> None:
            seen["alpn"] = protocols

        def wrap_socket(self, _raw: FakeSocket, *, server_hostname: str) -> FakeSocket:
            seen["sni"] = server_hostname
            return wrapped

    wrapped.selected_alpn_protocol = lambda: "http/1.1"  # type: ignore[attr-defined]
    monkeypatch.setattr(ssl, "create_default_context", Context)
    result = SafeArticleTransport(
        resolver=StaticResolver(),
        socket_factory=SocketSequence(raw),
    ).fetch("https://publisher.example/story", telegram_date=TELEGRAM_DATE)

    assert result.result is ArticleResult.SUCCESS
    assert seen == {"alpn": ["http/1.1"], "sni": "publisher.example"}
    assert wrapped.closed
    assert raw.closed


def test_real_tls_harness_proves_host_sni_certificate_and_proxy_isolation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    private_key_object = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )
    subject = x509.Name(
        [
            x509.NameAttribute(
                NameOID.COMMON_NAME,
                "publisher.example",
            )
        ]
    )
    certificate_object = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(private_key_object.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(minutes=1))
        .not_valid_after(datetime.now(UTC) + timedelta(minutes=5))
        .add_extension(
            x509.SubjectAlternativeName([x509.DNSName("publisher.example")]),
            critical=False,
        )
        .sign(private_key_object, hashes.SHA256())
    )
    certificate = tmp_path / "server-cert.pem"
    private_key = tmp_path / "server-key.pem"
    certificate.write_bytes(certificate_object.public_bytes(serialization.Encoding.PEM))
    private_key.write_bytes(
        private_key_object.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(
        certificate,
        private_key,
    )
    server_context.set_alpn_protocols(["http/1.1"])
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    port = int(listener.getsockname()[1])
    received: list[bytes] = []
    failures: list[BaseException] = []

    def serve() -> None:
        try:
            connection, _ = listener.accept()
            with server_context.wrap_socket(connection, server_side=True) as secured:
                request = bytearray()
                while b"\r\n\r\n" not in request:
                    chunk = secured.recv(4096)
                    if not chunk:
                        break
                    request.extend(chunk)
                received.append(bytes(request))
                body = b"<html><body><main><p>" + b"Verified article evidence " * 20 + b"</p></main></body></html>"
                secured.sendall(response(body))
        except BaseException as error:
            failures.append(error)
        finally:
            listener.close()

    server = threading.Thread(target=serve, daemon=True)
    server.start()

    class PeerMappedSocket:
        def __init__(self, connection: socket.socket, peer: str) -> None:
            self.connection = connection
            self.peer = peer

        def settimeout(self, timeout: float) -> None:
            self.connection.settimeout(timeout)

        def getpeername(self) -> tuple[str, int]:
            return self.peer, 443

        def sendall(self, data: bytes) -> None:
            self.connection.sendall(data)

        def recv(self, size: int) -> bytes:
            return self.connection.recv(size)

        def close(self) -> None:
            self.connection.close()

        def selected_alpn_protocol(self) -> str | None:
            selected = getattr(self.connection, "selected_alpn_protocol", None)
            return None if selected is None else selected()

    socket_calls: list[tuple[tuple[str, int], float]] = []

    def connect_via_loopback_nat(
        address: tuple[str, int],
        timeout: float,
    ) -> PeerMappedSocket:
        socket_calls.append((address, timeout))
        connection = socket.create_connection(("127.0.0.1", port), timeout)
        return PeerMappedSocket(connection, address[0])

    original_context_factory = ssl.create_default_context
    seen: dict[str, object] = {}

    class ClientContext:
        minimum_version = ssl.TLSVersion.TLSv1_2

        def set_alpn_protocols(self, protocols: list[str]) -> None:
            seen["alpn"] = protocols

        def wrap_socket(
            self,
            raw: PeerMappedSocket,
            *,
            server_hostname: str,
        ) -> PeerMappedSocket:
            seen["sni"] = server_hostname
            context = original_context_factory(cafile=str(certificate))
            context.set_alpn_protocols(["http/1.1"])
            secured = context.wrap_socket(
                raw.connection,
                server_hostname=server_hostname,
            )
            return PeerMappedSocket(secured, PUBLIC_IP)

    monkeypatch.setattr(ssl, "create_default_context", ClientContext)
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    result = SafeArticleTransport(
        resolver=StaticResolver(),
        socket_factory=connect_via_loopback_nat,
    ).fetch(
        "https://publisher.example/story",
        telegram_date=TELEGRAM_DATE,
    )
    server.join(timeout=2)

    assert not server.is_alive()
    assert failures == []
    assert result.result is ArticleResult.SUCCESS
    assert socket_calls
    assert socket_calls[0][0] == (PUBLIC_IP, 443)
    assert seen == {
        "alpn": ["http/1.1"],
        "sni": "publisher.example",
    }
    request = received[0].decode("ascii").casefold()
    assert "host: publisher.example\r\n" in request
    assert "proxy" not in request
    assert "authorization" not in request
