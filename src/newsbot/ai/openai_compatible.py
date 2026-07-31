"""Lazy stdlib HTTP adapter for OpenAI-compatible chat-completions APIs."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import urlparse

from newsbot.ai.base import GenerationRequest, ProviderError
from newsbot.ai.structured_copy import RESPONSE_SCHEMA as _RESPONSE_SCHEMA
from newsbot.ai.structured_copy import SYSTEM_INSTRUCTION, _mapping, serialize_evidence, validate_draft_mapping
from newsbot.ai.structured_copy import _draft_from_mapping as _shared_draft_from_mapping
from newsbot.copywriting import CopyDraft


def _draft_from_mapping(value: object) -> CopyDraft:
    """Compatibility alias for the former module-local strict parser."""
    return _shared_draft_from_mapping(value)


@dataclass(frozen=True, slots=True)
class OpenAICompatibleConfig:
    """Configuration only; it neither imports a client nor opens a socket."""

    base_url: str
    api_key: str = field(repr=False)
    model: str = ""
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        parsed = urlparse(self.base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("base_url must be an absolute HTTP(S) URL")
        if not self.api_key:
            raise ValueError("api_key must not be empty")
        if not self.model:
            raise ValueError("model must not be empty")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


class OpenAICompatibleProvider:
    """Provider instantiated only after a generation capability is selected.

    The HTTP modules are imported inside the transport method so merely importing
    this module, validating unrelated modes, or constructing another provider
    cannot initialize an optional SDK or make a network request.
    """

    def __init__(self, config: OpenAICompatibleConfig) -> None:
        self._config = config

    @classmethod
    def factory(cls, config: OpenAICompatibleConfig) -> Callable[[], OpenAICompatibleProvider]:
        """Return a lazy provider factory for lease-gated pipeline workers."""
        return lambda: cls(config)

    async def generate(self, request: GenerationRequest) -> CopyDraft:
        payload = {
            "model": self._config.model,
            "temperature": 0,
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": "newsbot-category-v1", "strict": True, "schema": _RESPONSE_SCHEMA},
            },
            "messages": [
                {"role": "system", "content": SYSTEM_INSTRUCTION},
                {"role": "user", "content": serialize_evidence(request)},
            ],
        }
        response = await asyncio.to_thread(self._post, payload)
        try:
            choices = response.get("choices")
            if not isinstance(choices, list) or not choices:
                raise TypeError("choices is not a non-empty list")
            choice = _mapping(choices[0], "choice")
            message = _mapping(choice.get("message"), "message")
            content = message.get("content")
            if not isinstance(content, str):
                raise TypeError("content is not text")
            return validate_draft_mapping(json.loads(content), request)
        except (KeyError, IndexError, TypeError, json.JSONDecodeError, ValueError) as exc:
            raise ProviderError("OpenAI-compatible provider returned an invalid draft") from exc

    def _post(self, payload: dict[str, object]) -> dict[str, object]:
        # Deliberately local imports: no HTTP machinery is reached before generate().
        from urllib.error import HTTPError, URLError
        from urllib.request import Request, urlopen

        endpoint = self._config.base_url.rstrip("/") + "/chat/completions"
        request = Request(
            endpoint,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Authorization": f"Bearer {self._config.api_key}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(request, timeout=self._config.timeout_seconds) as response:
                return _mapping(json.loads(response.read().decode("utf-8")), "response")
        except (HTTPError, URLError, OSError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            raise ProviderError("OpenAI-compatible provider request failed") from exc
