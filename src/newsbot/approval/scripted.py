"""Credential-free approval adapter for deterministic fixture runs."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from newsbot.candidates import ApprovalResult, CandidateApprovalService


@dataclass(frozen=True, slots=True)
class ScriptedAction:
    token: str
    chat_id: int
    user_id: int


class ScriptedApprovalAdapter:
    """Replay serialized callback deliveries without Telegram credentials."""

    def __init__(self, service: CandidateApprovalService) -> None:
        self._service = service

    def apply(self, action: ScriptedAction) -> ApprovalResult:
        return self._service.apply(action.token, chat_id=action.chat_id, user_id=action.user_id)

    def replay(self, actions: Iterable[ScriptedAction]) -> tuple[ApprovalResult, ...]:
        return tuple(self.apply(action) for action in actions)
