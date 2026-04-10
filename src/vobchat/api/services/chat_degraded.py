from __future__ import annotations

import re
from dataclasses import dataclass, field

from vobchat.api.schemas.chat import (
    ChatOperation,
    ChatThreadState,
    ClarificationOption,
    ClarificationState,
    DegradedModeOutcome,
    PlannerAction,
    PlannerResult,
    WorkflowRuntimeMode,
    WorkflowRuntimeState,
)
from vobchat.api.schemas.places import PlaceCandidateResponse, PlaceSearchQuery
from vobchat.api.schemas.themes import ThemeResolveQuery
from vobchat.api.services.places import PlacesService
from vobchat.api.services.themes import ThemesService
from vobchat.utils.constants import UNIT_TYPES


ENTITY_ID_RE = re.compile(r"^(?:what is|tell me about|explain)?\s*(?:the\s+)?([NTUV]_[A-Z0-9_]+)\??$", re.IGNORECASE)


@dataclass
class DegradedInterpretationResult:
    matched: bool
    runtime_state: WorkflowRuntimeState
    planner_result: PlannerResult | None = None
    clarification: ClarificationState | None = None
    place_candidates: list[PlaceCandidateResponse] = field(default_factory=list)
    fallback_text: str | None = None
    notices: list[str] = field(default_factory=list)


class DegradedInterpreter:
    _PLACE_ONLY_RE = re.compile(
        r"^(?:show|place|select|choose)\s+(?P<place>[A-Za-z][A-Za-z' -]+)$",
        re.IGNORECASE,
    )
    _BARE_PLACE_RE = re.compile(r"^[A-Za-z][A-Za-z' -]{1,80}$")
    _THEME_ONLY_RE = re.compile(
        r"^(?:theme\s+|show\s+theme\s+)?(?P<theme>[A-Za-z][A-Za-z &/'-]+)$",
        re.IGNORECASE,
    )
    _INFO_RE = re.compile(
        r"^(?:what is|what's|tell me about|explain)\s+(?:an?\s+|the\s+)?(?P<target>.+?)\??$",
        re.IGNORECASE,
    )

    def __init__(
        self,
        *,
        places_service: PlacesService,
        themes_service: ThemesService,
    ) -> None:
        self.places_service = places_service
        self.themes_service = themes_service

    def interpret(
        self,
        *,
        state: ChatThreadState,
        message: str,
        degraded_reason: str,
        detail: str,
    ) -> DegradedInterpretationResult:
        stripped = " ".join((message or "").split()).strip()
        info_target = self._extract_info_target(stripped)

        entity_id = self._resolve_entity_id(stripped, info_target)
        if entity_id is not None:
            return self._success(
                degraded_reason=degraded_reason,
                detail=detail,
                planner_result=PlannerResult(
                    source="fallback",
                    action=PlannerAction(
                        operation=ChatOperation.FETCH_DATA_ENTITY_INFO,
                        entity_id=entity_id,
                        confidence=1.0,
                    ),
                    notes="degraded_exact_data_entity_info",
                ),
            )

        unit_type = self._resolve_unit_type(stripped, info_target)
        if unit_type is not None:
            return self._success(
                degraded_reason=degraded_reason,
                detail=detail,
                planner_result=PlannerResult(
                    source="fallback",
                    action=PlannerAction(
                        operation=ChatOperation.FETCH_UNIT_TYPE_INFO,
                        unit_type=unit_type,
                        confidence=1.0,
                    ),
                    notes="degraded_exact_unit_type_info",
                ),
            )

        if info_target is not None:
            place_profile = self._resolve_place_profile(
                target=info_target,
                degraded_reason=degraded_reason,
                detail=detail,
            )
            if place_profile is not None:
                return place_profile

        theme_result = self._resolve_theme_lookup(
            stripped=stripped,
            degraded_reason=degraded_reason,
            detail=detail,
        )
        if theme_result is not None:
            return theme_result

        place_result = self._resolve_place_lookup(
            stripped=stripped,
            state=state,
            degraded_reason=degraded_reason,
            detail=detail,
        )
        if place_result is not None:
            return place_result

        return self._unsupported(
            degraded_reason=degraded_reason,
            detail=detail,
        )

    def _resolve_place_lookup(
        self,
        *,
        stripped: str,
        state: ChatThreadState,
        degraded_reason: str,
        detail: str,
    ) -> DegradedInterpretationResult | None:
        query = self._extract_place_query(stripped)
        if query is None:
            return None
        search = self.places_service.search_places(
            PlaceSearchQuery(query=query, match_mode="exact", limit=10)
        )
        if search.result_count > 1:
            question = "Choose which place you mean. Degraded mode can only continue with one exact place."
            return DegradedInterpretationResult(
                matched=True,
                runtime_state=self._runtime_state(
                    degraded_reason=degraded_reason,
                    detail=detail,
                    degraded_outcome=DegradedModeOutcome.GUIDED_CLARIFICATION,
                    notice=question,
                ),
                clarification=ClarificationState(
                    question=question,
                    slot="place_identity",
                    options=[
                        ClarificationOption(
                            option_id=str(item.place_id),
                            label=item.name,
                            kind="place_candidate",
                            metadata={"place_id": item.place_id},
                        )
                        for item in search.results
                    ],
                ),
                place_candidates=list(search.results),
                fallback_text=question,
                notices=[question],
            )
        if search.result_count == 0:
            return self._success(
                degraded_reason=degraded_reason,
                detail=detail,
                planner_result=PlannerResult(
                    source="fallback",
                    action=PlannerAction(
                        operation=ChatOperation.SEARCH_PLACES,
                        place_query=query,
                        match_mode="exact",
                        confidence=1.0,
                    ),
                    notes="degraded_exact_place_search",
                ),
            )
        return self._success(
            degraded_reason=degraded_reason,
            detail=detail,
            planner_result=PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.RESOLVE_PLACE,
                    place_id=search.results[0].place_id,
                    place_query=query,
                    confidence=1.0,
                ),
                notes="degraded_exact_place_lookup",
            ),
        )

    def _resolve_place_profile(
        self,
        *,
        target: str,
        degraded_reason: str,
        detail: str,
    ) -> DegradedInterpretationResult | None:
        if self._resolve_unit_type(target, target) is not None:
            return None
        if self._resolve_entity_id(target, target) is not None:
            return None
        search = self.places_service.search_places(
            PlaceSearchQuery(query=target, match_mode="exact", limit=10)
        )
        if search.result_count > 1:
            question = "Choose which place profile you want. Degraded mode needs one exact place."
            return DegradedInterpretationResult(
                matched=True,
                runtime_state=self._runtime_state(
                    degraded_reason=degraded_reason,
                    detail=detail,
                    degraded_outcome=DegradedModeOutcome.GUIDED_CLARIFICATION,
                    notice=question,
                ),
                clarification=ClarificationState(
                    question=question,
                    slot="place_identity",
                    options=[
                        ClarificationOption(
                            option_id=str(item.place_id),
                            label=item.name,
                            kind="place_candidate",
                            metadata={"place_id": item.place_id},
                        )
                        for item in search.results
                    ],
                ),
                place_candidates=list(search.results),
                fallback_text=question,
                notices=[question],
            )
        if search.result_count == 1:
            return self._success(
                degraded_reason=degraded_reason,
                detail=detail,
                planner_result=PlannerResult(
                    source="fallback",
                    action=PlannerAction(
                        operation=ChatOperation.FETCH_PLACE_PROFILE,
                        place_query=target,
                        confidence=1.0,
                    ),
                    notes="degraded_exact_place_profile",
                ),
            )
        return None

    def _resolve_theme_lookup(
        self,
        *,
        stripped: str,
        degraded_reason: str,
        detail: str,
    ) -> DegradedInterpretationResult | None:
        match = self._THEME_ONLY_RE.match(stripped)
        if match is None:
            return None
        theme_query = match.group("theme").strip()
        if theme_query.lower() in {"show", "theme", "tell me"}:
            return None
        resolution = self.themes_service.resolve_theme(ThemeResolveQuery(query=theme_query))
        if resolution is None:
            return None
        normalized_query = self._normalize_lookup_text(theme_query)
        if normalized_query not in {
            self._normalize_lookup_text(resolution.result.label or ""),
            self._normalize_lookup_text(resolution.result.theme_id or ""),
        }:
            return None
        return self._success(
            degraded_reason=degraded_reason,
            detail=detail,
            planner_result=PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.RESOLVE_THEME,
                    theme_query=resolution.result.label,
                    theme_id=resolution.result.theme_id,
                    confidence=1.0,
                ),
                notes="degraded_exact_theme_lookup",
            ),
        )

    @staticmethod
    def _extract_info_target(message: str) -> str | None:
        match = DegradedInterpreter._INFO_RE.match(message)
        if match is None:
            return None
        return match.group("target").strip().strip("?")

    def _resolve_entity_id(self, message: str, info_target: str | None) -> str | None:
        for candidate in (message, info_target):
            if not candidate:
                continue
            match = ENTITY_ID_RE.match(candidate.strip())
            if match is not None:
                return match.group(1).upper()
        return None

    @staticmethod
    def _resolve_unit_type(message: str, info_target: str | None) -> str | None:
        normalized_candidates = []
        for candidate in (message, info_target):
            if not candidate:
                continue
            normalized = DegradedInterpreter._normalize_lookup_text(candidate)
            if normalized:
                normalized_candidates.append(normalized)
        for normalized in normalized_candidates:
            for code, metadata in UNIT_TYPES.items():
                long_name = DegradedInterpreter._normalize_lookup_text(
                    str(metadata.get("long_name", ""))
                )
                if normalized == DegradedInterpreter._normalize_lookup_text(code) or (
                    long_name and normalized == long_name
                ):
                    return code
        return None

    @staticmethod
    def _normalize_lookup_text(value: str) -> str:
        return re.sub(r"\s+", " ", (value or "")).strip().lower().rstrip("?")

    @classmethod
    def _extract_place_query(cls, message: str) -> str | None:
        wrapped = cls._PLACE_ONLY_RE.match(message)
        if wrapped is not None:
            return wrapped.group("place").strip()
        if not cls._BARE_PLACE_RE.match(message):
            return None
        lowered = message.lower()
        if lowered in {"population", "housing", "industry", "language", "learning", "life", "death"}:
            return None
        if len(message.split()) > 4:
            return None
        return message.strip()

    def _unsupported(
        self,
        *,
        degraded_reason: str,
        detail: str,
    ) -> DegradedInterpretationResult:
        guidance = [
            "Try an exact place, theme, or metadata request.",
            "Examples: `York`, `population`, `what is N_TOT_POP`, `tell me about York`.",
            "Explicit catalog questions still work, such as `what themes are available for York?`.",
            "Receipt-local follow-ups still work, such as `show the table` or `use 1901 instead`.",
        ]
        if degraded_reason == "llm_not_configured":
            guidance.append("Run `vobchat setup-llm` or set LLM_OPENAI_BASE_URL and LLM_MODEL.")
            degraded_outcome = DegradedModeOutcome.SETUP_GUIDANCE
        else:
            guidance.append("Run `vobchat doctor llm` or `vobchat setup-llm` to restore full chat interpretation.")
            degraded_outcome = DegradedModeOutcome.UNSUPPORTED_IN_DEGRADED_MODE
        fallback_text = (
            "The chat model is unavailable, so I can only handle exact place/theme/info lookups, "
            "explicit catalog questions, and simple receipt-based follow-ups right now."
        )
        return DegradedInterpretationResult(
            matched=True,
            runtime_state=self._runtime_state(
                degraded_reason=degraded_reason,
                detail=detail,
                degraded_outcome=degraded_outcome,
                notice=fallback_text,
                setup_guidance=guidance,
            ),
            fallback_text=fallback_text,
            notices=[fallback_text, *guidance],
        )

    def _success(
        self,
        *,
        degraded_reason: str,
        detail: str,
        planner_result: PlannerResult,
    ) -> DegradedInterpretationResult:
        notice = "Handled in deterministic degraded mode because the chat model is unavailable."
        return DegradedInterpretationResult(
            matched=True,
            runtime_state=self._runtime_state(
                degraded_reason=degraded_reason,
                detail=detail,
                degraded_outcome=DegradedModeOutcome.DETERMINISTIC_SUCCESS,
                notice=notice,
            ),
            planner_result=planner_result,
            notices=[notice],
        )

    @staticmethod
    def _runtime_state(
        *,
        degraded_reason: str,
        detail: str,
        degraded_outcome: DegradedModeOutcome,
        notice: str,
        setup_guidance: list[str] | None = None,
    ) -> WorkflowRuntimeState:
        return WorkflowRuntimeState(
            mode=WorkflowRuntimeMode.DETERMINISTIC_DEGRADED,
            degraded_reason=degraded_reason,
            degraded_outcome=degraded_outcome,
            detail=detail,
            notice=notice,
            setup_guidance=list(setup_guidance or []),
        )
