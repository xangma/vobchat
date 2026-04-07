from __future__ import annotations

from functools import lru_cache
import re

from vobchat.api.schemas.chat import (
    ChatOperation,
    ChatThreadState,
    PlannerAction,
    PlannerResult,
)
from vobchat.core.llm.client import OpenAICompatibleLLMClient, get_llm_client
from vobchat.core.llm.prompts import build_planner_messages
from vobchat.utils.constants import UNIT_TYPES


POSTCODE_RE = re.compile(r"\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})\b", re.IGNORECASE)
ENTITY_ID_RE = re.compile(r"\b([NTUV]_[A-Z0-9_]+)\b", re.IGNORECASE)
YEAR_RE = re.compile(r"\b(1[6-9]\d{2}|20\d{2})\b")
PLACE_ID_RE = re.compile(
    r"\b(?:select|choose|resolve)\s+(?:place\s*)?#?(\d{1,9})\b",
    re.IGNORECASE,
)

KNOWN_THEME_TERMS = (
    "population",
    "housing",
    "industry",
    "learning",
    "language",
    "life",
    "death",
    "political",
    "religion",
    "social",
    "work",
    "poverty",
    "agriculture",
    "land",
)


class ChatPlanner:
    def __init__(
        self,
        llm_client: OpenAICompatibleLLMClient | None = None,
    ) -> None:
        self.llm_client = llm_client or get_llm_client()

    async def plan(self, state: ChatThreadState, user_message: str) -> PlannerResult:
        messages = build_planner_messages(state, user_message)
        try:
            raw = await self.llm_client.complete_json(
                messages,
                PlannerResult,
                temperature=0.0,
            )
            if isinstance(raw, PlannerResult):
                result = raw
            else:
                result = PlannerResult.model_validate(raw)
            result.source = "llm"
            return result
        except Exception:
            return self._fallback_plan(state, user_message)

    def _fallback_plan(self, state: ChatThreadState, user_message: str) -> PlannerResult:
        message = " ".join((user_message or "").split())
        lowered = message.lower()
        selected_theme = state.selected_theme.label if state.selected_theme else None
        place_query = self._extract_place_query(message)
        theme_query = self._extract_theme_query(message) or selected_theme
        entity_id_match = ENTITY_ID_RE.search(message)
        postcode_match = POSTCODE_RE.search(message)
        place_id_match = PLACE_ID_RE.search(message)
        year = self._extract_year(message)
        unit_type = self._extract_unit_type(message)

        if self._is_greeting(lowered):
            return self._reply_only("Reply with a brief greeting.")

        if place_id_match:
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.RESOLVE_PLACE,
                    place_id=int(place_id_match.group(1)),
                    confidence=0.95,
                ),
                notes="Fallback matched an explicit place id selection.",
            )

        if postcode_match:
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.LOOKUP_POSTCODE,
                    postcode=postcode_match.group(1).upper().replace(" ", ""),
                    confidence=0.95,
                ),
                notes="Fallback matched a postcode pattern.",
            )

        if state.selected_places and any(word in lowered for word in ("remove", "delete", "clear")):
            remove_query = None if "all" in lowered else place_query
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.REMOVE_PLACE,
                    place_query=remove_query,
                    confidence=0.8,
                ),
                notes="Fallback matched a place removal request.",
            )

        if "theme" in lowered and any(word in lowered for word in ("list", "show", "available", "what")):
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.LIST_THEMES,
                    place_query=place_query,
                    confidence=0.8,
                ),
                notes="Fallback matched a theme listing request.",
            )

        if any(word in lowered for word in ("measure", "measures", "cube", "cubes", "metric", "metrics")):
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.LIST_CUBES_FOR_THEME_AND_UNIT,
                    theme_query=theme_query,
                    confidence=0.75,
                ),
                notes="Fallback matched a cube listing request.",
            )

        if entity_id_match:
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.FETCH_DATA_ENTITY_INFO,
                    entity_id=entity_id_match.group(1).upper(),
                    confidence=0.9,
                ),
                notes="Fallback matched an explicit data entity id.",
            )

        if unit_type and any(
            phrase in lowered
            for phrase in ("unit type", "what is", "tell me about", "explain")
        ):
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.FETCH_UNIT_TYPE_INFO,
                    unit_type=unit_type,
                    confidence=0.8,
                ),
                notes="Fallback matched a unit type info request.",
            )

        if any(phrase in lowered for phrase in ("data entity", "entity", "what is")) and "population" not in lowered:
            entity_query = entity_id_match.group(1).upper() if entity_id_match else message
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.RESOLVE_DATA_ENTITY,
                    entity_query=entity_query,
                    confidence=0.7,
                ),
                notes="Fallback matched a data entity resolution request.",
            )

        if any(phrase in lowered for phrase in ("over time", "trend", "time series", "line chart", "graph", "plot")):
            if not state.selected_places and not place_query:
                return PlannerResult(
                    source="fallback",
                    action=PlannerAction(
                        operation=ChatOperation.CLARIFY,
                        confidence=0.6,
                        assistant_task="Which place should I use for that time-series request?",
                    ),
                    clarification_question="Which place should I use for that time-series request?",
                    notes="Fallback chart request is missing a place context.",
                )
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.FETCH_TIME_SERIES,
                    place_query=place_query,
                    theme_query=theme_query,
                    start_year=year,
                    end_year=year,
                    confidence=0.8,
                ),
                notes="Fallback matched a time-series request.",
            )

        if any(phrase in lowered for phrase in ("category", "categories", "breakdown")):
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.FETCH_CATEGORY_BREAKDOWN,
                    place_query=place_query,
                    theme_query=theme_query,
                    year=year,
                    confidence=0.75,
                ),
                notes="Fallback matched a category request.",
            )

        if any(phrase in lowered for phrase in ("map", "polygon", "boundary", "boundaries")):
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.FETCH_MAP_FEATURES,
                    place_query=place_query,
                    unit_type=unit_type,
                    theme_query=theme_query,
                    confidence=0.7,
                ),
                notes="Fallback matched a map request.",
            )

        if any(phrase in lowered for phrase in ("profile", "tell me about", "history of", "about this place", "place profile")):
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.FETCH_PLACE_PROFILE,
                    place_query=place_query,
                    confidence=0.7,
                ),
                notes="Fallback matched a place profile request.",
            )

        if theme_query and any(keyword in lowered for keyword in KNOWN_THEME_TERMS + ("theme",)):
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.RESOLVE_THEME,
                    theme_query=theme_query,
                    confidence=0.7,
                ),
                notes="Fallback matched a theme selection request.",
            )

        if place_query:
            return PlannerResult(
                source="fallback",
                action=PlannerAction(
                    operation=ChatOperation.SEARCH_PLACES,
                    place_query=place_query,
                    match_mode="exact" if len(place_query.split()) <= 2 else "fuzzy",
                    confidence=0.65,
                ),
                notes="Fallback matched a place search request.",
            )

        return self._reply_only("Reply helpfully without changing structured state.")

    @staticmethod
    def _reply_only(task: str) -> PlannerResult:
        return PlannerResult(
            source="fallback",
            action=PlannerAction(
                operation=ChatOperation.REPLY_ONLY,
                confidence=0.5,
                assistant_task=task,
            ),
            notes="Fallback defaulted to a conversational reply.",
        )

    @staticmethod
    def _is_greeting(lowered: str) -> bool:
        normalized = lowered.strip(" !?.")
        return normalized in {"hi", "hello", "hey", "good morning", "good afternoon", "good evening"}

    @staticmethod
    def _extract_year(message: str) -> int | None:
        match = YEAR_RE.search(message)
        return int(match.group(1)) if match else None

    @staticmethod
    def _extract_theme_query(message: str) -> str | None:
        lowered = message.lower()
        for term in KNOWN_THEME_TERMS:
            if term in lowered:
                return term.title()
        match = re.search(r"theme\s+([A-Za-z][A-Za-z &/-]+)", message, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return None

    @staticmethod
    def _extract_unit_type(message: str) -> str | None:
        lowered = message.lower()
        for code, metadata in UNIT_TYPES.items():
            if code.lower() in lowered:
                return code
            long_name = str(metadata.get("long_name", "")).lower()
            if long_name and long_name in lowered:
                return code
        return None

    @staticmethod
    def _extract_place_query(message: str) -> str | None:
        patterns = [
            r"(?:add|find|search for|search|select|choose)\s+(.+)$",
            r"(?:remove|delete|clear)\s+(.+)$",
            r"(?:show|plot|chart|graph|compare|map|display|load)\s+(.+)$",
            r"(?:for|about|in)\s+([A-Za-z][A-Za-z' -]+?)(?:\s+(?:over time|by category|on the map|map|theme|themes)\b|$)",
            r"(?:tell me about|profile of|history of)\s+([A-Za-z][A-Za-z' -]+)$",
        ]
        for pattern in patterns:
            match = re.search(pattern, message, re.IGNORECASE)
            if match:
                query = ChatPlanner._clean_place_query(match.group(1))
                if query:
                    return query
        if len(message.split()) <= 3 and not any(char.isdigit() for char in message):
            stripped = ChatPlanner._clean_place_query(message)
            if stripped and stripped.lower() not in KNOWN_THEME_TERMS:
                return stripped
        lowered = message.lower()
        semantic_markers = KNOWN_THEME_TERMS + (
            "over time",
            "time series",
            "line chart",
            "category",
            "categories",
            "breakdown",
            "map",
            "profile",
            "history",
        )
        if any(marker in lowered for marker in semantic_markers):
            stripped = ChatPlanner._clean_place_query(message)
            if stripped and stripped.lower() not in KNOWN_THEME_TERMS:
                return stripped
        return None

    @staticmethod
    def _clean_place_query(value: str) -> str | None:
        cleaned = YEAR_RE.sub(" ", value or "")
        removable_phrases = [
            "over time",
            "time series",
            "line chart",
            "category breakdown",
            "by category",
            "on the map",
            "place profile",
            "unit type",
            "data entity",
            "show",
            "plot",
            "chart",
            "graph",
            "compare",
            "map",
            "display",
            "load",
            "theme",
            "themes",
            "for",
            "about",
            "in",
            "on",
            "the",
            "a",
            "an",
            "me",
            "please",
        ]
        for phrase in sorted(KNOWN_THEME_TERMS + tuple(removable_phrases), key=len, reverse=True):
            cleaned = re.sub(rf"\b{re.escape(phrase)}\b", " ", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"[^A-Za-z' -]", " ", cleaned)
        normalized = " ".join(cleaned.split()).strip(" -")
        return normalized or None


@lru_cache(maxsize=1)
def get_chat_planner() -> ChatPlanner:
    return ChatPlanner()
