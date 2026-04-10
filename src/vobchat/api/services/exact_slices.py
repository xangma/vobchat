from __future__ import annotations

from dataclasses import dataclass, field

from vobchat.api.schemas.chat import (
    CategoryEntityRef,
    ExactSliceCandidate,
    ExactSliceRef,
    SlotStatus,
)
from vobchat.db.models import ExactSliceSummary
from vobchat.db.repositories import ExactSliceRepository


@dataclass
class ExactSliceResolution:
    candidates: list[ExactSliceCandidate] = field(default_factory=list)
    resolved: ExactSliceRef | None = None


class ExactSliceCatalogService:
    _SUPPORTED_FAMILIES: dict[str, set[str]] = {
        "T_POP": {"N_TOT_POP"},
    }

    def __init__(self, repository: ExactSliceRepository | None = None) -> None:
        self.repository = repository or ExactSliceRepository()

    def list_exact_slices(
        self,
        *,
        unit_id: int,
        theme_id: str,
        cube_id: str | None = None,
        cube_query: str | None = None,
    ) -> list[ExactSliceCandidate]:
        supported_cubes = self._SUPPORTED_FAMILIES.get(theme_id)
        if supported_cubes is None:
            return []
        if cube_id is not None and cube_id not in supported_cubes:
            return []

        summaries = self.repository.list_exact_slices_for_unit_theme(
            unit_id=unit_id,
            theme_id=theme_id,
            cube_id=cube_id,
        )
        candidates = [self._to_candidate(item) for item in summaries]

        if cube_query:
            lowered = cube_query.lower()
            candidates = [
                candidate
                for candidate in candidates
                if lowered in (candidate.label or "").lower()
                or lowered in (candidate.cube_id or "").lower()
            ]

        candidates = [
            candidate
            for candidate in candidates
            if candidate.cube_id in supported_cubes
        ]
        return candidates

    def list_catalog_inventory(
        self,
        *,
        unit_id: int,
        theme_id: str,
        cube_id: str | None = None,
        cube_query: str | None = None,
    ) -> list[ExactSliceCandidate]:
        summaries = self.repository.list_exact_slices_for_unit_theme(
            unit_id=unit_id,
            theme_id=theme_id,
            cube_id=cube_id,
        )
        candidates = [self._to_candidate(item) for item in summaries]
        if cube_query:
            lowered = cube_query.lower()
            candidates = [
                candidate
                for candidate in candidates
                if lowered in (candidate.label or "").lower()
                or lowered in (candidate.cube_id or "").lower()
                or lowered in (candidate.description or "").lower()
                or (
                    candidate.category_entity is not None
                    and lowered in (candidate.category_entity.label or "").lower()
                )
            ]
        return candidates

    def supported_cubes(self, theme_id: str | None) -> set[str]:
        if not theme_id:
            return set()
        return set(self._SUPPORTED_FAMILIES.get(theme_id, set()))

    def resolve_exact_slice(
        self,
        *,
        unit_id: int,
        theme_id: str,
        cube_id: str | None = None,
        cube_query: str | None = None,
    ) -> ExactSliceResolution:
        candidates = self.list_exact_slices(
            unit_id=unit_id,
            theme_id=theme_id,
            cube_id=cube_id,
            cube_query=cube_query,
        )
        if len(candidates) == 1:
            return ExactSliceResolution(
                candidates=candidates,
                resolved=ExactSliceRef.from_candidate(
                    candidates[0],
                    source="exact_slice_catalog",
                    slot_status=SlotStatus.RESOLVED,
                ),
            )
        return ExactSliceResolution(candidates=candidates)

    @staticmethod
    def _to_candidate(summary: ExactSliceSummary) -> ExactSliceCandidate:
        return ExactSliceCandidate(
            cube_id=summary.cube_id,
            cube_ids=[summary.cube_id],
            label=summary.slice_label or summary.cube_label,
            description=summary.slice_text or summary.cube_text,
            cellref=summary.cell_ref,
            dataitem_id=summary.dataitem_id,
            cat_id=summary.cat_id,
            view_id=summary.view_id,
            has_categories=summary.has_categories,
            category_entity=(
                CategoryEntityRef(
                    entity_id=summary.category_entity.entity_id,
                    label=summary.category_entity.label,
                    group_label=summary.category_entity.group_label,
                    source=summary.category_entity.source,
                    provenance=summary.category_entity.provenance,
                )
                if summary.category_entity is not None
                else None
            ),
            start_year=summary.start_year,
            end_year=summary.end_year,
            observation_count=summary.observation_count,
            metadata_provenance=summary.provenance,
            slot_status=SlotStatus.CANDIDATE_SET,
            source="exact_slice_catalog",
        )


def get_exact_slice_catalog_service() -> ExactSliceCatalogService:
    return ExactSliceCatalogService()
