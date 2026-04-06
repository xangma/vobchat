from __future__ import annotations

from vobchat.api.schemas.metadata import (
    DataEntityInfoResponse,
    DataEntityReferenceResponse,
    DataEntityResolutionResponse,
    DataEntityResolveQuery,
    PlaceKeyFindingResponse,
    PlaceKeyFindingsResponse,
    PlaceProfileResponse,
    UnitTypeInfoResponse,
)
from vobchat.db.repositories import MetadataRepository


class MetadataService:
    def __init__(self, repository: MetadataRepository | None = None) -> None:
        self.repository = repository or MetadataRepository()

    def get_place_profile(self, place_id: int) -> PlaceProfileResponse | None:
        profile = self.repository.fetch_place_profile(place_id)
        if profile is None:
            return None
        return PlaceProfileResponse.model_validate(profile)

    def get_place_key_findings(self, unit_id: int) -> PlaceKeyFindingsResponse:
        items = self.repository.fetch_place_key_findings(unit_id)
        return PlaceKeyFindingsResponse(
            unit_id=unit_id,
            item_count=len(items),
            items=[PlaceKeyFindingResponse.model_validate(item) for item in items],
        )

    def get_unit_type_info(self, unit_type: str) -> UnitTypeInfoResponse | None:
        info = self.repository.fetch_unit_type_info(unit_type)
        if info is None:
            return None
        return UnitTypeInfoResponse.model_validate(info)

    def resolve_data_entity(
        self,
        query: DataEntityResolveQuery,
    ) -> DataEntityResolutionResponse | None:
        result = self.repository.resolve_data_entity(query.query)
        if result is None:
            return None
        return DataEntityResolutionResponse(
            query=query.query,
            result=DataEntityReferenceResponse.model_validate(result),
        )

    def get_data_entity_info(self, entity_id: str) -> DataEntityInfoResponse | None:
        info = self.repository.fetch_data_entity_info(entity_id)
        if info is None:
            return None
        return DataEntityInfoResponse.model_validate(info)


def get_metadata_service() -> MetadataService:
    return MetadataService()
