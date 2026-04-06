from __future__ import annotations

import json
from typing import Any

from vobchat.api.schemas.maps import (
    MapFeatureCollectionResponse,
    MapFeatureResponse,
    MapFeaturesByIdsQuery,
    MapFeaturesQuery,
)
from vobchat.db.models import BoundingBox, MapFeatureRow
from vobchat.db.repositories import MapsRepository


class MapsService:
    def __init__(self, repository: MapsRepository | None = None) -> None:
        self.repository = repository or MapsRepository()

    def get_features(self, query: MapFeaturesQuery) -> MapFeatureCollectionResponse:
        if query.has_bbox:
            bbox = BoundingBox(
                min_x=query.min_x or 0.0,
                min_y=query.min_y or 0.0,
                max_x=query.max_x or 0.0,
                max_y=query.max_y or 0.0,
            )
            features = self.repository.fetch_map_features_by_bbox(
                unit_types=(query.unit_type,),
                bbox=bbox,
                start_year=query.start_year,
                end_year=query.end_year,
                exclude_ids=query.exclude_ids,
                theme_id=query.theme_id,
            )
            mode = "bbox"
        else:
            features = self.repository.fetch_map_features(
                unit_type=query.unit_type,
                start_year=query.start_year,
                end_year=query.end_year,
                theme_id=query.theme_id,
            )
            mode = "all"

        return MapFeatureCollectionResponse(
            mode=mode,
            unit_type=query.unit_type,
            feature_count=len(features),
            start_year=query.start_year,
            end_year=query.end_year,
            theme_id=query.theme_id,
            bbox=query.bbox,
            requested_ids=[],
            features=[self._to_feature_response(feature) for feature in features],
        )

    def get_features_by_ids(
        self,
        query: MapFeaturesByIdsQuery,
    ) -> MapFeatureCollectionResponse:
        features = self.repository.fetch_map_features_by_ids(
            unit_type=query.unit_type,
            feature_ids=query.ids,
            start_year=query.start_year,
            end_year=query.end_year,
            theme_id=query.theme_id,
        )
        return MapFeatureCollectionResponse(
            mode="ids",
            unit_type=query.unit_type,
            feature_count=len(features),
            start_year=query.start_year,
            end_year=query.end_year,
            theme_id=query.theme_id,
            bbox=None,
            requested_ids=query.ids,
            features=[self._to_feature_response(feature) for feature in features],
        )

    @staticmethod
    def _to_feature_response(feature: MapFeatureRow) -> MapFeatureResponse:
        geometry: dict[str, Any] = {}
        if feature.geometry_geojson:
            geometry = json.loads(feature.geometry_geojson)
        return MapFeatureResponse(
            unit_id=feature.unit_id,
            unit_name=feature.unit_name,
            unit_type=feature.unit_type,
            geometry=geometry,
            start_year=feature.start_year,
            end_year=feature.end_year,
            has_theme=feature.has_theme,
        )


def get_maps_service() -> MapsService:
    return MapsService()
