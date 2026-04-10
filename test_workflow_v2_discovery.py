from __future__ import annotations

import asyncio

from vobchat.api.schemas.chat import (
    ChatOperation,
    ChatTurnRequest,
    DiscoveryTopic,
    ReceiptKind,
)
from vobchat.api.schemas.themes import ThemeSummaryResponse
from vobchat.api.services.chat_threads import InMemoryChatThreadStore
from test_chat_phase4 import (
    FakeLLMClient,
    build_orchestrator,
    make_multi_geography_place,
    make_resolved_place,
    seed_analysis_receipt,
)


def test_v2_catalog_overview_discovers_place_catalog_for_resolved_place() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Catalog loaded."))
    thread = store.create_thread()

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="what data do you have for York?",
                stream=False,
            )
        )
    )

    assert response.ui_delta.operation == ChatOperation.LIST_CATALOG_OVERVIEW
    assert response.ui_delta.discovery_result is not None
    assert response.ui_delta.discovery_result.topic == DiscoveryTopic.CATALOG_OVERVIEW
    assert response.ui_delta.discovery_result.place_context[0].label == "York"
    assert response.ui_delta.discovery_result.items[0].item_id == "T_POP"
    assert response.ui_delta.discovery_result.reporting_geographies[0].unit_type == "MOD_DIST"
    assert response.ui_delta.discovery_result.supported_outputs
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY


def test_v2_geography_discovery_returns_structured_candidates_and_follow_ups() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Geographies loaded."))
    thread = store.create_thread()
    thread.selected_places = [make_multi_geography_place()]
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="which geographies can I use here?",
                stream=False,
            )
        )
    )

    assert response.ui_delta.discovery_result is not None
    assert response.ui_delta.discovery_result.topic == DiscoveryTopic.REPORTING_GEOGRAPHIES
    assert {item.item_id for item in response.ui_delta.discovery_result.items} == {
        "MOD_DIST",
        "PAR_UNIT",
    }
    assert response.ui_delta.discovery_result.suggested_actions
    assert response.ui_delta.discovery_result.suggested_actions[0].operation == ChatOperation.LIST_THEMES


def test_v2_discovery_available_years_uses_exact_slice_subset() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Years loaded."))
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread.selected_theme = ThemeSummaryResponse(theme_id="T_POP", label="Population")
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="what years are available for population?",
                stream=False,
            )
        )
    )

    assert response.ui_delta.discovery_result is not None
    assert response.ui_delta.discovery_result.topic == DiscoveryTopic.AVAILABLE_YEARS
    assert response.ui_delta.discovery_result.available_years
    assert response.ui_delta.discovery_result.available_years[0].executable is True
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY


def test_v2_exact_slice_option_discovery_surfaces_supported_and_inventory_only_population_options() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(
        store=store,
        llm_client=FakeLLMClient(text="Exact slice options loaded."),
    )
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="what population breakdowns are available?",
                stream=False,
            )
        )
    )

    assert response.ui_delta.discovery_result is not None
    assert response.ui_delta.discovery_result.topic == DiscoveryTopic.EXACT_SLICE_OPTIONS
    assert response.ui_delta.discovery_result.exact_slice_options
    assert {candidate.cube_id for candidate in response.ui_delta.discovery_result.exact_slice_options} == {
        "N_TOT_POP"
    }
    assert any(
        "N_GENDER" in note for note in response.ui_delta.discovery_result.availability_notes
    )
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY


def test_v2_comparable_options_discovery_is_structured_and_safe() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Comparable options loaded."))
    thread = store.create_thread()
    thread.selected_places = [
        make_resolved_place(),
        make_resolved_place(place_id=4, name="Leeds", unit_id=102),
    ]
    thread = store.save_thread(thread)

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="what comparable options exist?",
                stream=False,
            )
        )
    )

    assert response.ui_delta.discovery_result is not None
    assert response.ui_delta.discovery_result.topic == DiscoveryTopic.COMPARABLE_OPTIONS
    assert response.ui_delta.discovery_result.items
    assert response.ui_delta.discovery_result.comparability_notes
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY


def test_v2_discovery_follow_up_can_select_reporting_geography_from_receipt_local_context() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Handled."))
    thread = store.create_thread()
    thread.selected_places = [make_multi_geography_place()]
    thread = store.save_thread(thread)

    initial = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="which geographies can I use here?",
                stream=False,
            )
        )
    )
    assert initial.thread_state.current_receipt is not None
    assert initial.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY

    follow_up = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="use parish level",
                stream=False,
            )
        )
    )

    assert follow_up.planner_result.action.operation == ChatOperation.LIST_THEMES
    assert follow_up.ui_delta.discovery_result is not None
    assert follow_up.ui_delta.discovery_result.topic == DiscoveryTopic.THEMES
    assert follow_up.thread_state.analysis_state.reporting_geography is not None
    assert follow_up.thread_state.analysis_state.reporting_geography.unit_type == "PAR_UNIT"
    assert follow_up.thread_state.current_receipt is not None
    assert follow_up.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY


def test_v2_discovery_follow_up_reuses_exact_slice_catalog_for_available_years() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Handled."))
    thread = store.create_thread()
    thread.selected_places = [make_resolved_place()]
    thread = store.save_thread(thread)

    first = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="what population breakdowns are available?",
                stream=False,
            )
        )
    )
    assert first.thread_state.current_receipt is not None
    assert first.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY

    second = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="show the available years",
                stream=False,
            )
        )
    )

    assert second.ui_delta.discovery_result is not None
    assert second.ui_delta.discovery_result.topic == DiscoveryTopic.AVAILABLE_YEARS
    assert second.ui_delta.discovery_result.available_years
    assert second.thread_state.current_receipt is not None
    assert second.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY


def test_v2_discovery_output_view_remains_distinct_from_analysis_receipts() -> None:
    store = InMemoryChatThreadStore()
    orchestrator = build_orchestrator(store=store, llm_client=FakeLLMClient(text="Outputs loaded."))
    thread = seed_analysis_receipt(store, places=[make_resolved_place()])

    response = asyncio.run(
        orchestrator.handle_turn(
            ChatTurnRequest(
                thread_id=thread.thread_id,
                message="what outputs are feasible here?",
                stream=False,
            )
        )
    )

    assert response.ui_delta.discovery_result is not None
    assert response.ui_delta.time_series is None
    assert response.thread_state.current_receipt is not None
    assert response.thread_state.current_receipt.kind == ReceiptKind.DISCOVERY
    assert response.thread_state.current_receipt.analysis_spec is None
