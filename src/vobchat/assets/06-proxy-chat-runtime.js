(function () {
  "use strict";

  const runtime = {
    eventSource: null,
    threadId: null,
    connectPromise: null,
    bootstrapped: false,
    storeCache: Object.create(null),
  };

  function clone(value) {
    if (value === null || value === undefined) {
      return value;
    }
    return JSON.parse(JSON.stringify(value));
  }

  function nowIso() {
    return new Date().toISOString();
  }

  function currentYear() {
    return new Date().getFullYear();
  }

  function safeParse(text) {
    try {
      return JSON.parse(text);
    } catch (_) {
      return null;
    }
  }

  function normalizeBasePath(path) {
    if (!path || path === "/") {
      return "";
    }
    return path.endsWith("/") ? path.slice(0, -1) : path;
  }

  function joinPath(base, path) {
    const normalizedBase = normalizeBasePath(base);
    if (!normalizedBase) {
      return path;
    }
    return `${normalizedBase}${path}`;
  }

  function getRuntimeConfig() {
    const element = document.getElementById("web-runtime-config");
    const basePath = normalizeBasePath(element?.dataset?.basePath || "");
    return {
      basePath,
      proxyChatPrefix:
        element?.dataset?.proxyChatPrefix || joinPath(basePath, "/proxy/chat"),
    };
  }

  function getDashSetProps() {
    return window.dash_clientside && window.dash_clientside.set_props;
  }

  function isPersistentStore(id) {
    return [
      "thread-state-store",
      "selection-state-store",
      "map-state-store",
      "visualization-state-store",
      "metadata-state-store",
    ].includes(id);
  }

  function readPersistentStore(id) {
    const storages = [window.sessionStorage, window.localStorage];
    for (const storage of storages) {
      if (!storage || typeof storage.getItem !== "function") {
        continue;
      }
      const raw = storage.getItem(id);
      if (!raw) {
        continue;
      }
      const parsed = safeParse(raw);
      if (parsed !== null || raw === "null") {
        return parsed;
      }
    }
    return null;
  }

  function writePersistentStore(id, data) {
    if (!isPersistentStore(id) || !window.sessionStorage) {
      return;
    }
    try {
      window.sessionStorage.setItem(id, JSON.stringify(data));
    } catch (_) {
      // Ignore persistence failures and keep the in-memory cache authoritative.
    }
  }

  function readStore(id) {
    const element = document.getElementById(id);
    if (!element) {
      if (Object.prototype.hasOwnProperty.call(runtime.storeCache, id)) {
        return clone(runtime.storeCache[id]);
      }
      const persisted = readPersistentStore(id);
      if (persisted !== null || window.sessionStorage?.getItem?.(id) === "null") {
        runtime.storeCache[id] = clone(persisted);
        return clone(persisted);
      }
      return null;
    }
    if (element._dash_value !== undefined) {
      runtime.storeCache[id] = clone(element._dash_value);
      return clone(element._dash_value);
    }
    const text = (element.textContent || "").trim();
    const parsed = text ? safeParse(text) : null;
    runtime.storeCache[id] = clone(parsed);
    return parsed;
  }

  function setProps(id, props) {
    const setter = getDashSetProps();
    if (!setter) {
      return false;
    }
    setter(id, props);
    return true;
  }

  function writeStore(id, data) {
    runtime.storeCache[id] = clone(data);
    writePersistentStore(id, data);
    setProps(id, { data });
    return data;
  }

  function initialSelectionState() {
    return {
      current_receipt_id: null,
      current_receipt_kind: null,
      ui_projection: null,
      render_projection: null,
      selected_places: [],
      selected_theme: null,
      selected_cubes: [],
      reporting_geography: null,
      analysis_spec: null,
      exact_slice: null,
      time_scope: null,
      active_output_mode: null,
      available_themes: [],
      available_cubes: [],
      pending_place_candidates: [],
      pending_clarification: null,
      discovery_result: null,
      defaults_used: [],
      notices: [],
      runtime_state: null,
      provenance_summary: null,
    };
  }

  function initialMapState() {
    return {
      ui_projection: null,
      render_projection: null,
      reporting_geography: null,
      active_output_mode: null,
      unit_type: "MOD_REG",
      year_range: [1801, currentYear()],
      bbox: null,
      selected_ids: [],
      with_theme_ids: [],
      feature_count: 0,
      last_loaded_unit_types: [],
      notices: [],
      provenance_summary: null,
      status: "Choose a place or geography level to load a map.",
    };
  }

  function initialVisualizationState() {
    return {
      current_receipt_id: null,
      ui_projection: null,
      render_projection: null,
      dataset_family: null,
      exact_slice: null,
      time_scope: null,
      active_output_mode: null,
      active_tab: "line",
      time_series: null,
      category_breakdown: null,
      category_year: null,
      provenance_summary: null,
      status: "Choose a place and a data theme to see a chart or table.",
    };
  }

  function initialMetadataState() {
    return {
      render_projection: null,
      place_profile: null,
      place_key_findings: null,
      unit_type_info: null,
      data_entity_resolution: null,
      data_entity_info: null,
      requested_unit_type: null,
      requested_entity_id: null,
      provenance_summary: null,
    };
  }

  function initialRequestStatus() {
    return {
      mode: "stream",
      busy: false,
      error: null,
      last_turn_id: null,
    };
  }

  function coerceState(current, fallbackFactory) {
    const fallback = fallbackFactory();
    if (!current || typeof current !== "object") {
      return fallback;
    }
    return Object.assign(fallback, clone(current));
  }

  function coerceThreadState() {
    const current = readStore("thread-state-store");
    return current && typeof current === "object" ? clone(current) : null;
  }

  function coerceSelectionState() {
    return coerceState(readStore("selection-state-store"), initialSelectionState);
  }

  function coerceMapState() {
    return coerceState(readStore("map-state-store"), initialMapState);
  }

  function coerceVisualizationState() {
    return coerceState(readStore("visualization-state-store"), initialVisualizationState);
  }

  function coerceMetadataState() {
    return coerceState(readStore("metadata-state-store"), initialMetadataState);
  }

  function coerceRequestStatus() {
    return coerceState(readStore("request-status-store"), initialRequestStatus);
  }

  function setInputValue(value) {
    const input = document.getElementById("chat-input");
    if (input) {
      input.value = value;
    }
    setProps("chat-input", { value });
  }

  function setControlsBusy(busy) {
    const sendButton = document.getElementById("send-button");
    const resetButton = document.getElementById("reset-button");
    const input = document.getElementById("chat-input");
    if (sendButton) {
      sendButton.disabled = busy;
    }
    if (resetButton) {
      resetButton.disabled = false;
    }
    if (input) {
      input.disabled = busy;
    }
  }

  function updateRequestStatus(partial) {
    const next = Object.assign(coerceRequestStatus(), partial || {});
    writeStore("request-status-store", next);
    setControlsBusy(!!next.busy);
    return next;
  }

  function selectedUnitIds(selectionState) {
    if (
      selectionState.reporting_geography &&
      Array.isArray(selectionState.reporting_geography.unit_ids) &&
      selectionState.reporting_geography.unit_ids.length > 0
    ) {
      return selectionState.reporting_geography.unit_ids
        .map((unitId) => Number(unitId))
        .filter((unitId) => Number.isFinite(unitId));
    }
    return [];
  }

  function projectedSelectedPlaces(source) {
    if (Array.isArray(source?.selected_places) && source.selected_places.length > 0) {
      return clone(source.selected_places);
    }
    if (Array.isArray(source?.ui_projection?.selected_places)) {
      return clone(source.ui_projection.selected_places);
    }
    return [];
  }

  function projectedSelectedTheme(source) {
    if (source?.selected_theme) {
      return clone(source.selected_theme);
    }
    if (source?.ui_projection?.dataset_family) {
      return clone(source.ui_projection.dataset_family);
    }
    return null;
  }

  function projectedSelectedCubes(source) {
    if (Array.isArray(source?.selected_cubes) && source.selected_cubes.length > 0) {
      return clone(source.selected_cubes);
    }
    if (Array.isArray(source?.ui_projection?.selected_cubes)) {
      return clone(source.ui_projection.selected_cubes);
    }
    return [];
  }

  function projectedUnitType(source) {
    if (source?.ui_projection?.reporting_geography?.unit_type) {
      return String(source.ui_projection.reporting_geography.unit_type);
    }
    if (source?.analysis_state?.reporting_geography?.unit_type) {
      return String(source.analysis_state.reporting_geography.unit_type);
    }
    return null;
  }

  function authoritativeUIProjection(source) {
    if (source?.ui_projection) {
      return source.ui_projection;
    }
    if (source?.current_receipt?.ui_projection) {
      return source.current_receipt.ui_projection;
    }
    return null;
  }

  function authoritativeRenderProjection(source) {
    if (source?.render_projection) {
      return source.render_projection;
    }
    if (source?.current_receipt?.render_projection) {
      return source.current_receipt.render_projection;
    }
    return null;
  }

  function applyProjectionToSelectionState(selectionState, source) {
    const uiProjection = authoritativeUIProjection(source);
    const renderProjection = authoritativeRenderProjection(source);
    if (uiProjection) {
      selectionState.ui_projection = clone(uiProjection);
      selectionState.selected_places = clone(uiProjection.selected_places || []);
      selectionState.selected_theme = clone(uiProjection.dataset_family || null);
      selectionState.selected_cubes = clone(uiProjection.selected_cubes || []);
      selectionState.reporting_geography = clone(uiProjection.reporting_geography || null);
      selectionState.analysis_spec = clone(uiProjection.analysis_spec || null);
      selectionState.exact_slice = clone(uiProjection.exact_slice || null);
      selectionState.time_scope = clone(uiProjection.time_scope || null);
      selectionState.active_output_mode = uiProjection.active_output_mode || null;
      selectionState.available_themes = clone(uiProjection.available_themes || []);
      selectionState.available_cubes = clone(uiProjection.available_cubes || []);
      selectionState.pending_clarification = clone(uiProjection.clarification || null);
      selectionState.discovery_result = clone(uiProjection.discovery_result || null);
      selectionState.defaults_used = clone(uiProjection.defaults_used || []);
      selectionState.notices = clone(uiProjection.notices || []);
      selectionState.runtime_state = clone(uiProjection.runtime_state || null);
      selectionState.provenance_summary = clone(uiProjection.provenance_summary || null);
      selectionState.current_receipt_id = uiProjection.current_receipt_id || null;
      selectionState.current_receipt_kind = uiProjection.current_receipt_kind || null;
    } else if (source?.runtime_state) {
      selectionState.runtime_state = clone(source.runtime_state);
    }
    if (renderProjection) {
      selectionState.render_projection = clone(renderProjection);
    }
    if (Array.isArray(source?.place_search_results) && source.place_search_results.length > 0) {
      selectionState.pending_place_candidates = clone(source.place_search_results);
    } else if (source?.operation) {
      selectionState.pending_place_candidates = [];
    }
    if (source?.cleared_places) {
      selectionState.selected_places = [];
      selectionState.selected_theme = null;
      selectionState.selected_cubes = [];
      selectionState.reporting_geography = null;
      selectionState.analysis_spec = null;
      selectionState.exact_slice = null;
      selectionState.time_scope = null;
      selectionState.active_output_mode = null;
      selectionState.available_themes = [];
      selectionState.available_cubes = [];
      selectionState.pending_clarification = null;
      selectionState.discovery_result = null;
      selectionState.defaults_used = [];
      selectionState.runtime_state = null;
      selectionState.provenance_summary = null;
    }
  }

  function applyProjectionToMapState(mapState, source) {
    const uiProjection = authoritativeUIProjection(source);
    const renderProjection = authoritativeRenderProjection(source);
    if (uiProjection) {
      mapState.ui_projection = clone(uiProjection);
      mapState.reporting_geography = clone(uiProjection.reporting_geography || null);
      mapState.active_output_mode = uiProjection.active_output_mode || null;
      mapState.selected_ids = Array.isArray(uiProjection.reporting_geography?.unit_ids)
        ? uiProjection.reporting_geography.unit_ids
            .map((unitId) => Number(unitId))
            .filter((unitId) => Number.isFinite(unitId))
        : [];
      if (uiProjection.reporting_geography?.unit_type) {
        mapState.unit_type = String(uiProjection.reporting_geography.unit_type);
      }
      mapState.notices = clone(uiProjection.notices || []);
      mapState.provenance_summary = clone(uiProjection.provenance_summary || null);
    }
    if (renderProjection) {
      mapState.render_projection = clone(renderProjection);
      if (renderProjection.provenance_summary) {
        mapState.provenance_summary = clone(renderProjection.provenance_summary);
      }
      if (renderProjection.boundary_map) {
        mapState.feature_count = Number(renderProjection.boundary_map.feature_count || 0);
        if (renderProjection.boundary_map.unit_type) {
          mapState.unit_type = String(renderProjection.boundary_map.unit_type);
        }
      }
    }
  }

  function applyProjectionToVisualizationState(visualizationState, source) {
    const uiProjection = authoritativeUIProjection(source);
    const renderProjection = authoritativeRenderProjection(source);
    if (uiProjection) {
      visualizationState.ui_projection = clone(uiProjection);
      visualizationState.current_receipt_id = uiProjection.current_receipt_id || null;
      visualizationState.dataset_family = clone(uiProjection.dataset_family || null);
      visualizationState.exact_slice = clone(uiProjection.exact_slice || null);
      visualizationState.time_scope = clone(uiProjection.time_scope || null);
      visualizationState.active_output_mode = uiProjection.active_output_mode || null;
      visualizationState.provenance_summary = clone(uiProjection.provenance_summary || null);
    }
    if (renderProjection) {
      visualizationState.render_projection = clone(renderProjection);
      if (renderProjection.provenance_summary) {
        visualizationState.provenance_summary = clone(renderProjection.provenance_summary);
      }
      if (renderProjection.active_output_mode) {
        visualizationState.active_output_mode = renderProjection.active_output_mode;
      }
      if (renderProjection.chart) {
        if (String(renderProjection.active_output_mode || "").includes("category")) {
          visualizationState.category_breakdown = clone(renderProjection.chart);
          visualizationState.active_tab = "categories";
          visualizationState.category_year = renderProjection.chart.year || null;
        } else if (Array.isArray(renderProjection.chart.rows)) {
          const firstRow = renderProjection.chart.rows[0] || {};
          if (Object.prototype.hasOwnProperty.call(firstRow, "category_label")) {
            visualizationState.category_breakdown = clone(renderProjection.chart);
            visualizationState.active_tab = "categories";
            visualizationState.category_year = renderProjection.chart.year || null;
          } else {
            visualizationState.time_series = clone(renderProjection.chart);
            visualizationState.active_tab = "line";
          }
        }
      }
      if (renderProjection.table && visualizationState.active_output_mode === "table") {
        visualizationState.active_tab = "table";
      }
      if (renderProjection.answer_text) {
        visualizationState.status = renderProjection.answer_text;
      }
    }
  }

  function applyProjectionToMetadataState(metadataState, source) {
    const renderProjection = authoritativeRenderProjection(source);
    if (!renderProjection) {
      return;
    }
    metadataState.render_projection = clone(renderProjection);
    metadataState.provenance_summary = clone(renderProjection.provenance_summary || null);
    const metadataPayload = renderProjection.metadata_payload || {};
    [
      "place_profile",
      "place_key_findings",
      "unit_type_info",
      "data_entity_resolution",
      "data_entity_info",
    ].forEach((field) => {
      metadataState[field] = clone(metadataPayload[field] || null);
    });
    if (metadataPayload.unit_type_info?.identifier) {
      metadataState.requested_unit_type = metadataPayload.unit_type_info.identifier;
    }
    if (metadataPayload.data_entity_info?.entity_id) {
      metadataState.requested_entity_id = metadataPayload.data_entity_info.entity_id;
    } else if (metadataPayload.data_entity_resolution?.result?.entity_id) {
      metadataState.requested_entity_id = metadataPayload.data_entity_resolution.result.entity_id;
    }
  }

  function appendUserMessage(userMessage) {
    const current = coerceThreadState() || {
      thread_id: runtime.threadId,
      messages: [],
      selected_places: [],
      selected_theme: null,
      selected_cubes: [],
      conversation_state: {
        transcript: [],
        current_focus: null,
        pending_clarification: null,
        current_receipt_id: null,
        recent_receipt_ids: [],
        discourse_anchors: {},
      },
    };
    const next = Object.assign({}, current);
    next.messages = Array.isArray(current.messages) ? current.messages.slice() : [];
    next.messages.push(clone(userMessage));
    next.updated_at = nowIso();
    writeStore("thread-state-store", next);
  }

  function applyThreadSnapshot(threadState) {
    if (!threadState || typeof threadState !== "object") {
      return;
    }

    writeStore("thread-state-store", clone(threadState));

    const selectionState = coerceSelectionState();
    applyProjectionToSelectionState(selectionState, threadState);
    writeStore("selection-state-store", selectionState);

    const mapState = coerceMapState();
    applyProjectionToMapState(mapState, threadState);
    writeStore("map-state-store", mapState);

    const visualizationState = coerceVisualizationState();
    applyProjectionToVisualizationState(visualizationState, threadState);
    writeStore("visualization-state-store", visualizationState);

    const metadataState = coerceMetadataState();
    applyProjectionToMetadataState(metadataState, threadState);
    writeStore("metadata-state-store", metadataState);

    if (threadState.latest_ui_delta) {
      applyUIDelta(threadState.latest_ui_delta);
    }
  }

  function upsertAssistantDraft(turnId, accumulatedText) {
    const current = coerceThreadState();
    if (!current) {
      return;
    }
    const next = Object.assign({}, current);
    const draftId = `draft-${turnId}`;
    const draft = {
      message_id: draftId,
      role: "assistant",
      content: accumulatedText,
      created_at: nowIso(),
    };
    const messages = Array.isArray(current.messages) ? current.messages.slice() : [];
    const lastIndex = messages.length - 1;
    if (lastIndex >= 0 && messages[lastIndex] && messages[lastIndex].message_id === draftId) {
      messages[lastIndex] = draft;
    } else {
      messages.push(draft);
    }
    next.messages = messages;
    next.updated_at = nowIso();
    writeStore("thread-state-store", next);
  }

  function finalizeAssistantMessage(turnId, assistantMessage) {
    const current = coerceThreadState();
    if (!current || !assistantMessage) {
      return;
    }
    const next = Object.assign({}, current);
    const draftId = `draft-${turnId}`;
    const messages = Array.isArray(current.messages) ? current.messages.slice() : [];
    const index = messages.findIndex((message) => message && message.message_id === draftId);
    if (index >= 0) {
      messages[index] = clone(assistantMessage);
    } else {
      messages.push(clone(assistantMessage));
    }
    next.messages = messages;
    next.updated_at = nowIso();
    writeStore("thread-state-store", next);
  }

  function applyUIDelta(uiDelta) {
    if (!uiDelta || typeof uiDelta !== "object") {
      return;
    }

    const selectionState = coerceSelectionState();
    const visualizationState = coerceVisualizationState();
    const metadataState = coerceMetadataState();
    const mapState = coerceMapState();

    applyProjectionToSelectionState(selectionState, uiDelta);
    applyProjectionToVisualizationState(visualizationState, uiDelta);
    applyProjectionToMetadataState(metadataState, uiDelta);
    applyProjectionToMapState(mapState, uiDelta);

    writeStore("selection-state-store", selectionState);
    writeStore("visualization-state-store", visualizationState);
    writeStore("metadata-state-store", metadataState);
    writeStore("map-state-store", mapState);
  }

  async function parseResponse(response) {
    const text = await response.text();
    const payload = text ? safeParse(text) : null;
    return {
      payload: payload !== null ? payload : text,
      detail:
        payload && typeof payload === "object" && payload.detail
          ? payload.detail
          : text || response.statusText || "Request failed",
    };
  }

  async function createThread() {
    const { proxyChatPrefix } = getRuntimeConfig();
    const response = await fetch(`${proxyChatPrefix}/threads`, {
      method: "POST",
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    });
    const parsed = await parseResponse(response);
    if (!response.ok || !parsed.payload || typeof parsed.payload !== "object") {
      throw new Error(parsed.detail);
    }

    runtime.threadId = parsed.payload.thread_id;
    applyThreadSnapshot(parsed.payload.state);
    updateRequestStatus({
      mode: "stream",
      busy: false,
      error: null,
      last_turn_id: null,
    });
    return parsed.payload;
  }

  async function fetchThread(threadId) {
    const { proxyChatPrefix } = getRuntimeConfig();
    const response = await fetch(`${proxyChatPrefix}/threads/${encodeURIComponent(threadId)}`, {
      method: "GET",
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    });
    const parsed = await parseResponse(response);
    if (!response.ok || !parsed.payload || typeof parsed.payload !== "object") {
      throw new Error(parsed.detail);
    }
    applyThreadSnapshot(parsed.payload);
    runtime.threadId = parsed.payload.thread_id;
    return parsed.payload;
  }

  function closeStream() {
    if (runtime.eventSource) {
      runtime.eventSource.close();
    }
    runtime.eventSource = null;
    runtime.connectPromise = null;
  }

  function registerStreamHandlers(eventSource) {
    eventSource.addEventListener("thread.created", (event) => {
      const payload = safeParse(event.data);
      if (!payload || !payload.data || !payload.data.state) {
        return;
      }
      applyThreadSnapshot(payload.data.state);
    });

    eventSource.addEventListener("turn.started", (event) => {
      const payload = safeParse(event.data);
      const data = payload && payload.data;
      if (!data || !data.user_message) {
        return;
      }
      appendUserMessage(data.user_message);
      updateRequestStatus({
        mode: "stream",
        busy: true,
        error: null,
        last_turn_id: data.turn_id || null,
      });
    });

    eventSource.addEventListener("assistant.delta", (event) => {
      const payload = safeParse(event.data);
      const data = payload && payload.data;
      if (!data) {
        return;
      }
      upsertAssistantDraft(data.turn_id, data.accumulated_text || data.delta || "");
    });

    eventSource.addEventListener("ui.delta", (event) => {
      const payload = safeParse(event.data);
      const data = payload && payload.data;
      if (!data || !data.ui_delta) {
        return;
      }
      applyUIDelta(data.ui_delta);
    });

    eventSource.addEventListener("assistant.completed", (event) => {
      const payload = safeParse(event.data);
      const data = payload && payload.data;
      if (!data || !data.assistant_message) {
        return;
      }
      finalizeAssistantMessage(data.turn_id, data.assistant_message);
    });

    eventSource.addEventListener("turn.completed", (event) => {
      const payload = safeParse(event.data);
      const data = payload && payload.data;
      const response = data && data.response;
      if (!response) {
        return;
      }
      applyThreadSnapshot(response.thread_state);
      updateRequestStatus({
        mode: "stream",
        busy: false,
        error: null,
        last_turn_id: response.turn_id || null,
      });
    });

    eventSource.addEventListener("error", (event) => {
      const payload = safeParse(event.data);
      const data = payload && payload.data;
      const message = data && data.error ? data.error : "Chat request failed.";
      updateRequestStatus({
        mode: "stream",
        busy: false,
        error: message,
      });
    });

  }

  function connectStream(threadId) {
    if (!threadId) {
      return Promise.reject(new Error("thread_id is required"));
    }
    if (runtime.eventSource && runtime.threadId === threadId) {
      return Promise.resolve();
    }
    if (runtime.connectPromise) {
      return runtime.connectPromise;
    }
    if (typeof window.EventSource !== "function") {
      return Promise.reject(new Error("This browser does not support EventSource."));
    }

    const { proxyChatPrefix } = getRuntimeConfig();
    closeStream();

    runtime.connectPromise = new Promise((resolve, reject) => {
      const url = `${proxyChatPrefix}/stream/${encodeURIComponent(threadId)}`;
      const eventSource = new EventSource(url);
      let opened = false;
      const timeout = window.setTimeout(() => {
        if (!opened) {
          eventSource.close();
          runtime.eventSource = null;
          runtime.connectPromise = null;
          reject(new Error("Timed out connecting to the chat stream."));
        }
      }, 5000);

      eventSource.onopen = function () {
        opened = true;
        window.clearTimeout(timeout);
        runtime.eventSource = eventSource;
        runtime.threadId = threadId;
        runtime.connectPromise = null;
        resolve();
      };

      eventSource.onerror = function () {
        if (!opened) {
          window.clearTimeout(timeout);
          runtime.eventSource = null;
          runtime.connectPromise = null;
          reject(new Error("Unable to connect to the chat stream."));
          return;
        }
        updateRequestStatus({
          mode: "stream",
          busy: false,
          error: "Chat stream disconnected.",
        });
      };

      registerStreamHandlers(eventSource);
    });

    return runtime.connectPromise;
  }

  async function ensureThread(options) {
    const forceNew = !!(options && options.forceNew);
    const currentThread = coerceThreadState();
    if (!forceNew && currentThread && currentThread.thread_id) {
      runtime.threadId = currentThread.thread_id;
      try {
        await fetchThread(currentThread.thread_id);
      } catch (_) {
        const created = await createThread();
        await connectStream(created.thread_id);
        return created.thread_id;
      }
      await connectStream(currentThread.thread_id);
      return currentThread.thread_id;
    }

    const created = await createThread();
    await connectStream(created.thread_id);
    return created.thread_id;
  }

  function resetStoresForNewThread() {
    writeStore("thread-state-store", null);
    writeStore("selection-state-store", initialSelectionState());
    writeStore("map-state-store", initialMapState());
    writeStore("visualization-state-store", initialVisualizationState());
    writeStore("metadata-state-store", initialMetadataState());
    writeStore("request-status-store", initialRequestStatus());
    setInputValue("");
    setControlsBusy(false);
  }

  async function submitTurn(message) {
    const { proxyChatPrefix } = getRuntimeConfig();
    const threadId = await ensureThread();
    await connectStream(threadId);

    const response = await fetch(`${proxyChatPrefix}/turn`, {
      method: "POST",
      headers: {
        Accept: "application/json",
        "Content-Type": "application/json",
      },
      credentials: "same-origin",
      body: JSON.stringify({
        thread_id: threadId,
        message: message,
        stream: true,
      }),
    });
    const parsed = await parseResponse(response);
    if (!response.ok) {
      throw new Error(parsed.detail);
    }
    updateRequestStatus({
      mode: "stream",
      busy: true,
      error: null,
      last_turn_id:
        parsed.payload && typeof parsed.payload === "object"
          ? parsed.payload.turn_id || null
          : null,
    });
  }

  async function sendCurrentMessage() {
    const requestStatus = coerceRequestStatus();
    if (requestStatus.busy) {
      return;
    }

    const input = document.getElementById("chat-input");
    const message = ((input && input.value) || "").trim();
    if (!message) {
      return;
    }

    try {
      updateRequestStatus({
        mode: "stream",
        busy: true,
        error: null,
      });
      await submitTurn(message);
      setInputValue("");
    } catch (error) {
      updateRequestStatus({
        mode: "stream",
        busy: false,
        error: error instanceof Error ? error.message : "Chat request failed.",
      });
    }
  }

  async function sendCandidateSelection(placeId) {
    if (!placeId) {
      return;
    }
    try {
      updateRequestStatus({
        mode: "stream",
        busy: true,
        error: null,
      });
      await submitTurn(`select place ${placeId}`);
    } catch (error) {
      updateRequestStatus({
        mode: "stream",
        busy: false,
        error: error instanceof Error ? error.message : "Chat request failed.",
      });
    }
  }

  async function resetConversation() {
    try {
      closeStream();
      resetStoresForNewThread();
      await ensureThread({ forceNew: true });
    } catch (error) {
      updateRequestStatus({
        mode: "stream",
        busy: false,
        error: error instanceof Error ? error.message : "Unable to reset the chat thread.",
      });
    }
  }

  function bindControls() {
    const sendButton = document.getElementById("send-button");
    const resetButton = document.getElementById("reset-button");
    const input = document.getElementById("chat-input");
    const candidateContainer = document.getElementById("chat-place-candidates");

    if (sendButton && sendButton.dataset.proxyChatBound !== "true") {
      sendButton.dataset.proxyChatBound = "true";
      sendButton.addEventListener("click", function () {
        void sendCurrentMessage();
      });
    }

    if (resetButton && resetButton.dataset.proxyChatBound !== "true") {
      resetButton.dataset.proxyChatBound = "true";
      resetButton.addEventListener("click", function () {
        void resetConversation();
      });
    }

    if (input && input.dataset.proxyChatBound !== "true") {
        input.dataset.proxyChatBound = "true";
        input.addEventListener("keydown", function (event) {
          if (event.key === "Enter" && !event.shiftKey) {
            event.preventDefault();
            void sendCurrentMessage();
          }
        });
    }

    if (candidateContainer && candidateContainer.dataset.proxyChatBound !== "true") {
      candidateContainer.dataset.proxyChatBound = "true";
      candidateContainer.addEventListener("click", function (event) {
        const target = event.target && event.target.closest
          ? event.target.closest("[data-vob-place-candidate='true']")
          : null;
        if (!target) {
          return;
        }
        const placeId = target.dataset.placeId;
        if (!placeId) {
          return;
        }
        event.preventDefault();
        void sendCandidateSelection(placeId);
      });
    }
  }

  function boot() {
    if (runtime.bootstrapped) {
      bindControls();
      return;
    }
    if (!getDashSetProps()) {
      window.setTimeout(boot, 100);
      return;
    }
    if (
      !document.getElementById("send-button") ||
      !document.getElementById("chat-input")
    ) {
      window.setTimeout(boot, 100);
      return;
    }

    bindControls();
    runtime.bootstrapped = true;
    updateRequestStatus({
      mode: "stream",
      busy: false,
      error: null,
    });
    void ensureThread().catch((error) => {
      updateRequestStatus({
        mode: "stream",
        busy: false,
        error: error instanceof Error ? error.message : "Unable to start the chat runtime.",
      });
    });
  }

  document.addEventListener("DOMContentLoaded", boot);
  window.addEventListener("load", boot);
  window.setTimeout(boot, 0);
})();
