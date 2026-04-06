(function () {
  "use strict";

  const runtime = {
    eventSource: null,
    threadId: null,
    connectPromise: null,
    bootstrapped: false,
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

  function readStore(id) {
    const element = document.getElementById(id);
    if (!element) {
      return null;
    }
    if (element._dash_value !== undefined) {
      return element._dash_value;
    }
    const text = (element.textContent || "").trim();
    return text ? safeParse(text) : null;
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
    setProps(id, { data });
    return data;
  }

  function initialSelectionState() {
    return {
      selected_places: [],
      selected_theme: null,
      selected_cubes: [],
      available_themes: [],
      available_cubes: [],
      pending_place_candidates: [],
      notices: [],
    };
  }

  function initialMapState() {
    return {
      unit_type: "MOD_REG",
      year_range: [1801, currentYear()],
      bbox: null,
      selected_ids: [],
      with_theme_ids: [],
      feature_count: 0,
      last_loaded_unit_types: [],
      status: "Choose a place or unit type to load map features.",
    };
  }

  function initialVisualizationState() {
    return {
      active_tab: "line",
      time_series: null,
      category_breakdown: null,
      category_year: null,
      status: "Choose a place, theme, and cube to load chart data.",
    };
  }

  function initialMetadataState() {
    return {
      place_profile: null,
      place_key_findings: null,
      unit_type_info: null,
      data_entity_resolution: null,
      data_entity_info: null,
      requested_unit_type: null,
      requested_entity_id: null,
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
    const ids = [];
    (selectionState.selected_places || []).forEach((place) => {
      (place.units || []).forEach((unit) => {
        const unitId = Number(unit.unit_id);
        if (Number.isFinite(unitId) && !ids.includes(unitId)) {
          ids.push(unitId);
        }
      });
    });
    return ids;
  }

  function firstSelectedUnitType(selectionState) {
    for (const place of selectionState.selected_places || []) {
      for (const unit of place.units || []) {
        if (unit && unit.unit_type) {
          return String(unit.unit_type);
        }
      }
    }
    return null;
  }

  function appendUserMessage(userMessage) {
    const current = coerceThreadState() || {
      thread_id: runtime.threadId,
      messages: [],
      selected_places: [],
      selected_theme: null,
      selected_cubes: [],
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
    selectionState.selected_places = Array.isArray(threadState.selected_places)
      ? clone(threadState.selected_places)
      : [];
    selectionState.selected_theme = threadState.selected_theme
      ? clone(threadState.selected_theme)
      : null;
    selectionState.selected_cubes = Array.isArray(threadState.selected_cubes)
      ? clone(threadState.selected_cubes)
      : [];
    writeStore("selection-state-store", selectionState);

    const mapState = coerceMapState();
    mapState.selected_ids = selectedUnitIds(selectionState);
    const unitType = firstSelectedUnitType(selectionState);
    if (unitType) {
      mapState.unit_type = unitType;
    }
    writeStore("map-state-store", mapState);

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

    selectionState.selected_places = Array.isArray(uiDelta.selected_places)
      ? clone(uiDelta.selected_places)
      : [];
    selectionState.selected_theme = uiDelta.selected_theme
      ? clone(uiDelta.selected_theme)
      : null;
    selectionState.selected_cubes = Array.isArray(uiDelta.selected_cubes)
      ? clone(uiDelta.selected_cubes)
      : [];
    selectionState.notices = Array.isArray(uiDelta.notices)
      ? clone(uiDelta.notices)
      : [];

    if (uiDelta.cleared_places) {
      selectionState.selected_places = [];
      selectionState.selected_theme = null;
      selectionState.selected_cubes = [];
      selectionState.available_themes = [];
      selectionState.available_cubes = [];
      mapState.selected_ids = [];
    }

    if (uiDelta.operation === "search_places") {
      selectionState.pending_place_candidates = Array.isArray(uiDelta.place_search_results)
        ? clone(uiDelta.place_search_results)
        : [];
    } else if (uiDelta.operation) {
      selectionState.pending_place_candidates = [];
    }

    if (uiDelta.themes && Array.isArray(uiDelta.themes.items)) {
      selectionState.available_themes = clone(uiDelta.themes.items);
    }

    if (
      uiDelta.operation === "list_cubes_for_theme_and_unit" ||
      (Array.isArray(uiDelta.cubes) && uiDelta.cubes.length > 0)
    ) {
      selectionState.available_cubes = Array.isArray(uiDelta.cubes)
        ? clone(uiDelta.cubes)
        : [];
    }

    if (uiDelta.time_series) {
      visualizationState.time_series = clone(uiDelta.time_series);
      visualizationState.active_tab = "line";
    }

    if (uiDelta.category_breakdown) {
      visualizationState.category_breakdown = clone(uiDelta.category_breakdown);
      visualizationState.category_year = uiDelta.category_breakdown.year || null;
      visualizationState.active_tab = "categories";
    }

    if (uiDelta.place_profile) {
      metadataState.place_profile = clone(uiDelta.place_profile);
    }

    if (uiDelta.unit_type_info) {
      metadataState.unit_type_info = clone(uiDelta.unit_type_info);
      metadataState.requested_unit_type = uiDelta.unit_type_info.identifier || null;
    }

    if (uiDelta.data_entity_resolution) {
      metadataState.data_entity_resolution = clone(uiDelta.data_entity_resolution);
      metadataState.requested_entity_id =
        uiDelta.data_entity_resolution.result?.entity_id || null;
    }

    if (uiDelta.data_entity_info) {
      metadataState.data_entity_info = clone(uiDelta.data_entity_info);
      metadataState.requested_entity_id = uiDelta.data_entity_info.entity_id || null;
    }

    mapState.selected_ids = selectedUnitIds(selectionState);
    if (uiDelta.map_features && uiDelta.map_features.unit_type) {
      mapState.unit_type = uiDelta.map_features.unit_type;
    } else {
      const unitType = firstSelectedUnitType(selectionState);
      if (unitType) {
        mapState.unit_type = unitType;
      }
    }

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
      !document.getElementById("thread-state-store") ||
      !document.getElementById("selection-state-store") ||
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
