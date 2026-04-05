# Target architecture

## 1. Core decision

Keep the product surfaces that already work well:

- Dash UI shell
- Dash Leaflet map
- Plotly charts
- local-model support
- PostgreSQL/PostGIS analytics

But move the application to a **clear two-layer architecture**:

- **Web layer**: Dash app for UI, auth-facing pages, and thin same-origin proxy endpoints
- **API layer**: FastAPI service for orchestration, semantic querying, streaming, and typed contracts

The browser should interact with the web layer only. The web layer can proxy authenticated requests to the API layer over the internal Docker network. That preserves the current login requirement without forcing the browser to manage a second auth scheme during the rewrite.

## 2. Service layout

### 2.1 Web service (Dash)

Responsibilities:

- page shell and layout
- chat UI
- map UI
- chart / table UI
- login/logout
- frontend state stores
- authenticated proxy routes to API
- rendering backend responses into Dash components

What the web service should **not** do anymore:

- direct database queries
- orchestration logic
- SQL generation
- LLM planning
- place/theme resolution logic
- polygon cache ownership

### 2.2 API service (FastAPI)

Responsibilities:

- typed REST endpoints for domain operations
- SSE or streaming endpoints for chat / long-running requests
- LLM orchestration using deterministic tools
- semantic layer over Vision of Britain / GBHGIS schema
- render-spec generation for charts / map layers / tables
- provenance / metadata responses

What the API service should **not** do:

- render Dash components
- own browser state
- expose arbitrary text-to-SQL execution

## 3. New code layout inside `src/vobchat`

Use one Python package with clear internal boundaries.

```text
src/vobchat/
  api/
    main.py
    routers/
      chat.py
      places.py
      themes.py
      maps.py
      series.py
      metadata.py
    schemas/
      chat.py
      places.py
      themes.py
      maps.py
      series.py
      common.py
    services/
      chat_orchestrator.py
      place_service.py
      theme_service.py
      series_service.py
      map_service.py
      metadata_service.py
      provenance_service.py
    streaming/
      sse.py

  web/
    app.py
    layout/
    callbacks/
    components/
    clients/
      api_client.py
    proxy/
      routes.py
    assets/

  core/
    settings.py
    logging.py
    auth.py
    models/
    llm/
      client.py
      planner.py
      prompts.py
      tool_schemas.py
    render_specs/
      chart_specs.py
      map_specs.py
      table_specs.py

  db/
    engine.py
    session.py
    repositories/
      places.py
      themes.py
      series.py
      maps.py
      metadata.py
    semantic/
      views.sql
      functions.sql
      README.md

  auth/
    models.py
    storage.py
    routes.py
    cli.py
```

The old top-level `src/vobchat/app.py` monolith should eventually disappear in favor of `src/vobchat/web/app.py` and `src/vobchat/api/main.py`.

## 4. Domain contracts

The system should stop passing around loosely defined state blobs wherever possible. Instead, define typed request/response models for the main operations.

### 4.1 Core request types

- `ResolvePlaceRequest`
- `ResolveThemeRequest`
- `TimeSeriesRequest`
- `CrossSectionRequest`
- `MapLayerRequest`
- `PlaceProfileRequest`
- `UnitTypeInfoRequest`
- `DataEntityInfoRequest`
- `ChatTurnRequest`

### 4.2 Core response types

- `PlaceCandidate`
- `ResolvedPlace`
- `ThemeCandidate`
- `ResolvedTheme`
- `SeriesDataset`
- `CrossSectionDataset`
- `MapFeatureCollection`
- `ChartRenderSpec`
- `TableRenderSpec`
- `ChatTurnResponse`
- `ProvenanceBundle`

## 5. Semantic data layer

This is the most important design rule in the rewrite.

The LLM must never be allowed to freely compose SQL against raw tables. Instead, the API should expose deterministic service methods backed by curated repositories. Typical operations:

- `search_places(name, filters)`
- `resolve_place(place_id or candidate)`
- `search_postcode(postcode)`
- `list_themes()`
- `resolve_theme(theme_query)`
- `list_cubes(place, theme)`
- `get_time_series(units, cube_ids, year_range)`
- `get_category_breakdown(units, cube_ids, year)`
- `get_map_features(unit_type, bbox, year_range, selected_ids, theme)`
- `get_place_profile(place_id)`
- `get_unit_type_info(unit_type)`
- `get_data_entity_info(entity_id)`
- `get_provenance(dataset identifiers)`

The planner’s job is to choose between these operations and fill typed arguments, not to reach into the database directly.

## 6. Database access approach

### 6.1 Libraries

Use:

- SQLAlchemy 2.x
- psycopg3
- GeoAlchemy2 where it helps readability
- PostGIS functions directly for spatial filtering / serialization

### 6.2 Query style

- prefer parameterized SQLAlchemy Core or parameterized text queries
- use repository methods with explicit return types
- keep heavy joins in named query helpers or SQL files
- keep connection role read-only

### 6.3 Semantic SQL assets

Create a small curated semantic layer in `src/vobchat/db/semantic/` for reusable views/functions, for example:

- place-name canonicalization / preferred names
- unit validity / start-end year helpers
- theme/cube lookup views
- ready-to-chart series views
- map feature views with unit metadata

These do not need to model the whole GBHGIS schema on day one; they only need to cover the current app’s feature set cleanly.

## 7. LLM integration design

### 7.1 Client

Use an OpenAI-compatible client wrapper pointed at a local endpoint:

- Ollama initially
- easy future swap to vLLM or another compatible server

### 7.2 Planner behavior

The planner should produce a typed plan, for example:

- intent category
- chosen operation
- resolved arguments
- clarifying question if required
- optional render goal

### 7.3 No LangGraph as the default control plane

Do not recreate the current node graph unless a real requirement appears. For the target stack, a simpler orchestrator is preferable:

1. inspect current conversation state
2. run planner / resolver
3. call deterministic service methods
4. build typed response + render specs
5. stream assistant text and UI deltas back to the frontend

## 8. Frontend state model

The web layer should keep only the state required for rendering and user interaction. Suggested client-visible stores:

- session/thread id
- selected places
- selected theme
- selected cubes / measures
- map viewport / active unit type / year filters
- current chart spec
- current table spec
- current provenance bundle
- current chat transcript
- pending action / streaming status

Avoid dozens of transport-specific toggle stores where possible.

## 9. Auth approach for the rewrite

Preserve the existing login requirement, but simplify the architecture:

- keep the user store and login flow in the web service initially
- expose only the web service publicly
- put the FastAPI service on the internal network
- have the web service proxy authenticated requests to the API service

This avoids having to solve cross-service browser auth in the same rewrite that also replaces orchestration and data access.

## 10. Deployment target

### 10.1 Docker Compose

Target shape:

- `web`
- `api`
- optional `postgres` only for local development if desired
- external or sibling `ollama`

Notably absent:

- Redis, unless later justified

### 10.2 Environment/config

Unify configuration in typed settings classes shared by web and API. Stop relying on loosely coordinated module-level env reads.

## 11. Cleanup policy

Once the new path is live and tested:

- delete old workflow modules
- delete old callback paths that directly query the DB
- delete Redis-specific infrastructure
- delete duplicate polygon APIs and caches
- delete compatibility imports

The final repo should have **one obvious way** to do chat orchestration, data access, map serving, and chart generation.
