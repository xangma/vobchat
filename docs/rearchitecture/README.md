# VobChat rearchitecture plan

This directory is the working plan for the **non-backward-compatible rewrite** of VobChat onto the target stack:

- **Dash** frontend for chat + map + plotting UI
- **FastAPI** backend for orchestration, streaming, and data access
- **PostgreSQL + PostGIS** as the source of truth
- **OpenAI-compatible LLM endpoint** (provider-neutral, with Ollama as an easy local option)
- **Semantic data layer** between the LLM and the database

This rewrite is being done on a **new branch** and the repo is still in prototype stage, so the goal is **clarity and maintainability over compatibility shims**. We should not keep old code around once the new implementation is in place and verified.

## How Codex should use these docs

1. Read `current-state-audit.md` to understand what exists today and what must be preserved.
2. Read `target-architecture.md` to understand the desired end state.
3. Execute work in the order laid out in `implementation-phases.md`.
4. Use `parity-and-progress.md` as the checklist and running work log.

## Non-negotiables

- Preserve existing **user-visible functionality** unless the replacement is strictly better and equivalent in outcome.
- Do **not** preserve internal architecture just because it exists today.
- Do **not** keep dead modules, compatibility wrappers, or duplicate paths once a new path is live.
- The LLM must **not** generate arbitrary SQL against raw tables.
- All database access should move behind typed repositories / services and a semantic API contract.
- Keep the database connection **read-only** for analytics queries.
- Prefer **typed schemas, deterministic tools, and render specs** over ad-hoc dictionaries.

## What exists today at a high level

The current prototype already has the core product surfaces we want to keep: a Dash chat UI, a Dash Leaflet map, plotting, auth, SSE streaming, OpenAI-compatible endpoint integration, and Postgres-backed data access. But those concerns are tightly coupled inside one Dash/Flask/LangGraph application, with business logic spread across callbacks, workflow nodes, and raw SQL helper functions.

## Success criteria for the rewrite

- A clean split between:
  - **web UI shell**
  - **backend orchestration API**
  - **data/semantic layer**
- No Redis/LangGraph dependency for the core happy path unless reintroduced for a clearly justified reason.
- Chat, map, and chart state stay synchronized through explicit API contracts.
- The codebase is easier for Codex and humans to extend.
- The old monolithic implementation is removed after parity is reached.

## Document map

- `current-state-audit.md` — grounded audit of the current repo
- `target-architecture.md` — desired stack, service boundaries, and code layout
- `implementation-phases.md` — ordered execution plan for Codex
- `parity-and-progress.md` — parity checklist, cleanup checklist, and work log template

## Working style for Codex

- Make changes in vertical slices, but keep the final architecture in mind.
- Prefer replacing modules wholesale over threading new logic through old abstractions.
- When a replacement is complete, delete the superseded code in the same branch.
- Keep these docs updated as reality changes.
