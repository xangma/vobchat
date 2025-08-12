# Repository Guidelines

## Project Structure & Module Organization
- Source code: `src/vobchat/` (Dash + Flask app). Key areas:
  - `components/`, `callbacks/`, `assets/` for UI, interactivity, and static files.
  - `api/` for API route handlers.
  - `nodes/`, `workflow*.py`, `intent_*` for LangChain/LangGraph workflow logic.
- Tests: top-level `test_*.py` (e.g., `test_vobchat_integration.py`) and `src/test_intent_handling.py`.
- Config and scripts: `docker-compose.yml`, `Dockerfile`, `create_user.py`, `add_test_user_sql.py`.

## Build, Test, and Development Commands
- Create env: `python -m venv .venv && source .venv/bin/activate`.
- Install deps: `pip install -r requirements.txt` (or `pip install -e .`).
- Run app (local): `python -m vobchat.app` → launches Dash server.
- Run via Docker: `cp .env.example .env && docker-compose up --build`.
- Tests: `pytest -q` (runs root and `src/` tests).

## Coding Style & Naming Conventions
- Python: PEP 8, 4-space indentation, 88–100 char lines, type hints where practical.
- Names: modules/functions `snake_case`, classes `CamelCase`, constants `UPPER_SNAKE`.
- Structure: keep UI code in `components/` and `callbacks/`; workflow logic in `nodes/` + `workflow*.py`; server-facing routes in `api/`.
- Logging: prefer `logging` over prints; use `configure_logging.py` patterns.

## Testing Guidelines
- Framework: `pytest` with `test_*.py` files and `test_*` functions.
- Scope: unit tests for nodes/workflow functions; integration tests against app state where feasible.
- Running: `pytest -q` locally; add fixtures for DB/Redis where needed.
- Aim for meaningful coverage of critical paths (routing, intent handling, API endpoints).

## Commit & Pull Request Guidelines
- Commits: imperative, concise subject (≤72 chars), details in body when needed.
  - Example: `Refactor theme nodes and clean text parsing`.
- PRs: include purpose, scope, test plan (`pytest -q` output), screenshots for UI changes, and linked issues.
- Require: no secrets in diffs, docs updated if behavior changes, `docker-compose up` sanity check when touching env/config.

## Security & Configuration Tips
- Secrets: use `.env` (copy from `.env.example`). Never commit `.env` or credentials.
- Services: app expects PostgreSQL, Redis, and an LLM endpoint (Ollama/OpenAI). Configure hosts/ports via env vars.
- Users: create via `docker-compose exec vobchat flask --app vobchat.app:server add-user EMAIL`.

