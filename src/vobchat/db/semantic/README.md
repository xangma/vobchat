# Semantic SQL Foundation

This directory is the first semantic SQL foundation for the rewrite branch.

The goal is not to mirror the whole GBHGIS schema. The goal is to capture the
small set of reusable database concepts that repeatedly appear in repository
queries:

- preferred unit names
- reusable theme/cube lookups
- stat-map feature selection

Phase 2 keeps these assets as documented SQL rather than a migration system.
Repository code in `src/vobchat/db/repositories/` is the source of truth for
the active runtime path. These SQL files exist to make repeated semantics easy
to review and to prepare for later phases where shared views/functions may be
materialized in the database.

New rewrite-path data access should use repositories, not `src/vobchat/tools.py`.
