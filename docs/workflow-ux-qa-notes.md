# Workflow UX QA Notes

## Scope

This pass focused on the recently added UX surfaces:

- discovery card
- trust / provenance card
- richer active context summary
- improved clarification copy
- map / visualization semantic status text

It was a polish pass, not a workflow expansion pass.

## How I Checked It

I ran the local Dash app and verified the real browser boot/login path first.

For multi-step UI journeys, I then injected projection-backed state into the running app through the existing Dash stores. That kept the review focused on the real rendered surfaces and reload-safe state path, without needing to build a broader QA backend just to exercise copy and presentation.

## Journeys Exercised

1. Initial load and thread bootstrap
2. Ambiguous place clarification
3. Discovery-first result
4. Blocked-analysis result
5. Executed analysis result
6. Output switch to table
7. Output switch to map with safe downgrade
8. Degraded-mode trust / notice path

## What Felt Good

- Discovery now reads like an exploration surface rather than a hidden backend side channel.
- Clarification options are understandable and point to a concrete next move.
- The active context summary is useful once a real result exists.
- Trust / provenance is much clearer when it appears next to a result instead of inside a generic alert.
- Boundary-map downgrade text is now understandable in user terms.

## What Felt Awkward

- The trust card appeared on an empty thread before there was any result to explain.
- Discovery and blocked-analysis states still triggered map loading attempts even when no reporting geography had been chosen yet.
- Map status text could read as `District level level` when the projected geography label already included the word `level`.
- The first streamed thread boot could stall long enough to show a misleading stream-timeout message.

## Fixed In This Pass

- Removed the empty trust card on first load unless there is meaningful provenance or degraded-mode guidance to show.
- Stopped the map pane from trying to load during discovery / blocked / info states that do not yet have a usable geography or projected boundary map.
- Reworded the empty map prompt so it asks for the missing geography level instead of surfacing an error.
- Fixed duplicated map status wording like `level level`.
- Fixed the proxy runtime bootstrap so it no longer waits for invisible store DOM nodes.
- Added an immediate SSE keepalive so the initial chat stream does not show a false timeout before the first turn.

## Real But Worth Deferring

- Discovery trust text can still feel slightly repetitive when the trust summary and discovery title say nearly the same thing.
- Blocked-analysis states still spread explanation across assistant text, trust, discovery, and status banner. It is better than before, but it could be tighter.
- Full end-to-end QA of chart/table/map pane coordination still deserves one pass against a stable richer backend fixture, rather than projection injection alone.

## Outcome

After the fixes in this pass, the workflow feels materially cleaner:

- initial load is calmer
- discovery no longer causes map-side confusion
- trust appears when it has something to explain
- map language is more natural

That is enough for commit/review from a polish standpoint, with the deferred items above kept as genuine follow-up candidates rather than blockers.
