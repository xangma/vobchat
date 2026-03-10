from __future__ import annotations

import json
import logging
from typing import Dict, Any, List

from langgraph.types import Command

from vobchat.state_schema import lg_State
from vobchat.nodes.utils import _append_ai, clean_database_text
from vobchat.tools import (
    get_unit_type_info,
    get_data_entity_info,
    find_data_entity_id,
    get_theme_text,
)

logger = logging.getLogger(__name__)


def UnitTypeInfo_node(state: lg_State) -> Dict:
    """Return DB-backed unit type information formatted for readability.

    Expects state["last_intent_payload"]["arguments"]["unit_type"] containing
    a unit type label (e.g., "Modern District") or code (e.g., "MOD_DIST").
    Renders a concise, structured summary using database values without adding
    extra model-generated content.
    """
    payload = state.get("last_intent_payload") or {}
    args = payload.get("arguments") or {}
    unit_type = (args.get("unit_type") or "").strip()

    try:
        # Support either tool invocation styles
        data_json = (
            get_unit_type_info.invoke({"unit_type": unit_type})
            if hasattr(get_unit_type_info, "invoke")
            else get_unit_type_info(unit_type)
        )
    except Exception as e:
        logger.warning(f"UnitTypeInfo_node: tool call failed: {e}")
        data_json = "{}"

    # Parse and format for readability
    formatted = None
    try:
        info = json.loads(data_json) if data_json else {}
        if not isinstance(info, dict) or not info:
            raise ValueError("empty info")

        label = str(info.get("label") or "").strip()
        code = str(info.get("identifier") or "").strip()
        level = info.get("level")
        level_label = str(info.get("level_label") or "").strip()
        if level_label.lower() == "nan":
            level_label = ""
        adl = str(info.get("adl_feature_type") or "").strip()
        unit_count = info.get("unit_count")

        # Descriptions
        desc = info.get("description") or info.get("full_description") or ""
        if isinstance(desc, str):
            desc = clean_database_text(desc)
        else:
            desc = ""

        parts: list[str] = []
        title = f"**{label}** ({code})" if label or code else "**Unit Type**"
        parts.append(title)

        meta_lines: list[str] = []
        if level is not None or level_label:
            if level_label:
                meta_lines.append(
                    f"Level: {level} ({level_label})"
                    if level is not None
                    else f"Level: {level_label}"
                )
            elif level is not None:
                meta_lines.append(f"Level: {level}")
        if adl:
            meta_lines.append(f"ADL Feature Type: {adl}")
        if unit_count is not None:
            meta_lines.append(f"Number of units: {unit_count}")
        if meta_lines:
            parts.append("\n".join(meta_lines))

        if desc:
            parts.append("")
            parts.append(desc)

        def fmt_rel(key: str, title_rel: str) -> str | None:
            rels = info.get(key) or []
            if not isinstance(rels, list) or not rels:
                return None
            labels = []
            for r in rels:
                if not isinstance(r, dict):
                    continue
                lbl = str(r.get("label") or r.get("unit_type") or "").strip()
                if lbl:
                    labels.append(lbl)
            if not labels:
                return None
            return f"{title_rel}: " + ", ".join(labels)

        for k, t in (
            ("may_be_part_of", "May be part of"),
            ("may_have_parts", "May have parts"),
            ("may_have_succeeded", "May have succeeded"),
            ("may_have_preceded", "May have preceded"),
        ):
            section = fmt_rel(k, t)
            if section:
                parts.append(section)

        statuses = info.get("statuses") or []
        if isinstance(statuses, list) and statuses:
            st_items = []
            for s in statuses:
                if not isinstance(s, dict):
                    continue
                code_s = str(s.get("code") or "").strip()
                label_s = str(s.get("label") or "").strip()
                if code_s and label_s:
                    st_items.append(f"{label_s} ({code_s})")
                elif code_s or label_s:
                    st_items.append(code_s or label_s)
            if st_items:
                parts.append("Possible statuses: " + ", ".join(st_items))

        formatted = "\n\n".join([p for p in parts if p])
    except Exception:
        formatted = None

    content = formatted if formatted else (data_json or "{}")
    msg = _append_ai(state, content)
    return {"messages": [msg]}


def DataEntityInfo_node(state: lg_State) -> Dict[str, Any]:
    """Return details about a data entity (theme, nCube, variable, universe).

    Expects state["last_intent_payload"]["arguments"]["entity_id"].
    Fetches DB-backed fields and formats a concise, human-readable summary.
    """
    payload = state.get("last_intent_payload") or {}
    args = payload.get("arguments") or {}
    ent_id_input = (args.get("entity_id") or "").strip()

    if not ent_id_input:
        msg = _append_ai(
            state,
            "Please provide an entity identifier, e.g. 'N_SOCIAL_GRADE_TOT_M' or 'T_SOC'.",
        )
        return {"messages": [msg]}

    # Resolve entity_id_input to a concrete ent_id if needed (supports code or label)
    resolved_id = ent_id_input
    data_json = "{}"

    def _call_get_info(eid: str) -> str:
        try:
            return (
                get_data_entity_info.invoke({"entity_id": eid})
                if hasattr(get_data_entity_info, "invoke")
                else get_data_entity_info(eid)
            )
        except Exception as e:
            logger.warning(f"DataEntityInfo_node: tool call failed: {e}")
            return "{}"

    # First attempt: treat input as an ID directly
    data_json = _call_get_info(resolved_id)
    try:
        parsed_try = json.loads(data_json) if data_json else {}
        entity_try = parsed_try.get("entity") if isinstance(parsed_try, dict) else None
    except Exception:
        entity_try = None

    # If not found, try resolving by label/code lookup via find_data_entity_id
    if not entity_try:
        try:
            lookup_json = (
                find_data_entity_id.invoke({"query": ent_id_input})
                if hasattr(find_data_entity_id, "invoke")
                else find_data_entity_id(ent_id_input)
            )
        except Exception as e:
            logger.warning(f"DataEntityInfo_node: resolver tool failed: {e}")
            lookup_json = "{}"
        try:
            lookup = json.loads(lookup_json) if lookup_json else {}
            maybe_id = (lookup or {}).get("ent_id")
            if maybe_id:
                resolved_id = str(maybe_id)
                data_json = _call_get_info(resolved_id)
        except Exception:
            # keep data_json as-is
            pass

    # Parse and format
    try:
        info = json.loads(data_json) if data_json else {}
        entity = info.get("entity") or {}
        if not entity:
            raise ValueError("empty entity")

        ent_type = str(entity.get("ent_type") or "").strip()
        type_name = str(entity.get("type_name") or "").strip()
        type_text = clean_database_text(entity.get("type_text") or "")
        name = str(entity.get("ent_name") or "").strip()
        short_name = str(entity.get("ent_short_name") or "").strip()
        ent_text = clean_database_text(entity.get("ent_text") or "")

        # Fallbacks: fill missing name/type_name using resolver and static map
        if not name:
            try:
                lookup_json = (
                    find_data_entity_id.invoke({"query": resolved_id})
                    if hasattr(find_data_entity_id, "invoke")
                    else find_data_entity_id(resolved_id)
                )
                lk = json.loads(lookup_json) if lookup_json else {}
                name = str((lk or {}).get("labl") or "").strip() or name
                if not ent_type:
                    ent_type = str((lk or {}).get("ent_type") or "").strip()
            except Exception:
                pass

        if not type_name and ent_type:
            type_map = {
                "T": "Theme",
                "N": "nCube",
                "V": "Variable",
                "U": "Universe",
                "R": "Rate",
            }
            type_name = type_map.get(ent_type, type_name)

        parts: List[str] = []
        title = f"**{type_name} : {name}**" if type_name or name else "**Data Entity**"
        parts.append(title)
        if type_text:
            parts.append(type_text)

        # Attributes
        meta: List[str] = []
        meta.append(f"Identifier: {resolved_id}")
        if name:
            meta.append(f"Name: {name}")
        if short_name:
            meta.append(f"Short name: {short_name}")
        if type_name or ent_type:
            meta.append(
                f"Type: {type_name} ({ent_type})" if type_name else f"Type: {ent_type}"
            )

        # Theme label if available
        theme_id = str(entity.get("theme_id") or "").strip()
        if theme_id:
            theme_label = None
            try:
                theme_json = (
                    get_theme_text.invoke({"theme_code": theme_id})
                    if hasattr(get_theme_text, "invoke")
                    else get_theme_text(theme_id)
                )
                import io as _io
                import pandas as _pd

                df_theme = _pd.read_json(_io.StringIO(theme_json), orient="records")
                if not df_theme.empty:
                    theme_label = str(df_theme.iloc[0].get("labl") or "").strip()
            except Exception:
                theme_label = None
            if theme_label:
                meta.append(f"Theme: {theme_label} ({theme_id})")
            else:
                meta.append(f"Theme: {theme_id}")

        # nCube-specific fields
        if ent_type == "N":
            root_unit = entity.get("cube_root_unit")
            root_name = entity.get("cube_root_name")
            if root_unit or root_name:
                if root_unit and root_name:
                    meta.append(f"Root unit: {root_name} ({root_unit})")
                elif root_unit:
                    meta.append(f"Root unit: {root_unit}")
                elif root_name:
                    meta.append(f"Root unit: {root_name}")
            additivity = entity.get("ent_additivity")
            if additivity in ("Y", "N"):
                meta.append(f"Additive: {'Yes' if additivity == 'Y' else 'No'}")
            # cube_display = entity.get("cube_display")
            # if cube_display in ("Y", "N"):
            #     meta.append(f"Cube Display: {'Yes' if cube_display == 'Y' else 'No'}")
            # cube_download = entity.get("cube_download")
            # if cube_download in ("Y", "N"):
            #     meta.append(
            #         f"Download available: {'Yes' if cube_download == 'Y' else 'No'}"
            #     )

        # Rate-specific fields
        if ent_type == "R":
            rate_type = str(entity.get("rate_type") or "").strip()
            if rate_type:
                meta.append(f"Rate type: {rate_type}")
            top = entity.get("rate_top")
            bottom = entity.get("rate_bottom")
            mult = entity.get("rate_mult")
            if top or bottom or mult is not None:
                try:
                    mult_str = f"{float(mult):g}" if mult is not None else "1"
                except Exception:
                    mult_str = str(mult) if mult is not None else "1"
                defn = f"Definition: {top or ''} * {mult_str} / {bottom or ''}"
                meta.append(defn)
            cont = entity.get("rate_continuous")
            if cont in ("Y", "N"):
                meta.append(
                    f"Display as: {'Continuous time series' if cont == 'Y' else 'Separate data values'}"
                )

        if meta:
            parts.append("\n".join(meta))

        if ent_text:
            parts.append("")
            parts.append(ent_text)

        # Relationships
        def fmt_rel_list(items: List[dict], heading: str) -> str | None:
            if not items:
                return None
            lines = [heading]
            lines.append("\n" + "Entity ID\tEntity Name")
            for it in items:
                rid = str(
                    it.get("id") or it.get("higher_id") or it.get("lower_id") or ""
                ).strip()
                rname = str(
                    it.get("name")
                    or it.get("higher_name")
                    or it.get("lower_name")
                    or ""
                ).strip()
                if rid or rname:
                    lines.append(f"{rid}\t{rname}")
            return "\n".join(lines)

        higher = info.get("higher_entities") or []
        lower = info.get("lower_entities") or []
        if higher:
            parts.append("")
            parts.append(
                fmt_rel_list(
                    higher, f'{type_name or "Entity"} "{name}" is contained within:'
                )
                or ""
            )
        if lower:
            parts.append("")
            parts.append(
                fmt_rel_list(lower, f'{type_name or "Entity"} "{name}" contains:') or ""
            )

        content = "\n\n".join([p for p in parts if p])
    except Exception:
        # Fallback to raw JSON if parsing/formatting fails
        content = data_json or "{}"

    msg = _append_ai(state, content)
    return {"messages": [msg]}


def ExplainVisibleData_node(state: lg_State) -> Dict[str, Any]:
    """Explain the data currently shown by summarising all visible data entities.

    Collects entity IDs from `state['selected_cubes']` (or `state['cubes']`),
    fetches DB-backed info for each via `get_data_entity_info`, and formats a
    concise, human-readable summary for each entity.
    """
    logger.info("ExplainVisibleData_node: start")
    # Collect entity ids from current visualization state
    try:
        src_key = None
        cubes_json = state.get("selected_cubes")
        if cubes_json:
            src_key = "selected_cubes"
        else:
            cubes_json = state.get("cubes")
            if cubes_json:
                src_key = "cubes"
        logger.info(
            "ExplainVisibleData_node: source=%s present=%s",
            src_key or "(none)",
            bool(cubes_json),
        )
        if not cubes_json:
            msg = _append_ai(
                state,
                "I can't see any data entities currently shown. Try selecting a theme or fetching data first.",
            )
            return {"messages": [msg]}

        import io as _io
        import pandas as _pd

        df = _pd.read_json(_io.StringIO(cubes_json), orient="records")
        if df is None or df.empty:
            logger.info("ExplainVisibleData_node: parsed dataframe is empty")
            msg = _append_ai(state, "No visible data entities were found to explain.")
            return {"messages": [msg]}
        id_col = None
        for c in ("Cube_ID", "cube_id", "CubeID", "ent_ID", "ent_id"):
            if c in df.columns:
                id_col = c
                break
        if not id_col:
            logger.warning("ExplainVisibleData_node: no id column found in columns=%s", list(df.columns))
            msg = _append_ai(state, "I couldn't extract entity identifiers from the current data.")
            return {"messages": [msg]}
        uniq_ids = [str(x) for x in list(df[id_col].dropna().unique())]
        logger.info(
            "ExplainVisibleData_node: id_col=%s unique_ids=%d",
            id_col,
            len(uniq_ids),
        )
    except Exception:
        logger.exception("ExplainVisibleData_node: failed to parse visible entities")
        msg = _append_ai(state, "I couldn't read the current data selection to explain it.")
        return {"messages": [msg]}

    # Limit to a reasonable number to keep answers readable
    if len(uniq_ids) > 12:
        logger.info(
            "ExplainVisibleData_node: capping entities from %d to 12",
            len(uniq_ids),
        )
        uniq_ids = uniq_ids[:12]
    if not uniq_ids:
        msg = _append_ai(state, "No visible data entities were found to explain.")
        return {"messages": [msg]}

    parts: list[str] = ["Here’s a quick explanation of the data you’re seeing:"]
    for eid in uniq_ids:
        try:
            logger.debug("ExplainVisibleData_node: fetching info for %s", eid)
            data_json = (
                get_data_entity_info.invoke({"entity_id": eid})
                if hasattr(get_data_entity_info, "invoke")
                else get_data_entity_info(eid)
            )
            info = json.loads(data_json) if data_json else {}
            entity = info.get("entity") or {}
            if not entity:
                logger.warning("ExplainVisibleData_node: no entity details for %s", eid)
                parts.append(f"• {eid}: (no details available)")
                continue
            type_name = str(entity.get("type_name") or "").strip()
            name = str(entity.get("ent_name") or "").strip()
            ent_text = clean_database_text(entity.get("ent_text") or "")
            if type_name and name:
                title = f"**{name}** — {type_name} ({eid})"
            else:
                title = f"**{name or eid}**"
            summary_lines = [title]
            if ent_text:
                summary_lines.append(ent_text)
            parts.append("\n".join(summary_lines))
        except Exception:
            logger.exception("ExplainVisibleData_node: error fetching details for %s", eid)
            parts.append(f"• {eid}: (error fetching details)")

    content = "\n\n".join(parts)
    logger.info("ExplainVisibleData_node: completed for %d entities", len(uniq_ids))
    msg = _append_ai(state, content)
    return {"messages": [msg]}
