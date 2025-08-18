from __future__ import annotations

import json
import logging
from typing import Dict

from langgraph.types import Command

from vobchat.state_schema import lg_State
from vobchat.nodes.utils import _append_ai, clean_database_text
from vobchat.tools import get_unit_type_info

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
                meta_lines.append(f"Level: {level} ({level_label})" if level is not None else f"Level: {level_label}")
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
