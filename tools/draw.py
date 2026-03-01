"""
tools/draw.py

Persistent storage for chart drawings (lines, boxes, horizontal rays, etc.)
per pair. Drawings are stored as JSON in a local file so they survive restarts.

Each drawing is a dict:
  {
    "id":      str,          -- unique id (uuid4 short)
    "type":    str,          -- "line" | "hline" | "box" | "ray"
    "chart":   str,          -- "price" | "cvd"
    "points":  [...],        -- list of {time, value} anchor points
    "style": {
      "color":     str,      -- hex or rgba
      "lineWidth": int,
      "lineStyle": int,      -- 0=solid 1=dotted 2=dashed
    },
    "label":   str | None,   -- optional text label
    "created": float,        -- unix timestamp
  }
"""

import json
import os
import time
import uuid
from typing import List, Dict, Any, Optional

# ── Storage path ───────────────────────────────────────────────────────────────

def _drawings_path(pair_id: str) -> str:
    base = os.path.join(os.path.dirname(__file__), '..', 'data', 'drawings')
    os.makedirs(base, exist_ok=True)
    safe = pair_id.replace('/', '_').replace('\\', '_')
    return os.path.join(base, f'{safe}.json')


# ── CRUD ───────────────────────────────────────────────────────────────────────

def load_drawings(pair_id: str) -> List[Dict[str, Any]]:
    """Return all drawings for a pair, newest first."""
    path = _drawings_path(pair_id)
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return []


def save_drawings(pair_id: str, drawings: List[Dict[str, Any]]) -> None:
    path = _drawings_path(pair_id)
    with open(path, 'w') as f:
        json.dump(drawings, f, indent=2)


def add_drawing(pair_id: str, drawing: Dict[str, Any]) -> Dict[str, Any]:
    """Add a new drawing. Assigns id + created if missing. Returns the saved drawing."""
    drawing = dict(drawing)
    if not drawing.get('id'):
        drawing['id'] = uuid.uuid4().hex[:8]
    if not drawing.get('created'):
        drawing['created'] = time.time()

    drawings = load_drawings(pair_id)
    drawings.append(drawing)
    save_drawings(pair_id, drawings)
    return drawing


def delete_drawing(pair_id: str, drawing_id: str) -> bool:
    """Delete a drawing by id. Returns True if found and deleted."""
    drawings = load_drawings(pair_id)
    before = len(drawings)
    drawings = [d for d in drawings if d.get('id') != drawing_id]
    if len(drawings) == before:
        return False
    save_drawings(pair_id, drawings)
    return True


def clear_drawings(pair_id: str) -> int:
    """Delete all drawings for a pair. Returns count deleted."""
    drawings = load_drawings(pair_id)
    count = len(drawings)
    save_drawings(pair_id, [])
    return count


# ── Default styles per tool ────────────────────────────────────────────────────

DEFAULT_STYLES = {
    'line':   {'color': '#e0c080', 'lineWidth': 1, 'lineStyle': 0},
    'hline':  {'color': '#7090d0', 'lineWidth': 1, 'lineStyle': 2},
    'ray':    {'color': '#c080e0', 'lineWidth': 1, 'lineStyle': 0},
    'box':    {'color': '#50b896', 'lineWidth': 1, 'lineStyle': 0},
}

def default_style(tool_type: str) -> Dict[str, Any]:
    return dict(DEFAULT_STYLES.get(tool_type, DEFAULT_STYLES['line']))
