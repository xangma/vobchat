"""Thread ownership helpers backed by Redis.

Binds a logical chat/workflow thread_id to a specific authenticated user.
Enforcement callers should verify the current_user matches the stored owner
before allowing access to SSE or workflow routes.
"""

from __future__ import annotations

import os
from typing import Optional
from uuid import uuid4

from vobchat.utils.redis_pool import redis_pool_manager


_KEY_PREFIX = "thread_owner:"
_DEFAULT_TTL_SECONDS = int(os.getenv("VOBCHAT_THREAD_OWNER_TTL", "86400"))  # 24h


def _key(thread_id: str) -> str:
    return f"{_KEY_PREFIX}{thread_id}"


def get_thread_owner(thread_id: str) -> Optional[str]:
    """Return the user_id (str) that owns thread_id, or None if unbound."""
    if not thread_id:
        return None
    r = redis_pool_manager.get_sync_client()
    owner = r.get(_key(thread_id))
    # Refresh TTL on access if present
    if owner:
        try:
            r.expire(_key(thread_id), _DEFAULT_TTL_SECONDS)
        except Exception:
            pass
    return owner


def bind_thread_owner(thread_id: str, user_id: str, *, ttl_seconds: Optional[int] = None) -> bool:
    """Bind thread_id to user_id if unbound, or refresh TTL when already owned by same user.

    Returns True on success (bound to this user); False if owned by a different user.
    """
    if not thread_id or user_id is None:
        return False
    r = redis_pool_manager.get_sync_client()
    ttl = int(ttl_seconds or _DEFAULT_TTL_SECONDS)
    key = _key(thread_id)

    # If already owned by someone else, reject
    existing = r.get(key)
    if existing and str(existing) != str(user_id):
        return False

    # Set if absent, else just refresh TTL to keep ownership alive
    if not existing:
        # SET if Not eXists with expiry
        # decode_responses=True, so values are strings
        ok = r.set(key, str(user_id), nx=True, ex=ttl)
        if not ok:
            # Another writer may have won; re-check
            current = r.get(key)
            if current and str(current) == str(user_id):
                try:
                    r.expire(key, ttl)
                except Exception:
                    pass
                return True
            return False
        return True

    # Already owned by this user → refresh TTL
    try:
        r.expire(key, ttl)
    except Exception:
        pass
    return True


def mint_thread_id(owner_token: str, *, ttl_seconds: Optional[int] = None) -> Optional[str]:
    """Generate a new thread_id (uuid4), bind it to the provided owner token, and return it.

    Returns None if binding fails (very unlikely in single writer scenario).
    """
    if not owner_token:
        return None
    tid = str(uuid4())
    ok = bind_thread_owner(tid, owner_token, ttl_seconds=ttl_seconds)
    return tid if ok else None
