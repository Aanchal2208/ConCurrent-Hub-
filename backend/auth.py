"""
PostgreSQL-backed authentication for ConcurrentHub.
Manages user accounts in the `users` table.
Uses PBKDF2-HMAC-SHA256 (100,000 rounds) for password hashing.
Uses HMAC-SHA256 signed tokens (no external JWT library required).

Team Nexus — OSDBMS-V-2026-T185
"""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import time
from typing import Any, Dict, List, Optional

from database.postgres_db import DatabaseUnavailableError, PostgresPoolManager

SECRET_KEY: str = os.getenv("AUTH_SECRET_KEY", "concurrenthub-secret-key-change-me-in-production")
TOKEN_EXPIRATION_SECONDS: int = int(os.getenv("TOKEN_EXPIRATION_SECONDS", "86400"))  # 24h default


# ---------------------------------------------------------------------------
# Password helpers
# ---------------------------------------------------------------------------

def hash_password(password: str, salt: Optional[str] = None) -> str:
    """Hash password using PBKDF2-HMAC-SHA256 with 100 000 rounds and random salt."""
    if salt is None:
        salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 100_000)
    return f"pbkdf2_sha256$100000${salt}${dk.hex()}"


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a plain password against a stored PBKDF2 hash."""
    try:
        parts = hashed_password.split("$")
        if len(parts) == 4 and parts[0] == "pbkdf2_sha256":
            rounds = int(parts[1])
            salt = parts[2]
            expected_hex = parts[3]
        elif len(parts) == 2:
            # Legacy format: salt$hex
            rounds = 100_000
            salt = parts[0]
            expected_hex = parts[1]
        else:
            return False
        dk = hashlib.pbkdf2_hmac("sha256", plain_password.encode("utf-8"), salt.encode("utf-8"), rounds)
        return hmac.compare_digest(dk.hex(), expected_hex)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Token helpers
# ---------------------------------------------------------------------------

def generate_token(username: str, role: str) -> str:
    """Create an HMAC-signed token: username:role:expires:nonce.signature"""
    expires = int(time.time()) + TOKEN_EXPIRATION_SECONDS
    nonce = secrets.token_hex(8)
    payload = f"{username}:{role}:{expires}:{nonce}"
    sig = hmac.new(SECRET_KEY.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload}.{sig}"


def verify_token(token: str) -> Optional[Dict[str, Any]]:
    """Verify an HMAC-signed token; return payload dict or None."""
    try:
        payload, sig = token.rsplit(".", 1)
        expected_sig = hmac.new(SECRET_KEY.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            return None
        username, role, expires_str, _ = payload.split(":")
        if int(expires_str) < time.time():
            return None
        return {"username": username, "role": role}
    except Exception:
        return None


# ---------------------------------------------------------------------------
# PostgreSQL user repository
# ---------------------------------------------------------------------------

_CREATE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    full_name VARCHAR(200) NOT NULL DEFAULT '',
    role VARCHAR(20) NOT NULL DEFAULT 'editor' CHECK (role IN ('admin', 'editor', 'viewer')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_users_username ON users (username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users (email);
"""

_INSERT_USER_SQL = """
INSERT INTO users (username, email, password_hash, full_name, role)
VALUES (%(username)s, %(email)s, %(password_hash)s, %(full_name)s, %(role)s)
RETURNING id, username, email, full_name, role, is_active, created_at;
"""

_GET_USER_BY_USERNAME_SQL = """
SELECT id, username, email, password_hash, full_name, role, is_active, created_at, last_login
FROM users WHERE username = %(username)s AND is_active = TRUE;
"""

_UPDATE_LAST_LOGIN_SQL = """
UPDATE users SET last_login = CURRENT_TIMESTAMP WHERE username = %(username)s;
"""

_LIST_USERS_SQL = """
SELECT id, username, email, full_name, role, is_active, created_at, last_login
FROM users ORDER BY id ASC;
"""

_GET_USER_COUNT_SQL = "SELECT COUNT(*) AS cnt FROM users;"

_DELETE_USER_SQL = """
UPDATE users SET is_active = FALSE WHERE username = %(username)s RETURNING username;
"""


class UserRepository:
    """
    PostgreSQL-backed user management.
    All user records are persisted in the `users` table.
    """

    def __init__(self, pool_manager: PostgresPoolManager) -> None:
        self._pool = pool_manager

    def init_schema(self) -> None:
        """Ensure the users table exists."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_CREATE_USERS_TABLE)

    def register(
        self,
        username: str,
        password: str,
        email: str,
        full_name: str = "",
        role: str = "editor",
    ) -> Dict[str, Any]:
        """
        Register a new user account.
        Raises ValueError if username or email is already taken.
        """
        u = username.lower().strip()
        if not u:
            raise ValueError("Username must not be empty.")
        if len(password) < 8:
            raise ValueError("Password must be at least 8 characters.")
        if not email or "@" not in email:
            raise ValueError("A valid email address is required.")

        pw_hash = hash_password(password)
        try:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        _INSERT_USER_SQL,
                        {
                            "username": u,
                            "email": email.lower().strip(),
                            "password_hash": pw_hash,
                            "full_name": (full_name or u).strip(),
                            "role": role if role in ("admin", "editor", "viewer") else "editor",
                        },
                    )
                    row = cur.fetchone()
                    if row is None:
                        raise RuntimeError("User insert returned no row.")
                    return self._safe_row(row)
        except DatabaseUnavailableError:
            raise
        except Exception as exc:
            msg = str(exc).lower()
            if "unique" in msg and "username" in msg:
                raise ValueError(f"Username '{u}' is already taken.")
            if "unique" in msg and "email" in msg:
                raise ValueError("That email address is already registered.")
            raise ValueError(f"Registration failed: {exc}") from exc

    def authenticate(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """
        Verify credentials against the database.
        Returns the user dict on success, None on failure.
        """
        u = username.lower().strip()
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_GET_USER_BY_USERNAME_SQL, {"username": u})
                row = cur.fetchone()
        if row is None:
            return None
        if not verify_password(password, row["password_hash"]):
            return None
        # Update last_login timestamp asynchronously
        try:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(_UPDATE_LAST_LOGIN_SQL, {"username": u})
        except Exception:
            pass
        return self._safe_row(row)

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        """Retrieve a user record by username (without password hash)."""
        u = username.lower().strip()
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_GET_USER_BY_USERNAME_SQL, {"username": u})
                row = cur.fetchone()
        return self._safe_row(row) if row else None

    def list_users(self) -> List[Dict[str, Any]]:
        """Return all active users (no password hashes)."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_LIST_USERS_SQL)
                rows = cur.fetchall()
        return [self._safe_row(r) for r in rows]

    def user_count(self) -> int:
        """Return total number of user accounts."""
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(_GET_USER_COUNT_SQL)
                row = cur.fetchone()
                return int(row["cnt"]) if row else 0

    @staticmethod
    def _safe_row(row: Dict[str, Any]) -> Dict[str, Any]:
        """Strip password_hash and serialize timestamps."""
        from datetime import datetime
        out = {k: v for k, v in row.items() if k != "password_hash"}
        for key in ("created_at", "last_login"):
            val = out.get(key)
            if isinstance(val, datetime):
                out[key] = val.isoformat()
        return out
