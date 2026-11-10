"""
Checks whether the version a client expects matches the current record version.
If a task is already at version 2 but a request expects version 1, that request is stale and should be rejected.
OS & DBMS Phase 3 (Team Nexus - OSDBMS-V-2026-T185)
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class VersionCheckResult:
    """Immutable result of an optimistic version check."""

    is_valid: bool
    current_version: int
    expected_version: int
    next_version: Optional[int]
    reason: str

    @property
    def message(self) -> str:
        return self.reason


def check_version_match(current_version: int, expected_version: int) -> bool:
    """
    Return True if and only if expected_version matches current_version.
    Both versions must be positive integers (>= 1).
    """
    if expected_version < 1 or current_version < 1:
        return False
    return current_version == expected_version


def validate_version(current_version: int, expected_version: int) -> VersionCheckResult:
    """
    Validate whether an update expecting `expected_version` may proceed against
    a record currently at `current_version`.
    """
    if expected_version < 1:
        return VersionCheckResult(
            is_valid=False,
            current_version=current_version,
            expected_version=expected_version,
            next_version=None,
            reason=f"expected_version must be >= 1 (got {expected_version})",
        )

    if current_version < 1:
        return VersionCheckResult(
            is_valid=False,
            current_version=current_version,
            expected_version=expected_version,
            next_version=None,
            reason=f"current_version must be >= 1 (got {current_version})",
        )

    if expected_version != current_version:
        return VersionCheckResult(
            is_valid=False,
            current_version=current_version,
            expected_version=expected_version,
            next_version=None,
            reason=(
                f"Version conflict: Record has been modified. Expected version "
                f"{expected_version}, but current version is {current_version}."
            ),
        )

    return VersionCheckResult(
        is_valid=True,
        current_version=current_version,
        expected_version=expected_version,
        next_version=current_version + 1,
        reason=f"Version match: Validated expected version {expected_version}.",
    )
