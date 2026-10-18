

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

    Example:
      - Current version is 3.
      - Request A expects version 3 -> is_valid=True, next_version=4.
      - Request B still expects version 3 (against new current_version=4)
        -> is_valid=False, next_version=None.
    """
    if expected_version < 1:
        return VersionCheckResult(
            is_valid=False,
            current_version=current_version,
            expected_version=expected_version,
            next_version=None,
            reason=f"Invalid expected_version ({expected_version}); version numbers start at 1.",
        )

    if check_version_match(current_version=current_version, expected_version=expected_version):
        return VersionCheckResult(
            is_valid=True,
            current_version=current_version,
            expected_version=expected_version,
            next_version=current_version + 1,
            reason="Version matched; update permitted.",
        )

    return VersionCheckResult(
        is_valid=False,
        current_version=current_version,
        expected_version=expected_version,
        next_version=None,
        reason=(
            f"Stale version detected: expected version {expected_version}, "
            f"but current version is {current_version}."
        ),
    )
