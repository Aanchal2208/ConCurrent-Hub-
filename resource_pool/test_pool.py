resource_pool/test_pool.py
Owner: Parth Gauniyal (Member 2)

Unit tests for BoundedObjectPool covering object reuse, capacity limits,
automatic state reset, double-release prevention, foreign-object rejection,
and cleanup.
"""

import threading
import pytest

from resource_pool.object_pool import (
    BoundedObjectPool,
    PoolClosedError,
    PoolDoubleReleaseError,
    PoolExhaustedError,
    PoolInvalidObjectError,
    ReusableUpdateBuffer,
)