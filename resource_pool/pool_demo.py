"""
resource_pool/pool_demo.py
Owner: Parth Gauniyal (Member 2)

Standalone demonstration of the BareHeap-inspired BoundedObjectPool.
Shows object reuse, capacity limits, automatic buffer reset on release,
and double-release protection.

Run command:
    python -m resource_pool.pool_demo
"""

from resource_pool.object_pool import (
    BoundedObjectPool,
    PoolDoubleReleaseError,
    PoolExhaustedError,
    ReusableUpdateBuffer,
)
