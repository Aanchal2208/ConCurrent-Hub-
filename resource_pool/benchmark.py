resource_pool/benchmark.py
Owner: Parth Gauniyal (Member 2)

Micro-benchmark measuring object reuse and contention behaviour of
BoundedObjectPool across single-threaded and multi-threaded workloads.

Run command:
    python -m resource_pool.benchmark
"""

import statistics
import threading
import time
from typing import Dict, List

from resource_pool.object_pool import (
    BoundedObjectPool,
    ReusableUpdateBuffer,
    measure_pool_vs_allocation,
)