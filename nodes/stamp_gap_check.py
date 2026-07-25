"""Message-gap check for any rostopic: looks at log_time intervals to catch
dropouts and stalls in a stream (works on joint states, images, anything).

env:
  GAP_MS      absolute gap threshold in milliseconds; if unset, uses
              GAP_FACTOR x median interval
  GAP_FACTOR  relative threshold vs the median interval (default 3.0)
  MAX_GAPS    max acceptable number of over-threshold gaps (default 0)
"""

import os

import numpy as np
import pyarrow as pa
from bagflow import BagflowNode


def main():
    gap_ms = os.environ.get("GAP_MS")
    gap_factor = float(os.environ.get("GAP_FACTOR", "3.0"))
    max_gaps = int(os.environ.get("MAX_GAPS", "0"))

    with BagflowNode() as node:
        stamps = []
        for name, value, meta in node.messages():
            ts = value.field("log_time").cast(pa.int64()).to_numpy(zero_copy_only=True)
            stamps.append(ts.copy())  # shm buffer is recycled per event
        if stamps:
            ts = np.concatenate(stamps)
            deltas_ms = np.diff(ts) / 1e6
        else:
            deltas_ms = np.array([])

        if len(deltas_ms) == 0:
            node.report({"check": "stamp_gap", "messages": len(stamps), "ok": False})
            return

        median = float(np.median(deltas_ms))
        threshold = float(gap_ms) if gap_ms else median * gap_factor
        over = int(np.sum(deltas_ms > threshold))
        node.report(
            {
                "check": "stamp_gap",
                "messages": int(len(deltas_ms) + 1),
                "median_interval_ms": round(median, 2),
                "max_gap_ms": round(float(np.max(deltas_ms)), 2),
                "threshold_ms": round(threshold, 2),
                "gaps_over_threshold": over,
                "ok": over <= max_gaps,
            }
        )


if __name__ == "__main__":
    main()
