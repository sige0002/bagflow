"""Topic presence / rate check straight from the bag metadata — costs no
decoding at all. Subscribes to nothing; it only validates that every topic
recorded, and that configured topics hit their expected rate.

env (set via the flow file):
  EXPECT_HZ  JSON map of topic -> expected rate in Hz, e.g.
             '{"/camera/x/image_raw/compressed": 30}'
  TOLERANCE  accept actual_hz >= expected_hz * TOLERANCE (default 0.9)
"""

import json
import os

from bagflow import BagflowNode


def main():
    expect_hz = json.loads(os.environ.get("EXPECT_HZ", "{}"))
    tolerance = float(os.environ.get("TOLERANCE", "0.9"))
    expected = json.loads(os.environ.get("BAGFLOW_EXPECTED", "{}"))
    bag_info = json.loads(os.environ.get("BAGFLOW_BAGINFO", "{}"))
    duration = bag_info.get("duration_s")

    with BagflowNode() as node:  # no data inputs: runs its check and finishes
        failures = []
        empty = [t for t, c in expected.items() if c == 0]
        for t in empty:
            failures.append({"topic": t, "reason": "no messages recorded"})
        for topic, hz in expect_hz.items():
            count = expected.get(topic)
            if count is None:
                failures.append({"topic": topic, "reason": "topic not in bag"})
                continue
            if not duration:
                continue
            actual = count / duration
            if actual < hz * tolerance:
                failures.append(
                    {
                        "topic": topic,
                        "reason": "rate below expectation",
                        "expected_hz": hz,
                        "actual_hz": round(actual, 2),
                    }
                )
        node.report(
            {
                "check": "topic_rate",
                "topics_in_bag": len(expected),
                "checked_rates": len(expect_hz),
                "failures": failures,
                "ok": not failures,
            }
        )


if __name__ == "__main__":
    main()
