"""Frozen-camera check: detects runs of (nearly) identical consecutive
frames, which indicate a stalled driver or stuck sensor. Frames are
compared downscaled so the check stays cheap.

env:
  FREEZE_EPS  mean absolute pixel difference below which two consecutive
              frames count as identical (default 0.3)
  MAX_RUN     longest acceptable run of identical frames (default 5)
"""

import os

import cv2
import numpy as np
from bagflow import BagflowNode


def main():
    eps = float(os.environ.get("FREEZE_EPS", "0.3"))
    max_run = int(os.environ.get("MAX_RUN", "5"))

    with BagflowNode() as node:
        frames = 0
        frozen_pairs = 0
        run = 0
        longest_run = 0
        prev = None
        for name, value, meta in node.messages():
            w, h, c = int(meta["width"]), int(meta["height"]), int(meta["channels"])
            img = value.to_numpy(zero_copy_only=True).reshape(h, w, c)
            small = cv2.resize(img, (160, 120), interpolation=cv2.INTER_AREA)
            frames += 1
            if prev is not None:
                diff = float(np.mean(cv2.absdiff(prev, small)))
                if diff < eps:
                    frozen_pairs += 1
                    run += 1
                    longest_run = max(longest_run, run)
                else:
                    run = 0
            prev = small
        node.report(
            {
                "check": "freeze",
                "frames": frames,
                "frozen_pairs": frozen_pairs,
                "longest_freeze_run": longest_run,
                "ok": frames > 0 and longest_run < max_run,
            }
        )


if __name__ == "__main__":
    main()
