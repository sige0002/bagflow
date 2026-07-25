"""Exposure check on decoded frames: flags too-dark / blown-out images.

env:
  DARK_MEAN    frame is "too dark" below this mean intensity (default 30)
  BRIGHT_MEAN  frame is "too bright" above this mean intensity (default 225)
  MAX_RATIO    max acceptable ratio of bad frames (default 0.05)
"""

import os

import numpy as np
from bagflow import BagflowNode


def main():
    dark = float(os.environ.get("DARK_MEAN", "30"))
    bright = float(os.environ.get("BRIGHT_MEAN", "225"))
    max_ratio = float(os.environ.get("MAX_RATIO", "0.05"))

    with BagflowNode() as node:
        frames = 0
        too_dark = 0
        too_bright = 0
        m_min = None
        m_max = None
        m_sum = 0.0
        for name, value, meta in node.messages():
            m = float(np.mean(value.to_numpy(zero_copy_only=True)))
            frames += 1
            m_sum += m
            m_min = m if m_min is None else min(m_min, m)
            m_max = m if m_max is None else max(m_max, m)
            if m < dark:
                too_dark += 1
            elif m > bright:
                too_bright += 1
        bad = too_dark + too_bright
        ratio = bad / frames if frames else 0.0
        node.report(
            {
                "check": "brightness",
                "frames": frames,
                "too_dark_frames": too_dark,
                "too_bright_frames": too_bright,
                "bad_ratio": round(ratio, 4),
                "mean": round(m_sum / frames, 2) if frames else None,
                "min": round(m_min, 2) if m_min is not None else None,
                "max": round(m_max, 2) if m_max is not None else None,
                "ok": frames > 0 and ratio <= max_ratio,
            }
        )


if __name__ == "__main__":
    main()
