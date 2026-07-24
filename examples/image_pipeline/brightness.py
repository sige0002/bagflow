"""Brightness validator: consumes the same decoded frames as the grayscale
branch (fan-out, no second JPEG decode) and reports exposure statistics."""

import numpy as np
from bagflow import BagflowNode


def main():
    with BagflowNode() as node:
        means = []
        too_dark = 0
        too_bright = 0
        for name, value, meta in node.messages():
            img = value.to_numpy(zero_copy_only=True)
            m = float(np.mean(img))
            means.append(m)
            if m < 30:
                too_dark += 1
            elif m > 225:
                too_bright += 1
        node.report(
            {
                "check": "brightness",
                "frames": len(means),
                "mean": round(float(np.mean(means)), 2) if means else None,
                "min": round(min(means), 2) if means else None,
                "max": round(max(means), 2) if means else None,
                "too_dark_frames": too_dark,
                "too_bright_frames": too_bright,
                "ok": too_dark == 0 and too_bright == 0,
            }
        )


if __name__ == "__main__":
    main()
