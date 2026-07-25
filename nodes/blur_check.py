"""Blur check: variance of the Laplacian per decoded frame. Low variance
means few edges — a blurred (or badly defocused) camera image.

env:
  BLUR_MIN   minimum acceptable Laplacian variance (default 60)
  MAX_RATIO  max acceptable ratio of blurry frames (default 0.05)
"""

import os

import cv2
from bagflow import BagflowNode


def main():
    blur_min = float(os.environ.get("BLUR_MIN", "60"))
    max_ratio = float(os.environ.get("MAX_RATIO", "0.05"))

    with BagflowNode() as node:
        frames = 0
        blurry = 0
        v_min = None
        v_sum = 0.0
        for name, value, meta in node.messages():
            w, h, c = int(meta["width"]), int(meta["height"]), int(meta["channels"])
            img = value.to_numpy(zero_copy_only=True).reshape(h, w, c)
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            v = cv2.Laplacian(gray, cv2.CV_64F).var()
            frames += 1
            v_sum += v
            v_min = v if v_min is None else min(v_min, v)
            if v < blur_min:
                blurry += 1
        ratio = blurry / frames if frames else 0.0
        node.report(
            {
                "check": "blur",
                "frames": frames,
                "blurry_frames": blurry,
                "blurry_ratio": round(ratio, 4),
                "laplacian_var_min": round(v_min, 2) if v_min is not None else None,
                "laplacian_var_mean": round(v_sum / frames, 2) if frames else None,
                "threshold": blur_min,
                "ok": frames > 0 and ratio <= max_ratio,
            }
        )


if __name__ == "__main__":
    main()
