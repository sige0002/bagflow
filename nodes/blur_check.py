"""Blur check: variance of the Laplacian per decoded frame. Low variance
means few edges — a blurred (or badly defocused) camera image.

The event loop hands frames to a small worker pool (cv2 releases the GIL),
so the dora input queue is drained fast enough to keep full coverage even
when the producer decodes faster than one core can analyze.

env:
  BLUR_MIN   minimum acceptable Laplacian variance (default 60)
  MAX_RATIO  max acceptable ratio of blurry frames (default 0.05)
  RESIZE     evaluate at this resolution, e.g. "224x224" (default: native).
             Checking at the resolution the downstream model actually
             consumes is usually the right semantics; variance statistics
             change with resolution, so retune BLUR_MIN when changing it.
  WORKERS    analysis threads (default 4; 0 = analyze inline)
"""

import os
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import cv2
from bagflow import BagflowNode

MAX_IN_FLIGHT = 256  # bounds buffered frame copies (~0.9 MB each at VGA)


def _parse_resize(spec):
    if not spec:
        return None
    w, h = spec.lower().split("x")
    return int(w), int(h)


def main():
    blur_min = float(os.environ.get("BLUR_MIN", "60"))
    max_ratio = float(os.environ.get("MAX_RATIO", "0.05"))
    resize = _parse_resize(os.environ.get("RESIZE", ""))
    workers = int(os.environ.get("WORKERS", "4"))

    def variance(flat, w, h, c):
        gray = cv2.cvtColor(flat.reshape(h, w, c), cv2.COLOR_BGR2GRAY)
        if resize:
            gray = cv2.resize(gray, resize, interpolation=cv2.INTER_AREA)
        return cv2.Laplacian(gray, cv2.CV_64F).var()

    frames = 0
    blurry = 0
    v_min = None
    v_sum = 0.0

    def account(v):
        nonlocal frames, blurry, v_min, v_sum
        frames += 1
        v_sum += v
        v_min = v if v_min is None else min(v_min, v)
        if v < blur_min:
            blurry += 1

    with BagflowNode() as node:
        if workers > 0:
            pool = ThreadPoolExecutor(workers)
            pending = deque()
            for name, value, meta in node.messages():
                w, h, c = int(meta["width"]), int(meta["height"]), int(meta["channels"])
                flat = value.to_numpy(zero_copy_only=True).copy()  # shm is recycled
                pending.append(pool.submit(variance, flat, w, h, c))
                while len(pending) > MAX_IN_FLIGHT:
                    account(pending.popleft().result())
            while pending:
                account(pending.popleft().result())
            pool.shutdown()
        else:
            for name, value, meta in node.messages():
                w, h, c = int(meta["width"]), int(meta["height"]), int(meta["channels"])
                account(variance(value.to_numpy(zero_copy_only=True), w, h, c))

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
                "eval_resolution": f"{resize[0]}x{resize[1]}" if resize else "native",
                "workers": workers,
                "ok": frames > 0 and ratio <= max_ratio,
            }
        )


if __name__ == "__main__":
    main()
