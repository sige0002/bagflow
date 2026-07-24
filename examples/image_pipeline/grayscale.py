"""Grayscale node: consumes already-decoded BGR frames (zero-copy view of
shared memory) and emits grayscale frames. No JPEG decoding here."""

import cv2
import pyarrow as pa
from bagflow import BagflowNode


def main():
    with BagflowNode() as node:
        frames = 0
        for name, value, meta in node.messages():
            w, h, c = int(meta["width"]), int(meta["height"]), int(meta["channels"])
            img = value.to_numpy(zero_copy_only=True).reshape(h, w, c)
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            node.send(
                "gray",
                pa.array(gray.reshape(-1)),
                {
                    "rows": 1,
                    "width": w,
                    "height": h,
                    "stamp_ns": int(meta.get("stamp_ns", 0)),
                },
            )
            frames += 1
        node.report({"check": "grayscale", "frames_converted": frames})


if __name__ == "__main__":
    main()
