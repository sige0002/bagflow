"""Standard-style decode node: JPEG (CompressedImage) batches -> raw BGR
frames, one message per frame. Decoding happens once here; every downstream
consumer reads the decoded pixels zero-copy from shared memory."""

import os
from concurrent.futures import ThreadPoolExecutor

import cv2
import numpy as np
import pyarrow as pa
from bagflow import BagflowNode

# cv2.imdecode releases the GIL, so a thread pool gives real parallelism
WORKERS = min(8, os.cpu_count() or 4)


def _decode(jpg):
    return cv2.imdecode(np.frombuffer(jpg, np.uint8), cv2.IMREAD_COLOR)


def main():
    with BagflowNode() as node, ThreadPoolExecutor(WORKERS) as pool:
        frames = 0
        for name, value, meta in node.messages():
            data = value.field("data")
            stamps = value.field("log_time")  # per-row timestamps from the bag
            jpgs = [data[i].as_py() for i in range(len(data))]
            for i, img in enumerate(pool.map(_decode, jpgs)):  # order preserved
                if img is None:
                    continue
                h, w, c = img.shape
                node.send(
                    "frames",
                    pa.array(img.reshape(-1)),
                    {
                        "rows": 1,
                        "width": w,
                        "height": h,
                        "channels": c,
                        "stamp_ns": stamps[i].value,
                    },
                )
                frames += 1
        node.report({"check": "decode", "frames_decoded": frames})


if __name__ == "__main__":
    main()
