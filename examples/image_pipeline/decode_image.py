"""Standard-style decode node: JPEG (CompressedImage) batches -> raw BGR
frames, one message per frame. Decoding happens once here; every downstream
consumer reads the decoded pixels zero-copy from shared memory."""

import cv2
import numpy as np
import pyarrow as pa
from bagflow import BagflowNode


def main():
    with BagflowNode() as node:
        frames = 0
        for name, value, meta in node.messages():
            data = value.field("data")
            stamps = value.field("log_time")  # per-row timestamps from the bag
            for i in range(len(data)):
                img = cv2.imdecode(
                    np.frombuffer(data[i].as_py(), np.uint8), cv2.IMREAD_COLOR
                )
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
