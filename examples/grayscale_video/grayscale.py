"""Example node: decode CompressedImage JPEG batches, convert to grayscale,
emit one raw frame per message."""

import cv2
import numpy as np
import pyarrow as pa
from bagflow import BagflowNode


def main():
    with BagflowNode() as node:
        frames = 0
        for name, value, meta in node.messages():
            data = value.field("data")  # LargeBinary column of the topic batch
            stamps = value.field("log_time")
            for i in range(len(data)):
                img = cv2.imdecode(
                    np.frombuffer(data[i].as_py(), np.uint8), cv2.IMREAD_COLOR
                )
                if img is None:
                    continue
                gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
                h, w = gray.shape
                node.send(
                    "gray",
                    pa.array(gray.reshape(-1)),
                    {
                        "rows": 1,
                        "width": w,
                        "height": h,
                        "stamp_ns": stamps[i].value,
                    },
                )
                frames += 1
        node.report({"check": "grayscale", "frames_converted": frames})


if __name__ == "__main__":
    main()
