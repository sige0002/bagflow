"""Streaming video sink: estimates fps from the first frames' bag
timestamps, then writes every following frame straight to the encoder.
Holds at most WARMUP frames in memory regardless of bag length."""

import os

import cv2
from bagflow import BagflowNode

WARMUP = 60  # frames buffered to estimate fps before opening the encoder


def main():
    out_dir = os.path.abspath(os.environ.get("OUT_DIR", "out"))
    os.makedirs(out_dir, exist_ok=True)
    video_path = os.path.join(out_dir, "gray.mp4")

    with BagflowNode() as node:
        writer = None
        warmup = []
        stamps = []
        frames = 0
        fps = 30.0

        def open_writer(w, h):
            nonlocal fps
            if len(stamps) >= 2 and stamps[-1] > stamps[0]:
                fps = (len(stamps) - 1) / ((stamps[-1] - stamps[0]) / 1e9)
            return cv2.VideoWriter(
                video_path,
                cv2.VideoWriter_fourcc(*"mp4v"),
                fps,
                (w, h),
                isColor=False,
            )

        for name, value, meta in node.messages():
            w, h = int(meta["width"]), int(meta["height"])
            img = value.to_numpy(zero_copy_only=True).reshape(h, w)
            frames += 1
            if writer is None:
                warmup.append(img.copy())  # shm buffer is recycled per event
                stamps.append(int(meta.get("stamp_ns", 0)))
                if len(warmup) >= WARMUP:
                    writer = open_writer(w, h)
                    for f in warmup:
                        writer.write(f)
                    warmup = []
            else:
                writer.write(img)  # streaming: no copy, no accumulation

        if writer is None and warmup:  # short bag: flush what we have
            hh, ww = warmup[0].shape
            writer = open_writer(ww, hh)
            for f in warmup:
                writer.write(f)
        if writer is not None:
            writer.release()

        node.report(
            {
                "check": "video",
                "video_dir": out_dir,
                "video": video_path,
                "frames": frames,
                "fps": round(fps, 2),
            }
        )


if __name__ == "__main__":
    main()
