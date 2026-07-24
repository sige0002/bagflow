"""Example node: collect grayscale frames into an mp4 encoded at the bag's
real frame rate; the video location lands in report.json."""

import os

import cv2
from bagflow import BagflowNode


def main():
    out_dir = os.path.abspath(os.environ.get("OUT_DIR", "out"))
    os.makedirs(out_dir, exist_ok=True)

    with BagflowNode() as node:
        frames = []
        dims = None
        t_min = None
        t_max = None
        for name, value, meta in node.messages():
            w, h = int(meta["width"]), int(meta["height"])
            dims = (w, h)
            img = value.to_numpy(zero_copy_only=True).reshape(h, w)
            frames.append(img.copy())  # the shm buffer is recycled per event
            t0, t1 = int(meta.get("t0", 0)), int(meta.get("t1", 0))
            t_min = t0 if t_min is None else min(t_min, t0)
            t_max = t1 if t_max is None else max(t_max, t1)

        n = len(frames)
        span_s = (t_max - t_min) / 1e9 if n >= 2 and t_max and t_max > t_min else 0.0
        fps = (n - 1) / span_s if span_s > 0 else 30.0
        video_path = os.path.join(out_dir, "gray.mp4")
        if frames:
            w, h = dims
            writer = cv2.VideoWriter(
                video_path,
                cv2.VideoWriter_fourcc(*"mp4v"),
                fps,
                (w, h),
                isColor=False,
            )
            for f in frames:
                writer.write(f)
            writer.release()

        node.report(
            {
                "check": "video",
                "video_dir": out_dir,
                "video": video_path,
                "frames": n,
                "fps": round(fps, 2),
                "span_s": round(span_s, 2),
            }
        )


if __name__ == "__main__":
    main()
