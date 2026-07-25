"""bagflow report aggregator node.

Collects the `result` stream of every node (including the source), computes
per-input coverage against the bag metadata, writes the final report.json,
and broadcasts `done` so every upstream node may exit safely.
"""

import json
import os
import time

import pyarrow as pa
from dora import Node

SOURCE_ID = "bagflow_source"


def main():
    report_path = os.environ["BAGFLOW_REPORT"]
    expected = json.loads(os.environ.get("BAGFLOW_EXPECTED", "{}"))
    wiring = json.loads(os.environ.get("BAGFLOW_WIRING", "{}"))
    bag_info = json.loads(os.environ.get("BAGFLOW_BAGINFO", "{}"))
    inputs = {x for x in os.environ.get("BAGFLOW_INPUTS", "").split(",") if x}

    node = Node()
    t0 = time.time()
    results = {}
    counts = {}
    eos = set()

    for event in node:
        if event["type"] == "INPUT":
            name = event["id"]  # "result_<node id>"
            meta = event["metadata"] or {}
            node_id = name[len("result_"):]
            if meta.get("eos"):
                eos.add(name)
                if eos >= inputs:
                    break
                continue
            for raw in event["value"].to_pylist():
                record = json.loads(raw)
                if "_bagflow_counts" in record:
                    counts[node_id] = record["_bagflow_counts"]
                elif "_bagflow_source" in record:
                    counts[node_id] = record["_bagflow_source"]
                else:
                    results.setdefault(node_id, []).append(record)
        elif event["type"] == "STOP":
            break

    source_sent = counts.get(SOURCE_ID, {})
    coverage = {}
    for node_id, wires in wiring.items():
        for input_name, ref in wires.items():
            if not ref.startswith("/"):
                continue  # node-to-node edge; only bag topics have bag counts
            received = counts.get(node_id, {}).get(input_name, 0)
            in_bag = expected.get(ref)
            coverage[f"{node_id}.{input_name}"] = {
                "topic": ref,
                "rows_received": received,
                "rows_sent_by_source": source_sent.get(ref),
                "rows_in_bag": in_bag,
                "ratio_vs_bag": round(received / in_bag, 4) if in_bag else None,
            }

    # per-topic stats for the whole bag straight from metadata.yaml — lets a
    # quick post-recording flow flag missing topics / rate drops for free
    duration_s = bag_info.get("duration_s")
    bag_info["topics"] = {
        topic: {
            "count": count,
            "hz": round(count / duration_s, 2) if duration_s else None,
        }
        for topic, count in sorted(expected.items())
    }

    report = {
        "bag": bag_info,
        "results": results,
        "coverage": coverage,
        "node_received_rows": counts,
        "incomplete": sorted(inputs - eos),
        "wall_s": round(time.time() - t0, 3),
    }
    os.makedirs(os.path.dirname(os.path.abspath(report_path)) or ".", exist_ok=True)
    # write atomically so `bagflow run --no-attach` never reads a partial file
    tmp = report_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    os.replace(tmp, report_path)
    print(f"BAGFLOW_REPORT_WRITTEN {report_path}", flush=True)

    node.send_output("done", pa.array([1]))
    # linger until the daemon closes our inputs so `done` is delivered first
    while True:
        event = node.next(timeout=1.0)
        if event is None or event["type"] == "STOP":
            break


if __name__ == "__main__":
    main()
