"""bagflow node helper for Python validator authors.

Hides the bagflow wire protocol (EOS markers, completion ack, coverage
counting) so a node only deals with its own inputs and outputs:

    from bagflow import BagflowNode

    with BagflowNode() as node:
        for name, value, meta in node.messages():
            ...                       # value is a pyarrow array
            node.send("out", arr)     # optional downstream output
            node.report({...})        # optional result records (end up in report.json)

`messages()` ends when every data input reached end-of-stream. On context
exit the helper publishes received-count statistics, propagates EOS on all
declared outputs, and waits for the report node's `done` ack so that no
in-flight shared-memory buffer is reclaimed early.
"""

import json
import os

import pyarrow as pa
from dora import Node


class BagflowNode:
    def __init__(self):
        self._node = Node()
        self._data_inputs = _env_set("BAGFLOW_INPUTS")
        self._outputs = _env_set("BAGFLOW_OUTPUTS")
        self._eos = set()
        self._received = {}
        self._done = False
        self._stopped = False

    def messages(self):
        """Yield (input_name, pyarrow value, metadata dict) for data messages."""
        if not self._data_inputs:
            return
        for event in self._node:
            if event["type"] == "INPUT":
                name = event["id"]
                if name == "done":
                    self._done = True
                    return
                meta = event["metadata"] or {}
                if meta.get("eos"):
                    self._eos.add(name)
                    if self._eos >= self._data_inputs:
                        return
                    continue
                rows = int(meta.get("rows", 1))
                self._received[name] = self._received.get(name, 0) + rows
                yield name, event["value"], meta
            elif event["type"] == "STOP":
                self._stopped = True
                return

    def send(self, output, data, metadata=None):
        """Send a data message on one of this node's declared outputs."""
        self._node.send_output(output, data, metadata or {})

    def report(self, record):
        """Send a result record (any JSON-serializable dict) to report.json."""
        self._node.send_output("result", pa.array([json.dumps(record)]), {})

    def close(self):
        eos = {"eos": True}
        self.report({"_bagflow_counts": self._received})
        for out in self._outputs:
            self._node.send_output(out, pa.array([], type=pa.uint8()), eos)
        self._node.send_output("result", pa.array([], type=pa.string()), eos)
        while not (self._done or self._stopped):
            event = self._node.next(timeout=60.0)
            if event is None:
                break
            if event["type"] == "INPUT" and event["id"] == "done":
                self._done = True
            elif event["type"] == "STOP":
                self._stopped = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.close()
        return False


def _env_set(name):
    raw = os.environ.get(name, "")
    return {x for x in raw.split(",") if x}
