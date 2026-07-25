//! Rust node SDK for bagflow — the counterpart of the Python `bagflow`
//! helper. A node only handles its own inputs and outputs; end-of-stream
//! propagation, the report node's done-ack, and coverage counting are
//! managed here.
//!
//! ```no_run
//! use bagflow_node::BagflowNode;
//! # fn main() -> anyhow::Result<()> {
//! let mut node = BagflowNode::init()?;
//! while let Some(msg) = node.next_message()? {
//!     // msg.data is an arrow ArrayRef (topic batches are StructArrays)
//!     node.report(serde_json::json!({"check": "demo", "ok": true}))?;
//! }
//! # Ok(())
//! # }
//! ```

use anyhow::{anyhow, Result};
use arrow::array::{Array, ArrayRef, StringArray, UInt8Array};
use dora_node_api::{
    dora_core::config::DataId, DoraNode, Event, EventStream, Parameter,
};
use std::collections::{BTreeMap, HashSet};

pub use dora_node_api::Parameter as Param;
pub type Params = BTreeMap<String, Parameter>;

pub struct Msg {
    pub input: String,
    pub data: ArrayRef,
    pub params: Params,
}

pub struct BagflowNode {
    node: DoraNode,
    events: EventStream,
    data_inputs: HashSet<String>,
    outputs: Vec<String>,
    eos: HashSet<String>,
    received: BTreeMap<String, u64>,
    sent: BTreeMap<String, u64>,
    done: bool,
    stopped: bool,
    closed: bool,
}

fn env_list(name: &str) -> Vec<String> {
    std::env::var(name)
        .unwrap_or_default()
        .split(',')
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn rows_of(params: &Params) -> u64 {
    match params.get("rows") {
        Some(Parameter::Integer(n)) => *n as u64,
        _ => 1,
    }
}

fn is_eos(params: &Params) -> bool {
    matches!(params.get("eos"), Some(Parameter::Bool(true)))
}

impl BagflowNode {
    pub fn init() -> Result<Self> {
        // dora-node-api needs an ambient tokio runtime; a node is a process,
        // so leaking one runtime for the process lifetime is fine
        let rt = Box::leak(Box::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?,
        ));
        std::mem::forget(rt.enter());
        let (node, events) = DoraNode::init_from_env().map_err(|e| anyhow!("{e:?}"))?;
        Ok(BagflowNode {
            node,
            events,
            data_inputs: env_list("BAGFLOW_INPUTS").into_iter().collect(),
            outputs: env_list("BAGFLOW_OUTPUTS"),
            eos: HashSet::new(),
            received: BTreeMap::new(),
            sent: BTreeMap::new(),
            done: false,
            stopped: false,
            closed: false,
        })
    }

    /// Next data message, or None once every data input reached end-of-stream.
    pub fn next_message(&mut self) -> Result<Option<Msg>> {
        if self.data_inputs.is_empty() {
            return Ok(None);
        }
        while let Some(event) = self.events.recv() {
            match event {
                Event::Input { id, metadata, data } => {
                    if id.as_str() == "done" {
                        self.done = true;
                        return Ok(None);
                    }
                    let params = metadata.parameters;
                    if is_eos(&params) {
                        self.eos.insert(id.to_string());
                        if self.data_inputs.iter().all(|i| self.eos.contains(i)) {
                            return Ok(None);
                        }
                        continue;
                    }
                    *self.received.entry(id.to_string()).or_default() += rows_of(&params);
                    return Ok(Some(Msg {
                        input: id.to_string(),
                        data: data.into(),
                        params,
                    }));
                }
                Event::Stop(_) => {
                    self.stopped = true;
                    return Ok(None);
                }
                _ => {}
            }
        }
        Ok(None)
    }

    /// Send a data message on one of this node's declared outputs.
    pub fn send(&mut self, output: &str, data: impl Array, params: Params) -> Result<()> {
        *self.sent.entry(output.to_string()).or_default() += rows_of(&params);
        self.node
            .send_output(DataId::from(output.to_owned()), params, data)
            .map_err(|e| anyhow!("{e:?}"))
    }

    /// Send a result record (ends up in report.json under this node's id).
    pub fn report(&mut self, record: serde_json::Value) -> Result<()> {
        self.node
            .send_output(
                DataId::from("result".to_owned()),
                Params::new(),
                StringArray::from(vec![record.to_string()]),
            )
            .map_err(|e| anyhow!("{e:?}"))
    }

    /// Publish counts, propagate EOS, and wait for the report node's ack.
    /// Called automatically on drop; call explicitly to handle errors.
    pub fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.report(serde_json::json!({
            "_bagflow_counts": { "received": self.received, "sent": self.sent }
        }))?;
        let eos = Params::from([("eos".to_string(), Parameter::Bool(true))]);
        for out in self.outputs.clone() {
            self.node
                .send_output(
                    DataId::from(out),
                    eos.clone(),
                    UInt8Array::from(Vec::<u8>::new()),
                )
                .map_err(|e| anyhow!("{e:?}"))?;
        }
        self.node
            .send_output(
                DataId::from("result".to_owned()),
                eos,
                StringArray::from(Vec::<String>::new()),
            )
            .map_err(|e| anyhow!("{e:?}"))?;
        while !(self.done || self.stopped) {
            match self.events.recv() {
                Some(Event::Input { id, .. }) if id.as_str() == "done" => self.done = true,
                Some(Event::Stop(_)) | None => self.stopped = true,
                _ => {}
            }
        }
        Ok(())
    }
}

impl Drop for BagflowNode {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
