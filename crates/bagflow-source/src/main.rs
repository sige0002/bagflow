//! bagflow source node: streams the subscribed rostopics of a rosbag (mcap)
//! into dora as Arrow StructArrays (one per batch, all decoded columns).
//!
//! Termination protocol: after the bag is exhausted it sends an `eos`-flagged
//! empty message on every output, a final counts record on `result`, and then
//! waits for the report node's `done` signal before exiting (dora reclaims
//! unconsumed shared-memory buffers shortly after a node exits).

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, LargeBinaryArray, StringArray, StructArray, TimestampNanosecondArray};
use dora_node_api::{dora_core::config::DataId, DoraNode, Event, Parameter};
use mcap2dora::{map_file, McapArrowReader, Mode, ReaderOptions};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::Instant;

fn mcap_files(bag: &Path) -> Result<Vec<PathBuf>> {
    if bag.is_file() {
        return Ok(vec![bag.to_path_buf()]);
    }
    let mut v: Vec<PathBuf> = std::fs::read_dir(bag)
        .with_context(|| format!("read bag dir {}", bag.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "mcap"))
        .collect();
    v.sort();
    if v.is_empty() {
        bail!("no .mcap files in {}", bag.display());
    }
    Ok(v)
}

fn eos_params() -> BTreeMap<String, Parameter> {
    BTreeMap::from([("eos".to_string(), Parameter::Bool(true))])
}

fn main() -> Result<()> {
    let bag = PathBuf::from(std::env::var("BAGFLOW_BAG").context("BAGFLOW_BAG not set")?);
    // topic name -> dora output id
    let topics: HashMap<String, String> =
        serde_json::from_str(&std::env::var("BAGFLOW_TOPICS").context("BAGFLOW_TOPICS not set")?)?;

    let batch_rows: usize = std::env::var("BAGFLOW_BATCH_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(64);
    let batch_bytes: usize = std::env::var("BAGFLOW_BATCH_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8 << 20);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let _guard = rt.enter();
    let (mut node, mut events) = DoraNode::init_from_env().map_err(|e| anyhow!("{e:?}"))?;

    let t0 = Instant::now();
    let mut counts: HashMap<String, u64> = HashMap::new();
    let mut bytes = 0u64;

    for file in mcap_files(&bag)? {
        let mapped = map_file(&file)?;
        let mut reader = McapArrowReader::new(
            &mapped,
            ReaderOptions {
                mode: Mode::Decoded,
                max_batch_rows: batch_rows,
                max_batch_bytes: batch_bytes,
            },
        )?;
        while let Some(tb) = reader.next_batch()? {
            let Some(out_id) = topics.get(&tb.topic) else {
                continue;
            };
            let batch = tb.batch;
            let n = batch.num_rows();
            if n == 0 {
                continue;
            }
            let log_time = batch
                .column_by_name("log_time")
                .context("no log_time column")?
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .context("log_time is not a timestamp column")?;
            let params = BTreeMap::from([
                ("rows".to_string(), Parameter::Integer(n as i64)),
                ("t0".to_string(), Parameter::Integer(log_time.value(0))),
                ("t1".to_string(), Parameter::Integer(log_time.value(n - 1))),
            ]);
            let sa = StructArray::from(batch);
            bytes += sa.get_array_memory_size() as u64;
            *counts.entry(tb.topic.clone()).or_default() += n as u64;
            node.send_output(DataId::from(out_id.clone()), params, sa)
                .map_err(|e| anyhow!("{e:?}"))?;
        }
    }

    for out_id in topics.values() {
        node.send_output(
            DataId::from(out_id.clone()),
            eos_params(),
            LargeBinaryArray::from_vec(Vec::<&[u8]>::new()),
        )
        .map_err(|e| anyhow!("{e:?}"))?;
    }

    let result = DataId::from("result".to_owned());
    let record = serde_json::json!({ "_bagflow_source": counts }).to_string();
    node.send_output(result.clone(), BTreeMap::new(), StringArray::from(vec![record]))
        .map_err(|e| anyhow!("{e:?}"))?;
    node.send_output(
        result,
        eos_params(),
        StringArray::from(Vec::<String>::new()),
    )
    .map_err(|e| anyhow!("{e:?}"))?;

    let wall = t0.elapsed().as_secs_f64();
    let total: u64 = counts.values().sum();
    println!(
        "BAGFLOW_SOURCE_DONE rows={total} topics={} mb={:.1} wall_s={wall:.2}",
        counts.len(),
        bytes as f64 / 1e6
    );

    while let Some(event) = events.recv() {
        match event {
            Event::Input { id, .. } if id.as_str() == "done" => break,
            Event::Stop(_) => break,
            _ => {}
        }
    }
    Ok(())
}
