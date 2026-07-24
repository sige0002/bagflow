//! bagflow CLI: preflight-checks a validation flow against the bag metadata,
//! generates the dora dataflow (source node, report aggregator, done/EOS
//! wiring), and runs it.

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

const SOURCE_ID: &str = "bagflow_source";
const REPORT_ID: &str = "bagflow_report";
/// conservative built-in default: bounds worst-case shm backlog per edge
const DEFAULT_QUEUE: usize = 256;

const PY_HELPER: &str = include_str!("../../../python/bagflow/__init__.py");
const PY_REPORT: &str = include_str!("../../../python/report.py");

#[derive(Parser)]
#[command(about = "Offline rosbag validation flows on dora")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Preflight-check and run a flow
    Run {
        flow: PathBuf,
        /// Start detached and return as soon as report.json is written,
        /// leaving dataflow teardown to the daemon (fastest turnaround;
        /// pair with a pre-started `dora up` daemon)
        #[arg(long)]
        no_attach: bool,
    },
    /// Preflight-check only
    Check { flow: PathBuf },
}

// ---------- user flow definition ----------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Flow {
    bag: PathBuf,
    #[serde(default = "default_report")]
    report: PathBuf,
    /// flow-wide defaults, overridable per node and per input
    #[serde(default)]
    defaults: Defaults,
    /// source batching (rows/bytes per Arrow batch sent per topic)
    #[serde(default)]
    source: SourceCfg,
    nodes: Vec<FlowNode>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct Defaults {
    #[serde(default)]
    queue_size: Option<usize>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SourceCfg {
    #[serde(default)]
    batch_rows: Option<usize>,
    #[serde(default)]
    batch_bytes: Option<usize>,
}

fn default_report() -> PathBuf {
    PathBuf::from("report.json")
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FlowNode {
    id: String,
    path: PathBuf,
    #[serde(default)]
    inputs: BTreeMap<String, FlowInput>,
    #[serde(default)]
    outputs: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    /// default queue_size for every input of this node
    #[serde(default)]
    queue_size: Option<usize>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum FlowInput {
    /// "/some/rostopic" or "node_id/output"
    Short(String),
    Long {
        #[serde(default)]
        topic: Option<String>,
        #[serde(default)]
        node: Option<String>,
        #[serde(default)]
        queue_size: Option<usize>,
    },
}

impl FlowInput {
    fn reference(&self) -> Result<&str> {
        match self {
            FlowInput::Short(s) => Ok(s),
            FlowInput::Long { topic, node, .. } => match (topic, node) {
                (Some(t), None) => Ok(t),
                (None, Some(n)) => Ok(n),
                _ => bail!("input must set exactly one of `topic` or `node`"),
            },
        }
    }
    fn queue_size(&self) -> Option<usize> {
        match self {
            FlowInput::Short(_) => None,
            FlowInput::Long { queue_size, .. } => *queue_size,
        }
    }
}

// ---------- rosbag2 metadata.yaml ----------

#[derive(Deserialize)]
struct MetaRoot {
    rosbag2_bagfile_information: MetaInfo,
}

#[derive(Deserialize)]
struct MetaInfo {
    #[serde(default)]
    duration: Option<MetaNanos>,
    #[serde(default)]
    message_count: Option<u64>,
    #[serde(default)]
    topics_with_message_count: Vec<MetaTopic>,
}

#[derive(Deserialize)]
struct MetaNanos {
    nanoseconds: u64,
}

#[derive(Deserialize)]
struct MetaTopic {
    topic_metadata: MetaTopicMeta,
    message_count: u64,
}

#[derive(Deserialize)]
struct MetaTopicMeta {
    name: String,
    r#type: String,
}

// ---------- generated dora dataflow ----------

#[derive(Serialize)]
struct DoraFlow {
    nodes: Vec<DoraNodeDef>,
}

#[derive(Serialize)]
struct DoraNodeDef {
    id: String,
    path: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    inputs: BTreeMap<String, DoraInput>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    outputs: Vec<String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    env: BTreeMap<String, String>,
}

#[derive(Serialize)]
struct DoraInput {
    source: String,
    queue_size: usize,
}

fn sanitize_topic(topic: &str) -> String {
    let s = topic.trim_start_matches('/').replace('/', "__");
    if s.is_empty() {
        "_root".to_string()
    } else {
        s
    }
}

fn abs(base: &Path, p: &Path) -> PathBuf {
    if p.is_absolute() {
        p.to_path_buf()
    } else {
        base.join(p)
    }
}

struct Plan {
    dataflow: DoraFlow,
    report_path: PathBuf,
    topics: Vec<(String, Option<u64>)>, // subscribed topic, rows in bag
    workdir: PathBuf,
}

fn preflight(flow_path: &Path) -> Result<Plan> {
    let flow_dir = flow_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    let flow_dir = flow_dir.canonicalize().unwrap_or(flow_dir);
    let flow: Flow = serde_yaml::from_str(
        &std::fs::read_to_string(flow_path)
            .with_context(|| format!("read {}", flow_path.display()))?,
    )
    .context("parse flow yaml")?;

    let bag = abs(&flow_dir, &flow.bag);
    if !bag.exists() {
        bail!("bag not found: {}", bag.display());
    }

    // bag metadata (optional but strongly recommended: enables preflight + coverage)
    let meta_path = if bag.is_dir() {
        bag.join("metadata.yaml")
    } else {
        bag.parent().unwrap_or(Path::new(".")).join("metadata.yaml")
    };
    let meta: Option<MetaInfo> = if meta_path.exists() {
        let root: MetaRoot = serde_yaml::from_str(&std::fs::read_to_string(&meta_path)?)
            .with_context(|| format!("parse {}", meta_path.display()))?;
        Some(root.rosbag2_bagfile_information)
    } else {
        eprintln!(
            "warning: {} not found — topic preflight and coverage are disabled",
            meta_path.display()
        );
        None
    };
    let bag_topics: BTreeMap<String, (String, u64)> = meta
        .as_ref()
        .map(|m| {
            m.topics_with_message_count
                .iter()
                .map(|t| {
                    (
                        t.topic_metadata.name.clone(),
                        (t.topic_metadata.r#type.clone(), t.message_count),
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    // validate node ids and wiring
    let mut node_outputs: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for n in &flow.nodes {
        if n.id.starts_with("bagflow") {
            bail!("node id `{}` is reserved", n.id);
        }
        if node_outputs.contains_key(&n.id) {
            bail!("duplicate node id `{}`", n.id);
        }
        node_outputs.insert(n.id.clone(), n.outputs.clone());
    }

    let mut subscribed: BTreeMap<String, String> = BTreeMap::new(); // topic -> output id
    let mut wiring: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for n in &flow.nodes {
        for (input_name, input) in &n.inputs {
            if input_name == "done" || input_name == "result" {
                bail!("input name `{input_name}` is reserved (node `{}`)", n.id);
            }
            let re = input.reference()?;
            if re.starts_with('/') {
                if let Some(m) = &meta {
                    if !bag_topics.contains_key(re) {
                        let mut avail: Vec<String> = bag_topics
                            .iter()
                            .map(|(k, (ty, c))| format!("  {k}  [{ty}] ({c} msgs)"))
                            .collect();
                        avail.sort();
                        let _ = m;
                        bail!(
                            "topic `{re}` (node `{}`, input `{input_name}`) is not in the bag.\navailable topics:\n{}",
                            n.id,
                            avail.join("\n")
                        );
                    }
                }
                subscribed
                    .entry(re.to_string())
                    .or_insert_with(|| sanitize_topic(re));
            } else {
                let (nid, out) = re
                    .split_once('/')
                    .with_context(|| format!("bad node reference `{re}` — expected `node_id/output`"))?;
                let outs = node_outputs
                    .get(nid)
                    .with_context(|| format!("unknown node `{nid}` referenced by `{}`", n.id))?;
                if !outs.iter().any(|o| o == out) {
                    bail!("node `{nid}` has no output `{out}` (referenced by `{}`)", n.id);
                }
            }
            wiring
                .entry(n.id.clone())
                .or_default()
                .insert(input_name.clone(), re.to_string());
        }
    }
    if subscribed.is_empty() {
        bail!("no rostopic inputs — at least one node must subscribe to a bag topic");
    }

    let workdir = flow_dir.join(".bagflow");
    let pylib = workdir.join("pylib");
    let report_path = abs(&flow_dir, &flow.report);

    let exe_dir = std::env::current_exe()?
        .parent()
        .context("exe dir")?
        .to_path_buf();
    let source_bin = exe_dir.join("bagflow-source");

    // ----- build the dora dataflow -----
    let mut nodes = Vec::new();
    let done_input = || DoraInput {
        source: format!("{REPORT_ID}/done"),
        queue_size: 100,
    };

    let mut source_outputs: Vec<String> = subscribed.values().cloned().collect();
    source_outputs.push("result".to_string());
    let mut source_env = BTreeMap::from([
        ("BAGFLOW_BAG".to_string(), bag.display().to_string()),
        (
            "BAGFLOW_TOPICS".to_string(),
            serde_json::to_string(&subscribed)?,
        ),
    ]);
    if let Some(rows) = flow.source.batch_rows {
        source_env.insert("BAGFLOW_BATCH_ROWS".to_string(), rows.to_string());
    }
    if let Some(bytes) = flow.source.batch_bytes {
        source_env.insert("BAGFLOW_BATCH_BYTES".to_string(), bytes.to_string());
    }
    nodes.push(DoraNodeDef {
        id: SOURCE_ID.to_string(),
        path: source_bin.display().to_string(),
        inputs: BTreeMap::from([("done".to_string(), done_input())]),
        outputs: source_outputs,
        env: source_env,
    });

    for n in &flow.nodes {
        let mut inputs = BTreeMap::new();
        inputs.insert("done".to_string(), done_input());
        for (input_name, input) in &n.inputs {
            let re = input.reference()?;
            let source = if re.starts_with('/') {
                format!("{SOURCE_ID}/{}", subscribed[re])
            } else {
                re.to_string()
            };
            // precedence: per-input > per-node > flow defaults > built-in
            let queue_size = input
                .queue_size()
                .or(n.queue_size)
                .or(flow.defaults.queue_size)
                .unwrap_or(DEFAULT_QUEUE);
            inputs.insert(input_name.clone(), DoraInput { source, queue_size });
        }
        let mut outputs = n.outputs.clone();
        outputs.push("result".to_string());

        let mut env = n.env.clone();
        env.insert(
            "BAGFLOW_INPUTS".to_string(),
            n.inputs.keys().cloned().collect::<Vec<_>>().join(","),
        );
        env.insert("BAGFLOW_OUTPUTS".to_string(), n.outputs.join(","));
        env.insert("BAGFLOW_NODE_ID".to_string(), n.id.clone());
        let pypath = pylib.display().to_string();
        env.entry("PYTHONPATH".to_string())
            .and_modify(|v| *v = format!("{pypath}:{v}"))
            .or_insert(pypath);

        nodes.push(DoraNodeDef {
            id: n.id.clone(),
            path: abs(&flow_dir, &n.path).display().to_string(),
            inputs,
            outputs,
            env,
        });
    }

    // report aggregator
    let mut report_inputs = BTreeMap::new();
    report_inputs.insert(
        format!("result_{SOURCE_ID}"),
        DoraInput {
            source: format!("{SOURCE_ID}/result"),
            queue_size: DEFAULT_QUEUE,
        },
    );
    for n in &flow.nodes {
        report_inputs.insert(
            format!("result_{}", n.id),
            DoraInput {
                source: format!("{}/result", n.id),
                queue_size: DEFAULT_QUEUE,
            },
        );
    }
    let expected: BTreeMap<&String, u64> =
        bag_topics.iter().map(|(k, (_, c))| (k, *c)).collect();
    let bag_info = serde_json::json!({
        "path": bag.display().to_string(),
        "duration_s": meta.as_ref().and_then(|m| m.duration.as_ref()).map(|d| d.nanoseconds as f64 / 1e9),
        "message_count": meta.as_ref().and_then(|m| m.message_count),
    });
    let report_input_names = report_inputs.keys().cloned().collect::<Vec<_>>().join(",");
    nodes.push(DoraNodeDef {
        id: REPORT_ID.to_string(),
        path: pylib.join("report.py").display().to_string(),
        inputs: report_inputs,
        outputs: vec!["done".to_string()],
        env: BTreeMap::from([
            (
                "BAGFLOW_REPORT".to_string(),
                report_path.display().to_string(),
            ),
            (
                "BAGFLOW_EXPECTED".to_string(),
                serde_json::to_string(&expected)?,
            ),
            (
                "BAGFLOW_WIRING".to_string(),
                serde_json::to_string(&wiring)?,
            ),
            ("BAGFLOW_BAGINFO".to_string(), bag_info.to_string()),
            ("BAGFLOW_INPUTS".to_string(), report_input_names),
        ]),
    });

    let topics = subscribed
        .keys()
        .map(|t| (t.clone(), bag_topics.get(t).map(|(_, c)| *c)))
        .collect();

    Ok(Plan {
        dataflow: DoraFlow { nodes },
        report_path,
        topics,
        workdir,
    })
}

fn write_workdir(plan: &Plan) -> Result<PathBuf> {
    let pylib = plan.workdir.join("pylib");
    std::fs::create_dir_all(pylib.join("bagflow"))?;
    std::fs::write(pylib.join("bagflow/__init__.py"), PY_HELPER)?;
    std::fs::write(pylib.join("report.py"), PY_REPORT)?;
    let dataflow_path = plan.workdir.join("dataflow.yml");
    std::fs::write(&dataflow_path, serde_yaml::to_string(&plan.dataflow)?)?;
    Ok(dataflow_path)
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::Check { flow } => {
            let plan = preflight(&flow)?;
            println!("preflight OK — subscribed topics:");
            for (t, count) in &plan.topics {
                match count {
                    Some(c) => println!("  {t} ({c} msgs)"),
                    None => println!("  {t} (count unknown)"),
                }
            }
            println!("report: {}", plan.report_path.display());
            Ok(())
        }
        Cmd::Run { flow, no_attach } => {
            let plan = preflight(&flow)?;
            let dataflow_path = write_workdir(&plan)?;

            // best effort: coordinator/daemon may already be running
            let _ = Command::new("dora").arg("up").status();

            let t0 = std::time::Instant::now();
            if no_attach {
                let _ = std::fs::remove_file(&plan.report_path);
                let status = Command::new("dora")
                    .arg("start")
                    .arg(&dataflow_path)
                    .arg("--detach")
                    .status()
                    .context("failed to run `dora` — is the dora CLI installed?")?;
                if !status.success() {
                    bail!("dora start failed with {status}");
                }
                // the report node writes report.json atomically once every
                // node has drained its inputs; teardown continues in the daemon
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3600);
                while !plan.report_path.exists() {
                    if std::time::Instant::now() > deadline {
                        bail!("timed out waiting for {}", plan.report_path.display());
                    }
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
            } else {
                let status = Command::new("dora")
                    .arg("start")
                    .arg(&dataflow_path)
                    .arg("--attach")
                    .status()
                    .context("failed to run `dora` — is the dora CLI installed?")?;
                if !status.success() {
                    bail!("dora start failed with {status}");
                }
            }
            let wall = t0.elapsed().as_secs_f64();
            println!("\nflow finished in {wall:.2}s");
            println!("report: {}", plan.report_path.display());
            if let Ok(report) = std::fs::read_to_string(&plan.report_path) {
                println!("{report}");
            }
            Ok(())
        }
    }
}
