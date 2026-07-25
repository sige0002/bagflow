//! CPU decode node: JPEG (CompressedImage topic batches) -> raw BGR frames,
//! one message per frame. Same contract as nodes/decode_image.py.
//!
//! env:
//!   RESIZE   output resolution "WxH" (default: native). libjpeg's DCT-domain
//!            scaling decodes at the nearest n/8 size above the target, then
//!            an exact SIMD resize produces WxH — changing resolution is a
//!            flow.yml edit, not a code change.
//!   WORKERS  decode threads (default: min(8, cpus))

use anyhow::{Context, Result};
use arrow::array::{Array, LargeBinaryArray, StructArray, TimestampNanosecondArray, UInt8Array};
use bagflow_node::{BagflowNode, Param, Params};
use fast_image_resize::images::Image as FirImage;
use fast_image_resize::{FilterType, PixelType, ResizeAlg, ResizeOptions, Resizer};
use rayon::prelude::*;
use std::cell::RefCell;
use std::time::Instant;
use turbojpeg::{Decompressor, Image, PixelFormat, ScalingFactor};

thread_local! {
    static DECOMP: RefCell<Option<Decompressor>> = const { RefCell::new(None) };
}

fn parse_resize(spec: &str) -> Result<Option<(usize, usize)>> {
    if spec.is_empty() {
        return Ok(None);
    }
    let (w, h) = spec
        .to_lowercase()
        .split_once('x')
        .map(|(a, b)| (a.to_string(), b.to_string()))
        .context("RESIZE must look like 224x224")?;
    Ok(Some((w.parse()?, h.parse()?)))
}

/// smallest n/8 DCT scaling whose output still covers the target size
fn pick_factor(w: usize, h: usize, tw: usize, th: usize) -> ScalingFactor {
    for num in 1..=8 {
        let f = ScalingFactor::new(num, 8);
        if f.scale(w) >= tw && f.scale(h) >= th {
            return f;
        }
    }
    ScalingFactor::ONE
}

fn resize_exact(pixels: Vec<u8>, w: usize, h: usize, tw: usize, th: usize) -> Result<Vec<u8>> {
    let src = FirImage::from_vec_u8(w as u32, h as u32, pixels, PixelType::U8x3)?;
    let mut dst = FirImage::new(tw as u32, th as u32, PixelType::U8x3);
    Resizer::new().resize(
        &src,
        &mut dst,
        &ResizeOptions::new().resize_alg(ResizeAlg::Convolution(FilterType::Box)),
    )?;
    Ok(dst.into_vec())
}

fn decode_one(jpg: &[u8], target: Option<(usize, usize)>) -> Option<(Vec<u8>, usize, usize)> {
    DECOMP.with(|cell| {
        let mut slot = cell.borrow_mut();
        let d = match slot.as_mut() {
            Some(d) => d,
            None => {
                *slot = Some(Decompressor::new().ok()?);
                slot.as_mut().unwrap()
            }
        };
        let header = d.read_header(jpg).ok()?;
        let (w, h) = (header.width, header.height);
        let factor = match target {
            Some((tw, th)) => pick_factor(w, h, tw, th),
            None => ScalingFactor::ONE,
        };
        d.set_scaling_factor(factor).ok()?;
        let (sw, sh) = (factor.scale(w), factor.scale(h));
        let mut image = Image {
            pixels: vec![0u8; sw * sh * 3],
            width: sw,
            pitch: sw * 3,
            height: sh,
            format: PixelFormat::BGR,
        };
        d.decompress(jpg, image.as_deref_mut()).ok()?;
        let mut pixels = image.pixels;
        let (mut ow, mut oh) = (sw, sh);
        if let Some((tw, th)) = target {
            if (ow, oh) != (tw, th) {
                pixels = resize_exact(pixels, ow, oh, tw, th).ok()?;
                (ow, oh) = (tw, th);
            }
        }
        Some((pixels, ow, oh))
    })
}

fn frame_params(w: usize, h: usize, stamp_ns: i64) -> Params {
    Params::from([
        ("rows".to_string(), Param::Integer(1)),
        ("width".to_string(), Param::Integer(w as i64)),
        ("height".to_string(), Param::Integer(h as i64)),
        ("channels".to_string(), Param::Integer(3)),
        ("stamp_ns".to_string(), Param::Integer(stamp_ns)),
    ])
}

fn main() -> Result<()> {
    let target = parse_resize(&std::env::var("RESIZE").unwrap_or_default())?;
    let workers = std::env::var("WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::cmp::min(8, num_cpus()));
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .build_global()?;

    let mut node = BagflowNode::init()?;
    let t0 = Instant::now();
    let mut frames = 0u64;
    let mut failed = 0u64;

    while let Some(msg) = node.next_message()? {
        let batch = msg
            .data
            .as_any()
            .downcast_ref::<StructArray>()
            .context("expected a topic batch (StructArray)")?;
        let data = batch
            .column_by_name("data")
            .context("no data column")?
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .context("data is not LargeBinary")?;
        let stamps = batch
            .column_by_name("log_time")
            .context("no log_time column")?
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .context("log_time is not a timestamp")?;

        let inputs: Vec<(&[u8], i64)> = (0..data.len())
            .map(|i| (data.value(i), stamps.value(i)))
            .collect();
        let decoded: Vec<(Option<(Vec<u8>, usize, usize)>, i64)> = inputs
            .par_iter()
            .map(|(jpg, ts)| (decode_one(jpg, target), *ts))
            .collect();
        for (result, ts) in decoded {
            match result {
                Some((pixels, w, h)) => {
                    node.send("frames", UInt8Array::from(pixels), frame_params(w, h, ts))?;
                    frames += 1;
                }
                None => failed += 1,
            }
        }
    }

    node.report(serde_json::json!({
        "check": "decode",
        "backend": "cpu-turbojpeg",
        "frames_decoded": frames,
        "decode_failures": failed,
        "output_resolution": target.map(|(w, h)| format!("{w}x{h}")).unwrap_or("native".into()),
        "wall_s": (t0.elapsed().as_secs_f64() * 1000.0).round() / 1000.0,
    }))?;
    node.close()
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}
