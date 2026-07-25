//! CUDA decode node: nvJPEG decodes on the GPU, only the raw frame comes
//! back to host memory. Same output contract as bagflow-decode (CPU), so the
//! two are swappable in flow.yml.
//!
//! CUDA libraries are loaded at runtime (dlopen), so this binary builds
//! without a CUDA toolchain and fails with a clear message on machines
//! without a GPU instead of failing to start.
//!
//! env:
//!   RESIZE            output "WxH" (default native); exact resize on CPU
//!   BAGFLOW_NVJPEG    override path of libnvjpeg (default libnvjpeg.so.13 -> .so)
//!   BAGFLOW_CUDART    override path of libcudart (default libcudart.so.13 -> .so)

use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, LargeBinaryArray, StructArray, TimestampNanosecondArray, UInt8Array};
use bagflow_node::{BagflowNode, Param, Params};
use fast_image_resize::images::Image as FirImage;
use fast_image_resize::{FilterType, PixelType, ResizeAlg, ResizeOptions, Resizer};
use libloading::{Library, Symbol};
use std::ffi::c_void;
use std::time::Instant;

const NVJPEG_OUTPUT_BGRI: i32 = 6;
const CUDA_MEMCPY_D2H: i32 = 2;

#[repr(C)]
struct NvjpegImage {
    channel: [*mut u8; 4],
    pitch: [usize; 4],
}

struct Gpu {
    _nvjpeg: Library,
    _cudart: Library,
    handle: *mut c_void,
    state: *mut c_void,
    dev_buf: *mut c_void,
    dev_cap: usize,
    // function pointers (transmuted symbols kept alive by the Library fields)
    fn_get_info: unsafe extern "C" fn(*mut c_void, *const u8, usize, *mut i32, *mut i32, *mut i32, *mut i32) -> i32,
    fn_decode: unsafe extern "C" fn(*mut c_void, *mut c_void, *const u8, usize, i32, *mut NvjpegImage, *mut c_void) -> i32,
    fn_malloc: unsafe extern "C" fn(*mut *mut c_void, usize) -> i32,
    fn_free: unsafe extern "C" fn(*mut c_void) -> i32,
    fn_memcpy: unsafe extern "C" fn(*mut c_void, *const c_void, usize, i32) -> i32,
    fn_sync: unsafe extern "C" fn(*mut c_void) -> i32,
}

fn open_lib(env: &str, candidates: &[&str]) -> Result<Library> {
    let names: Vec<String> = match std::env::var(env) {
        Ok(p) => vec![p],
        Err(_) => candidates.iter().map(|s| s.to_string()).collect(),
    };
    for name in &names {
        if let Ok(lib) = unsafe { Library::new(name) } {
            return Ok(lib);
        }
    }
    bail!("could not dlopen any of {names:?} — is CUDA available on this machine?")
}

impl Gpu {
    fn init() -> Result<Self> {
        let cudart = open_lib("BAGFLOW_CUDART", &["libcudart.so.13", "libcudart.so.12", "libcudart.so"])?;
        let nvjpeg = open_lib("BAGFLOW_NVJPEG", &["libnvjpeg.so.13", "libnvjpeg.so.12", "libnvjpeg.so"])?;
        unsafe {
            let create: Symbol<unsafe extern "C" fn(*mut *mut c_void) -> i32> =
                nvjpeg.get(b"nvjpegCreateSimple")?;
            let state_create: Symbol<unsafe extern "C" fn(*mut c_void, *mut *mut c_void) -> i32> =
                nvjpeg.get(b"nvjpegJpegStateCreate")?;
            let fn_get_info = *nvjpeg.get::<unsafe extern "C" fn(*mut c_void, *const u8, usize, *mut i32, *mut i32, *mut i32, *mut i32) -> i32>(b"nvjpegGetImageInfo")?;
            let fn_decode = *nvjpeg.get::<unsafe extern "C" fn(*mut c_void, *mut c_void, *const u8, usize, i32, *mut NvjpegImage, *mut c_void) -> i32>(b"nvjpegDecode")?;
            let fn_malloc = *cudart.get::<unsafe extern "C" fn(*mut *mut c_void, usize) -> i32>(b"cudaMalloc")?;
            let fn_free = *cudart.get::<unsafe extern "C" fn(*mut c_void) -> i32>(b"cudaFree")?;
            let fn_memcpy = *cudart.get::<unsafe extern "C" fn(*mut c_void, *const c_void, usize, i32) -> i32>(b"cudaMemcpy")?;
            let fn_sync = *cudart.get::<unsafe extern "C" fn(*mut c_void) -> i32>(b"cudaStreamSynchronize")?;

            let mut handle: *mut c_void = std::ptr::null_mut();
            let rc = create(&mut handle);
            if rc != 0 {
                bail!("nvjpegCreateSimple failed with status {rc} (no usable GPU?)");
            }
            let mut state: *mut c_void = std::ptr::null_mut();
            let rc = state_create(handle, &mut state);
            if rc != 0 {
                bail!("nvjpegJpegStateCreate failed with status {rc}");
            }
            Ok(Gpu {
                _nvjpeg: nvjpeg,
                _cudart: cudart,
                handle,
                state,
                dev_buf: std::ptr::null_mut(),
                dev_cap: 0,
                fn_get_info,
                fn_decode,
                fn_malloc,
                fn_free,
                fn_memcpy,
                fn_sync,
            })
        }
    }

    fn ensure_dev_buf(&mut self, size: usize) -> Result<()> {
        if size <= self.dev_cap {
            return Ok(());
        }
        unsafe {
            if !self.dev_buf.is_null() {
                (self.fn_free)(self.dev_buf);
            }
            let mut p: *mut c_void = std::ptr::null_mut();
            let rc = (self.fn_malloc)(&mut p, size);
            if rc != 0 {
                bail!("cudaMalloc({size}) failed with {rc}");
            }
            self.dev_buf = p;
            self.dev_cap = size;
        }
        Ok(())
    }

    /// decode one JPEG on the GPU, return interleaved BGR bytes on the host
    fn decode(&mut self, jpg: &[u8]) -> Result<(Vec<u8>, usize, usize)> {
        unsafe {
            let (mut ncomp, mut subsamp) = (0i32, 0i32);
            let mut widths = [0i32; 4];
            let mut heights = [0i32; 4];
            let rc = (self.fn_get_info)(
                self.handle, jpg.as_ptr(), jpg.len(),
                &mut ncomp, &mut subsamp, widths.as_mut_ptr(), heights.as_mut_ptr(),
            );
            if rc != 0 {
                bail!("nvjpegGetImageInfo failed with {rc}");
            }
            let (w, h) = (widths[0] as usize, heights[0] as usize);
            let size = w * h * 3;
            self.ensure_dev_buf(size)?;
            let mut image = NvjpegImage {
                channel: [self.dev_buf as *mut u8, std::ptr::null_mut(), std::ptr::null_mut(), std::ptr::null_mut()],
                pitch: [w * 3, 0, 0, 0],
            };
            let rc = (self.fn_decode)(
                self.handle, self.state, jpg.as_ptr(), jpg.len(),
                NVJPEG_OUTPUT_BGRI, &mut image, std::ptr::null_mut(),
            );
            if rc != 0 {
                bail!("nvjpegDecode failed with {rc}");
            }
            (self.fn_sync)(std::ptr::null_mut());
            let mut host = vec![0u8; size];
            let rc = (self.fn_memcpy)(
                host.as_mut_ptr() as *mut c_void, self.dev_buf as *const c_void, size, CUDA_MEMCPY_D2H,
            );
            if rc != 0 {
                bail!("cudaMemcpy D2H failed with {rc}");
            }
            Ok((host, w, h))
        }
    }
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
    let mut gpu = Gpu::init().map_err(|e| anyhow!("CUDA init failed: {e:#}"))?;
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

        for i in 0..data.len() {
            match gpu.decode(data.value(i)) {
                Ok((mut pixels, mut w, mut h)) => {
                    if let Some((tw, th)) = target {
                        if (w, h) != (tw, th) {
                            pixels = resize_exact(pixels, w, h, tw, th)?;
                            (w, h) = (tw, th);
                        }
                    }
                    node.send(
                        "frames",
                        UInt8Array::from(pixels),
                        frame_params(w, h, stamps.value(i)),
                    )?;
                    frames += 1;
                }
                Err(_) => failed += 1,
            }
        }
    }

    node.report(serde_json::json!({
        "check": "decode",
        "backend": "cuda-nvjpeg",
        "frames_decoded": frames,
        "decode_failures": failed,
        "output_resolution": target.map(|(w, h)| format!("{w}x{h}")).unwrap_or("native".into()),
        "wall_s": (t0.elapsed().as_secs_f64() * 1000.0).round() / 1000.0,
    }))?;
    node.close()
}
