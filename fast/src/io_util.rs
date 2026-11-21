//! Zero-copy memory mapping and newline-aligned byte-range work units across files.

use std::fs::{self, File};
use std::io;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::slice;

use crate::parse::find_newline;

unsafe extern "C" {
    fn mmap(
        addr: *mut core::ffi::c_void,
        len: usize,
        prot: i32,
        flags: i32,
        fd: i32,
        offset: i64,
    ) -> *mut core::ffi::c_void;
    fn munmap(addr: *mut core::ffi::c_void, len: usize) -> i32;
    fn madvise(addr: *mut core::ffi::c_void, len: usize, advice: i32) -> i32;
}

const MADV_SEQUENTIAL: i32 = 2;
const MADV_DONTNEED: i32 = 4;

/// Give a buffer's whole pages back to the kernel now, from the calling
/// thread, and never free it: the process ends with `_exit`, so the serial
/// teardown at exit has less to do. The buffer must not be used again.
pub fn release<T>(v: Vec<T>) {
    let bytes = v.capacity() * std::mem::size_of::<T>();
    let start = (v.as_ptr() as usize).div_ceil(4096) * 4096;
    let end = (v.as_ptr() as usize + bytes) / 4096 * 4096;
    if end > start {
        unsafe {
            madvise(start as *mut core::ffi::c_void, end - start, MADV_DONTNEED);
        }
    }
    std::mem::forget(v);
}
const PROT_READ: i32 = 1;
const MAP_PRIVATE: i32 = 2;
const MAP_FAILED: *mut core::ffi::c_void = !0usize as *mut core::ffi::c_void;

pub struct MmapFile {
    ptr: *mut core::ffi::c_void,
    len: usize,
    fallback: Vec<u8>,
}

unsafe impl Send for MmapFile {}
unsafe impl Sync for MmapFile {}

impl MmapFile {
    pub fn open(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let len = file.metadata()?.len() as usize;
        if len == 0 {
            return Ok(Self {
                ptr: core::ptr::null_mut(),
                len: 0,
                fallback: Vec::new(),
            });
        }
        let ptr = unsafe {
            mmap(
                core::ptr::null_mut(),
                len,
                PROT_READ,
                MAP_PRIVATE,
                file.as_raw_fd(),
                0,
            )
        };
        if ptr != MAP_FAILED {
            unsafe {
                madvise(ptr, len, MADV_SEQUENTIAL);
            }
        }
        if ptr == MAP_FAILED {
            let fallback = fs::read(path)?;
            Ok(Self {
                ptr: core::ptr::null_mut(),
                len: 0,
                fallback,
            })
        } else {
            Ok(Self {
                ptr,
                len,
                fallback: Vec::new(),
            })
        }
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        if self.len > 0 {
            unsafe { slice::from_raw_parts(self.ptr as *const u8, self.len) }
        } else {
            &self.fallback
        }
    }
}

impl Drop for MmapFile {
    fn drop(&mut self) {
        if self.len > 0 && !self.ptr.is_null() {
            unsafe {
                munmap(self.ptr, self.len);
            }
        }
    }
}

/// Every regular file directly in `folder` whose name ends in `.csv`, sorted by name.
pub fn csv_files(folder: &Path) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(folder) else {
        return Vec::new();
    };
    let mut paths = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path
            .file_name()
            .is_some_and(|n| n.as_bytes().ends_with(b".csv"))
            && path.is_file()
        {
            paths.push(path);
        }
    }
    paths.sort();
    paths
}

/// Iterate over each `\n`-terminated (or EOF-terminated) line in `slice`.
#[inline(always)]
pub fn for_each_line<'a>(mut slice: &'a [u8], mut f: impl FnMut(&'a [u8])) {
    while !slice.is_empty() {
        match find_newline(slice) {
            Some(nl) => {
                f(&slice[..nl]);
                slice = &slice[nl + 1..];
            }
            None => {
                f(slice);
                break;
            }
        }
    }
}
