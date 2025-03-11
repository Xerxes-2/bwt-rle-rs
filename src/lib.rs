use std::{
    fs::File,
    io::{Read, Result},
};

use index::gen_c_table;
use search::Cache;

pub mod index;
pub mod search;
pub const I32_SIZE: usize = std::mem::size_of::<i32>();
pub const ALPHABETS: usize = 98;
pub const OOC_TABLE_SIZE: usize = ALPHABETS * I32_SIZE;
pub const CHUNK_SIZE: usize = OOC_TABLE_SIZE + I32_SIZE;
pub const MAX_CACHE: usize = 250000;

pub trait TryReadExact: Read {
    fn try_read_exact(mut self, mut buf: &mut [u8]) -> Result<usize>
    where
        Self: Sized,
    {
        let mut read = 0;
        while !buf.is_empty() {
            match self.read(buf) {
                Ok(0) => break,
                Ok(n) => {
                    buf = &mut buf[n..];
                    read += n;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(read)
    }
}

pub trait MyReadAt: Read {
    fn my_read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize>;
    fn try_read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> Result<usize>
    where
        Self: Sized,
    {
        let mut read = 0;
        while !buf.is_empty() {
            match self.my_read_at(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    buf = &mut buf[n..];
                    read += n;
                    offset += n as u64;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(read)
    }
    fn my_read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()>;
}

impl TryReadExact for &File {}

impl MyReadAt for &File {
    fn my_read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        #[cfg(target_family = "windows")]
        {
            use std::os::windows::fs::FileExt;
            self.seek_read(buf, offset)
        }
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::fs::FileExt;
            self.read_at(buf, offset)
        }
    }

    fn my_read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        #[cfg(target_family = "windows")]
        {
            self.try_seek_read_exact(buf, offset).map(|_| ())
        }
        #[cfg(target_family = "unix")]
        {
            use std::os::unix::fs::FileExt;
            self.read_exact_at(buf, offset)
        }
    }
}

pub struct Context {
    rlb: File,                     // rlb file
    index: Option<File>,           // index file
    cps: usize,                    // number of checkpoints
    c_table: [i32; ALPHABETS + 1], // c table
    positions: Vec<i32>,           // positions
    min_id: i32,                   // minimum id
    recs: i32,                     // number of records
    cache: Cache,                  // cache
}

impl Context {
    pub fn new(rlb: File, index: Option<File>, cps: usize, positions: Vec<i32>) -> Self {
        let c_table = gen_c_table(&rlb, index.as_ref(), cps);
        Self {
            rlb,
            index,
            cps,
            c_table,
            positions,
            recs: 0,
            min_id: 0,
            cache: Cache::default(),
        }
    }

    pub fn summary(&self) {
        self.cache.summary();
    }
}
