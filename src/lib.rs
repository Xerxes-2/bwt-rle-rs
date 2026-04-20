use compio::buf::bytes::BytesMut;
use compio::fs::File;
use core::cell::RefCell;
use index::gen_c_table;
use search::Cache;

pub mod index;
pub mod search;
pub const I32_SIZE: usize = std::mem::size_of::<i32>();
pub const ALPHABETS: usize = 98;
pub const OOC_TABLE_SIZE: usize = ALPHABETS * I32_SIZE;
pub const CHUNK_SIZE: usize = OOC_TABLE_SIZE + I32_SIZE;
pub const MAX_CACHE: usize = 250000;
pub const BUF_POOL_CAP: usize = 128;
pub const READ_BUF_SIZE: usize = CHUNK_SIZE + 4;

pub struct Context {
    /// rlb file
    rlb: File,
    /// index file
    index: Option<File>,
    /// number of checkpoints
    cps: usize,
    /// c table
    c_table: [i32; ALPHABETS + 1],
    /// positions
    positions: Vec<i32>,
    /// minimum id
    min_id: i32,
    /// number of records
    recs: i32,
    /// cache
    cache: Cache,
    /// reusable IO buffers (single-threaded, compio is ST)
    bufs: RefCell<Vec<BytesMut>>,
}

impl Context {
    pub async fn new(rlb: File, index: Option<File>, cps: usize, positions: Vec<i32>) -> Self {
        let c_table = gen_c_table(&rlb, index.as_ref(), cps).await;
        Self {
            rlb,
            index,
            cps,
            c_table,
            positions,
            recs: 0,
            min_id: 0,
            cache: Cache::default(),
            bufs: RefCell::new(Vec::with_capacity(BUF_POOL_CAP)),
        }
    }

    pub fn take_buf(&self) -> BytesMut {
        self.bufs
            .borrow_mut()
            .pop()
            .map(|mut b| {
                b.clear();
                b
            })
            .unwrap_or_else(|| BytesMut::with_capacity(READ_BUF_SIZE))
    }

    pub fn put_buf(&self, buf: BytesMut) {
        let mut p = self.bufs.borrow_mut();
        if p.len() < BUF_POOL_CAP {
            p.push(buf);
        }
    }
}
