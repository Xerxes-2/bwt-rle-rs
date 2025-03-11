use compio::fs::File;
use index::gen_c_table;
use search::Cache;

pub mod index;
pub mod search;
pub const I32_SIZE: usize = std::mem::size_of::<i32>();
pub const ALPHABETS: usize = 98;
pub const OOC_TABLE_SIZE: usize = ALPHABETS * I32_SIZE;
pub const CHUNK_SIZE: usize = OOC_TABLE_SIZE + I32_SIZE;
pub const MAX_CACHE: usize = 250000;

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
        }
    }
}
