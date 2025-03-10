use std::{
    fs::File,
    io::{BufReader, Read},
};

use index::gen_c_table;
use search::Cache;

pub mod index;
pub mod search;
pub const I32_SIZE: usize = std::mem::size_of::<i32>();
pub const ALPHABETS: usize = 98;
pub const CHEATS: usize = 8;
pub const PIECE_LEN: usize = ALPHABETS * I32_SIZE + CHEATS * I32_SIZE * 3;
pub const CHECKPOINT_LEN: usize = PIECE_LEN + I32_SIZE;
pub const CACHE_SIZE: usize = 15000;

pub trait TryReadExact: Read {
    fn try_read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_size = buf.len();
        match self.read_exact(buf) {
            Ok(_) => Ok(buf_size),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                Ok(buf.iter().position(|&x| x == 0).unwrap())
            }
            Err(e) => Err(e),
        }
    }
}

impl TryReadExact for BufReader<File> {}

pub struct Context {
    rlb: BufReader<File>,           // rlb file
    index: Option<BufReader<File>>, // index file
    cps: usize,                     // number of checkpoints
    c_table: [i32; ALPHABETS + 1],  // c table
    positions: Vec<i32>,            // positions
    min_id: i32,                    // minimum id
    recs: i32,                      // number of records
    cache: Cache,                   // cache
}

impl Context {
    pub fn new(
        mut rlb: BufReader<File>,
        mut index: Option<BufReader<File>>,
        cps: usize,
        positions: Vec<i32>,
    ) -> Self {
        let c_table = gen_c_table(&mut rlb, index.as_mut(), cps);
        Self {
            rlb,
            index,
            cps,
            c_table,
            positions,
            recs: 0,
            min_id: 0,
            cache: Cache::new(),
        }
    }
}
