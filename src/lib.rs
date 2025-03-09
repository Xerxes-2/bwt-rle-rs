use std::{
    fs::File,
    io::{BufReader, Read},
};

use index::gen_c_table;

pub mod index;
pub mod search;
pub const U32_SIZE: usize = std::mem::size_of::<u32>();
pub const ALPHABETS: usize = 98;
pub const CHEATS: usize = 8;
pub const PIECE_LEN: usize = ALPHABETS * U32_SIZE + CHEATS * U32_SIZE * 3;
pub const CHECKPOINT_LEN: usize = PIECE_LEN + U32_SIZE;

pub trait TryReadExact: Read {
    fn try_read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let buf_size = buf.len();
        match self.read_exact(buf) {
            Ok(_) => Ok(buf_size),
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(buf_size - buf.len()),
            Err(e) => Err(e),
        }
    }
}

impl TryReadExact for BufReader<File> {}

pub struct Context {
    rlb: BufReader<File>,           // rlb file
    index: Option<BufReader<File>>, // index file
    cps: usize,                     // number of checkpoints
    c_table: [u32; ALPHABETS + 1],  // c table
    positions: Vec<u32>,            // positions
    min_id: u32,                    // minimum id
    recs: u32,                      // number of records
}

impl Context {
    pub fn new(
        mut rlb: BufReader<File>,
        mut index: Option<BufReader<File>>,
        cps: usize,
        positions: Vec<u32>,
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
        }
    }
}
