use std::{collections::BTreeSet, ops::Range};

use crate::{
    Context,
    index::{RunLength, map_char},
};

struct CacheRL {
    ch: u8,
    pos: u32,
    len: u32,
    rank: u32,
}

impl From<RunLength> for CacheRL {
    fn from(rl: RunLength) -> Self {
        Self {
            ch: rl.char,
            pos: rl.pos,
            len: rl.len,
            rank: rl.rank,
        }
    }
}

struct CacheCP {
    inner: BTreeSet<CacheRL>,
}

impl Context {
    fn search_pattern(&mut self, pattern: &[u8]) -> Range<u32> {
        let mut index_start = self.c_table[map_char(pattern[0]) as usize];
        let mut index_end = self.c_table[map_char(pattern[0]) as usize + 1];
        let mut ooc_start;
        let mut ooc_end;
        for &ch in pattern.iter().skip(1) {
            ooc_start = self.occ_fn(ch, index_start);
            ooc_end = self.occ_fn(ch, index_end);
            index_start = self.nth_char_pos(ooc_start, ch);
            index_end = self.nth_char_pos(ooc_end, ch);
        }
        index_start..index_end
    }
    fn find_metadata(&mut self) {
        let map_lb = map_char(b'[') as usize;
        self.recs = self.c_table[map_lb + 1] - self.c_table[map_lb];
        let mut l = 0;
    }

    fn cached_decode(&mut self, pos: u32) {}
}
