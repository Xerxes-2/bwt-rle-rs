use std::{collections::BTreeSet, ops::Range};

use crate::{
    Context, MAX_CACHE,
    index::{Mapper, RunLength},
};

const MAX_RECORD_LEN: usize = 5000;

#[derive(Debug, Clone, Copy)]
struct CacheRL {
    ch: u8,
    pos: i32,
    len: i32,
    rank: i32,
}

impl Default for CacheRL {
    fn default() -> Self {
        Self {
            ch: 0,
            pos: 0,
            len: 1,
            rank: 0,
        }
    }
}

impl CacheRL {
    fn hit(&self, pos: i32) -> Option<Self> {
        if self.pos <= pos && pos < self.pos + self.len {
            Some(*self)
        } else {
            None
        }
    }
}

impl PartialEq for CacheRL {
    fn eq(&self, other: &Self) -> bool {
        self.pos == other.pos
    }
}

impl Eq for CacheRL {}

impl PartialOrd for CacheRL {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.pos.cmp(&other.pos))
    }
}

impl Ord for CacheRL {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.pos.cmp(&other.pos)
    }
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

#[derive(Default)]
pub struct Cache {
    hit: usize,
    miss: usize,
    inner: BTreeSet<CacheRL>,
}

impl Cache {
    fn search(&mut self, pos: i32) -> Option<CacheRL> {
        let rl = self
            .inner
            .range(
                ..=CacheRL {
                    pos,
                    ..Default::default()
                },
            )
            .next_back()
            .and_then(|rl| rl.hit(pos))
            .map(|mut rl| {
                rl.rank = rl.rank + pos - rl.pos;
                rl
            });
        if let Some(rl) = rl {
            if rl.len == 1 {
                self.inner.remove(&rl);
            }
            self.hit += 1;
        } else {
            self.miss += 1;
        }
        rl
    }

    fn insert(&mut self, rl: CacheRL) {
        if self.inner.len() < MAX_CACHE {
            self.inner.insert(rl);
        }
    }

    pub fn summary(&self) {
        eprintln!("Cache hit: {}, miss: {}", self.hit, self.miss);
    }
}

impl Context {
    fn search_pattern(&self, pattern: &[u8]) -> Range<i32> {
        let mut index_start = self.c_table[pattern[0].map_char()];
        let mut index_end = self.c_table[pattern[0].map_char() + 1];
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
    fn get_metadata(&mut self) {
        let map_lb = b'['.map_char();
        self.recs = self.c_table[map_lb + 1] - self.c_table[map_lb];
        let mut l = 0;
        let mut r = self.search_id_in_pos(0);
        if r >= self.recs {
            l = r - self.recs;
        }
        while l < r {
            let mid = (l + r) / 2;
            let mut pattern = format!("[{}]", mid).into_bytes();
            pattern.reverse();
            let range = self.search_pattern(&pattern);
            if range.is_empty() {
                l = mid + 1;
            } else {
                r = mid;
            }
        }
        self.min_id = l;
    }

    fn search_id_in_pos(&mut self, mut pos: i32) -> i32 {
        let mut buf = Vec::with_capacity(16);
        let mut id = false;
        let mut rl = self.cached_decode(pos);
        loop {
            if rl.ch == b']' {
                id = true;
            }
            pos = self.nth_char_pos(rl.rank, rl.ch);
            rl = self.cached_decode(pos);
            if rl.ch == b'[' {
                break;
            }
            if id {
                buf.push(rl.ch);
            }
        }
        buf.reverse();
        let str_id = std::str::from_utf8(&buf).unwrap();
        str_id.parse().unwrap()
    }

    fn cached_decode(&mut self, pos: i32) -> CacheRL {
        if let Some(rl) = self.cache.search(pos) {
            return rl;
        }
        let rl: CacheRL = self.decode(pos).into();
        let mut cached_rl = rl;
        cached_rl.rank = rl.rank - pos + rl.pos;
        self.cache.insert(cached_rl);
        rl
    }

    fn search_pos_of_id(&self, id: i32) -> i32 {
        let mut pat = format!("[{}", id).into_bytes();
        pat.reverse();
        let mut pos = self.c_table[b']'.map_char()];
        pat.iter().for_each(|&ch| {
            let occ = self.occ_fn(ch, pos);
            pos = self.nth_char_pos(occ, ch);
        });
        pos
    }

    pub fn search(&mut self, pattern: &[u8]) {
        self.get_metadata();
        let range = self.search_pattern(pattern);
        let matches = range.len();
        let mut ids = Vec::with_capacity(matches);
        for i in range {
            let id = self.search_id_in_pos(i) + 1;
            ids.push(id);
        }
        ids.sort_unstable();
        ids.dedup();
        let mut buf = Vec::with_capacity(MAX_RECORD_LEN);
        let upper = self.min_id + self.recs;
        ids.into_iter().for_each(|id| {
            let start = if id == upper {
                self.search_pos_of_id(self.min_id)
            } else {
                self.search_pos_of_id(id)
            };
            let id = id - 1;
            buf.clear();
            self.rebuild_record(start, &mut buf);
            println!("[{}]{}", id, std::str::from_utf8(&buf).unwrap());
        });
        self.summary();
    }

    fn rebuild_record(&mut self, mut pos: i32, buf: &mut Vec<u8>) {
        let mut rl = self.cached_decode(pos);
        while rl.ch != b']' {
            buf.push(rl.ch);
            pos = self.nth_char_pos(rl.rank, rl.ch);
            rl = self.cached_decode(pos);
        }
        buf.reverse();
    }
}
