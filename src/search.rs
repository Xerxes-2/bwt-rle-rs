use futures::prelude::*;
use std::{collections::BTreeSet, ops::Range, sync::RwLock};

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
    inner: RwLock<BTreeSet<CacheRL>>,
}

impl Cache {
    fn search(&self, pos: i32) -> Option<CacheRL> {
        self.inner
            .read()
            .unwrap()
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
            })
    }

    fn insert(&self, rl: CacheRL) {
        if self.inner.read().unwrap().len() < MAX_CACHE {
            self.inner.write().unwrap().insert(rl);
        }
    }
}

impl Context {
    async fn search_pattern(&self, pattern: &[u8]) -> Range<i32> {
        let mut index_start = self.c_table[pattern[0].map_char()];
        let mut index_end = self.c_table[pattern[0].map_char() + 1];
        let mut ooc_start;
        let mut ooc_end;
        for &ch in pattern.iter().skip(1) {
            ooc_start = self.occ_fn(ch, index_start).await;
            ooc_end = self.occ_fn(ch, index_end).await;
            index_start = self.nth_char_pos(ooc_start, ch);
            index_end = self.nth_char_pos(ooc_end, ch);
        }
        index_start..index_end
    }
    async fn get_metadata(&mut self) {
        let map_lb = b'['.map_char();
        self.recs = self.c_table[map_lb + 1] - self.c_table[map_lb];
        let mut l = 0;
        let mut r = self.search_id_in_pos(0).await;
        if r >= self.recs {
            l = r - self.recs;
        }
        while l < r {
            let mid = (l + r) / 2;
            let mut pattern = format!("[{}]", mid).into_bytes();
            pattern.reverse();
            let range = self.search_pattern(&pattern).await;
            if range.is_empty() {
                l = mid + 1;
            } else {
                r = mid;
            }
        }
        self.min_id = l;
    }

    async fn search_id_in_pos(&self, mut pos: i32) -> i32 {
        let mut buf = Vec::with_capacity(16);
        let mut id = false;
        let mut rl = self.cached_decode(pos).await;
        loop {
            if rl.ch == b']' {
                id = true;
            }
            pos = self.nth_char_pos(rl.rank, rl.ch);
            rl = self.cached_decode(pos).await;
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

    async fn cached_decode(&self, pos: i32) -> CacheRL {
        if let Some(rl) = self.cache.search(pos) {
            return rl;
        }
        let rl: CacheRL = self.decode(pos).await.into();
        let mut cached_rl = rl;
        cached_rl.rank = rl.rank - pos + rl.pos;
        self.cache.insert(cached_rl);
        rl
    }

    async fn search_pos_of_id(&self, id: i32) -> i32 {
        let mut pat = format!("[{}", id).into_bytes();
        pat.reverse();
        let mut pos = self.c_table[b']'.map_char()];
        for ch in pat {
            let occ = self.occ_fn(ch, pos).await;
            pos = self.nth_char_pos(occ, ch);
        }
        pos
    }

    pub async fn search(&mut self, pattern: &[u8]) {
        self.get_metadata().await;
        let num_concurrent = 128;
        let range = self.search_pattern(pattern).await;
        let ids = stream::iter(range)
            .map(async |x| self.search_id_in_pos(x).await + 1)
            .buffer_unordered(num_concurrent)
            .collect::<Vec<_>>();
        let mut ids = ids.await;
        ids.sort_unstable();
        ids.dedup();
        let upper = self.min_id + self.recs;
        let ctx = &*self;
        stream::iter(ids)
            .map(|id| async move {
                let start = if id == upper {
                    ctx.search_pos_of_id(ctx.min_id).await
                } else {
                    ctx.search_pos_of_id(id).await
                };
                let mut buf = [0u8; MAX_RECORD_LEN];
                let str = ctx.rebuild_record(start, &mut buf).await;
                println!("[{}]{}", id - 1, str);
            })
            .buffered(num_concurrent)
            .collect::<Vec<_>>()
            .await;
    }

    async fn rebuild_record<'a>(&self, mut pos: i32, buf: &'a mut [u8]) -> &'a str {
        let mut rl = self.cached_decode(pos).await;
        let mut cur = 0;
        while rl.ch != b']' {
            buf[cur] = rl.ch;
            cur += 1;
            pos = self.nth_char_pos(rl.rank, rl.ch);
            rl = self.cached_decode(pos).await;
        }
        buf[..cur].reverse();
        std::str::from_utf8(&buf[..cur]).unwrap()
    }
}
