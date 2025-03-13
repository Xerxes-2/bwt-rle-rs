use std::iter::once;

use compio::{
    buf::bytes::BytesMut,
    fs::File,
    io::{AsyncReadAt, AsyncReadAtExt, AsyncWriteAtExt},
};

use crate::{ALPHABETS, CHUNK_SIZE, Context, I32_SIZE, OOC_TABLE_SIZE};
const MAP: [usize; 127] = [
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27,
    28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75,
    76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97,
];

#[inline]
#[cold]
fn cold() {}

#[inline]
#[allow(unused)]
fn likely(b: bool) -> bool {
    if !b {
        cold()
    }
    b
}

#[inline]
#[allow(unused)]
fn unlikely(b: bool) -> bool {
    if b {
        cold()
    }
    b
}

#[inline]
pub const fn map_char(c: u8) -> usize {
    MAP[c as usize]
}

pub trait Mapper {
    fn map_char(&self) -> usize;
}

impl Mapper for u8 {
    fn map_char(&self) -> usize {
        map_char(*self)
    }
}

trait IsRunLength {
    fn is_rl_tail(&self) -> bool;
}

const fn msb_u8(b: u8) -> bool {
    b & 0x80 != 0
}

impl IsRunLength for u8 {
    fn is_rl_tail(&self) -> bool {
        msb_u8(*self)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RunLength {
    pub char: u8,
    pub len: i32,
    size: u8,
    pub pos: i32,
    pub rank: i32,
}

impl RunLength {
    fn new(char: u8, pos: i32) -> Self {
        Self {
            char,
            len: 1,
            size: 0,
            pos,
            rank: 0,
        }
    }

    fn map_char(&self) -> usize {
        self.char.map_char()
    }

    fn occ(&self, occ: &OccTable) -> i32 {
        occ[self.map_char()]
    }

    fn extend_byte(&mut self, b: u8) -> bool {
        if !b.is_rl_tail() {
            return false;
        }
        let b = b & 0x7f;
        if self.size == 0 {
            self.len += 2 + (b as i32);
        } else {
            self.len += (b as i32) << (7 * self.size);
        }
        self.size += 1;
        true
    }

    fn update_occ(&self, occ: &mut OccTable) {
        occ[self.map_char()] += self.len;
    }
}

#[cold]
pub async fn gen_index(rlb: &File, mut index: &File, checkpoints: usize) -> Vec<i32> {
    let mut positions: Vec<i32> = Vec::with_capacity(checkpoints + 1);
    positions.push(0);
    let mut cur_pos = 0;
    let mut occ = [0i32; ALPHABETS];
    let mut pos_idx = (I32_SIZE * checkpoints) as u64;
    loop {
        let buf = BytesMut::zeroed(CHUNK_SIZE + 4);
        let (n, buf) = rlb
            .read_at(buf, (positions.len() - 1) as u64 * CHUNK_SIZE as u64)
            .await
            .unwrap();
        if n < CHUNK_SIZE {
            break;
        }
        let mut iter = buf
            .iter()
            .take(CHUNK_SIZE)
            .chain(buf.iter().skip(CHUNK_SIZE).take_while(|&&x| x.is_rl_tail()))
            .skip_while(|&x| x.is_rl_tail());

        let mut rl = RunLength::new(iter.next().unwrap().to_owned(), cur_pos);
        iter.for_each(|&b| {
            if !rl.extend_byte(b) {
                rl.rank = rl.occ(&occ);
                rl.update_occ(&mut occ);
                cur_pos += rl.len;
                rl = RunLength::new(b, cur_pos);
            }
        });
        rl.update_occ(&mut occ);
        cur_pos += rl.len;

        positions.push(cur_pos);

        let occ = occ
            .into_iter()
            .flat_map(i32::to_le_bytes)
            .collect::<Vec<_>>();
        index.write_all_at(occ, pos_idx).await.unwrap();
        pos_idx += OOC_TABLE_SIZE as u64;
    }
    let positions_raw = positions
        .iter()
        .skip(1)
        .copied()
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    index.write_all_at(positions_raw, 0).await.unwrap();
    positions
}

#[cold]
pub async fn gen_c_table(
    rlb: &File,
    index: Option<&File>,
    checkpoints: usize,
) -> [i32; ALPHABETS + 1] {
    let last_pos = checkpoints * CHUNK_SIZE;
    let mut c_table = [0; ALPHABETS + 1];
    if let Some(index) = index {
        let buf = BytesMut::zeroed(OOC_TABLE_SIZE);
        let pos = checkpoints * I32_SIZE + (checkpoints - 1) * OOC_TABLE_SIZE;
        let (_, buf) = index.read_exact_at(buf, pos as u64).await.unwrap();
        for (i, b) in buf.chunks_exact(I32_SIZE).enumerate() {
            c_table[i + 1] = i32::from_le_bytes(b.try_into().unwrap());
        }
    }
    let (_, buf) = rlb
        .read_to_end_at(Vec::with_capacity(CHUNK_SIZE), last_pos as u64)
        .await
        .unwrap();
    let mut iter = buf.iter().skip_while(|&&x| x.is_rl_tail());
    let Some(&ch) = iter.next() else {
        unreachable!("Empty run-length")
    };
    let mut rl = RunLength::new(ch, 0);
    iter.for_each(|&b| {
        if !rl.extend_byte(b) {
            c_table[rl.map_char() + 1] += rl.len;
            rl = RunLength::new(b, 0);
        }
    });
    c_table[rl.map_char() + 1] += rl.len;

    c_table.iter_mut().fold(0, |acc, x| {
        *x += acc;
        *x
    });
    c_table
}

impl Context {
    pub fn nth_char_pos(&self, nth: i32, ch: u8) -> i32 {
        self.c_table[ch.map_char()] + nth
    }

    pub fn find_checkpoint(&self, pos: i32) -> usize {
        self.positions.binary_search(&pos).unwrap_or_else(|x| x - 1)
    }

    pub async fn occ_fn(&self, ch: u8, pos: i32) -> i32 {
        let nearest_cp = self.find_checkpoint(pos);
        let pos_bwt = self.positions[nearest_cp];
        let pos_rlb = nearest_cp * CHUNK_SIZE;
        let cp = self.read_cp(nearest_cp).await;
        let occ = cp.occ;
        if pos_bwt == pos {
            return occ[ch.map_char()];
        }
        let buf = if ((pos - pos_bwt + 9) as usize) < CHUNK_SIZE + 4 {
            BytesMut::zeroed((pos - pos_bwt + 9) as usize)
        } else {
            BytesMut::zeroed(CHUNK_SIZE + 4)
        };
        let (n, buf) = self.rlb.read_at(buf, pos_rlb as u64).await.unwrap();
        let mut iter = buf
            .iter()
            .take(n)
            .skip_while(|x| x.is_rl_tail())
            .chain(once(&0));
        let b = iter.next().unwrap();
        let mut rl = RunLength::new(*b, pos_bwt);
        let mut occ = occ[ch.map_char()];
        for &b in iter {
            if !rl.extend_byte(b) {
                match (unlikely(rl.char == ch), unlikely(rl.pos + rl.len > pos)) {
                    (false, false) => {}
                    (true, false) => occ += rl.len,
                    (false, true) => return occ,
                    (true, true) => return occ + pos - rl.pos,
                }
                rl = RunLength::new(b, rl.pos + rl.len);
            }
        }
        unreachable!("Position not found");
    }

    async fn read_cp(&self, nearest_cp: usize) -> Checkpoint {
        match (self.index.as_ref(), nearest_cp) {
            (None, _) => Checkpoint::default(),
            (_, 0) => Checkpoint::default(),
            (Some(index), _) => {
                let read_pos = self.cps * I32_SIZE + (nearest_cp - 1) * OOC_TABLE_SIZE;
                let buf = BytesMut::zeroed(ALPHABETS * I32_SIZE);
                let (_, buf) = index.read_exact_at(buf, read_pos as u64).await.unwrap();
                let mut cp = Checkpoint::default();
                buf.chunks_exact(I32_SIZE)
                    .enumerate()
                    .for_each(|(i, b)| cp.occ[i] = i32::from_le_bytes(b.try_into().unwrap()));
                cp
            }
        }
    }

    pub async fn decode(&self, pos: i32) -> RunLength {
        let nearest_cp = self.find_checkpoint(pos);
        let pos_bwt = self.positions[nearest_cp];
        let pos_rlb = nearest_cp * CHUNK_SIZE;

        let cp = self.read_cp(nearest_cp).await;
        let mut occ = cp.occ;
        let buf = if ((pos - pos_bwt + 9) as usize) < CHUNK_SIZE + 4 {
            BytesMut::zeroed((pos - pos_bwt + 9) as usize)
        } else {
            BytesMut::zeroed(CHUNK_SIZE + 4)
        };
        let (n, buf) = self.rlb.read_at(buf, pos_rlb as u64).await.unwrap();
        let mut iter = buf
            .iter()
            .take(n)
            .skip_while(|x| x.is_rl_tail())
            .chain(once(&0));
        let b = iter.next().unwrap();
        let mut rl = RunLength::new(*b, pos_bwt);
        for &b in iter {
            if !rl.extend_byte(b) {
                if unlikely(rl.pos + rl.len > pos) {
                    rl.rank = rl.occ(&occ) + pos - rl.pos;
                    return rl;
                }
                rl.update_occ(&mut occ);
                rl = RunLength::new(b, rl.pos + rl.len);
            }
        }
        unreachable!("Position not found");
    }
}

type OccTable = [i32; ALPHABETS];

struct Checkpoint {
    pub occ: OccTable,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            occ: [0; ALPHABETS],
        }
    }
}
