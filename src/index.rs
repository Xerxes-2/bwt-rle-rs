use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

use crate::{ALPHABETS, CHUNK_SIZE, Context, I32_SIZE, MyReadAt, OOC_TABLE_SIZE, TryReadExact};

pub const fn map_char(c: u8) -> usize {
    match c {
        9 => 0,
        10 => 1,
        13 => 2,
        _ => c as usize - 29,
    }
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

    fn extend_byte(&mut self, b: u8) {
        let b = b & 0x7f;
        if self.size == 0 {
            self.len += 2 + (b as i32);
        } else {
            self.len += (b as i32) << (7 * self.size);
        }
        self.size += 1;
    }

    fn update_occ(&self, occ: &mut OccTable) {
        occ[self.map_char()] += self.len;
    }
}

pub fn gen_index(mut rlb: &File, mut index: &File, checkpoints: usize) -> Vec<i32> {
    rlb.rewind().unwrap();
    let mut positions: Vec<i32> = Vec::with_capacity(checkpoints + 1);
    positions.push(0);
    let mut cur_pos = 0;
    let mut occ = [0i32; ALPHABETS];
    let mut buf = [0u8; CHUNK_SIZE + 4];
    index
        .seek(SeekFrom::Start((I32_SIZE * checkpoints) as u64))
        .unwrap();
    while let Ok(n) = rlb.try_read_exact(&mut buf) {
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
            if b.is_rl_tail() {
                rl.extend_byte(b);
            } else {
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
        index.write_all(&occ).unwrap();
        rlb.seek_relative(CHUNK_SIZE as i64 - n as i64).unwrap();
    }

    index.rewind().unwrap();
    let positions_raw = positions
        .iter()
        .skip(1)
        .copied()
        .flat_map(i32::to_le_bytes)
        .collect::<Vec<_>>();
    index.write_all(&positions_raw).unwrap();
    positions
}

pub fn gen_c_table(
    mut rlb: &File,
    index: Option<&File>,
    checkpoints: usize,
) -> [i32; ALPHABETS + 1] {
    let last_pos = checkpoints * CHUNK_SIZE;
    let mut c_table = [0; ALPHABETS + 1];
    if let Some(mut index) = index {
        let mut buf = [0u8; OOC_TABLE_SIZE];
        index.seek(SeekFrom::End(-(OOC_TABLE_SIZE as i64))).unwrap();
        index.read_exact(&mut buf).unwrap();
        for (i, b) in buf.chunks_exact(I32_SIZE).enumerate() {
            c_table[i + 1] = i32::from_le_bytes(b.try_into().unwrap());
        }
    }
    let mut buf = Vec::with_capacity(CHUNK_SIZE);
    rlb.seek(SeekFrom::Start(last_pos as u64)).unwrap();
    rlb.read_to_end(&mut buf).unwrap();
    let mut iter = buf.iter().skip_while(|&&x| x.is_rl_tail());
    let Some(&ch) = iter.next() else {
        unreachable!("Empty run-length")
    };
    let mut rl = RunLength::new(ch, 0);
    iter.for_each(|&b| {
        if b.is_rl_tail() {
            rl.extend_byte(b);
        } else {
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

    pub fn occ_fn(&self, ch: u8, pos: i32) -> i32 {
        let nearest_cp = self.find_checkpoint(pos);
        let mut pos_bwt = self.positions[nearest_cp];
        let pos_rlb = nearest_cp * CHUNK_SIZE;
        let cp = self.read_cp(nearest_cp);
        let mut occ = cp.occ;
        if pos_bwt == pos {
            return occ[ch.map_char()];
        }
        // let mut rlb = &self.rlb;
        let mut buf = [0u8; CHUNK_SIZE + 4];
        let buf = if ((pos - pos_bwt + 9) as usize) < buf.len() {
            &mut buf[..(pos - pos_bwt + 9) as usize]
        } else {
            &mut buf
        };
        let n = (&self.rlb).try_read_exact_at(buf, pos_rlb as u64).unwrap();
        let mut iter = buf.iter_mut().take(n).skip_while(|x| x.is_rl_tail());
        let Some(&mut b) = iter.next() else {
            unreachable!("Empty run-length")
        };
        let mut rl = RunLength::new(b, 0);
        for &mut b in iter {
            if b.is_rl_tail() {
                rl.extend_byte(b);
            } else {
                if pos_bwt + rl.len > pos {
                    let addition = if rl.char == ch { pos - pos_bwt } else { 0 };
                    return occ[ch.map_char()] + addition;
                }
                pos_bwt += rl.len;
                rl.update_occ(&mut occ);
                rl = RunLength::new(b, 0);
            }
        }

        let addition = if rl.char == ch { pos - pos_bwt } else { 0 };
        occ[ch.map_char()] + addition
    }

    fn read_cp(&self, nearest_cp: usize) -> Checkpoint {
        match (self.index.as_ref(), nearest_cp) {
            (None, _) => Checkpoint::default(),
            (_, 0) => Checkpoint::default(),
            (Some(index), _) => {
                let read_pos = self.cps * I32_SIZE + (nearest_cp - 1) * OOC_TABLE_SIZE;
                let mut buf = [0u8; OOC_TABLE_SIZE];
                index.my_read_exact_at(&mut buf, read_pos as u64).unwrap();
                let mut occ = [0i32; ALPHABETS];
                buf.chunks_exact(I32_SIZE)
                    .enumerate()
                    .for_each(|(i, b)| occ[i] = i32::from_le_bytes(b.try_into().unwrap()));
                Checkpoint::new(occ)
            }
        }
    }

    pub fn decode(&self, pos: i32) -> RunLength {
        let nearest_cp = self.find_checkpoint(pos);
        let mut pos_bwt = self.positions[nearest_cp];
        let pos_rlb = nearest_cp * CHUNK_SIZE;

        let cp = self.read_cp(nearest_cp);
        let mut occ = cp.occ;
        let mut buf = [0u8; CHUNK_SIZE + 4];

        let buf = if ((pos - pos_bwt + 9) as usize) < buf.len() {
            &mut buf[..(pos - pos_bwt + 9) as usize]
        } else {
            &mut buf
        };
        let n = (&self.rlb).try_read_exact_at(buf, pos_rlb as u64).unwrap();
        let mut iter = buf.iter_mut().take(n).skip_while(|x| x.is_rl_tail());
        let Some(&mut b) = iter.next() else {
            unreachable!("Empty run-length")
        };
        let mut rl = RunLength::new(b, 0);
        for &mut b in iter {
            if b.is_rl_tail() {
                rl.extend_byte(b);
            } else {
                if pos_bwt + rl.len > pos {
                    rl.rank = rl.occ(&occ) + pos - pos_bwt;
                    rl.pos = pos_bwt;
                    return rl;
                }
                pos_bwt += rl.len;
                rl.update_occ(&mut occ);
                rl = RunLength::new(b, 0);
            }
        }
        if rl.size > 0 {
            rl.rank = rl.occ(&occ) + pos - pos_bwt;
        } else {
            rl.rank = rl.occ(&occ);
        }
        rl.pos = pos_bwt;
        rl
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

impl Checkpoint {
    fn new(occ: OccTable) -> Self {
        Self { occ }
    }
}
