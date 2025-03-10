use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

use crate::{ALPHABETS, CHECKPOINT_LEN, Context, I32_SIZE, PIECE_LEN, TryReadExact};

pub const fn map_char(c: u8) -> u8 {
    match c {
        9 => 0,
        10 => 1,
        13 => 2,
        _ => c - 29,
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

    fn map_char(&self) -> u8 {
        map_char(self.char)
    }

    fn set_rank(&mut self, rank: i32) {
        self.rank = rank;
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
        occ[map_char(self.char) as usize] += self.len;
    }
}

pub fn gen_index(rlb: &mut File, index: &mut File, checkpoints: usize) -> Vec<i32> {
    rlb.rewind().unwrap();
    index
        .seek(SeekFrom::Start((I32_SIZE * checkpoints) as u64))
        .unwrap();
    let mut positions: Vec<i32> = Vec::with_capacity(checkpoints + 1);
    positions.push(0);
    let mut cur_pos = 0;
    let mut occ = [0i32; ALPHABETS];
    let mut buf = [0u8; CHECKPOINT_LEN + 4];
    while let Ok(n) = rlb.try_read_exact(&mut buf) {
        if n < CHECKPOINT_LEN {
            break;
        }
        let mut iter = buf
            .iter()
            .take(CHECKPOINT_LEN)
            .chain(
                buf.iter()
                    .skip(CHECKPOINT_LEN)
                    .take_while(|&&x| x.is_rl_tail()),
            )
            .skip_while(|&x| x.is_rl_tail());

        let mut rl = RunLength::new(iter.next().unwrap().to_owned(), cur_pos);
        iter.for_each(|&b| {
            if b.is_rl_tail() {
                rl.extend_byte(b);
            } else {
                rl.set_rank(occ[rl.map_char() as usize]);
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
        rlb.seek_relative(CHECKPOINT_LEN as i64 - n as i64).unwrap();
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
    rlb: &mut File,
    index: Option<&mut File>,
    checkpoints: usize,
) -> [i32; ALPHABETS + 1] {
    let last_pos = checkpoints * CHECKPOINT_LEN;
    let mut c_table = [0; ALPHABETS + 1];
    if let Some(index) = index {
        index
            .seek(SeekFrom::End(-((ALPHABETS * I32_SIZE) as i64)))
            .unwrap();
        let mut buf = [0u8; ALPHABETS * I32_SIZE];
        index.read_exact(&mut buf).unwrap();
        for (i, b) in buf.chunks_exact(I32_SIZE).enumerate() {
            c_table[i + 1] = i32::from_le_bytes(b.try_into().unwrap());
        }
    }
    rlb.seek(SeekFrom::Start(last_pos as u64)).unwrap();
    let mut buf = Vec::with_capacity(CHECKPOINT_LEN);
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
            c_table[map_char(rl.char) as usize + 1] += rl.len;
            rl = RunLength::new(b, 0);
        }
    });
    c_table[map_char(rl.char) as usize + 1] += rl.len;

    c_table.iter_mut().fold(0, |acc, x| {
        *x += acc;
        *x
    });
    c_table
}

impl Context {
    pub fn nth_char_pos(&self, nth: i32, ch: u8) -> i32 {
        self.c_table[map_char(ch) as usize] + nth
    }

    pub fn find_checkpoint(&self, pos: i32) -> usize {
        self.positions.binary_search(&pos).unwrap_or_else(|x| x - 1)
    }

    pub fn occ_fn(&mut self, ch: u8, pos: i32) -> i32 {
        let nearest_cp = self.find_checkpoint(pos);
        let mut pos_bwt = self.positions[nearest_cp];
        let pos_rlb = nearest_cp * CHECKPOINT_LEN;
        let mut occ = [0i32; ALPHABETS];
        if nearest_cp > 0 {
            let index = self.index.as_mut().unwrap();
            index
                .seek(SeekFrom::Start(
                    (self.cps * I32_SIZE + (nearest_cp - 1) * PIECE_LEN) as u64,
                ))
                .unwrap();
            let mut buf = [0u8; ALPHABETS * I32_SIZE];
            index.read_exact(&mut buf).unwrap();
            buf.chunks_exact(I32_SIZE)
                .enumerate()
                .for_each(|(i, b)| occ[i] = i32::from_le_bytes(b.try_into().unwrap()));
        }
        if pos_bwt == pos {
            return occ[map_char(ch) as usize];
        }
        self.rlb.seek(SeekFrom::Start(pos_rlb as u64)).unwrap();
        let mut buf = [0u8; CHECKPOINT_LEN + 4];
        let buf = if ((pos - pos_bwt + 9) as usize) < buf.len() {
            &mut buf[..(pos - pos_bwt + 9) as usize]
        } else {
            &mut buf
        };
        let n = self.rlb.try_read_exact(buf).unwrap();
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
                    return occ[map_char(ch) as usize] + addition;
                }
                pos_bwt += rl.len;
                rl.update_occ(&mut occ);
                rl = RunLength::new(b, 0);
            }
        }

        let addition = if rl.char == ch { pos - pos_bwt } else { 0 };
        occ[map_char(ch) as usize] + addition
    }

    fn read_cp(&mut self, nearest_cp: usize) -> Checkpoint {
        match (&mut self.index, nearest_cp) {
            (None, _) => Checkpoint::default(),
            (_, 0) => Checkpoint::default(),
            (Some(index), _) => {
                let read_pos = self.cps * I32_SIZE + (nearest_cp - 1) * PIECE_LEN;
                index.seek(SeekFrom::Start(read_pos as u64)).unwrap();
                let mut buf = [0u8; ALPHABETS * I32_SIZE];
                index.read_exact(&mut buf).unwrap();
                let mut occ = [0i32; ALPHABETS];
                buf.chunks_exact(I32_SIZE)
                    .enumerate()
                    .for_each(|(i, b)| occ[i] = i32::from_le_bytes(b.try_into().unwrap()));
                Checkpoint::new(occ)
            }
        }
    }

    pub fn decode(&mut self, pos: i32) -> RunLength {
        let nearest_cp = self.find_checkpoint(pos);
        let mut pos_bwt = self.positions[nearest_cp];
        let pos_rlb = nearest_cp * CHECKPOINT_LEN;

        let cp = self.read_cp(nearest_cp);
        let mut occ = cp.occ;
        self.rlb.seek(SeekFrom::Start(pos_rlb as u64)).unwrap();
        let mut buf = [0u8; CHECKPOINT_LEN + 4];

        let buf = if ((pos - pos_bwt + 9) as usize) < buf.len() {
            &mut buf[..(pos - pos_bwt + 9) as usize]
        } else {
            &mut buf
        };
        let n = self.rlb.try_read_exact(buf).unwrap();
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
                    rl.set_rank(occ[map_char(rl.char) as usize] + pos - pos_bwt);
                    rl.pos = pos_bwt;
                    return rl;
                }
                pos_bwt += rl.len;
                rl.update_occ(&mut occ);
                rl = RunLength::new(b, 0);
            }
        }
        if rl.size > 0 {
            rl.set_rank(occ[map_char(rl.char) as usize] + pos - pos_bwt);
        } else {
            rl.set_rank(occ[map_char(rl.char) as usize]);
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
