use std::{
    collections::BinaryHeap,
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
};

use crate::{ALPHABETS, CHEATS, CHECKPOINT_LEN, Context, I32_SIZE, PIECE_LEN, TryReadExact};

pub const fn map_char(c: u8) -> u8 {
    match c {
        9 => 0,
        10 => 1,
        13 => 2,
        _ => c - 29,
    }
}

trait IsRunLength {
    fn is_rl_head(&self) -> bool;
    fn is_rl_tail(&self) -> bool;
}

const fn msb_u8(b: u8) -> bool {
    b & 0x80 != 0
}

impl IsRunLength for u8 {
    fn is_rl_head(&self) -> bool {
        !msb_u8(*self)
    }
    fn is_rl_tail(&self) -> bool {
        msb_u8(*self)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RunLength {
    pub char: u8,
    pub len: i32,
    size: u8,
    pub pos: i32,
    pub rank: i32,
}

impl Default for RunLength {
    fn default() -> Self {
        Self {
            char: 0,
            len: 0,
            size: 0,
            pos: 0,
            rank: 0,
        }
    }
}

impl RunLength {
    fn new(char: u8, pos: i32) -> Self {
        assert!(!char.is_rl_tail());
        assert!(pos >= 0);
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

    fn encode(&self) -> [u8; 3 * I32_SIZE] {
        // len and pos is at most 28 bits
        let u64 = (self.char as u64) << 56 | (self.len as u64) << 28 | self.pos as u64;
        let u64 = u64.to_le_bytes();
        let mut encoded = [0u8; 3 * I32_SIZE];
        encoded[..8].copy_from_slice(&u64);
        encoded[8..].copy_from_slice(&self.rank.to_le_bytes());
        encoded
    }

    fn decode(encoded: &[u8; 3 * I32_SIZE]) -> Self {
        let u64 = u64::from_le_bytes(encoded[..8].try_into().unwrap());
        let char = (u64 >> 56) as u8;
        let len = ((u64 >> 28) & 0x0fff_ffff) as i32;
        let pos = (u64 & 0x0fff_ffff) as i32;
        let rank = i32::from_le_bytes(encoded[8..].try_into().unwrap());
        assert!(!char.is_rl_tail());
        assert!(len > 0);
        assert!(pos >= 0);
        assert!(rank >= 0);
        Self {
            char,
            len,
            size: 0,
            pos,
            rank,
        }
    }

    fn set_rank(&mut self, rank: i32) {
        assert!(rank >= 0);
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

impl PartialEq for RunLength {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len
    }
}

impl Eq for RunLength {}

impl PartialOrd for RunLength {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.len.cmp(&other.len))
    }
}

impl Ord for RunLength {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.len.cmp(&other.len)
    }
}

pub fn gen_index(
    rlb: &mut BufReader<File>,
    index: &mut BufWriter<File>,
    checkpoints: usize,
) -> Vec<i32> {
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
                    .take_while(|&&x| x.is_rl_head()),
            )
            .skip_while(|&x| x.is_rl_tail());

        let mut rl = RunLength::new(iter.next().unwrap().to_owned(), cur_pos);
        let mut pq = BinaryHeap::new();
        iter.for_each(|&b| {
            if b.is_rl_tail() {
                rl.extend_byte(b);
            } else {
                rl.set_rank(occ[rl.map_char() as usize]);
                pq.push(rl);
                rl.update_occ(&mut occ);
                cur_pos += rl.len;
                rl = RunLength::new(b, cur_pos);
            }
        });
        rl.set_rank(occ[rl.map_char() as usize]);
        pq.push(rl);
        rl.update_occ(&mut occ);
        cur_pos += rl.len;

        positions.push(cur_pos);
        // keep top CHEATS run-lengths
        let cheat_sheet = CheatSheet::new(pq);
        let cheat_sheet = cheat_sheet.encode();

        index.write_all(&cheat_sheet).unwrap();
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
    return positions;
}

pub fn gen_c_table(
    rlb: &mut BufReader<File>,
    index: Option<&mut BufReader<File>>,
    checkpoints: usize,
) -> [i32; ALPHABETS + 1] {
    let last_pos = checkpoints * CHECKPOINT_LEN;
    let mut c_table = [0; ALPHABETS + 1];
    if let Some(index) = index {
        index
            .seek(SeekFrom::End((ALPHABETS * I32_SIZE) as i64 * -1))
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
                    ((self.cps + CHEATS * 3) * I32_SIZE + (nearest_cp - 1) * PIECE_LEN) as u64,
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
        let n = self.rlb.try_read_exact(&mut buf).unwrap();
        let mut iter = buf.into_iter().take(n).skip_while(|&x| x.is_rl_tail());
        let Some(b) = iter.next() else {
            unreachable!("Empty run-length")
        };
        let mut rl = RunLength::new(b, 0);
        for b in iter {
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
        return occ[map_char(ch) as usize] + addition;
    }

    fn read_cp(&mut self, nearest_cp: usize) -> Checkpoint {
        match self.index {
            None => Checkpoint::Tail([0; ALPHABETS]),
            Some(ref mut index) => {
                if nearest_cp == 0 {
                    let mut buf = [0u8; CHEATS * 3 * I32_SIZE];
                    index.rewind().unwrap();
                    index.read_exact(&mut buf).unwrap();
                    let cheat_sheet = CheatSheet::decode(&buf);
                    return Checkpoint::Body(cheat_sheet, [0; ALPHABETS]);
                }
                let read_pos = (self.cps + CHEATS * 3) * I32_SIZE + (nearest_cp - 1) * PIECE_LEN;
                index.seek(SeekFrom::Start(read_pos as u64)).unwrap();
                if nearest_cp == self.cps {
                    let mut buf = [0u8; ALPHABETS * I32_SIZE];
                    index.read_exact(&mut buf).unwrap();
                    let mut occ = [0i32; ALPHABETS];
                    buf.chunks_exact(I32_SIZE)
                        .enumerate()
                        .for_each(|(i, b)| occ[i] = i32::from_le_bytes(b.try_into().unwrap()));
                    Checkpoint::Tail(occ)
                } else {
                    let mut buf = [0u8; ALPHABETS * I32_SIZE + CHEATS * 3 * I32_SIZE];
                    index.read_exact(&mut buf).unwrap();
                    let cheat_sheet =
                        CheatSheet::decode(&buf[ALPHABETS * I32_SIZE..].try_into().unwrap());
                    let mut occ = [0i32; ALPHABETS];
                    buf.chunks_exact(I32_SIZE)
                        .take(ALPHABETS)
                        .enumerate()
                        .for_each(|(i, b)| {
                            occ[i] = i32::from_le_bytes(b.try_into().unwrap());
                        });
                    Checkpoint::Body(cheat_sheet, occ)
                }
            }
        }
    }

    pub fn decode(&mut self, pos: i32) -> RunLength {
        let nearest_cp = self.find_checkpoint(pos);
        let mut pos_bwt = self.positions[nearest_cp];
        let pos_rlb = nearest_cp * CHECKPOINT_LEN;

        let cp = self.read_cp(nearest_cp);
        if let Checkpoint::Body(ref cs, _) = cp {
            if let Some(mut rl) = cs.cheat(pos) {
                rl.set_rank(rl.rank + pos - rl.pos);
                return rl;
            }
        }
        let mut occ = match cp {
            Checkpoint::Body(_, occ) => occ,
            Checkpoint::Tail(occ) => occ,
        };
        self.rlb.seek(SeekFrom::Start(pos_rlb as u64)).unwrap();
        let mut buf = [0u8; CHECKPOINT_LEN + 4];
        let n = self.rlb.try_read_exact(&mut buf).unwrap();
        let mut iter = buf.into_iter().take(n).skip_while(|x| x.is_rl_tail());
        let Some(ch) = iter.next() else {
            unreachable!("Empty run-length")
        };
        let mut rl = RunLength::new(ch, 0);
        for b in iter {
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
        return rl;
    }
}

struct CheatSheet([RunLength; CHEATS]);

impl CheatSheet {
    fn new(pq: BinaryHeap<RunLength>) -> Self {
        let mut cheat_sheet = [RunLength::default(); CHEATS];
        for (i, rl) in pq.into_iter().take(CHEATS).enumerate() {
            cheat_sheet[i] = rl;
        }
        Self(cheat_sheet)
    }

    fn decode(encoded: &[u8; CHEATS * 3 * I32_SIZE]) -> Self {
        let iter = encoded.chunks_exact(3 * I32_SIZE);
        let mut cheat_sheet = [RunLength::default(); CHEATS];
        for (i, chunk) in iter.enumerate() {
            cheat_sheet[i] = RunLength::decode(chunk.try_into().unwrap());
        }
        Self(cheat_sheet)
    }

    fn encode(&self) -> [u8; CHEATS * 3 * I32_SIZE] {
        let mut encoded = [0u8; CHEATS * 3 * I32_SIZE];
        for (i, rl) in self.0.iter().enumerate() {
            let chunk = rl.encode();
            encoded[i * 3 * I32_SIZE..(i + 1) * 3 * I32_SIZE].copy_from_slice(&chunk);
        }
        encoded
    }

    fn cheat(&self, pos: i32) -> Option<RunLength> {
        self.0
            .iter()
            .find(|rl| rl.pos <= pos && pos < rl.pos + rl.len)
            .copied()
    }
}

type OccTable = [i32; ALPHABETS];

enum Checkpoint {
    Body(CheatSheet, OccTable),
    Tail(OccTable),
}
