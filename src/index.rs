use std::{
    collections::BinaryHeap,
    fs::File,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    vec,
};

use crate::{ALPHABETS, CHEATS, CHECKPOINT_LEN, Context, PIECE_LEN, TryReadExact, U32_SIZE};

pub fn map_char(c: u8) -> u8 {
    match c {
        9 => 0,
        10 => 1,
        13 => 2,
        _ => c - 29,
    }
}

fn msb(b: u8) -> u8 {
    b >> 7
}

#[derive(Debug, Clone, Copy)]
pub struct RunLength {
    pub char: u8,
    pub len: u32,
    size: u8,
    pub pos: u32,
    pub rank: u32,
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
    fn new(char: u8, pos: u32) -> Self {
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

    fn encode(&self) -> [u32; 3] {
        // len and pos is at most 28 bits
        let u64 = (self.char as u64) << 56 | (self.len as u64) << 28 | self.pos as u64;
        [(u64 >> 32) as u32, u64 as u32, self.rank]
    }

    fn decode(encoded: &[u8; 3 * U32_SIZE]) -> Self {
        let u64 = u64::from_le_bytes(encoded[..8].try_into().unwrap());
        let char = (u64 >> 56) as u8;
        let len = (u64 >> 28) as u32 & 0x0fff_ffff;
        let pos = u64 as u32 & 0x0fff_ffff;
        let rank = u32::from_le_bytes(encoded[8..].try_into().unwrap());
        Self {
            char,
            len,
            size: 0,
            pos,
            rank,
        }
    }

    fn set_rank(&mut self, rank: u32) {
        self.rank = rank;
    }

    fn extend_byte(&mut self, b: u8) {
        let b = b & 0x7f;
        if self.size == 0 {
            self.len += 2 + (b as u32);
        } else {
            self.len += (b as u32) << (7 * self.size);
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
) -> Vec<u32> {
    rlb.rewind().unwrap();
    index
        .seek(SeekFrom::Start((U32_SIZE * checkpoints) as u64))
        .unwrap();
    let mut positions: Vec<u32> = vec![0; checkpoints + 1];
    let positions_strip = &mut positions[1..];
    let mut cur_pos = 0;
    let mut cps = 0;
    let mut occ = [0u32; ALPHABETS];
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
                    .take_while(|x| msb(**x) != 0),
            )
            .skip_while(|x| msb(**x) != 0);

        let mut rl = RunLength::new(iter.next().unwrap().to_owned(), cur_pos);
        let mut pq = BinaryHeap::new();
        iter.for_each(|&b| {
            if msb(b) != 0 {
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

        positions_strip[cps] = cur_pos;
        // keep top CHEATS run-lengths
        let mut top_rl = pq.into_iter().take(CHEATS).collect::<Vec<_>>();
        while top_rl.len() < CHEATS {
            top_rl.push(RunLength::default());
        }
        let mut cheat_sheet = [0u32; CHEATS * 3];
        for (i, rl) in top_rl.into_iter().enumerate() {
            let encoded = rl.encode();
            cheat_sheet[i * 3] = encoded[0];
            cheat_sheet[i * 3 + 1] = encoded[1];
            cheat_sheet[i * 3 + 2] = encoded[2];
        }
        let cheat_sheet = cheat_sheet
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        index.write_all(&cheat_sheet).unwrap();
        let occ = occ
            .into_iter()
            .flat_map(u32::to_le_bytes)
            .collect::<Vec<_>>();
        index.write_all(&occ).unwrap();
        cps += 1;
        rlb.seek_relative(CHECKPOINT_LEN as i64 - n as i64).unwrap();
    }

    index.seek(SeekFrom::Start(0)).unwrap();
    let positions_strip = positions_strip
        .iter()
        .copied()
        .flat_map(u32::to_le_bytes)
        .collect::<Vec<_>>();
    index.write_all(&positions_strip).unwrap();
    return positions;
}

pub fn gen_c_table(
    rlb: &mut BufReader<File>,
    index: Option<&mut BufReader<File>>,
    checkpoints: usize,
) -> [u32; ALPHABETS + 1] {
    let last_pos = checkpoints * CHECKPOINT_LEN;
    let mut c_table = [0; ALPHABETS + 1];
    if let Some(index) = index {
        index
            .seek(SeekFrom::End((ALPHABETS * U32_SIZE) as i64 * -1))
            .unwrap();
        let mut buf = [0u8; ALPHABETS * U32_SIZE];
        index.read_exact(&mut buf).unwrap();
        for (i, b) in buf.chunks_exact(U32_SIZE).enumerate() {
            c_table[i + 1] = u32::from_le_bytes(b.try_into().unwrap());
        }
    }
    rlb.seek(SeekFrom::Start(last_pos as u64)).unwrap();
    let mut buf = Vec::with_capacity(CHECKPOINT_LEN);
    rlb.read_to_end(&mut buf).unwrap();
    let mut iter = buf.iter().skip_while(|x| msb(**x) != 0);
    if let Some(&ch) = iter.next() {
        let mut rl = RunLength::new(ch, 0);
        iter.for_each(|&b| {
            if msb(b) != 0 {
                rl.extend_byte(b);
            } else {
                c_table[map_char(rl.char) as usize + 1] = rl.len;
                rl = RunLength::new(b, 0);
            }
        });
        c_table[map_char(rl.char) as usize + 1] = rl.len;
    }
    c_table.iter_mut().fold(0, |acc, x| {
        *x += acc;
        *x
    });
    c_table
}

impl Context {
    pub fn nth_char_pos(&self, nth: u32, ch: u8) -> u32 {
        self.c_table[map_char(ch) as usize] + nth
    }

    pub fn find_checkpoint(&self, pos: u32) -> usize {
        self.positions.binary_search(&pos).unwrap_or_else(|x| x)
    }

    pub fn occ_fn(&mut self, ch: u8, pos: u32) -> u32 {
        let nearest_cp = self.find_checkpoint(pos);
        let mut pos_bwt = self.positions[nearest_cp];
        let pos_rlb = nearest_cp * CHECKPOINT_LEN;
        let mut occ = [0u32; ALPHABETS];
        if nearest_cp > 0 {
            let index = self.index.as_mut().unwrap();
            index
                .seek(SeekFrom::Start(
                    ((self.cps + CHEATS * 3) * U32_SIZE + (nearest_cp - 1) * PIECE_LEN) as u64,
                ))
                .unwrap();
            let mut buf = [0u8; ALPHABETS * U32_SIZE];
            index.read_exact(&mut buf).unwrap();
            buf.chunks_exact(U32_SIZE)
                .enumerate()
                .for_each(|(i, b)| occ[i] = u32::from_le_bytes(b.try_into().unwrap()));
        }
        if pos_bwt == pos {
            return occ[map_char(ch) as usize];
        }
        self.rlb.seek(SeekFrom::Start(pos_rlb as u64)).unwrap();
        let mut buf = [0u8; CHECKPOINT_LEN + 4];
        let n = self.rlb.try_read_exact(&mut buf).unwrap();
        let mut iter = buf.into_iter().take(n).skip_while(|x| msb(*x) != 0);
        if let Some(ch) = iter.next() {
            let mut rl = RunLength::new(ch, 0);
            for b in iter {
                if msb(b) != 0 {
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
        } else {
            unreachable!("Empty run-length")
        }
    }

    fn read_cp(&mut self, nearest_cp: usize) -> Checkpoint {
        match self.index {
            None => Checkpoint::Tail([0; ALPHABETS]),
            Some(ref mut index) => {
                if nearest_cp == 0 {
                    let mut buf = [0u8; CHEATS * 3 * U32_SIZE];
                    index.read_exact(&mut buf).unwrap();
                    let mut cheat_sheet = [RunLength::default(); CHEATS];
                    buf.chunks_exact(U32_SIZE * 3)
                        .enumerate()
                        .for_each(|(i, chunk)| {
                            cheat_sheet[i] = RunLength::decode(chunk.try_into().unwrap());
                        });
                    return Checkpoint::Body(CheatSheet(cheat_sheet), [0; ALPHABETS]);
                }
                let read_pos = (self.cps + CHEATS * 3) * U32_SIZE + (nearest_cp - 1) * PIECE_LEN;
                index.seek(SeekFrom::Start(read_pos as u64)).unwrap();
                if nearest_cp == self.cps {
                    let mut buf = [0u8; ALPHABETS * U32_SIZE];
                    index.read_exact(&mut buf).unwrap();
                    let mut occ = [0u32; ALPHABETS];
                    buf.chunks_exact(U32_SIZE)
                        .enumerate()
                        .for_each(|(i, b)| occ[i] = u32::from_le_bytes(b.try_into().unwrap()));
                    Checkpoint::Tail(occ)
                } else {
                    let mut buf = [0u8; ALPHABETS * U32_SIZE + CHEATS * 3 * U32_SIZE];
                    index.read_exact(&mut buf).unwrap();
                    let mut cheat_sheet = [RunLength::default(); CHEATS];
                    buf.chunks_exact(U32_SIZE * 3)
                        .take(CHEATS)
                        .enumerate()
                        .for_each(|(i, chunk)| {
                            cheat_sheet[i] = RunLength::decode(chunk.try_into().unwrap());
                        });
                    let mut occ = [0u32; ALPHABETS];
                    buf.chunks_exact(U32_SIZE)
                        .skip(CHEATS * 3)
                        .enumerate()
                        .for_each(|(i, b)| {
                            occ[i] = u32::from_le_bytes(b.try_into().unwrap());
                        });
                    Checkpoint::Body(CheatSheet(cheat_sheet), occ)
                }
            }
        }
    }

    fn decode(&mut self, pos: u32) -> RunLength {
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
        let mut iter = buf.into_iter().take(n).skip_while(|x| msb(*x) != 0);
        if let Some(ch) = iter.next() {
            let mut rl = RunLength::new(ch, 0);
            for b in iter {
                if msb(b) != 0 {
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
        } else {
            unreachable!("Empty run-length")
        }
    }
}

struct CheatSheet([RunLength; CHEATS]);

impl CheatSheet {
    fn decode(encoded: &[u8; CHEATS * 3 * U32_SIZE]) -> Self {
        let iter = encoded.chunks_exact(3 * U32_SIZE);
        let mut cheat_sheet = [RunLength::default(); CHEATS];
        for (i, chunk) in iter.enumerate() {
            cheat_sheet[i] = RunLength::decode(chunk.try_into().unwrap());
        }
        Self(cheat_sheet)
    }

    fn cheat(&self, pos: u32) -> Option<RunLength> {
        self.0
            .iter()
            .find(|rl| rl.pos <= pos && pos < rl.pos + rl.len)
            .copied()
    }
}

type OccTable = [u32; ALPHABETS];

enum Checkpoint {
    Body(CheatSheet, OccTable),
    Tail(OccTable),
}
