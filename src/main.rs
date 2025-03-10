use bwt_rle_rs::{CHECKPOINT_LEN, I32_SIZE, index::gen_index};
use std::{
    fs::{File, OpenOptions},
    io::Read,
};
fn main() {
    let args = std::env::args().collect::<Vec<String>>();
    if args.len() < 4 {
        eprintln!("Usage: {} <rlb_file> <index_file> <pattern>", args[0]);
        std::process::exit(1);
    }
    let rlb_name = &args[1];
    let index_name = &args[2];
    let mut rlb = OpenOptions::new().read(true).open(rlb_name).unwrap();
    let rlb_size = rlb.metadata().unwrap().len();
    let checkpoints = rlb_size as usize / CHECKPOINT_LEN;
    let positions: Vec<i32>;
    let mut index: Option<File>;
    if checkpoints > 0 {
        if let Ok(index_file) = OpenOptions::new().read(true).open(index_name) {
            let mut p = vec![0u8; (checkpoints + 1) * I32_SIZE];
            let p_tail = &mut p[I32_SIZE..];
            index = Some(index_file);
            index.as_mut().unwrap().read_exact(p_tail).unwrap();
            positions = p
                .chunks_exact(I32_SIZE)
                .map(|x| i32::from_le_bytes(x.try_into().unwrap()))
                .collect();
        } else {
            // Create index file
            File::create(index_name).unwrap();
            let mut index_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(index_name)
                .unwrap();
            positions = gen_index(&mut rlb, &mut index_file, checkpoints);
            index = Some(index_file);
        }
    } else {
        positions = vec![0];
        index = None;
    }
    let mut pat = args[3].to_owned().into_bytes();
    pat.reverse();
    let mut ctx = bwt_rle_rs::Context::new(rlb, index, checkpoints, positions);
    ctx.search(&pat);
}
