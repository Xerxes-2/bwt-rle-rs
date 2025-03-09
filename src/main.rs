use bwt_rle_rs::{CHECKPOINT_LEN, U32_SIZE, index::gen_index};
use cap::Cap;
use std::{
    alloc,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Seek},
};

#[global_allocator]
#[cfg(debug_assertions)]
static ALLOCATOR: Cap<alloc::System> = Cap::new(alloc::System, usize::max_value());

fn main() {
    #[cfg(debug_assertions)]
    ALLOCATOR.set_limit(11 * 1024 * 1024).unwrap();
    let args = std::env::args().collect::<Vec<String>>();
    if args.len() < 4 {
        eprintln!("Usage: {} <rlb_file> <index_file> <pattern>", args[0]);
        std::process::exit(1);
    }
    let rlb_name = &args[1];
    let index_name = &args[2];
    let rlb = OpenOptions::new().read(true).open(rlb_name).unwrap();
    let rlb_size = rlb.metadata().unwrap().len();
    let mut rlb = BufReader::new(rlb);
    let checkpoints = rlb_size as usize / CHECKPOINT_LEN;
    let positions: Vec<u32>;
    let mut index: Option<BufReader<File>>;
    if checkpoints > 0 {
        if let Ok(index_file) = OpenOptions::new().read(true).open(index_name) {
            let mut p = vec![0u8; (checkpoints + 1) * U32_SIZE];
            let p_tail = &mut p[U32_SIZE..];
            index = Some(BufReader::new(index_file));
            index.as_mut().unwrap().read_exact(p_tail).unwrap();
            positions = p
                .chunks_exact(U32_SIZE)
                .map(|x| u32::from_le_bytes(x.try_into().unwrap()))
                .collect();
        } else {
            // Create index file
            File::create(index_name).unwrap();
            let index_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(index_name)
                .unwrap();
            let mut writer = BufWriter::new(index_file);
            positions = gen_index(&mut rlb, &mut writer, checkpoints);
            index = Some(BufReader::new(writer.into_inner().unwrap()));
        }
    } else {
        positions = vec![0];
        index = None;
    }
    let reverse = args[3].chars().map(|x| x as u8).collect::<Vec<_>>();
    let mut ctx = bwt_rle_rs::Context::new(rlb, index, checkpoints, positions);
}
