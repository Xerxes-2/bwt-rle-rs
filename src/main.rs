use bwt_rle_rs::{CHUNK_SIZE, Context, I32_SIZE, index::gen_index};
use compio::{
    buf::IoBuf,
    fs::{File, OpenOptions},
    io::AsyncReadAtExt,
};

#[compio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<String>>();
    if args.len() < 4 {
        eprintln!("Usage: {} <rlb_file> <index_file> <pattern>", args[0]);
        std::process::exit(1);
    }
    let rlb_name = &args[1];
    let index_name = &args[2];
    let rlb = OpenOptions::new().read(true).open(rlb_name).await.unwrap();
    let rlb_size = rlb.metadata().await.unwrap().len();
    let checkpoints = rlb_size as usize / CHUNK_SIZE;
    let positions: Vec<i32>;
    let index: Option<File>;
    if checkpoints > 0 {
        if let Ok(index_file) = OpenOptions::new().read(true).open(index_name).await {
            let p = vec![0u8; (checkpoints + 1) * I32_SIZE];
            let p_tail = p.slice(I32_SIZE..);
            index = Some(index_file);
            let (_, p) = index
                .as_ref()
                .unwrap()
                .read_exact_at(p_tail, 0)
                .await
                .unwrap();
            positions = p
                .as_inner()
                .chunks_exact(I32_SIZE)
                .map(|x| i32::from_le_bytes(x.try_into().unwrap()))
                .collect();
        } else {
            // Create index file
            let index_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(index_name)
                .await
                .unwrap();
            positions = gen_index(&rlb, &index_file, checkpoints).await;
            index = Some(index_file);
        }
    } else {
        positions = vec![0];
        index = None;
    }
    let mut pat = args[3].to_owned().into_bytes();
    pat.reverse();
    let ctx = Context::new(rlb, index, checkpoints, positions).await;
    ctx.search(&pat).await;
    println!("Async Driver: {:?}", compio::driver::DriverType::current());
}
