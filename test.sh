cargo build -r
sudo -v
hyperfine --warmup 5 --prepare 'sync; echo 3 | sudo tee /proc/sys/vm/drop_caches' 'target/release/bwt-rle-rs test/large2.rlb test/large2.idx '$1''
