use std::hash::Hasher;
use std::collections::BinaryHeap;
use std::io::Read;
use std::io::Write;

use crossbeam_channel::bounded;
use crossbeam_utils::thread;
use num_cpus;
use siphasher::sip128::{Hasher128, SipHasher};

struct HashWriter<T> {
    hash: T,
    len: u64,
}

#[derive(Debug, Default)]
struct HashResult {
    hash: u128,
    len: u64,
}

impl HashResult {
    fn add(&mut self, other: HashResult) {
        self.hash ^= other.hash;
        self.len += other.len;
    }
}

impl<T> HashWriter<T>
where
    T: Hasher128,
{
    fn new(hash: T) -> Self {
        Self { hash, len: 0 }
    }

    fn close(self) -> HashResult {
        HashResult {
            hash: self.hash.finish128().into(),
            len: self.len,
        }
    }
}

impl<T> Write for HashWriter<T>
where
    T: std::hash::Hasher,
{
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.len += bytes.len() as u64;
        self.hash.write(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct Buf(u64, Vec<u8>);
struct HashResultIdx(u64, HashResult);

impl PartialEq for HashResultIdx {
    fn eq(&self, o: &Self) -> bool {
        o.0.eq(&self.0)
    }
}
impl Eq for HashResultIdx {}
impl PartialOrd for HashResultIdx {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        o.0.partial_cmp(&self.0)
    }
}
impl Ord for HashResultIdx {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        o.0.cmp(&self.0)
    }
}

const BUF_SIZE: usize = 1024 * 512;

fn main() {
    let mut exitcode = 0;
    let workers = num_cpus::get();

    let (full_buf_tx, full_buf_rx) = bounded::<Buf>(workers * 2);
    let (empty_buf_tx, empty_buf_rx) = bounded::<Buf>(workers * 2);
    let (results_tx, results_rx) = bounded::<HashResultIdx>(workers * 2);
    let (finish_tx, finish_rx) = bounded::<HashResult>(1);

    thread::scope(|s| {
        for _ in 0..workers {
            let _ = empty_buf_tx.send(Buf(0, Vec::with_capacity(BUF_SIZE)));
            let _ = empty_buf_tx.send(Buf(0, Vec::with_capacity(BUF_SIZE)));
            let empty_buf_tx = empty_buf_tx.clone();
            let full_buf_rx = full_buf_rx.clone();
            let results_tx = results_tx.clone();

            s.spawn(move |_| {
                for mut buf in full_buf_rx {
                    let mut hasher = HashWriter::new(SipHasher::new());
                    hasher.hash.write_u64(buf.0);
                    hasher.write_all(&buf.1[..]).expect("hash should not fail");
                    if results_tx
                        .send(HashResultIdx(buf.0, hasher.close()))
                        .is_err()
                    {
                        break;
                    }

                    buf.1.clear();
                    let _ = empty_buf_tx.send(buf);
                }
            });
        }
        drop(empty_buf_tx);
        drop(full_buf_rx);
        drop(results_tx);

        s.spawn(|_| {
            let mut results = BinaryHeap::new();
            let mut next = 0;

            let mut total = HashResult::default();

            for result in results_rx {
                results.push(result);

                while results.peek().map(|x| x.0) == Some(next) {
                    let HashResultIdx(_, hash) = results.pop().expect("binary heap pop");
                    next += 1;

                    total.add(hash);
                }
            }

            let _ = finish_tx.send(total);
        });

        let stdin = std::io::stdin();
        let mut input = stdin.lock();
        let mut block = 0;
        for mut buf in empty_buf_rx {
            match input.by_ref().take(BUF_SIZE as u64).read_to_end(&mut buf.1) {
                Ok(0) => {
                    break;
                }
                Ok(_) => {
                    buf.0 = block;
                    full_buf_tx.send(buf).expect("worker thread must live");
                    block += 1;
                }
                Err(e) => {
                    eprintln!("{}", e);
                    exitcode = 1;
                    break;
                }
            }
        }
        drop(full_buf_tx);
    })
    .expect("thread");

    let result = finish_rx.recv().expect("result");
    println!("SIP128/{} = {:x}", BUF_SIZE, result.hash);
    println!("LEN = {}", result.len);

    std::process::exit(exitcode);
}
