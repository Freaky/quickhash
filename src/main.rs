
use std::io::Write;
use siphasher::sip128::{SipHasher, Hasher128};

struct HashWriter<T> {
    hash: T,
    len: u64
}

struct HashResult {
    hash: u128,
    len: u64,
}

impl<T> HashWriter<T> where T: Hasher128 {
    fn new(hash: T) -> Self {
        Self {
            hash,
            len: 0,
        }
    }

    fn close(self) -> HashResult {
        HashResult {
            hash: self.hash.finish128().into(),
            len: self.len
        }
    }
}

impl<T> Write for HashWriter<T> where T: std::hash::Hasher  {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.len += bytes.len() as u64;
        self.hash.write(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn main() {
    let mut exitcode = 0;
    let mut hasher = HashWriter::new(SipHasher::new());

    if let Err(e) = std::io::copy(&mut std::io::stdin(), &mut hasher) {
        eprintln!("{}", e);
        exitcode = 1;
    }

    let result = hasher.close();

    println!("SIP128 = {:x}", result.hash);
    println!("SIZE = {}", result.len);
    std::process::exit(exitcode);
}
