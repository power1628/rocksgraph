use std::{
    borrow::BorrowMut,
    collections::VecDeque,
    fmt::DebugList,
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom},
    ops::Deref,
    sync::{Arc, Mutex},
};

use crate::graph::r#type::*;

pub(crate) fn shard_file_to_parts(
    fpath: String,
    shard_num: u64,
) -> std::result::Result<Vec<(u64, u64)>, std::io::Error> {
    // Calculate sharding info for this file
    let file = File::open(fpath.clone())?;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let bytes_per_shard = (file_len + shard_num - 1) / shard_num;
    let mut shard_offset = vec![];
    shard_offset.push(0u64);
    println!(
        "file {:?} size {:?} bytes_per_shard {:?}",
        fpath, file_len, bytes_per_shard
    );

    for i in 0..shard_num {
        let start = shard_offset[i as usize];
        let end = std::cmp::min(std::cmp::max(start, bytes_per_shard * (i + 1)), file_len);

        let offset = end - start;
        reader.seek_relative(offset as i64)?;
        let mut buf = Vec::new();
        let bytes_read = reader.read_until('\n' as u8, &mut buf)?;
        let end = start + offset + bytes_read as u64;
        if start == end {
            break;
        }
        shard_offset.push(end);
    }

    // show the splitting line
    {
        let file = File::open(fpath.clone())?;
        let mut reader = BufReader::new(file);

        for offset in shard_offset.iter() {
            reader.rewind()?;
            reader.seek_relative(*offset as i64)?;
            let mut buf = String::new();
            reader.read_line(&mut buf)?;
            println!("shard {} {:?}", offset, buf);
        }
    }

    let mut shards = vec![];
    for i in 0..shard_offset.len() - 1 as usize {
        if shard_offset[i] < shard_offset[i + 1] {
            shards.push((shard_offset[i], shard_offset[i + 1]));
        }
    }
    println!("shards: {:?}", shards);

    Ok(shards)
}

#[cfg(test)]
mod test {
    use std::{
        fs::File,
        io::{BufRead, BufReader, Seek, SeekFrom},
    };

    use super::shard_file_to_parts;

    #[test]
    fn test_seek() {
        let fpath = "./test.dat";
        let mut file = File::open(fpath).unwrap();
        file.seek(SeekFrom::Current(0)).unwrap();
        let mut reader = BufReader::new(file);
        let mut buffer = Vec::new();
        let bytes_read = reader.read_until('\n' as u8, &mut buffer).unwrap();
        //reader.read_line(&mut buffer).unwrap();
        println!("{:?} {:?}", String::from_utf8(buffer.clone()), bytes_read);
        reader.seek_relative(0).unwrap();
        buffer.clear();
        //        reader.seek(SeekFrom::Current(10)).unwrap();
        let bytes_read = reader.read_until('1' as u8, &mut buffer).unwrap();
        println!("{:?} {:?}", String::from_utf8(buffer.clone()), bytes_read);
    }

    #[test]
    fn test_sharding() {
        let shard_num = 100;
        let fpath = String::from("./test.dat");
        shard_file_to_parts(fpath, shard_num).unwrap();
    }
}
