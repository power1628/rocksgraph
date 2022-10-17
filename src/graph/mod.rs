///! graph storage on rocksdb
///! Key:
///!     label_id(u16) | vertex_id (u64) | page_id (u8)
///! Value : nei_size(u16) | flag (u8)| [ to_label_id(u16) | vertex_id(u64) ]
///!   flag : 0x01 on has next.
pub mod r#type;

pub mod graph;
pub mod tools;
