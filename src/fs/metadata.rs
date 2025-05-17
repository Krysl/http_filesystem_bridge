use std::{
    collections::HashMap,
    sync::{Arc, RwLock, Weak},
    time::SystemTime,
};

use winapi::um::winnt;

use super::entry::{DirEntry, EntryName};
use crate::security::SecurityDescriptor;

#[derive(Debug, serde::Serialize)]
pub struct AltStream {
    pub handle_count: u32,
    pub delete_pending: bool,
    pub data: Vec<u8>,
    pub content_length: u64,
    pub ctime: SystemTime,
}

impl AltStream {
    pub fn new() -> Self {
        Self {
            handle_count: 0,
            delete_pending: false,
            data: Vec::new(),
            content_length: 0,
            ctime: SystemTime::now(),
        }
    }
}
#[derive(Debug, Copy, Clone, Eq, PartialEq, serde::Serialize)]
pub struct Attributes {
    pub value: u32,
}

impl Attributes {
    pub fn new(attrs: u32) -> Self {
        const SUPPORTED_ATTRS: u32 = winnt::FILE_ATTRIBUTE_ARCHIVE
            | winnt::FILE_ATTRIBUTE_NORMAL
            | winnt::FILE_ATTRIBUTE_HIDDEN
            | winnt::FILE_ATTRIBUTE_NOT_CONTENT_INDEXED
            | winnt::FILE_ATTRIBUTE_OFFLINE
            | winnt::FILE_ATTRIBUTE_READONLY
            | winnt::FILE_ATTRIBUTE_SYSTEM
            | winnt::FILE_ATTRIBUTE_TEMPORARY;
        Self {
            value: attrs & SUPPORTED_ATTRS,
        }
    }

    pub fn get_output_attrs(&self, is_dir: bool) -> u32 {
        let mut attrs = self.value;
        if is_dir {
            attrs |= winnt::FILE_ATTRIBUTE_DIRECTORY;
        }
        if attrs == 0 {
            attrs = winnt::FILE_ATTRIBUTE_NORMAL
        }
        attrs
    }
}

#[derive(Debug)]
pub struct Stat {
    pub id: u64,
    pub attrs: Attributes,
    pub ctime: SystemTime,
    pub mtime: SystemTime,
    pub atime: SystemTime,
    pub sec_desc: SecurityDescriptor,
    pub handle_count: u32,
    pub delete_pending: bool,
    pub parent: Weak<DirEntry>,
    pub alt_streams: HashMap<EntryName, Arc<RwLock<AltStream>>>,
}

impl Stat {
    pub fn new(id: u64, attrs: u32, sec_desc: SecurityDescriptor, parent: Weak<DirEntry>) -> Self {
        let now = SystemTime::now();
        Self {
            id,
            attrs: Attributes::new(attrs),
            ctime: now,
            mtime: now,
            atime: now,
            sec_desc,
            handle_count: 0,
            delete_pending: false,
            parent,
            alt_streams: HashMap::new(),
        }
    }

    pub fn update_atime(&mut self, atime: SystemTime) {
        self.atime = atime;
    }

    pub fn update_mtime(&mut self, mtime: SystemTime) {
        self.update_atime(mtime);
        self.mtime = mtime;
    }
}
