use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::fs::metadata::Stat;

use super::EntryName;

#[derive(Debug)]
pub enum Entry {
    File(Arc<FileEntry>),
    Directory(Arc<DirEntry>),
}

impl Entry {
    pub fn stat(&self) -> &RwLock<Stat> {
        match self {
            Entry::File(file) => &file.stat,
            Entry::Directory(dir) => &dir.stat,
        }
    }

    pub fn is_dir(&self) -> bool {
        match self {
            Entry::File(_) => false,
            Entry::Directory(_) => true,
        }
    }
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Entry::File(file) => {
                if let Entry::File(other_file) = other {
                    Arc::ptr_eq(file, other_file)
                } else {
                    false
                }
            }
            Entry::Directory(dir) => {
                if let Entry::Directory(other_dir) = other {
                    Arc::ptr_eq(dir, other_dir)
                } else {
                    false
                }
            }
        }
    }
}

impl Eq for Entry {}

impl Clone for Entry {
    fn clone(&self) -> Self {
        match self {
            Entry::File(file) => Entry::File(Arc::clone(file)),
            Entry::Directory(dir) => Entry::Directory(Arc::clone(dir)),
        }
    }
}

#[derive(Debug)]
pub struct FileEntry {
    pub stat: RwLock<Stat>,
    pub data: RwLock<Vec<u8>>,
}

impl FileEntry {
    pub fn new(stat: Stat) -> Self {
        Self {
            stat: RwLock::new(stat),
            data: RwLock::new(Vec::new()),
        }
    }
}

// The compiler incorrectly believes that its usage in a public function of the private path module is public.
#[derive(Debug)]
pub struct DirEntry {
    pub stat: RwLock<Stat>,
    pub children: RwLock<HashMap<EntryName, Entry>>,
}

impl DirEntry {
    pub fn new(stat: Stat) -> Self {
        Self {
            stat: RwLock::new(stat),
            children: RwLock::new(HashMap::new()),
        }
    }
}
