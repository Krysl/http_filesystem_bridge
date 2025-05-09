use crate::fs::metadata::Stat;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use super::EntryName;

#[derive(Debug)]
pub enum Entry {
    File(Arc<FileEntry>),
    HttpFile(Arc<HttpFileEntry>),
    Directory(Arc<DirEntry>),
}

impl Entry {
    pub fn stat(&self) -> &RwLock<Stat> {
        match self {
            Entry::File(file) => &file.stat,
            Entry::HttpFile(http_file) => &http_file.stat,
            Entry::Directory(dir) => &dir.stat,
        }
    }

    pub fn is_dir(&self) -> bool {
        match self {
            Entry::File(_) => false,
            Entry::HttpFile(_) => false,
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
            Entry::HttpFile(http_file) => {
                if let Entry::HttpFile(other_http_file) = other {
                    Arc::ptr_eq(http_file, other_http_file)
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
            Entry::HttpFile(http_file) => Entry::HttpFile(Arc::clone(http_file)),
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

#[derive(Debug)]
pub struct HttpFileEntry {
    pub stat: RwLock<Stat>,
    pub url: String,
    data_cache: RwLock<Option<Vec<u8>>>,
}

impl HttpFileEntry {
    pub fn new(stat: Stat, url: String) -> Self {
        Self {
            stat: RwLock::new(stat),
            url,
            data_cache: RwLock::new(None),
        }
    }
    pub fn update_data(&self) -> usize {
        let mut cache = self.data_cache.write().unwrap();
        if cache.is_none() {
            if let Ok(response) = reqwest::blocking::get(&self.url) {
                if let Ok(bytes) = response.bytes() {
                    let bytes = if self.url.ends_with("main_module.bootstrap.js") {
                        String::from_utf8_lossy(&bytes)
                            .replace(
                                "'$requireDigestsPath?entrypoint=main_module.bootstrap.js'",
                                "'$requireDigestsPath$entrypoint=main_module.bootstrap.js'",
                            )
                            .into_bytes()
                    } else {
                        bytes.to_vec()
                    };
                    *cache = Some(bytes);
                }
            }
        }
        cache.as_ref().map_or(0, |data| data.len())
    }
    pub fn data_len(&self) -> usize {
        self.data_cache
            .read()
            .unwrap()
            .as_ref()
            .map_or(0, |data| data.len())
    }
    pub fn get_data(&self) -> Option<Vec<u8>> {
        self.data_cache.read().unwrap().clone()
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
