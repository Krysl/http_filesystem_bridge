use log::debug;
use std::{
    borrow::Borrow,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    time::SystemTime,
};

use crate::fs::metadata::{AltStream, Stat};

use super::super::entry::{Entry, EntryNameRef};

#[allow(unused)]
#[derive(Debug)]
pub struct EntryHandle {
    pub index: u64,
    pub entry: Arc<Entry>,
    pub alt_stream: RwLock<Option<Arc<RwLock<AltStream>>>>,
    pub delete_on_close: bool,
    pub mtime_delayed: Mutex<Option<SystemTime>>,
    pub atime_delayed: Mutex<Option<SystemTime>>,
    pub ctime_enabled: AtomicBool,
    pub mtime_enabled: AtomicBool,
    pub atime_enabled: AtomicBool,
}

// static mut INDEX: u32 = 0;
impl EntryHandle {
    pub fn new(
        index: u64,
        entry: Arc<Entry>,
        alt_stream: Option<Arc<RwLock<AltStream>>>,
        delete_on_close: bool,
    ) -> Self {
        entry.stat().write().unwrap().handle_count += 1;
        if let Some(s) = &alt_stream {
            s.write().unwrap().handle_count += 1;
        }
        debug!("EntryHandle::new: handle index={index}");
        Self {
            index: index,
            entry,
            alt_stream: RwLock::new(alt_stream),
            delete_on_close,
            mtime_delayed: Mutex::new(None),
            atime_delayed: Mutex::new(None),
            ctime_enabled: AtomicBool::new(true),
            mtime_enabled: AtomicBool::new(true),
            atime_enabled: AtomicBool::new(true),
        }
    }

    pub fn is_dir(&self) -> bool {
        if self.alt_stream.read().unwrap().is_some() {
            false
        } else {
            self.entry.is_dir()
        }
    }

    pub fn update_atime(&self, stat: &mut Stat, atime: SystemTime) {
        if self.atime_enabled.load(Ordering::Relaxed) {
            stat.atime = atime;
        }
    }

    #[allow(unused)]
    pub fn update_mtime(&self, stat: &mut Stat, mtime: SystemTime) {
        self.update_atime(stat, mtime);
        if self.mtime_enabled.load(Ordering::Relaxed) {
            stat.mtime = mtime;
        }
    }
}

impl Drop for EntryHandle {
    fn drop(&mut self) {
        // The read lock on stat will be released before locking parent. This avoids possible deadlocks with
        // create_file.
        let parent = self.entry.stat().read().unwrap().parent.upgrade();
        // Lock parent before checking. This avoids racing with create_file.
        let parent_children = parent.as_ref().map(|p| p.children.write().unwrap());
        let mut stat = self.entry.stat().write().unwrap();
        if self.delete_on_close && self.alt_stream.read().unwrap().is_none() {
            stat.delete_pending = true;
        }
        stat.handle_count -= 1;
        if stat.delete_pending && stat.handle_count == 0 {
            // The result of upgrade() can be safely unwrapped here because the root directory is the only case when the
            // reference can be null, which has been handled in delete_directory.
            parent
                .as_ref()
                .unwrap()
                .stat
                .write()
                .unwrap()
                .update_mtime(SystemTime::now());
            let mut parent_children = parent_children.unwrap();
            let key = parent_children
                .iter()
                .find_map(|(k, v)| if &self.entry == v { Some(k) } else { None })
                .unwrap()
                .clone();
            parent_children
                .remove(Borrow::<EntryNameRef>::borrow(&key))
                .unwrap();
        } else {
            // Ignore root directory.
            stat.delete_pending = false
        }
        let alt_stream = self.alt_stream.read().unwrap();
        if let Some(stream) = alt_stream.as_ref() {
            stat.mtime = SystemTime::now();
            let mut stream_locked = stream.write().unwrap();
            if self.delete_on_close {
                stream_locked.delete_pending = true;
            }
            stream_locked.handle_count -= 1;
            if stream_locked.delete_pending && stream_locked.handle_count == 0 {
                let key = stat
                    .alt_streams
                    .iter()
                    .find_map(|(k, v)| {
                        if Arc::ptr_eq(stream, v) {
                            Some(k)
                        } else {
                            None
                        }
                    })
                    .unwrap()
                    .clone();
                stat.alt_streams
                    .remove(Borrow::<EntryNameRef>::borrow(&key))
                    .unwrap();
                self.update_atime(&mut stat, SystemTime::now());
            }
        }
    }
}
