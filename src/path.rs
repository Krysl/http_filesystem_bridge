use std::{borrow::Borrow, sync::Arc};

use dokan::OperationResult;
use widestring::{U16CStr, U16Str, U16String};
use winapi::shared::ntstatus::*;

use crate::fs::{
    entry::{DirEntry, Entry, EntryName, EntryNameRef},
    handler::MemFsHandler,
};

// Use the same value as NTFS.
pub const MAX_COMPONENT_LENGTH: u32 = 255;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum StreamType {
    Data,
    IndexAllocation,
    Bitmap,
}

#[derive(Debug)]
pub struct StreamInfo<'a> {
    pub name: &'a U16Str,
    pub type_: StreamType,
}

impl StreamInfo<'_> {
    pub fn check_default(&self, is_dir: bool) -> OperationResult<bool> {
        if is_dir {
            if self.name.is_empty()
                || EntryNameRef::new(self.name) == EntryName(U16String::from_str("$I30")).borrow()
            {
                if self.type_ == StreamType::IndexAllocation {
                    Ok(true)
                } else {
                    Err(STATUS_OBJECT_NAME_INVALID)
                }
            } else if self.type_ == StreamType::Data {
                Ok(false)
            } else {
                Err(STATUS_OBJECT_NAME_INVALID)
            }
        } else if self.type_ == StreamType::Data {
            Ok(self.name.is_empty())
        } else {
            Err(STATUS_OBJECT_NAME_INVALID)
        }
    }
}

#[derive(Debug)]
pub struct FullName<'a> {
    pub file_name: &'a U16Str,
    pub stream_info: Option<StreamInfo<'a>>,
}

impl<'a> FullName<'a> {
    pub fn new(name: &'a U16Str) -> OperationResult<Self> {
        let name_slice = name.as_slice();
        if let Some(offset1) = name_slice.iter().position(|x| *x == ':' as u16) {
            let file_name = U16Str::from_slice(&name_slice[..offset1]);
            let stream_info = &name_slice[offset1 + 1..];
            if let Some(offset2) = stream_info.iter().position(|x| *x == ':' as u16) {
                let stream_type_str =
                    EntryNameRef::new(U16Str::from_slice(&stream_info[offset2 + 1..]));
                let stream_type = if stream_type_str
                    == EntryName(U16String::from_str("$DATA")).borrow()
                {
                    StreamType::Data
                } else if stream_type_str
                    == EntryName(U16String::from_str("$INDEX_ALLOCATION")).borrow()
                {
                    StreamType::IndexAllocation
                } else if stream_type_str == EntryName(U16String::from_str("$BITMAP")).borrow() {
                    StreamType::Bitmap
                } else {
                    return Err(STATUS_OBJECT_NAME_INVALID);
                };
                Ok(Self {
                    file_name,
                    stream_info: Some(StreamInfo {
                        name: U16Str::from_slice(&stream_info[..offset2]),
                        type_: stream_type,
                    }),
                })
            } else {
                Ok(Self {
                    file_name,
                    stream_info: Some(StreamInfo {
                        name: U16Str::from_slice(stream_info),
                        type_: StreamType::Data,
                    }),
                })
            }
        } else {
            Ok(Self {
                file_name: name,
                stream_info: None,
            })
        }
    }
}

fn find_dir_entry(
    index: u64,
    handler: &MemFsHandler,
    cur_entry: &Arc<DirEntry>,
    path: &[&U16Str],
) -> OperationResult<Arc<DirEntry>> {
    if let Some(name) = path.get(0) {
        if name.len() > MAX_COMPONENT_LENGTH as usize {
            return Err(STATUS_OBJECT_NAME_INVALID);
        }
        let children = cur_entry.children.read().unwrap();
        let child_entry = if let Some(a) = children.get(EntryNameRef::new(name)) {
            a
        } else {
            std::mem::drop(children);
            // let parent = cur_entry.stat.read().unwrap().parent.upgrade();
            // let key = if let Some(parent1) = parent {
            //     let parent_children = parent1.children.read().unwrap();
            //     log::debug!("parent_children = {:?}", parent_children);
            //     let ret = parent_children.iter().find_map(|(k, v)| {
            //         let dir_entry = match v.as_ref() {
            //             Entry::Directory(dir_entry) => dir_entry,
            //             _ => return None,
            //         };
            //         if Arc::ptr_eq(dir_entry, cur_entry) {
            //             Some(k)
            //         } else {
            //             None
            //         }
            //     });
            //     match ret {
            //         Some(name) => {
            //             let name_string = name.0.to_string().unwrap();
            //             // Store the string in a variable so it lives long enough
            //             Box::leak(name_string.into_boxed_str())
            //         }
            //         None => "",
            //     }
            // } else {
            //     "(can not find parent)"
            // };
            log::warn!(
                "find_dir_entry: {} not found",
                name.to_string_lossy(),
                // key // .to_string_lossy()
            );
            {
                let mut _children = cur_entry.children.write().unwrap();
                &handler.create_dir_entry(index, cur_entry, &mut _children, name.to_ustring())
            }
            // return Err(STATUS_OBJECT_PATH_NOT_FOUND);
        };
        match child_entry.as_ref() {
            Entry::Directory(dir) => find_dir_entry(index, handler, dir, &path[1..]),
            _ => Err(STATUS_OBJECT_PATH_NOT_FOUND),
        }
    } else {
        Ok(Arc::clone(cur_entry))
    }
}

pub fn split_path<'a>(
    index: u64,
    handler: &MemFsHandler,
    // root: &Arc<DirEntry>,
    path: &'a U16CStr,
) -> OperationResult<Option<(FullName<'a>, Arc<DirEntry>)>> {
    let root = &handler.root;
    let path = path
        .as_slice()
        .split(|x| *x == '\\' as u16)
        .filter(|s| !s.is_empty())
        .map(|s| U16Str::from_slice(s))
        .collect::<Vec<_>>();
    if path.is_empty() {
        log::trace!("Empty path");
        Ok(None)
    } else {
        let name = *path.iter().last().unwrap();
        if name.len() > MAX_COMPONENT_LENGTH as usize {
            return Err(STATUS_OBJECT_NAME_INVALID);
        }
        Ok(Some((
            FullName::new(name)?,
            find_dir_entry(index, handler, root, &path[..path.len() - 1])?,
        )))
    }
}
