use std::{
    collections::HashMap,
    os::windows::io::AsRawHandle,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock, Weak,
    },
    time::SystemTime,
};

use dokan::{
    CreateFileInfo, DiskSpaceInfo, FileInfo, FileSystemHandler, FileTimeOperation, FillDataError,
    FillDataResult, FindData, FindStreamData, OperationInfo, OperationResult, VolumeInfo,
    IO_SECURITY_CONTEXT,
};
use url::Url;
use winapi::{
    shared::{ntdef, ntstatus::*},
    um::winnt,
};

use crate::{
    fs::{
        entry::{DirEntry, Entry, EntryName, FileEntry, HttpFileEntry},
        metadata::{AltStream, Stat},
    },
    path::{self, FullName, StreamInfo, StreamType},
    security::SecurityDescriptor,
    utils::access_flags_to_string,
};

use dokan_sys::win32::{
    FILE_CREATE, FILE_DELETE_ON_CLOSE, FILE_DIRECTORY_FILE, FILE_MAXIMUM_DISPOSITION,
    FILE_NON_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_IF, FILE_OVERWRITE, FILE_OVERWRITE_IF,
    FILE_SUPERSEDE,
};
use log::{error, info, trace, warn};
use widestring::{U16CStr, U16CString, U16Str, U16String};

use super::super::entry::EntryNameRef;
use super::super::metadata::Attributes;

use super::EntryHandle;

#[derive(Debug)]
pub struct MemFsHandler {
    pub url: Url,
    pub id_counter: AtomicU64,
    pub root: Arc<DirEntry>,
    pub files: HashMap<EntryName, Entry>,
}

impl MemFsHandler {
    pub fn new(url: Url) -> Self {
        Self {
            url: url.clone(),
            id_counter: AtomicU64::new(1),
            root: Arc::new(DirEntry::new(Stat::new(
                0,
                0,
                SecurityDescriptor::new_default().unwrap(),
                Weak::new(),
            ))),
            files: HashMap::new(),
        }
    }

    fn next_id(&self) -> u64 {
        self.id_counter.fetch_add(1, Ordering::Relaxed)
    }

    pub fn create_new(
        &self,
        name: &FullName,
        attrs: u32,
        delete_on_close: bool,
        creator_desc: winnt::PSECURITY_DESCRIPTOR,
        token: ntdef::HANDLE,
        parent: &Arc<DirEntry>,
        // children: &mut HashMap<EntryName, Entry>,
        is_dir: bool,
    ) -> OperationResult<CreateFileInfo<EntryHandle>> {
        info!(
            "create_new: {:?} {:?} {:?}",
            name.file_name.to_string().unwrap(),
            attrs,
            delete_on_close
        );
        if attrs & winnt::FILE_ATTRIBUTE_READONLY > 0 && delete_on_close {
            return Err(STATUS_CANNOT_DELETE);
        }
        let mut stat = Stat::new(
            self.next_id(),
            attrs,
            SecurityDescriptor::new_inherited(
                &parent.stat.read().unwrap().sec_desc,
                creator_desc,
                token,
                is_dir,
            )?,
            Arc::downgrade(&parent),
        );
        let file_name = name.file_name.to_string().unwrap();
        let url = self
            .url
            .join(if name.file_name.is_empty() {
                "index.html"
            } else {
                file_name.as_str()
            })
            .unwrap();
        let stream = if let Some(stream_info) = &name.stream_info {
            if stream_info.check_default(is_dir)? {
                None
            } else {
                // let file_name = name.file_name.to_string().unwrap();
                // let _url = self.url.join(file_name.as_str()).unwrap();

                let mut content = reqwest::blocking::get(url.clone())
                    .and_then(|resp| resp.bytes())
                    .unwrap_or_default();
                if (file_name.ends_with("main_module.bootstrap.js")) {
                    content = String::from_utf8_lossy(&content)
                        .replace(
                            "'$requireDigestsPath?entrypoint=main_module.bootstrap.js'",
                            "'$requireDigestsPath$entrypoint=main_module.bootstrap.js'",
                        ).into();
                }
                info!(
                    "create_new: stream_info {:?}, {:}  url={:?}",
                    file_name,
                    name.file_name.is_empty(),
                    url
                );
                let mut stream = AltStream::new();
                stream.data.extend_from_slice(&content);
                let stream = Arc::new(RwLock::new(stream));
                assert!(stat
                    .alt_streams
                    .insert(EntryName(stream_info.name.to_owned()), Arc::clone(&stream))
                    .is_none());
                Some(stream)
            }
        } else {
            info!("create_new: url={:?}", url.to_string());
            None
        };
        let entry = if is_dir {
            Entry::Directory(Arc::new(DirEntry::new(stat)))
        } else {
            let _entry = HttpFileEntry::new(stat, url.to_string());
            _entry.update_data();
            Entry::HttpFile(Arc::new(_entry))
        };

        // assert!(self.files
        //     .insert(EntryName(name.file_name.to_owned()), entry.clone())
        //     .is_none());
        parent.stat.write().unwrap().update_mtime(SystemTime::now());
        let is_dir = is_dir && stream.is_some();
        Ok(CreateFileInfo {
            context: EntryHandle::new(entry, stream, delete_on_close),
            is_dir,
            new_file_created: true,
        })
    }
}

fn ignore_name_too_long(err: FillDataError) -> OperationResult<()> {
    match err {
        // Normal behavior.
        FillDataError::BufferFull => Err(STATUS_BUFFER_OVERFLOW),
        // Silently ignore this error because 1) file names passed to create_file should have been checked
        // by Windows. 2) We don't want an error on a single file to make the whole directory unreadable.
        FillDataError::NameTooLong => Ok(()),
    }
}

impl<'c, 'h: 'c> FileSystemHandler<'c, 'h> for MemFsHandler {
    type Context = EntryHandle;

    fn create_file(
        &'h self,
        file_name: &U16CStr,
        security_context: &IO_SECURITY_CONTEXT,
        desired_access: winnt::ACCESS_MASK,
        file_attributes: u32,
        _share_access: u32,
        create_disposition: u32,
        create_options: u32,
        info: &mut OperationInfo<'c, 'h, Self>,
    ) -> OperationResult<CreateFileInfo<Self::Context>> {
        let file_name = U16CString::from_str(&file_name.to_string().unwrap().replace(
            "$requireDigestsPath$entrypoint=main_module.bootstrap.js",
            "$requireDigestsPath?entrypoint=main_module.bootstrap.js",
        ))
        .unwrap();
        info!(
            "create_file: {:?} {:?}  {}",
            file_name.to_string().unwrap(),
            create_disposition,
            access_flags_to_string(desired_access)
        );
        if create_disposition > FILE_MAXIMUM_DISPOSITION {
            return Err(STATUS_INVALID_PARAMETER);
        }
        let delete_on_close = create_options & FILE_DELETE_ON_CLOSE > 0;
        // let path_info = path::split_path(&self.root, file_name)?;
        // if let Some((name, parent)) = path_info {
        //     let mut children = parent.children.write().unwrap();
        //     info!("create_file: name={:?} parent={:?}", name.file_name.to_string() ,parent.stat.read().unwrap().id);
        //     // chick if the child is exist
        //     if let Some(entry) = children.get(EntryNameRef::new(name.file_name)) {
        //         // file exist
        //         let stat = entry.stat().read().unwrap();
        //         info!("create_file: found entry attrs={:#X}", stat.attrs.value);

        //         let is_readonly = stat.attrs.value & winnt::FILE_ATTRIBUTE_READONLY > 0;
        //         let is_hidden_system = stat.attrs.value & winnt::FILE_ATTRIBUTE_HIDDEN > 0
        //             && stat.attrs.value & winnt::FILE_ATTRIBUTE_SYSTEM > 0
        //             && !(file_attributes & winnt::FILE_ATTRIBUTE_HIDDEN > 0
        //                 && file_attributes & winnt::FILE_ATTRIBUTE_SYSTEM > 0);
        //         if is_readonly
        //             && (desired_access & winnt::FILE_WRITE_DATA > 0
        //                 || desired_access & winnt::FILE_APPEND_DATA > 0)
        //         {
        //             return Err(STATUS_ACCESS_DENIED);
        //         }
        //         if stat.delete_pending {
        //             return Err(STATUS_DELETE_PENDING);
        //         }
        //         if is_readonly && delete_on_close {
        //             return Err(STATUS_CANNOT_DELETE);
        //         }
        //         std::mem::drop(stat);
        //         let ret = if let Some(stream_info) = &name.stream_info {
        //             if stream_info.check_default(entry.is_dir())? {
        //                 None
        //             } else {
        //                 let mut stat = entry.stat().write().unwrap();
        //                 let stream_name = EntryNameRef::new(stream_info.name);
        //                 if let Some(stream) =
        //                     stat.alt_streams.get(stream_name).map(|s| Arc::clone(s))
        //                 {
        //                     if stream.read().unwrap().delete_pending {
        //                         return Err(STATUS_DELETE_PENDING);
        //                     }
        //                     match create_disposition {
        //                         FILE_SUPERSEDE | FILE_OVERWRITE | FILE_OVERWRITE_IF => {
        //                             if create_disposition != FILE_SUPERSEDE && is_readonly {
        //                                 return Err(STATUS_ACCESS_DENIED);
        //                             }
        //                             stat.attrs.value |= winnt::FILE_ATTRIBUTE_ARCHIVE;
        //                             stat.update_mtime(SystemTime::now());
        //                             stream.write().unwrap().data.clear();
        //                         }
        //                         FILE_CREATE => return Err(STATUS_OBJECT_NAME_COLLISION),
        //                         _ => (),
        //                     }
        //                     Some((stream, false))
        //                 } else {
        //                     if create_disposition == FILE_OPEN
        //                         || create_disposition == FILE_OVERWRITE
        //                     {
        //                         return Err(STATUS_OBJECT_NAME_NOT_FOUND);
        //                     }
        //                     if is_readonly {
        //                         return Err(STATUS_ACCESS_DENIED);
        //                     }
        //                     let stream = Arc::new(RwLock::new(AltStream::new()));
        //                     stat.update_atime(SystemTime::now());
        //                     assert!(stat
        //                         .alt_streams
        //                         .insert(EntryName(stream_info.name.to_owned()), Arc::clone(&stream))
        //                         .is_none());
        //                     Some((stream, true))
        //                 }
        //             }
        //         } else {
        //             None
        //         };
        //         if let Some((stream, new_file_created)) = ret {
        //             return Ok(CreateFileInfo {
        //                 context: EntryHandle::new(entry.clone(), Some(stream), delete_on_close),
        //                 is_dir: false,
        //                 new_file_created,
        //             });
        //         }
        //         match entry {
        //             Entry::File(file) => {
        //                 if create_options & FILE_DIRECTORY_FILE > 0 {
        //                     return Err(STATUS_NOT_A_DIRECTORY);
        //                 }
        //                 match create_disposition {
        //                     FILE_SUPERSEDE | FILE_OVERWRITE | FILE_OVERWRITE_IF => {
        //                         if create_disposition != FILE_SUPERSEDE && is_readonly
        //                             || is_hidden_system
        //                         {
        //                             return Err(STATUS_ACCESS_DENIED);
        //                         }
        //                         file.data.write().unwrap().clear();
        //                         let mut stat = file.stat.write().unwrap();
        //                         stat.attrs = Attributes::new(
        //                             file_attributes | winnt::FILE_ATTRIBUTE_ARCHIVE,
        //                         );
        //                         stat.update_mtime(SystemTime::now());
        //                     }
        //                     FILE_CREATE => return Err(STATUS_OBJECT_NAME_COLLISION),
        //                     _ => (),
        //                 }
        //                 Ok(CreateFileInfo {
        //                     context: EntryHandle::new(
        //                         Entry::File(Arc::clone(&file)),
        //                         None,
        //                         delete_on_close,
        //                     ),
        //                     is_dir: false,
        //                     new_file_created: false,
        //                 })
        //             }
        //             Entry::HttpFile(file) => {
        //                 info!(
        //                     "create_file: found http file {:#X}",
        //                     file.stat.read().unwrap().attrs.value
        //                 );
        //                 if create_options & FILE_DIRECTORY_FILE > 0 {
        //                     return Err(STATUS_FILE_IS_A_DIRECTORY);
        //                 }
        //                 match create_disposition {
        //                     FILE_OPEN | FILE_OPEN_IF => Ok(CreateFileInfo {
        //                         context: EntryHandle::new(
        //                             Entry::HttpFile(Arc::clone(&file)),
        //                             None,
        //                             delete_on_close,
        //                         ),
        //                         is_dir: false,
        //                         new_file_created: false,
        //                     }),
        //                     FILE_CREATE => Err(STATUS_OBJECT_NAME_COLLISION),
        //                     _ => Err(STATUS_INVALID_PARAMETER),
        //                 }
        //             }
        //             Entry::Directory(dir) => {
        //                 if create_options & FILE_NON_DIRECTORY_FILE > 0 {
        //                     return Err(STATUS_FILE_IS_A_DIRECTORY);
        //                 }
        //                 match create_disposition {
        //                     FILE_OPEN | FILE_OPEN_IF => Ok(CreateFileInfo {
        //                         context: EntryHandle::new(
        //                             Entry::Directory(Arc::clone(&dir)),
        //                             None,
        //                             delete_on_close,
        //                         ),
        //                         is_dir: true,
        //                         new_file_created: false,
        //                     }),
        //                     FILE_CREATE => Err(STATUS_OBJECT_NAME_COLLISION),
        //                     _ => Err(STATUS_INVALID_PARAMETER),
        //                 }
        //             }
        //         }
        //     } else {
        // file not exist
        // if parent.stat.read().unwrap().delete_pending {
        //     return Err(STATUS_DELETE_PENDING);
        // }
        let token = info.requester_token().unwrap();
        // if create_options & FILE_DIRECTORY_FILE > 0 {
        //     match create_disposition {
        //         FILE_CREATE | FILE_OPEN_IF => self.create_new(
        //             &name,
        //             file_attributes,
        //             delete_on_close,
        //             security_context.AccessState.SecurityDescriptor,
        //             token.as_raw_handle(),
        //             &parent,
        //             &mut children,
        //             true,
        //         ),
        //         FILE_OPEN => Err(STATUS_OBJECT_NAME_NOT_FOUND),
        //         _ => Err(STATUS_INVALID_PARAMETER),
        //     }
        // } else {
        //     if create_disposition == FILE_OPEN || create_disposition == FILE_OVERWRITE {
        //         Err(STATUS_OBJECT_NAME_NOT_FOUND)
        //     } else {
        // let name = self.root.children.read().unwrap().get(EntryNameRef::new(name.file_name)).unwrap();
        let name = FullName {
            file_name: U16Str::from_slice(file_name.as_slice()),
            stream_info: Some(StreamInfo {
                name: U16Str::from_slice(file_name.as_slice()),
                type_: StreamType::Data,
            }),
        };
        self.create_new(
            &name,
            file_attributes | winnt::FILE_ATTRIBUTE_ARCHIVE,
            delete_on_close,
            security_context.AccessState.SecurityDescriptor,
            token.as_raw_handle(),
            &self.root, //  &parent,
            // &mut self.root.children.write().unwrap(),
            // &mut self.files,
            false,
        )
        //     }
        // }
        //     }
        // } else {
        //     if create_disposition == FILE_OPEN || create_disposition == FILE_OPEN_IF {
        //         if create_options & FILE_NON_DIRECTORY_FILE > 0 {
        //             Err(STATUS_FILE_IS_A_DIRECTORY)
        //         } else {
        //             Ok(CreateFileInfo {
        //                 context: EntryHandle::new(
        //                     Entry::Directory(Arc::clone(&self.root)),
        //                     None,
        //                     info.delete_on_close(),
        //                 ),
        //                 is_dir: true,
        //                 new_file_created: false,
        //             })
        //         }
        //     } else {
        //         Err(STATUS_INVALID_PARAMETER)
        //     }
        // }
    }

    fn close_file(
        &'h self,
        _file_name: &U16CStr,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) {
        info!("close_file: {:?}", _file_name.to_string().unwrap());
        let mut stat = context.entry.stat().write().unwrap();
        if let Some(mtime) = context.mtime_delayed.lock().unwrap().clone() {
            if mtime > stat.mtime {
                stat.mtime = mtime;
            }
        }
        if let Some(atime) = context.atime_delayed.lock().unwrap().clone() {
            if atime > stat.atime {
                stat.atime = atime;
            }
        }
    }

    fn read_file(
        &'h self,
        _file_name: &U16CStr,
        offset: i64,
        buffer: &mut [u8],
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<u32> {
        let _file_name = U16CString::from_str(&_file_name.to_string().unwrap().replace(
            "$requireDigestsPath$entrypoint=main_module.bootstrap.js",
            "$requireDigestsPath?entrypoint=main_module.bootstrap.js",
        ))
        .unwrap();
        info!("read_file: {:?}", _file_name.to_string().unwrap());
        let mut do_read = |data: &Vec<_>| {
            let offset = offset as usize;
            let len = std::cmp::min(buffer.len(), data.len() - offset);
            buffer[0..len].copy_from_slice(&data[offset..offset + len]);
            len as u32
        };
        let alt_stream = context.alt_stream.read().unwrap();
        if let Some(stream) = alt_stream.as_ref() {
            Ok(do_read(&stream.read().unwrap().data))
        } else if let Entry::File(file) = &context.entry {
            Ok(do_read(&file.data.read().unwrap()))
        } else if let Entry::HttpFile(http_file) = &context.entry {
            let data = http_file.get_data().unwrap();
            let offset = offset as usize;
            let len = std::cmp::min(buffer.len(), data.len().saturating_sub(offset));
            buffer[..len].copy_from_slice(&data[offset..offset + len]);
            Ok(len as u32)
        } else {
            Err(STATUS_INVALID_DEVICE_REQUEST)
        }
    }

    fn write_file(
        &'h self,
        _file_name: &U16CStr,
        offset: i64,
        buffer: &[u8],
        info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<u32> {
        let do_write = |data: &mut Vec<_>| {
            let offset = if info.write_to_eof() {
                data.len()
            } else {
                offset as usize
            };
            let len = buffer.len();
            if offset + len > data.len() {
                data.resize(offset + len, 0);
            }
            data[offset..offset + len].copy_from_slice(buffer);
            len as u32
        };
        let alt_stream = context.alt_stream.read().unwrap();
        let ret = if let Some(stream) = alt_stream.as_ref() {
            Ok(do_write(&mut stream.write().unwrap().data))
        } else if let Entry::File(file) = &context.entry {
            Ok(do_write(&mut file.data.write().unwrap()))
        } else {
            Err(STATUS_ACCESS_DENIED)
        };
        if ret.is_ok() {
            context.entry.stat().write().unwrap().attrs.value |= winnt::FILE_ATTRIBUTE_ARCHIVE;
            let now = SystemTime::now();
            if context.mtime_enabled.load(Ordering::Relaxed) {
                *context.mtime_delayed.lock().unwrap() = Some(now);
            }
            if context.atime_enabled.load(Ordering::Relaxed) {
                *context.atime_delayed.lock().unwrap() = Some(now);
            }
        }
        ret
    }

    fn flush_file_buffers(
        &'h self,
        _file_name: &U16CStr,
        _info: &OperationInfo<'c, 'h, Self>,
        _context: &'c Self::Context,
    ) -> OperationResult<()> {
        Ok(())
    }

    fn get_file_information(
        &'h self,
        _file_name: &U16CStr,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<FileInfo> {
        info!(
            "get_file_information: {:?}",
            _file_name.to_string().unwrap()
        );
        let stat = context.entry.stat().read().unwrap();
        let alt_stream = context.alt_stream.read().unwrap();
        Ok(FileInfo {
            attributes: stat.attrs.get_output_attrs(context.is_dir()),
            creation_time: stat.ctime,
            last_access_time: stat.atime,
            last_write_time: stat.mtime,
            file_size: if let Some(stream) = alt_stream.as_ref() {
                stream.read().unwrap().data.len() as u64
            } else {
                match &context.entry {
                    Entry::File(file) => file.data.read().unwrap().len() as u64,
                    Entry::HttpFile(http_file) => http_file.data_len() as u64,
                    Entry::Directory(_) => 0,
                }
            },
            number_of_links: 1,
            file_index: stat.id,
        })
    }

    fn find_files(
        &'h self,
        _file_name: &U16CStr,
        mut fill_find_data: impl FnMut(&FindData) -> FillDataResult,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        info!("find_files: {:?}", _file_name.to_string().unwrap());
        if context.alt_stream.read().unwrap().is_some() {
            return Err(STATUS_INVALID_DEVICE_REQUEST);
        }
        if let Entry::Directory(dir) = &context.entry {
            let children = dir.children.read().unwrap();
            for (k, v) in children.iter() {
                let stat = v.stat().read().unwrap();
                fill_find_data(&FindData {
                    attributes: stat.attrs.get_output_attrs(v.is_dir()),
                    creation_time: stat.ctime,
                    last_access_time: stat.atime,
                    last_write_time: stat.mtime,
                    file_size: match v {
                        Entry::File(file) => file.data.read().unwrap().len() as u64,
                        Entry::Directory(_) => 0,
                        Entry::HttpFile(http_file) => http_file.data_len() as u64,
                    },
                    file_name: U16CString::from_ustr(&k.0).unwrap(),
                })
                .or_else(ignore_name_too_long)?;
            }
            Ok(())
        } else {
            Err(STATUS_INVALID_DEVICE_REQUEST)
        }
    }

    fn set_file_attributes(
        &'h self,
        _file_name: &U16CStr,
        file_attributes: u32,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        let mut stat = context.entry.stat().write().unwrap();
        stat.attrs = Attributes::new(file_attributes);
        context.update_atime(&mut stat, SystemTime::now());
        Ok(())
    }

    fn set_file_time(
        &'h self,
        _file_name: &U16CStr,
        creation_time: FileTimeOperation,
        last_access_time: FileTimeOperation,
        last_write_time: FileTimeOperation,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        let mut stat = context.entry.stat().write().unwrap();
        let process_time_info = |time_info: &FileTimeOperation,
                                 time: &mut SystemTime,
                                 flag: &AtomicBool| match time_info
        {
            FileTimeOperation::SetTime(new_time) => {
                if flag.load(Ordering::Relaxed) {
                    *time = *new_time
                }
            }
            FileTimeOperation::DisableUpdate => flag.store(false, Ordering::Relaxed),
            FileTimeOperation::ResumeUpdate => flag.store(true, Ordering::Relaxed),
            FileTimeOperation::DontChange => (),
        };
        process_time_info(&creation_time, &mut stat.ctime, &context.ctime_enabled);
        process_time_info(&last_write_time, &mut stat.mtime, &context.mtime_enabled);
        process_time_info(&last_access_time, &mut stat.atime, &context.atime_enabled);
        Ok(())
    }

    fn delete_file(
        &'h self,
        _file_name: &U16CStr,
        info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        if context.entry.stat().read().unwrap().attrs.value & winnt::FILE_ATTRIBUTE_READONLY > 0 {
            return Err(STATUS_CANNOT_DELETE);
        }
        let alt_stream = context.alt_stream.read().unwrap();
        if let Some(stream) = alt_stream.as_ref() {
            stream.write().unwrap().delete_pending = info.delete_on_close();
        } else {
            context.entry.stat().write().unwrap().delete_pending = info.delete_on_close();
        }
        Ok(())
    }

    fn delete_directory(
        &'h self,
        _file_name: &U16CStr,
        info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        if context.alt_stream.read().unwrap().is_some() {
            return Err(STATUS_INVALID_DEVICE_REQUEST);
        }
        if let Entry::Directory(dir) = &context.entry {
            // Lock children first to avoid race conditions.
            let children = dir.children.read().unwrap();
            let mut stat = dir.stat.write().unwrap();
            if stat.parent.upgrade().is_none() {
                // Root directory can't be deleted.
                return Err(STATUS_ACCESS_DENIED);
            }
            if info.delete_on_close() && !children.is_empty() {
                Err(STATUS_DIRECTORY_NOT_EMPTY)
            } else {
                stat.delete_pending = info.delete_on_close();
                Ok(())
            }
        } else {
            Err(STATUS_INVALID_DEVICE_REQUEST)
        }
    }

    fn move_file(
        &'h self,
        file_name: &U16CStr,
        new_file_name: &U16CStr,
        replace_if_existing: bool,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        let src_path = file_name.as_slice();
        let offset = src_path
            .iter()
            .rposition(|x| *x == '\\' as u16)
            .ok_or(STATUS_INVALID_PARAMETER)?;
        let src_name = U16Str::from_slice(&src_path[offset + 1..]);
        let src_parent = context
            .entry
            .stat()
            .read()
            .unwrap()
            .parent
            .upgrade()
            .ok_or(STATUS_INVALID_DEVICE_REQUEST)?;
        if new_file_name.as_slice().first() == Some(&(':' as u16)) {
            let src_stream_info = FullName::new(src_name)?.stream_info;
            let dst_stream_info =
                FullName::new(U16Str::from_slice(new_file_name.as_slice()))?.stream_info;
            let src_is_default = context.alt_stream.read().unwrap().is_none();
            let dst_is_default = if let Some(stream_info) = &dst_stream_info {
                stream_info.check_default(context.entry.is_dir())?
            } else {
                true
            };
            let check_can_move = |streams: &mut HashMap<EntryName, Arc<RwLock<AltStream>>>,
                                  name: &U16Str| {
                let name_ref = EntryNameRef::new(name);
                if let Some(stream) = streams.get(name_ref) {
                    if context
                        .alt_stream
                        .read()
                        .unwrap()
                        .as_ref()
                        .map(|s| Arc::ptr_eq(s, stream))
                        .unwrap_or(false)
                    {
                        Ok(())
                    } else if !replace_if_existing {
                        Err(STATUS_OBJECT_NAME_COLLISION)
                    } else if stream.read().unwrap().handle_count > 0 {
                        Err(STATUS_ACCESS_DENIED)
                    } else {
                        streams.remove(name_ref).unwrap();
                        Ok(())
                    }
                } else {
                    Ok(())
                }
            };
            let mut stat = context.entry.stat().write().unwrap();
            match (src_is_default, dst_is_default) {
                (true, true) => {
                    if context.entry.is_dir() {
                        return Err(STATUS_OBJECT_NAME_INVALID);
                    }
                }
                (true, false) => {
                    if let Entry::File(file) = &context.entry {
                        let dst_name = dst_stream_info.unwrap().name;
                        check_can_move(&mut stat.alt_streams, dst_name)?;
                        let mut stream = AltStream::new();
                        let mut data = file.data.write().unwrap();
                        stream.handle_count = 1;
                        stream.delete_pending = stat.delete_pending;
                        stat.delete_pending = false;
                        stream.data = data.clone();
                        data.clear();
                        let stream = Arc::new(RwLock::new(stream));
                        assert!(stat
                            .alt_streams
                            .insert(EntryName(dst_name.to_owned()), Arc::clone(&stream))
                            .is_none());
                        *context.alt_stream.write().unwrap() = Some(stream);
                    } else {
                        return Err(STATUS_OBJECT_NAME_INVALID);
                    }
                }
                (false, true) => {
                    if let Entry::File(file) = &context.entry {
                        let mut context_stream = context.alt_stream.write().unwrap();
                        let src_stream = context_stream.as_ref().unwrap();
                        let mut src_stream_locked = src_stream.write().unwrap();
                        if src_stream_locked.handle_count > 1 {
                            return Err(STATUS_SHARING_VIOLATION);
                        }
                        if !replace_if_existing {
                            return Err(STATUS_OBJECT_NAME_COLLISION);
                        }
                        src_stream_locked.handle_count -= 1;
                        stat.delete_pending = src_stream_locked.delete_pending;
                        src_stream_locked.delete_pending = false;
                        *file.data.write().unwrap() = src_stream_locked.data.clone();
                        stat.alt_streams
                            .remove(EntryNameRef::new(src_stream_info.unwrap().name))
                            .unwrap();
                        std::mem::drop(src_stream_locked);
                        *context_stream = None;
                    } else {
                        return Err(STATUS_OBJECT_NAME_INVALID);
                    }
                }
                (false, false) => {
                    let dst_name = dst_stream_info.unwrap().name;
                    check_can_move(&mut stat.alt_streams, dst_name)?;
                    let stream = stat
                        .alt_streams
                        .remove(EntryNameRef::new(src_stream_info.unwrap().name))
                        .unwrap();
                    stat.alt_streams
                        .insert(EntryName(dst_name.to_owned()), Arc::clone(&stream));
                    *context.alt_stream.write().unwrap() = Some(stream);
                }
            }
            stat.update_atime(SystemTime::now());
        } else {
            if context.alt_stream.read().unwrap().is_some() {
                return Err(STATUS_OBJECT_NAME_INVALID);
            }
            let (dst_name, dst_parent) =
                path::split_path(&self.root, new_file_name)?.ok_or(STATUS_OBJECT_NAME_INVALID)?;
            if dst_name.stream_info.is_some() {
                return Err(STATUS_OBJECT_NAME_INVALID);
            }
            let now = SystemTime::now();
            let src_name_ref = EntryNameRef::new(src_name);
            let dst_name_ref = EntryNameRef::new(dst_name.file_name);
            let check_can_move = |children: &mut HashMap<EntryName, Entry>| {
                if let Some(entry) = children.get(dst_name_ref) {
                    if &context.entry == entry {
                        Ok(())
                    } else if !replace_if_existing {
                        Err(STATUS_OBJECT_NAME_COLLISION)
                    } else if context.entry.is_dir() || entry.is_dir() {
                        Err(STATUS_ACCESS_DENIED)
                    } else {
                        let stat = entry.stat().read().unwrap();
                        let can_replace = stat.handle_count > 0
                            || stat.attrs.value & winnt::FILE_ATTRIBUTE_READONLY > 0;
                        std::mem::drop(stat);
                        if can_replace {
                            Err(STATUS_ACCESS_DENIED)
                        } else {
                            children.remove(dst_name_ref).unwrap();
                            Ok(())
                        }
                    }
                } else {
                    Ok(())
                }
            };
            if Arc::ptr_eq(&src_parent, &dst_parent) {
                let mut children = src_parent.children.write().unwrap();
                check_can_move(&mut children)?;
                // Remove first in case moving to the same name.
                let entry = children.remove(src_name_ref).unwrap();
                assert!(children
                    .insert(EntryName(dst_name.file_name.to_owned()), entry)
                    .is_none());
                src_parent.stat.write().unwrap().update_mtime(now);
                context.update_atime(&mut context.entry.stat().write().unwrap(), now);
            } else {
                let mut src_children = src_parent.children.write().unwrap();
                let mut dst_children = dst_parent.children.write().unwrap();
                check_can_move(&mut dst_children)?;
                let entry = src_children.remove(src_name_ref).unwrap();
                assert!(dst_children
                    .insert(EntryName(dst_name.file_name.to_owned()), entry)
                    .is_none());
                src_parent.stat.write().unwrap().update_mtime(now);
                dst_parent.stat.write().unwrap().update_mtime(now);
                let mut stat = context.entry.stat().write().unwrap();
                stat.parent = Arc::downgrade(&dst_parent);
                context.update_atime(&mut stat, now);
            }
        }
        Ok(())
    }

    fn set_end_of_file(
        &'h self,
        _file_name: &U16CStr,
        offset: i64,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        let alt_stream = context.alt_stream.read().unwrap();
        let ret = if let Some(stream) = alt_stream.as_ref() {
            stream.write().unwrap().data.resize(offset as usize, 0);
            Ok(())
        } else if let Entry::File(file) = &context.entry {
            file.data.write().unwrap().resize(offset as usize, 0);
            Ok(())
        } else {
            Err(STATUS_INVALID_DEVICE_REQUEST)
        };
        if ret.is_ok() {
            context.update_mtime(
                &mut context.entry.stat().write().unwrap(),
                SystemTime::now(),
            );
        }
        ret
    }

    fn set_allocation_size(
        &'h self,
        _file_name: &U16CStr,
        alloc_size: i64,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        let set_alloc = |data: &mut Vec<_>| {
            let alloc_size = alloc_size as usize;
            let cap = data.capacity();
            if alloc_size < data.len() {
                data.resize(alloc_size, 0);
            } else if alloc_size < cap {
                let mut new_data = Vec::with_capacity(alloc_size);
                new_data.append(data);
                *data = new_data;
            } else if alloc_size > cap {
                data.reserve(alloc_size - cap);
            }
        };
        let alt_stream = context.alt_stream.read().unwrap();
        let ret = if let Some(stream) = alt_stream.as_ref() {
            set_alloc(&mut stream.write().unwrap().data);
            Ok(())
        } else if let Entry::File(file) = &context.entry {
            set_alloc(&mut file.data.write().unwrap());
            Ok(())
        } else {
            Err(STATUS_INVALID_DEVICE_REQUEST)
        };
        if ret.is_ok() {
            context.update_mtime(
                &mut context.entry.stat().write().unwrap(),
                SystemTime::now(),
            );
        }
        ret
    }

    fn get_disk_free_space(
        &'h self,
        _info: &OperationInfo<'c, 'h, Self>,
    ) -> OperationResult<DiskSpaceInfo> {
        Ok(DiskSpaceInfo {
            byte_count: 1024 * 1024 * 1024,
            free_byte_count: 512 * 1024 * 1024,
            available_byte_count: 512 * 1024 * 1024,
        })
    }

    fn get_volume_information(
        &'h self,
        _info: &OperationInfo<'c, 'h, Self>,
    ) -> OperationResult<VolumeInfo> {
        Ok(VolumeInfo {
            name: U16CString::from_str("dokan-rust memfs").unwrap(),
            serial_number: 0,
            max_component_length: path::MAX_COMPONENT_LENGTH,
            fs_flags: winnt::FILE_CASE_PRESERVED_NAMES
                | winnt::FILE_CASE_SENSITIVE_SEARCH
                | winnt::FILE_UNICODE_ON_DISK
                | winnt::FILE_PERSISTENT_ACLS
                | winnt::FILE_NAMED_STREAMS,
            // Custom names don't play well with UAC.
            fs_name: U16CString::from_str("NTFS").unwrap(),
        })
    }

    fn mounted(
        &'h self,
        _mount_point: &U16CStr,
        _info: &OperationInfo<'c, 'h, Self>,
    ) -> OperationResult<()> {
        Ok(())
    }

    fn unmounted(&'h self, _info: &OperationInfo<'c, 'h, Self>) -> OperationResult<()> {
        Ok(())
    }

    fn get_file_security(
        &'h self,
        _file_name: &U16CStr,
        security_information: u32,
        security_descriptor: winnt::PSECURITY_DESCRIPTOR,
        buffer_length: u32,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<u32> {
        context
            .entry
            .stat()
            .read()
            .unwrap()
            .sec_desc
            .get_security_info(security_information, security_descriptor, buffer_length)
    }

    fn set_file_security(
        &'h self,
        _file_name: &U16CStr,
        security_information: u32,
        security_descriptor: winnt::PSECURITY_DESCRIPTOR,
        _buffer_length: u32,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        let mut stat = context.entry.stat().write().unwrap();
        let ret = stat
            .sec_desc
            .set_security_info(security_information, security_descriptor);
        if ret.is_ok() {
            context.update_atime(&mut stat, SystemTime::now());
        }
        ret
    }

    fn find_streams(
        &'h self,
        _file_name: &U16CStr,
        mut fill_find_stream_data: impl FnMut(&FindStreamData) -> FillDataResult,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        info!("find_streams: {:?}", _file_name.to_string().unwrap());
        if let Entry::File(file) = &context.entry {
            fill_find_stream_data(&FindStreamData {
                size: file.data.read().unwrap().len() as i64,
                name: U16CString::from_str("::$DATA").unwrap(),
            })
            .or_else(ignore_name_too_long)?;
        }
        for (k, v) in context.entry.stat().read().unwrap().alt_streams.iter() {
            let mut name_buf = vec![':' as u16];
            name_buf.extend_from_slice(k.0.as_slice());
            name_buf.extend_from_slice(U16String::from_str(":$DATA").as_slice());
            fill_find_stream_data(&FindStreamData {
                size: v.read().unwrap().data.len() as i64,
                name: U16CString::from_ustr(U16Str::from_slice(&name_buf)).unwrap(),
            })
            .or_else(ignore_name_too_long)?;
        }
        Ok(())
    }
}
