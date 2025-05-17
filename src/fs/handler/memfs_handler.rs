use std::{
    collections::HashMap,
    os::windows::io::AsRawHandle,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock, Weak,
    },
    time::SystemTime,
};

use crate::{
    fs::{
        entry::{DirEntry, Entry, EntryName, FileEntry, HttpFileEntry},
        metadata::{AltStream, Stat},
    },
    path::{self, FullName},
    security::SecurityDescriptor,
    thread_pool::ThreadPool,
    utils::{access_flags_to_string, create_disposition_to_string, wait_with_timeout},
};
use dokan::{
    CreateFileInfo, DiskSpaceInfo, FileInfo, FileSystemHandler, FileTimeOperation, FillDataError,
    FillDataResult, FindData, FindStreamData, OperationInfo, OperationResult, VolumeInfo,
    IO_SECURITY_CONTEXT,
};
use ignore::gitignore::Gitignore;
use url::Url;
use winapi::{
    shared::{ntdef, ntstatus::*},
    um::winnt,
};

use colored::Colorize;
use dokan_sys::win32::{
    FILE_CREATE, FILE_DELETE_ON_CLOSE, FILE_DIRECTORY_FILE, FILE_MAXIMUM_DISPOSITION,
    FILE_NON_DIRECTORY_FILE, FILE_OPEN, FILE_OPEN_IF, FILE_OVERWRITE, FILE_OVERWRITE_IF,
    FILE_SUPERSEDE,
};
use futures_util::StreamExt;
use log::{debug, error, info, trace, warn};
use widestring::{U16CStr, U16CString, U16Str, U16String};

use super::super::entry::EntryNameRef;
use super::super::metadata::Attributes;

use super::super::super::windows::get_path_by_pid;
use super::EntryHandle;
use reqwest::Client;

use sha2::{Digest, Sha256};

#[derive(Debug)]
pub struct MemFsHandler {
    pub url: Url,
    pub id_counter: AtomicU64,
    pub root: Arc<DirEntry>,
    thread_pool: Arc<ThreadPool>,
    client: Client,
    pub ignore: Option<Gitignore>,
}

impl MemFsHandler {
    pub fn new(url: Url, thread_pool: Arc<ThreadPool>, ignore: Option<Gitignore>) -> Self {
        let root_stat = Stat::new(
            0,
            0,
            SecurityDescriptor::new_default().unwrap(),
            Weak::new(),
        );
        let root = Arc::new(DirEntry::new(root_stat));
        Self {
            url: url.clone(),
            id_counter: AtomicU64::new(1),
            root: root,
            thread_pool: thread_pool,
            client: Client::new(),
            ignore,
        }
    }

    pub fn next_id(&self) -> u64 {
        self.id_counter.fetch_add(1, Ordering::Relaxed)
    }

    pub fn get_client(&self) -> Client {
        self.client.clone()
    }

    pub fn create_dir_entry(
        &self,
        index: u64,
        cur_entry: &Arc<DirEntry>,
        children: &mut HashMap<EntryName, Arc<Entry>>,
        name: U16String,
    ) -> Arc<Entry> {
        let child_stat = Stat::new(
            index,
            0,
            SecurityDescriptor::new_default().unwrap(),
            Arc::downgrade(&cur_entry),
        );
        let dir_entry = Arc::new(DirEntry::new(child_stat));
        let child_entry = Entry::Directory(dir_entry);
        let arc_entry = Arc::new(child_entry);
        let ret = Arc::clone(&arc_entry);
        debug!("create_dir_entry {}", name.to_string_lossy());
        children.insert(EntryName(name), arc_entry);
        ret
    }

    pub fn create_new(
        &self,
        index: u64,
        name: &FullName,
        attrs: u32,
        delete_on_close: bool,
        creator_desc: winnt::PSECURITY_DESCRIPTOR,
        token: ntdef::HANDLE,
        parent: &Arc<DirEntry>,
        rw_children: &RwLock<HashMap<EntryName, Arc<Entry>>>,
        is_dir: bool,
    ) -> OperationResult<CreateFileInfo<EntryHandle>> {
        if attrs & winnt::FILE_ATTRIBUTE_READONLY > 0 && delete_on_close {
            return Err(STATUS_CANNOT_DELETE);
        }
        let mut stat = Stat::new(
            index,
            attrs,
            SecurityDescriptor::new_inherited(
                &parent.stat.read().unwrap().sec_desc,
                creator_desc,
                token,
                is_dir,
            )?,
            Arc::downgrade(&parent),
        );
        let stream = if let Some(stream_info) = &name.stream_info {
            if stream_info.check_default(is_dir)? {
                None
            } else {
                let stream = Arc::new(RwLock::new(AltStream::new()));
                assert!(stat
                    .alt_streams
                    .insert(EntryName(stream_info.name.to_owned()), Arc::clone(&stream))
                    .is_none());
                Some(stream)
            }
        } else {
            None
        };
        let entry = if is_dir {
            Entry::Directory(Arc::new(DirEntry::new(stat)))
        } else {
            Entry::File(Arc::new(FileEntry::new(stat)))
        };
        let arc_entry = Arc::new(entry);
        {
            debug!("{}", format!("create_new").red());
            let mut children = rw_children.write().unwrap();
            assert!(children
                .insert(EntryName(name.file_name.to_owned()), Arc::clone(&arc_entry))
                .is_none());
        }
        parent.stat.write().unwrap().update_mtime(SystemTime::now());
        let is_dir = is_dir && stream.is_some();
        Ok(CreateFileInfo {
            context: EntryHandle::new(index, Arc::clone(&arc_entry), stream, delete_on_close),
            is_dir,
            new_file_created: true,
        })
    }
    pub fn create_new_http(
        &self,
        index: u64,
        name: &String,
        attrs: u32,
        delete_on_close: bool,
        creator_desc: winnt::PSECURITY_DESCRIPTOR,
        token: ntdef::HANDLE,
        parent: &Arc<DirEntry>,
        rw_children: &RwLock<HashMap<EntryName, Arc<Entry>>>,
        is_dir: bool,
        full_download: bool,
    ) -> OperationResult<CreateFileInfo<EntryHandle>> {
        debug!(
            "[{index}] create_new_http: {:?} {:?} {:?}",
            name, attrs, delete_on_close
        );
        if attrs & winnt::FILE_ATTRIBUTE_READONLY > 0 && delete_on_close {
            return Err(STATUS_CANNOT_DELETE);
        }
        let stat = Stat::new(
            index,
            attrs,
            SecurityDescriptor::new_inherited(
                &parent.stat.read().unwrap().sec_desc,
                creator_desc,
                token,
                is_dir,
            )?,
            Arc::downgrade(&parent),
        );
        let url = self
            .url
            .join(if name.is_empty() {
                "index.html"
            } else {
                name.as_str()
            })
            .unwrap();
        let file = Arc::new(HttpFileEntry::new(stat));
        let _file = Arc::clone(&file);

        let arc_entry = Arc::new(Entry::HttpFile(file));
        let _arc_entry = Arc::clone(&arc_entry);
        let stream = self.create_new_http_stream(
            index,
            url,
            // _arc_entry,
            name,
            full_download,
            Some(Box::new(move || {
                *_file.download_pending.write().unwrap() = false;
            })),
        );
        assert!(arc_entry
            .stat()
            .write()
            .unwrap()
            .alt_streams
            .insert(
                EntryName(U16String::from_str(name.as_str())),
                Arc::clone(&stream.clone().unwrap())
            )
            .is_none());

        let _name = *name
            .split('\\')
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .iter()
            .last()
            .unwrap();
        {
            let mut children = rw_children.write().unwrap();
            let ret = children.insert(
                EntryName(U16String::from_str(_name)),
                Arc::clone(&arc_entry),
            );
            assert!(ret.is_none(), "Unexpected existing entry: {:?}", ret);
        }
        {
            let a = rw_children.try_read();
            // assert!(a.is_ok());
            if a.is_err() {
                error!("create_new_http not release RwLock of children");
            }
        }
        parent.stat.write().unwrap().update_mtime(SystemTime::now());
        let is_dir = is_dir && stream.is_some();
        assert!(stream.is_some());
        let handle = EntryHandle::new(index, Arc::clone(&arc_entry), stream, delete_on_close);
        debug!(
            "[{index}] create_new_http: finished! len={:?}",
            handle
                .alt_stream
                .read()
                .unwrap()
                .as_ref()
                .unwrap()
                .read()
                .unwrap()
                .data
                .len(),
        );
        Ok(CreateFileInfo {
            context: handle,
            is_dir,
            new_file_created: true,
        })
    }
    pub fn create_new_http_stream(
        &self,
        index: u64,
        url: Url,
        name: &String,
        full_download: bool,
        on_done: Option<Box<dyn Fn() + Send + Sync>>,
    ) -> Option<Arc<RwLock<AltStream>>> {
        let rw_stream = RwLock::new(AltStream::new());
        let arc_stream = Arc::new(rw_stream);
        let _url = url.clone();
        let _arc_stream = Arc::clone(&arc_stream);
        debug!(
            "{}",
            format!("[{index}] download from url={:?}", url.to_string())
                .yellow()
                .to_string()
        );
        let _name = name.clone();
        let client = self.get_client();
        self.thread_pool.execute_async(move || {
            Box::pin(async move {
                let mut _content_length = 0;
                let mut rsp_stream = match client.get(_url.clone()).send().await {
                    Ok(response) => {
                        let mut _rw_stream = _arc_stream.write().unwrap();
                        if let Some(content_length) = response.content_length() {
                            debug!(
                                "{}",
                                format!(
                                    "[{index}] {} Content length: {} {}",
                                    _url,
                                    content_length,
                                    if full_download {
                                        "(skip full download)"
                                    } else {
                                        ""
                                    }
                                )
                                .yellow()
                            );
                            _rw_stream.content_length = content_length;
                            _rw_stream.ctime = SystemTime::now();
                            _content_length = content_length;

                            if !full_download {
                                return Ok(()); // save time
                            }
                        } else {
                            warn!("Content length is not available");
                        }
                        response.bytes_stream()
                    }
                    Err(e) => {
                        error!("Failed to fetch URL {}: {:?}", _url, e);
                        return Err(e);
                    }
                };
                assert!(full_download);
                while let Some(item) = rsp_stream.next().await {
                    let mut _rw_stream = _arc_stream.write().unwrap();
                    let it = item.unwrap();
                    _rw_stream.data.extend_from_slice(&it.clone());
                    let count = _rw_stream.data.len();
                    debug!(
                        "{}",
                        format!(
                            "[{index}] ⬇️ {name:?} +{delta:?} {got:?}/{total:?}={percentage:.2}%",
                            name = &_name,
                            delta = it.len(),
                            got = count,
                            total  = _content_length,
                            percentage = (count as f64 / _content_length as f64) * 100.0
                        )
                        .yellow()
                    );
                }

                /* TODO:
                                   if file_name.ends_with("main_module.bootstrap.js") {
                                       content = String::from_utf8_lossy(&content)
                                           .replace(
                                               "'$requireDigestsPath?entrypoint=main_module.bootstrap.js'",
                                               "'$requireDigestsPath$entrypoint=main_module.bootstrap.js'",
                                           )
                                           .into();
                                   }
                */
                // match arc_entry.as_ref() {
                //     Entry::HttpFile(http_file) => {
                //         *http_file.download_pending.write().unwrap() = false;
                //     }
                //     _ => {}
                // }
                if let Some(callback) = on_done {
                    callback();
                }

                if log::log_enabled!(log::Level::Debug) {
                    let sha256 = {
                        let mut _rw_stream = _arc_stream.read().unwrap();
                        Sha256::digest(&_rw_stream.data)
                    };
                    debug!(
                        "{}",
                        format!(
                            "download [{index}] finished: stream_info {:?} url={:?} sha256={sha256:X}",
                            &_name,
                            _url.to_string()
                        )
                        .yellow()
                    );
                }
                Ok(())
            })
        });
        Some(Arc::clone(&arc_stream))
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
        let mut _file_name = file_name.to_string().unwrap();
        let index = self.next_id();

        if _file_name.ends_with("main_module.bootstrap.js") {
            _file_name = _file_name.replace(
                "$requireDigestsPath$entrypoint=main_module.bootstrap.js",
                "$requireDigestsPath?entrypoint=main_module.bootstrap.js",
            );
        }
        info!(
            "[{index}] {} {:?} {:?}  {} {:?}",
            "create_file: begin".green(),
            _file_name,
            create_disposition,
            access_flags_to_string(desired_access),
            get_path_by_pid(info.pid()),
        );
        if let Some(ignore) = &self.ignore {
            match ignore.matched(&_file_name.trim_matches('\\'), false) {
                // TODO: how to exactly ignore dir?
                ignore::Match::None => {
                    trace!("[{index}] create_file: not ignored file {:?}", &_file_name);
                }
                ignore::Match::Ignore(_) => {
                    info!("[{index}] create_file: ignoring file {:?}", &_file_name);
                    return Err(STATUS_ACCESS_DENIED);
                }
                ignore::Match::Whitelist(_) => {}
            }
            match ignore.matched(&_file_name.trim_matches('\\'), true) {
                // TODO: how to exactly ignore dir?
                ignore::Match::None => {
                    trace!("[{index}] create_file: not ignored dir {:?}", &_file_name);
                }
                ignore::Match::Ignore(_) => {
                    info!("[{index}] create_file: ignoring dir {:?}", &_file_name);
                    return Err(STATUS_ACCESS_DENIED);
                }
                ignore::Match::Whitelist(_) => {}
            }
        } else {
            info!("[{index}] create_file: no ignore {:?}", _file_name);
        }
        if create_disposition > FILE_MAXIMUM_DISPOSITION {
            return Err(STATUS_INVALID_PARAMETER);
        }
        let delete_on_close = create_options & FILE_DELETE_ON_CLOSE > 0;

        // find parent dir entry for file
        let path_info = path::split_path(index, self, file_name)?;
        if let Some((name, parent)) = path_info {
            // found parent DirEntry
            debug!(
                "[{index}] create_file: found parent DirEntry, name={:?} parent={:?}",
                name.file_name.to_string().unwrap(),
                parent.stat.read().unwrap().id
            );
            let children = parent.children.read().unwrap();
            // chick if the child's Entry is exist
            debug!(
                "[{index}] get {:?} in children: {:?}",
                name.file_name.to_string().unwrap(),
                children
                    .keys()
                    .map(|x| x.0.to_string().unwrap())
                    .collect::<Vec<_>>()
            );
            let token = info.requester_token().unwrap();
            if let Some(entry) = children.get(EntryNameRef::new(name.file_name)) {
                // file Entry exist
                let stat = entry.stat().read().unwrap();
                debug!(
                    "[{index}] create_file: found this entry, attrs={:#X}",
                    stat.attrs.value
                );

                let is_readonly = true;//stat.attrs.value & winnt::FILE_ATTRIBUTE_READONLY > 0;
                let is_hidden_system = stat.attrs.value & winnt::FILE_ATTRIBUTE_HIDDEN > 0
                    && stat.attrs.value & winnt::FILE_ATTRIBUTE_SYSTEM > 0
                    && !(file_attributes & winnt::FILE_ATTRIBUTE_HIDDEN > 0
                        && file_attributes & winnt::FILE_ATTRIBUTE_SYSTEM > 0);
                if is_readonly
                    && (desired_access & winnt::FILE_WRITE_DATA > 0
                        || desired_access & winnt::FILE_APPEND_DATA > 0)
                {
                    return Err(STATUS_ACCESS_DENIED);
                }
                if stat.delete_pending {
                    return Err(STATUS_DELETE_PENDING);
                }
                if is_readonly && delete_on_close {
                    return Err(STATUS_CANNOT_DELETE);
                }
                std::mem::drop(stat);
                let ret = if let Some(stream_info) = &name.stream_info {
                    if stream_info.check_default(entry.is_dir())? {
                        debug!("[{index}] stream_info: {}", "NONE".red());
                        None
                    } else {
                        let mut stat = entry.stat().write().unwrap();
                        let stream_name = EntryNameRef::new(stream_info.name);
                        debug!("[{index}] stream_info: {:?} {:?}", stream_name, stat.attrs);
                        if let Some(stream) =
                            stat.alt_streams.get(stream_name).map(|s| Arc::clone(s))
                        {
                            if stream.read().unwrap().delete_pending {
                                return Err(STATUS_DELETE_PENDING);
                            }
                            match create_disposition {
                                FILE_SUPERSEDE | FILE_OVERWRITE | FILE_OVERWRITE_IF => {
                                    if create_disposition != FILE_SUPERSEDE && is_readonly {
                                        return Err(STATUS_ACCESS_DENIED);
                                    }
                                    stat.attrs.value |= winnt::FILE_ATTRIBUTE_ARCHIVE;
                                    stat.update_mtime(SystemTime::now());
                                    stream.write().unwrap().data.clear();
                                }
                                FILE_CREATE => return Err(STATUS_OBJECT_NAME_COLLISION),
                                _ => (),
                            }
                            Some((stream, false))
                        } else {
                            if create_disposition == FILE_OPEN
                                || create_disposition == FILE_OVERWRITE
                            {
                                return Err(STATUS_OBJECT_NAME_NOT_FOUND);
                            }
                            if is_readonly {
                                return Err(STATUS_ACCESS_DENIED);
                            }
                            let stream = Arc::new(RwLock::new(AltStream::new()));
                            stat.update_atime(SystemTime::now());
                            assert!(stat
                                .alt_streams
                                .insert(EntryName(stream_info.name.to_owned()), Arc::clone(&stream))
                                .is_none());
                            // *context.alt_stream.write().unwrap() = Some(Arc::clone(&stream));
                            Some((stream, true))
                        }
                    }
                } else {
                    debug!("[{index}] stream_info: {}", "NONE".blue());
                    None
                };
                if let Some((stream, new_file_created)) = ret {
                    return Ok(CreateFileInfo {
                        context: EntryHandle::new(
                            index,
                            entry.clone(),
                            Some(stream),
                            delete_on_close,
                        ),
                        is_dir: false,
                        new_file_created,
                    });
                }
                match entry.as_ref() {
                    Entry::File(file) => {
                        if create_options & FILE_DIRECTORY_FILE > 0 {
                            return Err(STATUS_NOT_A_DIRECTORY);
                        }
                        match create_disposition {
                            FILE_SUPERSEDE | FILE_OVERWRITE | FILE_OVERWRITE_IF => {
                                if create_disposition != FILE_SUPERSEDE && is_readonly
                                    || is_hidden_system
                                {
                                    return Err(STATUS_ACCESS_DENIED);
                                }
                                file.data.write().unwrap().clear();
                                let mut stat = file.stat.write().unwrap();
                                stat.attrs = Attributes::new(
                                    file_attributes | winnt::FILE_ATTRIBUTE_ARCHIVE,
                                );
                                stat.update_mtime(SystemTime::now());
                            }
                            FILE_CREATE => return Err(STATUS_OBJECT_NAME_COLLISION),
                            _ => (),
                        }
                        Ok(CreateFileInfo {
                            context: EntryHandle::new(
                                index,
                                Arc::new(Entry::File(Arc::clone(&file))),
                                None,
                                delete_on_close,
                            ),
                            is_dir: false,
                            new_file_created: false,
                        })
                    }
                    Entry::HttpFile(file) => {
                        debug!(
                            "[{index}] create_file: is http file {:#X}",
                            file.stat.read().unwrap().attrs.value
                        );
                        if create_options & FILE_DIRECTORY_FILE > 0 {
                            return Err(STATUS_FILE_IS_A_DIRECTORY);
                        }
                        match create_disposition {
                            FILE_OPEN | FILE_OPEN_IF => Ok(CreateFileInfo {
                                context: {
                                    let _file = Arc::clone(&file);

                                    *_file.download_pending.write().unwrap() = true;
                                    let arc_entry = Arc::new(Entry::HttpFile(_file));
                                    let __file = Arc::clone(&file);
                                    EntryHandle::new(
                                        index,
                                        arc_entry,
                                        // None, // FIXME:
                                        self.create_new_http_stream(
                                            index,
                                            self.url
                                                .join(if _file_name.is_empty() {
                                                    "index.html"
                                                } else {
                                                    _file_name.as_str()
                                                })
                                                .unwrap(),
                                            // arc_entry,
                                            &_file_name,
                                            desired_access != winnt::FILE_READ_ATTRIBUTES,
                                            Some(Box::new(move || {
                                                *__file.download_pending.write().unwrap() = false;
                                            })),
                                        ),
                                        delete_on_close,
                                    )
                                },
                                is_dir: false,
                                new_file_created: false,
                            }),
                            FILE_CREATE => Err(STATUS_OBJECT_NAME_COLLISION),
                            _ => Err(STATUS_INVALID_PARAMETER),
                        }
                    }
                    Entry::Directory(dir) => {
                        if create_options & FILE_NON_DIRECTORY_FILE > 0 {
                            return Err(STATUS_FILE_IS_A_DIRECTORY);
                        }
                        match create_disposition {
                            FILE_OPEN | FILE_OPEN_IF => Ok(CreateFileInfo {
                                context: EntryHandle::new(
                                    index,
                                    Arc::new(Entry::Directory(Arc::clone(&dir))),
                                    None,
                                    delete_on_close,
                                ),
                                is_dir: true,
                                new_file_created: false,
                            }),
                            FILE_CREATE => Err(STATUS_OBJECT_NAME_COLLISION),
                            _ => Err(STATUS_INVALID_PARAMETER),
                        }
                    }
                }
            } else {
                // file not exist
                debug!(
                    "[{index}] create_file: NOT found this file entry {:?} {}",
                    _file_name,
                    create_disposition_to_string(create_disposition)
                );
                if parent.stat.read().unwrap().delete_pending {
                    return Err(STATUS_DELETE_PENDING);
                }
                std::mem::drop(children);
                let rw_children = &parent.children;
                if create_options & FILE_DIRECTORY_FILE > 0 {
                    match create_disposition {
                        FILE_CREATE | FILE_OPEN_IF => self.create_new(
                            index,
                            &name,
                            file_attributes,
                            delete_on_close,
                            security_context.AccessState.SecurityDescriptor,
                            token.as_raw_handle(),
                            &parent,
                            rw_children,
                            true,
                        ),
                        FILE_OPEN => Err(STATUS_OBJECT_NAME_NOT_FOUND),
                        _ => Err(STATUS_INVALID_PARAMETER),
                    }
                } else {
                    if create_disposition == FILE_OPEN || create_disposition == FILE_OVERWRITE {
                        // Err(STATUS_OBJECT_NAME_NOT_FOUND)

                        debug!(
                            "[{index}] create_file: --> create_new {:?}",
                            file_name.to_string().unwrap()
                        );
                        self.create_new_http(
                            index,
                            &file_name.to_string().unwrap(),
                            file_attributes | winnt::FILE_ATTRIBUTE_ARCHIVE,
                            delete_on_close,
                            security_context.AccessState.SecurityDescriptor,
                            token.as_raw_handle(),
                            &parent,
                            rw_children,
                            false,
                            desired_access != winnt::FILE_READ_ATTRIBUTES,
                        )
                    } else {
                        self.create_new(
                            index,
                            &name,
                            file_attributes | winnt::FILE_ATTRIBUTE_ARCHIVE,
                            delete_on_close,
                            security_context.AccessState.SecurityDescriptor,
                            token.as_raw_handle(),
                            &parent,
                            rw_children,
                            false,
                        )
                    }
                }
            }
        } else {
            debug!(
                "[{index}] create_file: NOT found parent DirEntry for {:?}",
                _file_name
            );
            // is Root
            if create_disposition == FILE_OPEN || create_disposition == FILE_OPEN_IF {
                if create_options & FILE_NON_DIRECTORY_FILE > 0 {
                    Err(STATUS_FILE_IS_A_DIRECTORY)
                } else {
                    debug!("[{index}] create_file: return ROOT {:?}", _file_name);
                    Ok(CreateFileInfo {
                        context: EntryHandle::new(
                            index,
                            Arc::new(Entry::Directory(Arc::clone(&self.root))),
                            None,
                            info.delete_on_close(),
                        ),
                        is_dir: true,
                        new_file_created: false,
                    })
                }
            } else {
                Err(STATUS_INVALID_PARAMETER)
            }
        }
    }

    fn close_file(
        &'h self,
        _file_name: &U16CStr,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) {
        let index = context.index;
        debug!(
            "[{index}] close_file: {name:?}",
            name = _file_name.to_string().unwrap()
        );
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
        let alt_stream = context.alt_stream.read().unwrap();
        let alt_streams = &context.entry.stat().read().unwrap().alt_streams;
        let index = context.index;
        let buflen = buffer.len();
        let full_len = alt_stream
            .as_ref()
            .map_or(0, |a| a.read().unwrap().content_length);
        info!(
            "[{index:?}] {}: {file_name:?} {found:?} [{offset},{end}]/{alt_stream},{full_len} {alt_streams:?}",
            "read_file".on_blue(),
            file_name = _file_name.to_string().unwrap(),
            found = if alt_stream.is_none() {
                "not found alt_stream"
            } else {
                "found alt_stream"
            },
            end = offset + buflen as i64,
            alt_stream = if alt_stream.is_none() {
                "".to_string()
            } else {
                format!(
                    "{}",
                    &alt_stream
                        .as_ref()
                        .unwrap()
                        .read()
                        .unwrap()
                        .data
                        .len()
                        .to_string()
                )
            },
            alt_streams = alt_streams
                .iter()
                .map(|(k, v)| (k.0.to_string().unwrap(), v.read().unwrap().data.len()))
                .collect::<Vec<_>>(),
        );
        let mut do_read = |data: &Vec<_>| {
            let offset = offset as usize;
            let len = std::cmp::min(buffer.len(), data.len() - offset);
            buffer[0..len].copy_from_slice(&data[offset..offset + len]);
            debug!(
                "[{index:?}] {}: {:?} read_len={:?}",
                "read_file".on_blue(),
                _file_name.to_string().unwrap(),
                len,
            );
            len as u32
        };
        if let Some(stream) = alt_stream.as_ref() {
            wait_with_timeout(
                || {
                    let len = stream.read().unwrap().data.len();
                    len == 0 || len < (offset as usize + buflen as usize)
                },
                5000,
                50,
                Some(|| {
                    return Err(STATUS_LOCK_NOT_GRANTED);
                }),
            )?;
            Ok(do_read(&stream.read().unwrap().data))
        } else if let Entry::File(file) = &context.entry.as_ref() {
            assert!(false, "can not be here! 2");
            Ok(do_read(&file.data.read().unwrap()))
        } else if let Entry::HttpFile(http_file) = &context.entry.as_ref() {
            wait_with_timeout(
                || *http_file.download_pending.read().unwrap(),
                5000,
                10,
                Some(|| {
                    error!("[{index:?}] Timeout while waiting for download to complete");
                    Err(STATUS_IO_TIMEOUT)
                }),
            )?;

            let data = http_file.get_data().unwrap();
            assert!(false, "can not be here!");
            let offset = offset as usize;
            let len = std::cmp::min(buffer.len(), data.len().saturating_sub(offset));
            buffer[..len].copy_from_slice(&data[offset..offset + len]);
            Ok(len as u32)
        } else {
            Err(STATUS_INVALID_DEVICE_REQUEST)
        }
    }

    #[allow(unused_variables)]
    fn write_file(
        &'h self,
        _file_name: &U16CStr,
        offset: i64,
        buffer: &[u8],
        info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<u32> {
        Err(STATUS_ACCESS_DENIED)
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
        let index = context.index;
        debug!(
            "[{index:?}] get_file_information: {:?} {:?}",
            _file_name.to_string().unwrap(),
            get_path_by_pid(_info.pid()),
        );
        let stat = context.entry.stat().read().unwrap();
        let alt_stream = context.alt_stream.read().unwrap();
        Ok(FileInfo {
            attributes: stat.attrs.get_output_attrs(context.is_dir()),
            creation_time: stat.ctime,
            last_access_time: stat.atime,
            last_write_time: stat.mtime,
            file_size: if let Some(stream) = alt_stream.as_ref() {
                let mut len = 0;
                wait_with_timeout(
                    || {
                        len = stream.read().unwrap().content_length;
                        len == 0
                    },
                    5000,
                    10,
                    Some(|| {
                        error!(
                            "[{index:?}] get_file_information: alt_stream {:?} timeout",
                            _file_name.to_string().unwrap()
                        );
                        Err(STATUS_IO_TIMEOUT)
                    }),
                )?;
                len
            } else {
                match &context.entry.as_ref() {
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
        let index = context.index;
        debug!(
            "[{index:?}] find_files: {:?}",
            _file_name.to_string().unwrap()
        );
        if context.alt_stream.read().unwrap().is_some() {
            return Err(STATUS_INVALID_DEVICE_REQUEST);
        }
        if let Entry::Directory(dir) = &context.entry.as_ref() {
            let children = dir.children.read().unwrap();
            for (k, v) in children.iter() {
                let stat = v.stat().read().unwrap();
                fill_find_data(&FindData {
                    attributes: stat.attrs.get_output_attrs(v.is_dir()),
                    creation_time: stat.ctime,
                    last_access_time: stat.atime,
                    last_write_time: stat.mtime,
                    file_size: match v.as_ref() {
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

    #[allow(unused_variables)]
    fn set_file_attributes(
        &'h self,
        _file_name: &U16CStr,
        file_attributes: u32,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        Err(STATUS_ACCESS_DENIED)
    }

    #[allow(unused_variables)]
    fn set_file_time(
        &'h self,
        _file_name: &U16CStr,
        creation_time: FileTimeOperation,
        last_access_time: FileTimeOperation,
        last_write_time: FileTimeOperation,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        Err(STATUS_ACCESS_DENIED)
    }

    #[allow(unused_variables)]
    fn delete_file(
        &'h self,
        _file_name: &U16CStr,
        info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        Err(STATUS_ACCESS_DENIED)
    }

    #[allow(unused_variables)]
    fn delete_directory(
        &'h self,
        _file_name: &U16CStr,
        info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        Err(STATUS_ACCESS_DENIED)
    }

    #[allow(unused_variables)]
    fn move_file(
        &'h self,
        file_name: &U16CStr,
        new_file_name: &U16CStr,
        replace_if_existing: bool,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        Err(STATUS_ACCESS_DENIED)
    }

    #[allow(unused_variables)]
    fn set_end_of_file(
        &'h self,
        _file_name: &U16CStr,
        _offset: i64,
        _info: &OperationInfo<'c, 'h, Self>,
        _context: &'c Self::Context,
    ) -> OperationResult<()> {
        Err(STATUS_ACCESS_DENIED)
    }

    #[allow(unused_variables)]
    fn set_allocation_size(
        &'h self,
        _file_name: &U16CStr,
        alloc_size: i64,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        Err(STATUS_ACCESS_DENIED)
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
            name: U16CString::from_str("Http FileSystem").unwrap(),
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

    #[allow(unused_variables)]
    fn set_file_security(
        &'h self,
        _file_name: &U16CStr,
        security_information: u32,
        security_descriptor: winnt::PSECURITY_DESCRIPTOR,
        _buffer_length: u32,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        Err(STATUS_ACCESS_DENIED)
    }

    fn find_streams(
        &'h self,
        _file_name: &U16CStr,
        mut fill_find_stream_data: impl FnMut(&FindStreamData) -> FillDataResult,
        _info: &OperationInfo<'c, 'h, Self>,
        context: &'c Self::Context,
    ) -> OperationResult<()> {
        let index = context.index;
        debug!(
            "[{index:?}] find_streams: {:?}",
            _file_name.to_string().unwrap()
        );
        if let Entry::File(file) = &context.entry.as_ref() {
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
