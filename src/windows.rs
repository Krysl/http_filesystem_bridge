use std::{ffi::OsString, os::windows::ffi::OsStringExt, ptr::null_mut};

use winapi::{
    shared::minwindef::FALSE,
    um::{
        processthreadsapi::OpenProcess,
        psapi::GetModuleFileNameExW,
        winnt::{PROCESS_QUERY_INFORMATION, PROCESS_VM_READ},
    },
};

pub fn get_path_by_pid(pid: u32) -> Option<String> {
    let process = unsafe { OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, FALSE, pid) };
    if process.is_null() {
        return None;
    }
    let mut path = vec![0u16; 260];
    if unsafe { GetModuleFileNameExW(process, null_mut(), path.as_mut_ptr(), path.len() as u32) }
        == 0
    {
        None
    } else {
        let end = path.iter().position(|&c| c == 0).unwrap_or(path.len());
        Some(
            OsString::from_wide(&path[..end])
                .to_string_lossy()
                .into_owned(),
        )
    }
}
