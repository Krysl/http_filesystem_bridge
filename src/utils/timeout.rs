use winapi::shared::{ntdef::NTSTATUS, ntstatus::STATUS_IO_TIMEOUT};

pub fn wait_with_timeout<F, E>(
    mut should_continue: F,
    mut timeout: i64,
    delay_ms: u64,
    on_timeout: Option<E>,
) -> Result<i64, NTSTATUS>
where
    F: FnMut() -> bool,
    E: FnOnce() -> Result<i64, NTSTATUS>,
{
    while timeout > 0 && should_continue() {
        std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        timeout -= delay_ms as i64;
    }
    if timeout <= 0 {
        if let Some(on_timeout) = on_timeout {
            return on_timeout();
        } else {
            return Err(STATUS_IO_TIMEOUT);
        }
    }
    Ok(timeout)
}
