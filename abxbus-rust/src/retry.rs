use std::{
    cell::RefCell,
    collections::HashMap,
    fs::{self, OpenOptions},
    future::Future,
    io::Write,
    path::{Path, PathBuf},
    pin::Pin,
    process,
    sync::{Arc, Condvar, Mutex, OnceLock},
    task::{Context, Poll},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde_json::json;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, PartialEq)]
pub enum RetryError {
    Timeout {
        timeout_seconds: f64,
        attempt: usize,
    },
    SemaphoreTimeout {
        semaphore_name: String,
        semaphore_limit: usize,
        timeout_seconds: f64,
    },
}

#[derive(Debug)]
struct RetrySemaphore {
    state: Mutex<RetrySemaphoreState>,
    cvar: Condvar,
}

#[derive(Debug)]
struct RetrySemaphoreState {
    limit: usize,
    in_use: usize,
}

impl RetrySemaphore {
    fn new(limit: usize) -> Self {
        Self {
            state: Mutex::new(RetrySemaphoreState { limit, in_use: 0 }),
            cvar: Condvar::new(),
        }
    }

    fn try_acquire(&self) -> bool {
        let mut state = self.state.lock().expect("retry semaphore state");
        if state.in_use >= state.limit {
            return false;
        }
        state.in_use += 1;
        true
    }

    fn acquire_sync(&self, timeout: Option<Duration>) -> bool {
        let mut state = self.state.lock().expect("retry semaphore state");
        match timeout {
            None => {
                while state.in_use >= state.limit {
                    state = self.cvar.wait(state).expect("retry semaphore wait");
                }
                state.in_use += 1;
                true
            }
            Some(timeout) if timeout.is_zero() => {
                while state.in_use >= state.limit {
                    state = self.cvar.wait(state).expect("retry semaphore wait");
                }
                state.in_use += 1;
                true
            }
            Some(timeout) => {
                let deadline = Instant::now() + timeout;
                while state.in_use >= state.limit {
                    let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                        return false;
                    };
                    let (next_state, wait_result) = self
                        .cvar
                        .wait_timeout(state, remaining)
                        .expect("retry semaphore wait");
                    state = next_state;
                    if wait_result.timed_out() && state.in_use >= state.limit {
                        return false;
                    }
                }
                state.in_use += 1;
                true
            }
        }
    }

    fn release(&self) {
        let mut state = self.state.lock().expect("retry semaphore state");
        state.in_use = state.in_use.saturating_sub(1);
        self.cvar.notify_one();
    }
}

static SEMAPHORES: OnceLock<Mutex<HashMap<String, Arc<RetrySemaphore>>>> = OnceLock::new();
const MULTIPROCESS_SEMAPHORE_DIRNAME: &str = "browser_use_semaphores";
const MULTIPROCESS_STALE_LOCK_SECONDS: u64 = 300;

thread_local! {
    static HELD_SEMAPHORES: RefCell<HashMap<String, usize>> = RefCell::new(HashMap::new());
}

pub struct RetrySemaphoreGuard {
    key: String,
    semaphore: Option<Arc<RetrySemaphore>>,
    multiprocess_lock: Option<MultiprocessLock>,
    acquired: bool,
    mark_held: bool,
}

impl Drop for RetrySemaphoreGuard {
    fn drop(&mut self) {
        if self.acquired {
            if let Some(semaphore) = &self.semaphore {
                semaphore.release();
            }
            if let Some(lock) = &self.multiprocess_lock {
                release_multiprocess_lock(lock);
            }
            if self.mark_held {
                decrement_held(&self.key);
            }
            self.acquired = false;
        }
    }
}

pub struct HeldFuture<F> {
    key: String,
    future: F,
}

impl<F: Future> Future for HeldFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        increment_held(&this.key);
        let result = unsafe { Pin::new_unchecked(&mut this.future) }.poll(cx);
        decrement_held(&this.key);
        result
    }
}

pub fn with_held_async<F: Future>(key: String, future: F) -> HeldFuture<F> {
    HeldFuture { key, future }
}

fn held_contains(key: &str) -> bool {
    HELD_SEMAPHORES.with(|held| held.borrow().get(key).copied().unwrap_or(0) > 0)
}

fn increment_held(key: &str) {
    HELD_SEMAPHORES.with(|held| {
        let mut held = held.borrow_mut();
        *held.entry(key.to_string()).or_insert(0) += 1;
    });
}

#[derive(Debug)]
struct MultiprocessLock {
    slot_file: PathBuf,
    token: String,
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn multiprocess_semaphore_dir() -> PathBuf {
    std::env::temp_dir().join(MULTIPROCESS_SEMAPHORE_DIRNAME)
}

fn multiprocess_lock_prefix(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    let hex = format!("{digest:x}");
    hex[..40].to_string()
}

#[cfg(unix)]
fn process_is_alive(pid: u64) -> bool {
    unsafe { libc::kill(pid as libc::pid_t, 0) == 0 }
}

#[cfg(not(unix))]
fn process_is_alive(_pid: u64) -> bool {
    true
}

fn maybe_remove_stale_lock(slot_file: &Path) {
    let Ok(raw) = fs::read_to_string(slot_file) else {
        return;
    };
    let current_owner: Option<serde_json::Value> = serde_json::from_str(raw.trim()).ok();
    let current_pid = current_owner
        .as_ref()
        .and_then(|owner| owner.get("pid"))
        .and_then(serde_json::Value::as_u64);

    if let Some(pid) = current_pid {
        if process_is_alive(pid) {
            return;
        }
        let _ = fs::remove_file(slot_file);
        return;
    }

    if let Ok(metadata) = fs::metadata(slot_file) {
        if let Ok(modified) = metadata.modified() {
            if modified.elapsed().unwrap_or_default()
                >= Duration::from_secs(MULTIPROCESS_STALE_LOCK_SECONDS)
            {
                let _ = fs::remove_file(slot_file);
            }
        }
    }
}

fn try_create_multiprocess_lock(
    semaphore_dir: &Path,
    lock_prefix: &str,
    slot: usize,
    key: &str,
) -> Result<Option<MultiprocessLock>, std::io::Error> {
    let slot_file = semaphore_dir.join(format!("{lock_prefix}.{slot:02}.lock"));
    let token = format!(
        "{}:{}:{:?}",
        process::id(),
        now_nanos(),
        thread::current().id()
    );
    let owner = json!({
        "token": token,
        "pid": process::id(),
        "semaphore_name": key,
        "created_at_ms": now_millis(),
    })
    .to_string();

    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&slot_file)
    {
        Ok(mut file) => {
            file.write_all(owner.as_bytes())?;
            file.sync_all()?;
            Ok(Some(MultiprocessLock { slot_file, token }))
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            maybe_remove_stale_lock(&slot_file);
            Ok(None)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(semaphore_dir)?;
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn release_multiprocess_lock(lock: &MultiprocessLock) {
    let Ok(raw) = fs::read_to_string(&lock.slot_file) else {
        return;
    };
    let current_owner: Option<serde_json::Value> = serde_json::from_str(raw.trim()).ok();
    let current_token = current_owner
        .as_ref()
        .and_then(|owner| owner.get("token"))
        .and_then(serde_json::Value::as_str);
    if current_token == Some(lock.token.as_str()) {
        let _ = fs::remove_file(&lock.slot_file);
    }
}

fn multiprocess_timeout_error(
    key: String,
    limit: usize,
    timeout: Option<f64>,
    semaphore_lax: bool,
) -> Result<Option<RetrySemaphoreGuard>, RetryError> {
    if semaphore_lax {
        Ok(None)
    } else {
        Err(RetryError::SemaphoreTimeout {
            semaphore_name: key,
            semaphore_limit: limit,
            timeout_seconds: timeout.unwrap_or(0.0),
        })
    }
}

pub fn acquire_multiprocess_semaphore_sync(
    key: String,
    limit: usize,
    timeout_seconds: Option<f64>,
    semaphore_lax: bool,
    mark_held: bool,
) -> Result<Option<RetrySemaphoreGuard>, RetryError> {
    let semaphore_dir = multiprocess_semaphore_dir();
    fs::create_dir_all(&semaphore_dir).map_err(|_| RetryError::SemaphoreTimeout {
        semaphore_name: key.clone(),
        semaphore_limit: limit,
        timeout_seconds: timeout_seconds.unwrap_or(0.0),
    })?;

    let lock_prefix = multiprocess_lock_prefix(&key);
    let start = Instant::now();
    let timeout = timeout_seconds.and_then(duration_from_seconds);
    let mut retry_delay = Duration::from_millis(100);

    loop {
        if timeout.is_some_and(|timeout| start.elapsed() >= timeout) {
            break;
        }

        for slot in 0..limit {
            match try_create_multiprocess_lock(&semaphore_dir, &lock_prefix, slot, &key) {
                Ok(Some(lock)) => {
                    if mark_held {
                        increment_held(&key);
                    }
                    return Ok(Some(RetrySemaphoreGuard {
                        key,
                        semaphore: None,
                        multiprocess_lock: Some(lock),
                        acquired: true,
                        mark_held,
                    }));
                }
                Ok(None) => {}
                Err(_) => {}
            }
        }

        let sleep_for = timeout
            .and_then(|timeout| timeout.checked_sub(start.elapsed()))
            .map(|remaining| remaining.min(retry_delay))
            .unwrap_or(retry_delay);
        if sleep_for.is_zero() {
            break;
        }
        thread::sleep(sleep_for);
        retry_delay = (retry_delay * 2).min(Duration::from_secs(1));
    }

    multiprocess_timeout_error(key, limit, timeout_seconds, semaphore_lax)
}

pub async fn acquire_multiprocess_semaphore_async(
    key: String,
    limit: usize,
    timeout_seconds: Option<f64>,
    semaphore_lax: bool,
) -> Result<Option<RetrySemaphoreGuard>, RetryError> {
    let semaphore_dir = multiprocess_semaphore_dir();
    fs::create_dir_all(&semaphore_dir).map_err(|_| RetryError::SemaphoreTimeout {
        semaphore_name: key.clone(),
        semaphore_limit: limit,
        timeout_seconds: timeout_seconds.unwrap_or(0.0),
    })?;

    let lock_prefix = multiprocess_lock_prefix(&key);
    let start = Instant::now();
    let timeout = timeout_seconds.and_then(duration_from_seconds);
    let mut retry_delay = Duration::from_millis(100);

    loop {
        if timeout.is_some_and(|timeout| start.elapsed() >= timeout) {
            break;
        }

        for slot in 0..limit {
            match try_create_multiprocess_lock(&semaphore_dir, &lock_prefix, slot, &key) {
                Ok(Some(lock)) => {
                    return Ok(Some(RetrySemaphoreGuard {
                        key,
                        semaphore: None,
                        multiprocess_lock: Some(lock),
                        acquired: true,
                        mark_held: false,
                    }));
                }
                Ok(None) => {}
                Err(_) => {}
            }
        }

        let sleep_for = timeout
            .and_then(|timeout| timeout.checked_sub(start.elapsed()))
            .map(|remaining| remaining.min(retry_delay))
            .unwrap_or(retry_delay);
        if sleep_for.is_zero() {
            break;
        }
        futures_timer::Delay::new(sleep_for).await;
        retry_delay = (retry_delay * 2).min(Duration::from_secs(1));
    }

    multiprocess_timeout_error(key, limit, timeout_seconds, semaphore_lax)
}

fn decrement_held(key: &str) {
    HELD_SEMAPHORES.with(|held| {
        let mut held = held.borrow_mut();
        if let Some(count) = held.get_mut(key) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                held.remove(key);
            }
        }
    });
}

fn registry() -> &'static Mutex<HashMap<String, Arc<RetrySemaphore>>> {
    SEMAPHORES.get_or_init(|| Mutex::new(HashMap::new()))
}

fn get_or_create_semaphore(key: &str, limit: usize) -> Arc<RetrySemaphore> {
    let mut semaphores = registry().lock().expect("retry semaphore registry");
    semaphores
        .entry(key.to_string())
        .or_insert_with(|| Arc::new(RetrySemaphore::new(limit)))
        .clone()
}

pub fn duration_from_seconds(seconds: f64) -> Option<Duration> {
    (seconds > 0.0).then(|| Duration::from_secs_f64(seconds))
}

pub fn scoped_semaphore_key(
    base_name: String,
    scope: &str,
    type_name: &str,
    instance_key: Option<usize>,
) -> String {
    match scope {
        "class" if !type_name.is_empty() => format!("{type_name}.{base_name}"),
        "instance" => instance_key
            .map(|key| format!("{key}.{base_name}"))
            .unwrap_or(base_name),
        _ => base_name,
    }
}

pub fn acquire_semaphore_sync(
    key: String,
    scope: &str,
    limit: Option<usize>,
    semaphore_timeout_seconds: Option<f64>,
    timeout_seconds: Option<f64>,
    semaphore_lax: bool,
) -> Result<Option<RetrySemaphoreGuard>, RetryError> {
    let Some(limit) = limit.filter(|limit| *limit > 0) else {
        return Ok(None);
    };
    if held_contains(&key) {
        return Ok(None);
    }

    let timeout = semaphore_timeout_seconds
        .or_else(|| timeout_seconds.map(|timeout| timeout * limit.saturating_sub(1).max(1) as f64));
    if scope == "multiprocess" {
        return acquire_multiprocess_semaphore_sync(key, limit, timeout, semaphore_lax, true);
    }

    let semaphore = get_or_create_semaphore(&key, limit);
    let acquired = semaphore.acquire_sync(timeout.and_then(duration_from_seconds));
    if acquired {
        increment_held(&key);
        return Ok(Some(RetrySemaphoreGuard {
            key,
            semaphore: Some(semaphore),
            multiprocess_lock: None,
            acquired,
            mark_held: true,
        }));
    }

    if semaphore_lax {
        Ok(None)
    } else {
        Err(RetryError::SemaphoreTimeout {
            semaphore_name: key,
            semaphore_limit: limit,
            timeout_seconds: timeout.unwrap_or(0.0),
        })
    }
}

pub async fn acquire_semaphore_async(
    key: String,
    scope: &str,
    limit: Option<usize>,
    semaphore_timeout_seconds: Option<f64>,
    timeout_seconds: Option<f64>,
    semaphore_lax: bool,
) -> Result<Option<RetrySemaphoreGuard>, RetryError> {
    let Some(limit) = limit.filter(|limit| *limit > 0) else {
        return Ok(None);
    };
    if held_contains(&key) {
        return Ok(None);
    }

    let timeout = semaphore_timeout_seconds
        .or_else(|| timeout_seconds.map(|timeout| timeout * limit.saturating_sub(1).max(1) as f64));
    if scope == "multiprocess" {
        return acquire_multiprocess_semaphore_async(key, limit, timeout, semaphore_lax).await;
    }

    let deadline = timeout
        .and_then(duration_from_seconds)
        .map(|duration| Instant::now() + duration);
    let semaphore = get_or_create_semaphore(&key, limit);
    loop {
        if semaphore.try_acquire() {
            return Ok(Some(RetrySemaphoreGuard {
                key,
                semaphore: Some(semaphore),
                multiprocess_lock: None,
                acquired: true,
                mark_held: false,
            }));
        }
        if let Some(deadline) = deadline {
            if Instant::now() >= deadline {
                break;
            }
        }
        futures_timer::Delay::new(Duration::from_millis(1)).await;
    }

    if semaphore_lax {
        Ok(None)
    } else {
        Err(RetryError::SemaphoreTimeout {
            semaphore_name: key,
            semaphore_limit: limit,
            timeout_seconds: timeout.unwrap_or(0.0),
        })
    }
}

#[macro_export]
macro_rules! __retry_max_attempts {
    ($default:expr;) => { $default };
    ($default:expr; max_attempts = $value:expr $(, $($rest:tt)*)?) => { $value };
    ($default:expr; $key:ident = $value:expr $(, $($rest:tt)*)?) => {
        $crate::__retry_max_attempts!($default; $($($rest)*)?)
    };
}

#[macro_export]
macro_rules! __retry_retry_after {
    ($default:expr;) => { $default };
    ($default:expr; retry_after = $value:expr $(, $($rest:tt)*)?) => { Some($value as f64) };
    ($default:expr; $key:ident = $value:expr $(, $($rest:tt)*)?) => { $crate::__retry_retry_after!($default; $($($rest)*)?) };
}

#[macro_export]
macro_rules! __retry_retry_backoff_factor {
    ($default:expr;) => { $default };
    ($default:expr; retry_backoff_factor = $value:expr $(, $($rest:tt)*)?) => { Some($value as f64) };
    ($default:expr; $key:ident = $value:expr $(, $($rest:tt)*)?) => { $crate::__retry_retry_backoff_factor!($default; $($($rest)*)?) };
}

#[macro_export]
macro_rules! __retry_timeout {
    ($default:expr;) => { $default };
    ($default:expr; timeout = $value:expr $(, $($rest:tt)*)?) => { Some($value as f64) };
    ($default:expr; $key:ident = $value:expr $(, $($rest:tt)*)?) => { $crate::__retry_timeout!($default; $($($rest)*)?) };
}

#[macro_export]
macro_rules! __retry_semaphore_timeout {
    ($default:expr;) => { $default };
    ($default:expr; semaphore_timeout = $value:expr $(, $($rest:tt)*)?) => { Some($value as f64) };
    ($default:expr; $key:ident = $value:expr $(, $($rest:tt)*)?) => { $crate::__retry_semaphore_timeout!($default; $($($rest)*)?) };
}

#[macro_export]
macro_rules! __retry_semaphore_limit {
    ($default:expr;) => { $default };
    ($default:expr; semaphore_limit = $value:expr $(, $($rest:tt)*)?) => { Some($value as usize) };
    ($default:expr; $key:ident = $value:expr $(, $($rest:tt)*)?) => { $crate::__retry_semaphore_limit!($default; $($($rest)*)?) };
}

#[macro_export]
macro_rules! __retry_semaphore_lax {
    ($default:expr;) => { $default };
    ($default:expr; semaphore_lax = $value:expr $(, $($rest:tt)*)?) => { $value };
    ($default:expr; $key:ident = $value:expr $(, $($rest:tt)*)?) => { $crate::__retry_semaphore_lax!($default; $($($rest)*)?) };
}

#[macro_export]
macro_rules! __retry_semaphore_scope {
    ($default:expr;) => { $default };
    ($default:expr; semaphore_scope = $value:expr $(, $($rest:tt)*)?) => { $value };
    ($default:expr; $key:ident = $value:expr $(, $($rest:tt)*)?) => { $crate::__retry_semaphore_scope!($default; $($($rest)*)?) };
}

#[macro_export]
macro_rules! __retry_semaphore_name {
    ($default:expr;) => { $default };
    ($default:expr; semaphore_name = $value:expr $(, $($rest:tt)*)?) => { ($value).to_string() };
    ($default:expr; $key:ident = $value:expr $(, $($rest:tt)*)?) => {
        $crate::__retry_semaphore_name!($default; $($($rest)*)?)
    };
}

#[macro_export]
macro_rules! __retry_should_retry {
    ($error:expr;) => { true };
    ($error:expr; retry_if = $value:expr $(, $($rest:tt)*)?) => { $value($error) };
    ($error:expr; $key:ident = $value:expr $(, $($rest:tt)*)?) => {
        $crate::__retry_should_retry!($error; $($($rest)*)?)
    };
}

#[macro_export]
macro_rules! __retry_sync_fn {
    (($($opts:tt)*) $vis:vis fn $name:ident($($args:tt)*) -> Result<$ok:ty, $err:ty> $body:block, $instance_key:expr, $type_name:expr) => {
        $vis fn $name($($args)*) -> Result<$ok, $err> {
            let __max_attempts: usize = ::std::cmp::max(1, $crate::__retry_max_attempts!(1usize; $($opts)*));
            let __retry_after: f64 = f64::max($crate::__retry_retry_after!(Some(0.0); $($opts)*).unwrap_or(0.0), 0.0);
            let __retry_backoff_factor: f64 = $crate::__retry_retry_backoff_factor!(Some(1.0); $($opts)*).unwrap_or(1.0);
            let __timeout: Option<f64> = $crate::__retry_timeout!(None; $($opts)*);
            let __semaphore_limit: Option<usize> = $crate::__retry_semaphore_limit!(None; $($opts)*);
            let __semaphore_lax: bool = $crate::__retry_semaphore_lax!(true; $($opts)*);
            let __semaphore_timeout: Option<f64> = $crate::__retry_semaphore_timeout!(None; $($opts)*);
            let __semaphore_scope: &str = $crate::__retry_semaphore_scope!("global"; $($opts)*);
            let __semaphore_name: String = $crate::__retry_semaphore_name!(stringify!($name).to_string(); $($opts)*);
            let __semaphore_key = $crate::retry::scoped_semaphore_key(__semaphore_name, __semaphore_scope, $type_name, $instance_key);
            let __guard = $crate::retry::acquire_semaphore_sync(
                __semaphore_key,
                __semaphore_scope,
                __semaphore_limit,
                __semaphore_timeout,
                __timeout,
                __semaphore_lax,
            ).map_err(<$err as ::std::convert::From<$crate::retry::RetryError>>::from)?;
            let _ = &__guard;
            for __attempt in 1..=__max_attempts {
                let __attempt_started = ::std::time::Instant::now();
                let __result: Result<$ok, $err> = (|| $body)();
                let __result = match (__result, __timeout) {
                    (Ok(_), Some(__timeout_seconds)) if __attempt_started.elapsed() > ::std::time::Duration::from_secs_f64(__timeout_seconds) => {
                        Err(<$err as ::std::convert::From<$crate::retry::RetryError>>::from($crate::retry::RetryError::Timeout {
                            timeout_seconds: __timeout_seconds,
                            attempt: __attempt,
                        }))
                    }
                    (result, _) => result,
                };
                match __result {
                    Ok(value) => {
                        let _ = &__guard;
                        return Ok(value);
                    }
                    Err(error) => {
                        if !$crate::__retry_should_retry!(&error; $($opts)*) || __attempt >= __max_attempts {
                            let _ = &__guard;
                            return Err(error);
                        }
                        let __delay = __retry_after * __retry_backoff_factor.powi((__attempt - 1) as i32);
                        if __delay > 0.0 {
                            ::std::thread::sleep(::std::time::Duration::from_secs_f64(__delay));
                        }
                    }
                }
            }
            unreachable!("retry loop should return")
        }
    };
}

#[macro_export]
macro_rules! __retry_async_fn {
    (($($opts:tt)*) $vis:vis async fn $name:ident($($args:tt)*) -> Result<$ok:ty, $err:ty> $body:block, $instance_key:expr, $type_name:expr) => {
        $vis async fn $name($($args)*) -> Result<$ok, $err> {
            let __max_attempts: usize = ::std::cmp::max(1, $crate::__retry_max_attempts!(1usize; $($opts)*));
            let __retry_after: f64 = f64::max($crate::__retry_retry_after!(Some(0.0); $($opts)*).unwrap_or(0.0), 0.0);
            let __retry_backoff_factor: f64 = $crate::__retry_retry_backoff_factor!(Some(1.0); $($opts)*).unwrap_or(1.0);
            let __timeout: Option<f64> = $crate::__retry_timeout!(None; $($opts)*);
            let __semaphore_limit: Option<usize> = $crate::__retry_semaphore_limit!(None; $($opts)*);
            let __semaphore_lax: bool = $crate::__retry_semaphore_lax!(true; $($opts)*);
            let __semaphore_timeout: Option<f64> = $crate::__retry_semaphore_timeout!(None; $($opts)*);
            let __semaphore_scope: &str = $crate::__retry_semaphore_scope!("global"; $($opts)*);
            let __semaphore_name: String = $crate::__retry_semaphore_name!(stringify!($name).to_string(); $($opts)*);
            let __semaphore_key = $crate::retry::scoped_semaphore_key(__semaphore_name, __semaphore_scope, $type_name, $instance_key);
            let __held_key = __semaphore_key.clone();
            let __guard = $crate::retry::acquire_semaphore_async(
                __semaphore_key,
                __semaphore_scope,
                __semaphore_limit,
                __semaphore_timeout,
                __timeout,
                __semaphore_lax,
            ).await.map_err(<$err as ::std::convert::From<$crate::retry::RetryError>>::from)?;
            let _ = &__guard;
            for __attempt in 1..=__max_attempts {
                let __result: Result<$ok, $err> = if let Some(__timeout_seconds) = __timeout {
                    let __future = $crate::retry::with_held_async(__held_key.clone(), async $body);
                    futures::pin_mut!(__future);
                    let __delay = futures_timer::Delay::new(::std::time::Duration::from_secs_f64(__timeout_seconds));
                    futures::pin_mut!(__delay);
                    match futures::future::select(__future, __delay).await {
                        futures::future::Either::Left((result, _)) => result,
                        futures::future::Either::Right((_, _)) => Err(<$err as ::std::convert::From<$crate::retry::RetryError>>::from($crate::retry::RetryError::Timeout {
                            timeout_seconds: __timeout_seconds,
                            attempt: __attempt,
                        })),
                    }
                } else {
                    $crate::retry::with_held_async(__held_key.clone(), async $body).await
                };
                match __result {
                    Ok(value) => {
                        let _ = &__guard;
                        return Ok(value);
                    }
                    Err(error) => {
                        if !$crate::__retry_should_retry!(&error; $($opts)*) || __attempt >= __max_attempts {
                            let _ = &__guard;
                            return Err(error);
                        }
                        let __delay = __retry_after * __retry_backoff_factor.powi((__attempt - 1) as i32);
                        if __delay > 0.0 {
                            futures_timer::Delay::new(::std::time::Duration::from_secs_f64(__delay)).await;
                        }
                    }
                }
            }
            unreachable!("retry loop should return")
        }
    };
}

#[macro_export]
macro_rules! retry {
    ($($opt_key:ident = $opt_value:expr),* $(,)? ; $vis:vis async fn $name:ident(&$self_arg:ident $(, $arg:ident : $argty:ty)* $(,)?) -> Result<$ok:ty, $err:ty> $body:block) => {
        $crate::__retry_async_fn!(($($opt_key = $opt_value),*) $vis async fn $name(&$self_arg $(, $arg : $argty)*) -> Result<$ok, $err> $body, Some($self_arg as *const Self as usize), ::std::any::type_name::<Self>());
    };
    ($($opt_key:ident = $opt_value:expr),* $(,)? ; $vis:vis fn $name:ident(&$self_arg:ident $(, $arg:ident : $argty:ty)* $(,)?) -> Result<$ok:ty, $err:ty> $body:block) => {
        $crate::__retry_sync_fn!(($($opt_key = $opt_value),*) $vis fn $name(&$self_arg $(, $arg : $argty)*) -> Result<$ok, $err> $body, Some($self_arg as *const Self as usize), ::std::any::type_name::<Self>());
    };
    ($($opt_key:ident = $opt_value:expr),* $(,)? ; $vis:vis async fn $name:ident($($arg:ident : $argty:ty),* $(,)?) -> Result<$ok:ty, $err:ty> $body:block) => {
        $crate::__retry_async_fn!(($($opt_key = $opt_value),*) $vis async fn $name($($arg : $argty),*) -> Result<$ok, $err> $body, None, "");
    };
    ($($opt_key:ident = $opt_value:expr),* $(,)? ; $vis:vis fn $name:ident($($arg:ident : $argty:ty),* $(,)?) -> Result<$ok:ty, $err:ty> $body:block) => {
        $crate::__retry_sync_fn!(($($opt_key = $opt_value),*) $vis fn $name($($arg : $argty),*) -> Result<$ok, $err> $body, None, "");
    };
}
