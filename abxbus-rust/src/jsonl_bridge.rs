//! Forward ordinary event JSON through a local newline-delimited file.
use crate::{
    base_event::BaseEvent,
    event_bus::EventBus,
    event_handler::EventHandler,
    id::uuid_v7_string,
    typed::{EventMarker, TypedEventHandler, TypedEventObject},
};
use parking_lot::Mutex;
use serde_json::Value;
use std::{
    fs::{self, File, OpenOptions},
    future::Future,
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{mpsc, Arc},
    thread,
    time::Duration,
};

enum Listener {
    New,
    Running(mpsc::Sender<()>, thread::JoinHandle<()>),
    Closed,
}

/// Append one wire event per line and redispatch newly read events.
pub struct JsonlEventBridge {
    path: PathBuf,
    poll_interval: Duration,
    inbound_bus: Arc<EventBus>,
    listener: Mutex<Listener>,
}

impl JsonlEventBridge {
    /// The default polling interval is 250 ms. A zero duration uses the default.
    pub fn new(
        path: impl Into<PathBuf>,
        poll_interval: Option<Duration>,
        name: Option<String>,
    ) -> Self {
        let name = name.filter(|name| !name.is_empty()).unwrap_or_else(|| {
            let id = uuid_v7_string();
            format!("JSONLEventBridge_{}", &id[id.len() - 8..])
        });
        Self {
            path: path.into(),
            poll_interval: poll_interval
                .filter(|interval| !interval.is_zero())
                .unwrap_or(Duration::from_millis(250))
                .max(Duration::from_millis(1)),
            inbound_bus: EventBus::new_with_history(Some(name), Some(0), true),
            listener: Mutex::new(Listener::New),
        }
    }

    /// Start listening at the current end of the file.
    pub fn start(&self) -> io::Result<()> {
        let mut listener = self.listener.lock();
        match &*listener {
            Listener::Running(..) => return Ok(()),
            Listener::Closed => {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "JSONL bridge is closed",
                ))
            }
            Listener::New => (),
        }
        let offset = append_file(&self.path)?.metadata()?.len();
        let (stop, stopped) = mpsc::channel();
        let path = self.path.clone();
        let interval = self.poll_interval;
        let bus = self.inbound_bus.clone();
        let thread = thread::Builder::new().spawn(move || {
            let mut offset = offset;
            let mut pending = Vec::new();
            while let Err(mpsc::RecvTimeoutError::Timeout) = stopped.recv_timeout(interval) {
                let _ = poll_lines(&path, &mut offset, &mut pending, &bus);
            }
        })?;
        *listener = Listener::Running(stop, thread);
        Ok(())
    }

    #[track_caller]
    pub fn on<M>(
        &self,
        marker: M,
        handler: impl TypedEventHandler<M::Event>,
    ) -> io::Result<EventHandler>
    where
        M: EventMarker,
        M::Event: TypedEventObject,
    {
        self.start()?;
        Ok(self.inbound_bus.on(marker, handler))
    }

    #[track_caller]
    pub fn on_raw<F, Fut>(
        &self,
        pattern: &str,
        handler_name: &str,
        handler: F,
    ) -> io::Result<EventHandler>
    where
        F: Fn(Arc<BaseEvent>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, String>> + Send + 'static,
    {
        self.start()?;
        Ok(self.inbound_bus.on_raw(pattern, handler_name, handler))
    }

    #[track_caller]
    pub fn on_raw_sync<F>(
        &self,
        pattern: &str,
        handler_name: &str,
        handler: F,
    ) -> io::Result<EventHandler>
    where
        F: Fn(Arc<BaseEvent>) -> Result<Value, String> + Send + Sync + 'static,
    {
        self.start()?;
        Ok(self.inbound_bus.on_raw_sync(pattern, handler_name, handler))
    }

    /// Append the original event's wire data without changing it.
    pub fn emit_base(&self, event: Arc<BaseEvent>) -> io::Result<Arc<BaseEvent>> {
        self.start()?;
        let mut payload = serde_json::to_vec(&event.to_json_value())?;
        payload.push(b'\n');
        append_file(&self.path)?.write_all(&payload)?;
        Ok(event)
    }

    pub fn emit<E: TypedEventObject>(&self, event: E) -> io::Result<E> {
        self.emit_base(E::_inner_event(&event))
            .map(E::_from_inner_event)
    }

    pub fn dispatch<E: TypedEventObject>(&self, event: E) -> io::Result<E> {
        self.emit(event)
    }

    pub fn dispatch_base(&self, event: Arc<BaseEvent>) -> io::Result<Arc<BaseEvent>> {
        self.emit_base(event)
    }

    /// Stop tailing before destroying the internal bus. Closing is idempotent.
    pub fn close(&self) {
        let mut listener = self.listener.lock();
        match std::mem::replace(&mut *listener, Listener::Closed) {
            Listener::Running(stop, thread) => {
                let _ = stop.send(());
                let _ = thread.join();
            }
            Listener::New => (),
            Listener::Closed => return,
        }
        self.inbound_bus.destroy();
    }
}

impl Drop for JsonlEventBridge {
    fn drop(&mut self) {
        self.close();
    }
}

fn append_file(path: &Path) -> io::Result<File> {
    if path.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "JSONLEventBridge path is required",
        ));
    }
    if let Some(parent) = path.parent().filter(|path| !path.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }
    OpenOptions::new().create(true).append(true).open(path)
}

fn poll_lines(
    path: &Path,
    offset: &mut u64,
    pending: &mut Vec<u8>,
    bus: &EventBus,
) -> io::Result<()> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            *offset = 0;
            pending.clear();
            return Ok(());
        }
        Err(error) => return Err(error),
    };
    let size = file.metadata()?.len();
    let start = if size < *offset { 0 } else { *offset };
    if start < *offset {
        pending.clear();
        *offset = 0;
    }
    if start == size {
        return Ok(());
    }
    file.seek(SeekFrom::Start(start))?;
    let mut read = Vec::new();
    let count = file.take(size - start).read_to_end(&mut read)?;
    *offset = start + count as u64;
    pending.extend(read);

    // A UTF-8 code point may straddle reads; parse only whole records.
    if let Some(last_newline) = pending.iter().rposition(|byte| *byte == b'\n') {
        for line in pending[..last_newline].split(|byte| *byte == b'\n') {
            let Ok(value) = serde_json::from_slice::<Value>(line) else {
                continue;
            };
            let Ok(event) = BaseEvent::try_from_json_value(value) else {
                continue;
            };
            if EventBus::try_validate_event_execution_timeout_fields(&event).is_ok() {
                bus.emit_base(event.event_reset());
            }
        }
        pending.drain(..=last_newline);
    }
    Ok(())
}
