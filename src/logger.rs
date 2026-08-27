use std::{
    collections::VecDeque,
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
        mpsc,
    },
    thread,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Local, NaiveDate};
use serde::{Deserialize, Serialize};

const MAX_MEMORY_EVENTS: usize = 500;
const LOG_RETENTION_DAYS: i64 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
#[repr(u8)]
pub enum LogLevel {
    Off = 0,
    Error = 1,
    Warn = 2,
    #[default]
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl LogLevel {
    pub const ALL: [Self; 6] = [
        Self::Off,
        Self::Error,
        Self::Warn,
        Self::Info,
        Self::Debug,
        Self::Trace,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Off => "Off",
            Self::Error => "Error",
            Self::Warn => "Warn",
            Self::Info => "Info",
            Self::Debug => "Debug",
            Self::Trace => "Trace",
        }
    }

    pub fn permits(self, event_level: Self) -> bool {
        self != Self::Off && event_level as u8 <= self as u8
    }
}

impl FromStr for LogLevel {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|level| level.label().eq_ignore_ascii_case(value))
            .with_context(|| format!("未知日志等级: {value}"))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEvent {
    pub timestamp: DateTime<Local>,
    pub level: LogLevel,
    pub category: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_headers_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_chunk_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_tag: Option<String>,
    #[serde(skip)]
    pub debug: Option<RequestDebugDetails>,
}

#[derive(Debug, Clone, Default)]
pub struct RequestDebugDetails {
    pub client_headers: String,
    pub upstream_headers: String,
    pub parameters: String,
    pub upstream_request_structure: String,
    pub request_body: String,
    pub response_events: String,
    pub response_body: String,
}

impl LogEvent {
    pub fn system(level: LogLevel, message: impl Into<String>) -> Self {
        Self {
            timestamp: Local::now(),
            level,
            category: "system".to_string(),
            message: message.into(),
            client_ip: None,
            method: None,
            path: None,
            upstream: None,
            status: None,
            duration_seconds: None,
            upstream_headers_seconds: None,
            first_chunk_seconds: None,
            session_tag: None,
            debug: None,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn request(
        level: LogLevel,
        message: impl Into<String>,
        client_ip: impl Into<String>,
        method: impl Into<String>,
        path: impl Into<String>,
        upstream: Option<String>,
        status: u16,
        duration_seconds: f64,
    ) -> Self {
        Self {
            timestamp: Local::now(),
            level,
            category: "request".to_string(),
            message: message.into(),
            client_ip: Some(client_ip.into()),
            method: Some(method.into()),
            path: Some(path.into()),
            upstream,
            status: Some(status),
            duration_seconds: Some(duration_seconds),
            upstream_headers_seconds: None,
            first_chunk_seconds: None,
            session_tag: None,
            debug: None,
        }
    }

    pub fn with_request_timings(
        mut self,
        upstream_headers_seconds: Option<f64>,
        first_chunk_seconds: Option<f64>,
    ) -> Self {
        self.upstream_headers_seconds = upstream_headers_seconds;
        self.first_chunk_seconds = first_chunk_seconds;
        self
    }

    pub fn with_session_tag(mut self, session_tag: Option<String>) -> Self {
        self.session_tag = session_tag;
        self
    }

    pub fn with_debug(mut self, debug: Option<RequestDebugDetails>) -> Self {
        self.debug = debug;
        self
    }

    pub fn display_line(&self) -> String {
        let request = match (&self.method, &self.path, self.status, self.duration_seconds) {
            (Some(method), Some(path), Some(status), Some(duration)) => {
                format!(
                    "{method} {path}  {status}  {}",
                    format_duration_seconds(duration)
                )
            }
            _ => self.message.clone(),
        };
        format!(
            "{}  {:<5}  {}",
            self.timestamp.format("%H:%M:%S"),
            self.level.label().to_uppercase(),
            request
        )
    }
}

pub fn format_duration_seconds(duration_seconds: f64) -> String {
    format!("{duration_seconds:.3} 秒")
}

struct LoggerInner {
    level: AtomicU8,
    debug_capture: AtomicBool,
    version: AtomicU64,
    events: Mutex<VecDeque<LogEvent>>,
    writer: mpsc::Sender<LogEvent>,
}

#[derive(Clone)]
pub struct AppLogger {
    inner: Arc<LoggerInner>,
}

impl AppLogger {
    pub fn new(log_dir: PathBuf, level: LogLevel) -> Result<Self> {
        fs::create_dir_all(&log_dir)?;
        cleanup_old_logs(&log_dir)?;
        let (writer, receiver) = mpsc::channel();
        thread::Builder::new()
            .name("rust-ai-bridge-log-writer".to_string())
            .spawn(move || writer_loop(log_dir, receiver))?;
        Ok(Self {
            inner: Arc::new(LoggerInner {
                level: AtomicU8::new(level as u8),
                debug_capture: AtomicBool::new(false),
                version: AtomicU64::new(0),
                events: Mutex::new(VecDeque::with_capacity(MAX_MEMORY_EVENTS)),
                writer,
            }),
        })
    }

    pub fn set_level(&self, level: LogLevel) {
        self.inner.level.store(level as u8, Ordering::Relaxed);
        self.emit(LogEvent::system(
            LogLevel::Info,
            format!("日志等级已切换为 {}", level.label()),
        ));
    }

    pub fn level(&self) -> LogLevel {
        match self.inner.level.load(Ordering::Relaxed) {
            0 => LogLevel::Off,
            1 => LogLevel::Error,
            2 => LogLevel::Warn,
            3 => LogLevel::Info,
            4 => LogLevel::Debug,
            _ => LogLevel::Trace,
        }
    }

    pub fn emit(&self, event: LogEvent) {
        let write_file = self.level().permits(event.level);
        let keep_in_memory = write_file || (event.debug.is_some() && self.debug_capture_enabled());
        if !keep_in_memory {
            return;
        }
        {
            let mut events = self
                .inner
                .events
                .lock()
                .expect("logger event lock poisoned");
            if events.len() == MAX_MEMORY_EVENTS {
                events.pop_front();
            }
            events.push_back(event.clone());
        }
        self.inner.version.fetch_add(1, Ordering::Relaxed);
        if write_file {
            let _ = self.inner.writer.send(event);
        }
    }

    pub fn set_debug_capture(&self, enabled: bool) {
        self.inner.debug_capture.store(enabled, Ordering::Release);
        if !enabled {
            let mut events = self
                .inner
                .events
                .lock()
                .expect("logger event lock poisoned");
            for event in events.iter_mut() {
                event.debug = None;
            }
            self.inner.version.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn debug_capture_enabled(&self) -> bool {
        self.inner.debug_capture.load(Ordering::Acquire)
    }

    pub fn snapshot(&self) -> Vec<LogEvent> {
        self.inner
            .events
            .lock()
            .expect("logger event lock poisoned")
            .iter()
            .cloned()
            .collect()
    }

    pub fn clear_memory(&self) {
        self.inner
            .events
            .lock()
            .expect("logger event lock poisoned")
            .clear();
        self.inner.version.fetch_add(1, Ordering::Relaxed);
    }

    pub fn version(&self) -> u64 {
        self.inner.version.load(Ordering::Relaxed)
    }
}

fn writer_loop(log_dir: PathBuf, receiver: mpsc::Receiver<LogEvent>) {
    let mut current_date: Option<NaiveDate> = None;
    let mut writer: Option<BufWriter<File>> = None;
    while let Ok(event) = receiver.recv() {
        let date = event.timestamp.date_naive();
        if current_date != Some(date) {
            writer = open_log_writer(&log_dir, date).ok();
            current_date = Some(date);
            let _ = cleanup_old_logs(&log_dir);
        }
        if let Some(writer) = writer.as_mut()
            && serde_json::to_writer(&mut *writer, &event).is_ok()
        {
            let _ = writer.write_all(b"\n");
            let _ = writer.flush();
        }
    }
}

fn open_log_writer(log_dir: &Path, date: NaiveDate) -> Result<BufWriter<File>> {
    let path = log_dir.join(format!("{}.jsonl", date.format("%Y-%m-%d")));
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    Ok(BufWriter::new(file))
}

fn cleanup_old_logs(log_dir: &Path) -> Result<()> {
    let cutoff = Local::now().date_naive() - Duration::days(LOG_RETENTION_DAYS - 1);
    for entry in fs::read_dir(log_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("jsonl") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if let Ok(date) = NaiveDate::parse_from_str(stem, "%Y-%m-%d")
            && date < cutoff
        {
            let _ = fs::remove_file(path);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn level_filtering_is_ordered() {
        assert!(LogLevel::Info.permits(LogLevel::Error));
        assert!(LogLevel::Info.permits(LogLevel::Info));
        assert!(!LogLevel::Info.permits(LogLevel::Debug));
        assert!(!LogLevel::Off.permits(LogLevel::Error));
    }

    #[test]
    fn debug_bodies_are_never_serialized() {
        let event = LogEvent::request(
            LogLevel::Info,
            "done",
            "127.0.0.1",
            "POST",
            "/v1/responses",
            Some("test".to_string()),
            200,
            0.010,
        )
        .with_request_timings(Some(0.004), Some(0.007))
        .with_session_tag(Some("rabs_safe12".to_string()))
        .with_debug(Some(RequestDebugDetails {
            client_headers: "authorization: secret-client-key".to_string(),
            upstream_headers: "authorization: secret-upstream-key".to_string(),
            parameters: "secret-parameters".to_string(),
            upstream_request_structure: "secret-structure".to_string(),
            request_body: "secret-request".to_string(),
            response_events: "secret-events".to_string(),
            response_body: "secret-response".to_string(),
        }));

        let serialized = serde_json::to_string(&event).unwrap();
        assert!(!serialized.contains("secret-client-key"));
        assert!(!serialized.contains("secret-upstream-key"));
        assert!(!serialized.contains("secret-parameters"));
        assert!(!serialized.contains("secret-structure"));
        assert!(!serialized.contains("secret-request"));
        assert!(!serialized.contains("secret-events"));
        assert!(!serialized.contains("secret-response"));
        assert!(serialized.contains("\"duration_seconds\":0.01"));
        assert!(serialized.contains("\"upstream_headers_seconds\":0.004"));
        assert!(serialized.contains("\"first_chunk_seconds\":0.007"));
        assert!(serialized.contains("\"session_tag\":\"rabs_safe12\""));
        assert!(!serialized.contains("duration_ms"));
    }

    #[test]
    fn request_duration_is_displayed_in_seconds() {
        let event = LogEvent::request(
            LogLevel::Info,
            "done",
            "127.0.0.1",
            "POST",
            "/v1/responses",
            Some("test".to_string()),
            200,
            19.706,
        );
        assert!(event.display_line().contains("19.706 秒"));
    }
}
