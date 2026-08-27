use std::{
    collections::BTreeMap,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use arc_swap::ArcSwap;
use async_stream::stream;
use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{ConnectInfo, Path, Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use rand::RngCore;
use serde_json::{Map, Value, json};
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tokio::{
    net::TcpListener,
    task::JoinHandle,
    time::{MissedTickBehavior, interval_at, timeout},
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{UpstreamKind, UpstreamProfile, build_upstream_url},
    logger::{AppLogger, LogEvent, LogLevel, RequestDebugDetails},
};

const DEBUG_PREVIEW_LIMIT: usize = 64 * 1024;
const DEBUG_PARAMETERS_LIMIT: usize = 16 * 1024;
const DEBUG_HEADERS_LIMIT: usize = 16 * 1024;
const DEBUG_STRUCTURE_LIMIT: usize = 32 * 1024;
const DEBUG_EVENTS_LIMIT: usize = 16 * 1024;
const SSE_LINE_LIMIT: usize = 256 * 1024;
const SSE_EVENT_DATA_LIMIT: usize = 256 * 1024;
const SSE_EVENT_TYPE_LIMIT: usize = 128;
const SSE_DISTINCT_EVENT_LIMIT: usize = 128;
const SAFE_RETRY_BUFFER_LIMIT: usize = 2 * 1024 * 1024;
const SSE_FILTER_BUFFER_LIMIT: usize = 2 * 1024 * 1024;
const SAFE_RETRY_MAX_ATTEMPTS: u8 = 1;
const SSE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const SSE_HEARTBEAT: &[u8] = b": rab-keepalive\n\n";
const REASONING_REQUEST_LIMIT: usize = 4 * 1024 * 1024;
const SUPPORTED_REASONING_EFFORTS: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];
const SESSION_FIELD_MAX_CHARS: usize = 256;
const PROMPT_CACHE_KEY_MAX_CHARS: usize = 64;
const SESSION_TAG_CHARS: usize = 12;

#[derive(Clone)]
struct BodyCapture {
    inner: Arc<Mutex<BodyCaptureState>>,
}

struct BodyCaptureState {
    head: Vec<u8>,
    tail: Vec<u8>,
    total_bytes: usize,
    content_type: Option<String>,
    parameters: Option<String>,
}

struct BodyCaptureSnapshot {
    head: Vec<u8>,
    tail: Vec<u8>,
    total_bytes: usize,
    content_type: Option<String>,
    parameters: Option<String>,
}

impl BodyCapture {
    fn new(content_type: Option<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(BodyCaptureState {
                head: Vec::with_capacity(DEBUG_PREVIEW_LIMIT.min(4096)),
                tail: Vec::with_capacity(DEBUG_PREVIEW_LIMIT.min(4096)),
                total_bytes: 0,
                content_type,
                parameters: None,
            })),
        }
    }

    fn from_complete_body(
        content_type: Option<String>,
        body: &[u8],
        parameters: Option<String>,
    ) -> Self {
        let capture = Self::new(content_type);
        capture.push(body);
        capture
            .inner
            .lock()
            .expect("debug body capture poisoned")
            .parameters = parameters;
        capture
    }

    fn push(&self, chunk: &[u8]) {
        let mut state = self.inner.lock().expect("debug body capture poisoned");
        state.total_bytes = state.total_bytes.saturating_add(chunk.len());

        let head_remaining = DEBUG_PREVIEW_LIMIT.saturating_sub(state.head.len());
        let head_take = head_remaining.min(chunk.len());
        state.head.extend_from_slice(&chunk[..head_take]);

        if chunk.len() >= DEBUG_PREVIEW_LIMIT {
            state.tail.clear();
            state
                .tail
                .extend_from_slice(&chunk[chunk.len() - DEBUG_PREVIEW_LIMIT..]);
        } else {
            let overflow = state
                .tail
                .len()
                .saturating_add(chunk.len())
                .saturating_sub(DEBUG_PREVIEW_LIMIT);
            if overflow > 0 {
                state.tail.drain(..overflow);
            }
            state.tail.extend_from_slice(chunk);
        }
    }

    fn snapshot(&self) -> BodyCaptureSnapshot {
        let state = self.inner.lock().expect("debug body capture poisoned");
        BodyCaptureSnapshot {
            head: state.head.clone(),
            tail: state.tail.clone(),
            total_bytes: state.total_bytes,
            content_type: state.content_type.clone(),
            parameters: state.parameters.clone(),
        }
    }
}

impl BodyCaptureSnapshot {
    fn complete_bytes(&self) -> Option<Vec<u8>> {
        if self.total_bytes > DEBUG_PREVIEW_LIMIT * 2 {
            return None;
        }
        if self.total_bytes <= DEBUG_PREVIEW_LIMIT {
            return Some(self.head.clone());
        }

        let overlap = self
            .head
            .len()
            .saturating_add(self.tail.len())
            .saturating_sub(self.total_bytes);
        let mut bytes = Vec::with_capacity(self.total_bytes);
        bytes.extend_from_slice(&self.head);
        bytes.extend_from_slice(&self.tail[overlap.min(self.tail.len())..]);
        Some(bytes)
    }
}

#[derive(Clone)]
struct ResponseCapture {
    body: Option<BodyCapture>,
    events: Option<SseEventCapture>,
    retry: Arc<Mutex<SafeRetryDebug>>,
}

#[derive(Clone, Default)]
struct SafeRetryDebug {
    attempts: u8,
    reason: Option<String>,
    outcome: Option<String>,
}

impl ResponseCapture {
    fn new(content_type: Option<String>, capture_body: bool, capture_events: bool) -> Self {
        Self {
            body: capture_body.then(|| BodyCapture::new(content_type)),
            events: capture_events.then(SseEventCapture::new),
            retry: Arc::new(Mutex::new(SafeRetryDebug::default())),
        }
    }

    fn push(&self, chunk: &[u8]) {
        if let Some(body) = &self.body {
            body.push(chunk);
        }
        if let Some(events) = &self.events {
            events.push(chunk);
        }
    }

    fn finish(&self) {
        if let Some(events) = &self.events {
            events.finish();
        }
    }

    fn terminal_type(&self) -> Option<String> {
        self.events
            .as_ref()
            .and_then(SseEventCapture::terminal_type)
    }

    fn terminal_error_message(&self) -> Option<String> {
        let terminal = self.events.as_ref()?.snapshot().terminal?;
        match terminal.event_type.as_str() {
            "response.completed" => None,
            "response.failed" => {
                let mut message = "Responses SSE 返回 response.failed".to_string();
                if let Some(code) = terminal.error_code {
                    message.push_str(&format!("，错误代码: {code}"));
                }
                if let Some(detail) = terminal.error_message {
                    message.push_str(&format!("，错误信息: {detail}"));
                }
                Some(message)
            }
            "response.incomplete" => Some(terminal.incomplete_reason.map_or_else(
                || "Responses SSE 返回 response.incomplete".to_string(),
                |reason| format!("Responses SSE 返回 response.incomplete，原因: {reason}"),
            )),
            "response.cancelled" => Some("Responses SSE 返回 response.cancelled".to_string()),
            "error" => {
                let mut message = "Responses SSE 返回 error 事件".to_string();
                if let Some(code) = terminal.error_code {
                    message.push_str(&format!("，错误代码: {code}"));
                }
                if let Some(detail) = terminal.error_message {
                    message.push_str(&format!("，错误信息: {detail}"));
                }
                Some(message)
            }
            _ => None,
        }
    }

    fn mark_retry_started(&self, reason: String) {
        let mut retry = self.retry.lock().expect("safe retry capture poisoned");
        retry.attempts = retry.attempts.saturating_add(1);
        if retry.reason.is_none() {
            retry.reason = Some(reason);
        }
        retry.outcome = Some("已发起，等待上游响应".to_string());
    }

    fn mark_retry_outcome(&self, outcome: String) {
        self.retry
            .lock()
            .expect("safe retry capture poisoned")
            .outcome = Some(outcome);
    }

    fn retry_snapshot(&self) -> SafeRetryDebug {
        self.retry
            .lock()
            .expect("safe retry capture poisoned")
            .clone()
    }
}

#[derive(Default)]
struct SseDataEventFilter {
    pending: Vec<u8>,
    scan_position: usize,
    line_start: usize,
    event_has_data: bool,
    disabled: bool,
}

impl SseDataEventFilter {
    fn push(&mut self, chunk: &[u8]) -> Option<Bytes> {
        if self.disabled {
            return Some(Bytes::copy_from_slice(chunk));
        }
        self.pending.extend_from_slice(chunk);
        let mut output = Vec::new();

        'events: loop {
            while self.scan_position < self.pending.len() {
                let (line_end, line_ending_end) = match self.pending[self.scan_position] {
                    b'\n' => (self.scan_position, self.scan_position + 1),
                    b'\r' => {
                        if self.scan_position + 1 >= self.pending.len() {
                            break 'events;
                        }
                        let ending_end = if self.pending[self.scan_position + 1] == b'\n' {
                            self.scan_position + 2
                        } else {
                            self.scan_position + 1
                        };
                        (self.scan_position, ending_end)
                    }
                    _ => {
                        self.scan_position += 1;
                        continue;
                    }
                };

                let line = &self.pending[self.line_start..line_end];
                if line.is_empty() {
                    if self.event_has_data {
                        output.extend_from_slice(&self.pending[..line_ending_end]);
                    }
                    self.pending.drain(..line_ending_end);
                    self.scan_position = 0;
                    self.line_start = 0;
                    self.event_has_data = false;
                    continue 'events;
                }

                if sse_line_has_nonempty_data(line) {
                    self.event_has_data = true;
                }
                self.scan_position = line_ending_end;
                self.line_start = line_ending_end;
            }
            break;
        }

        if self.pending.len() > SSE_FILTER_BUFFER_LIMIT {
            self.disabled = true;
            output.extend_from_slice(&self.pending);
            self.pending.clear();
            self.scan_position = 0;
            self.line_start = 0;
            self.event_has_data = false;
        }

        (!output.is_empty()).then(|| Bytes::from(output))
    }

    fn finish(&mut self) -> Option<Bytes> {
        if self.disabled {
            return None;
        }
        let mut line = &self.pending[self.line_start..];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
        if sse_line_has_nonempty_data(line) {
            self.event_has_data = true;
        }
        let output = self
            .event_has_data
            .then(|| Bytes::from(std::mem::take(&mut self.pending)));
        self.scan_position = 0;
        self.line_start = 0;
        self.event_has_data = false;
        output
    }
}

fn sse_line_has_nonempty_data(line: &[u8]) -> bool {
    let (field, value) =
        line.iter()
            .position(|byte| *byte == b':')
            .map_or((line, &[][..]), |colon| {
                let mut value = &line[colon + 1..];
                if value.first() == Some(&b' ') {
                    value = &value[1..];
                }
                (&line[..colon], value)
            });
    field == b"data" && value.iter().any(|byte| !byte.is_ascii_whitespace())
}

fn prepare_downstream_chunk(
    response_capture: Option<&ResponseCapture>,
    filter: &mut Option<SseDataEventFilter>,
    chunk: Bytes,
) -> Option<Bytes> {
    if let Some(capture) = response_capture {
        capture.push(&chunk);
    }
    filter
        .as_mut()
        .map_or_else(|| Some(chunk.clone()), |filter| filter.push(&chunk))
}

#[derive(Clone)]
struct SseEventCapture {
    inner: Arc<Mutex<SseEventState>>,
}

#[derive(Default)]
struct SseEventState {
    pending_line: Vec<u8>,
    discarding_line: bool,
    current_event: Option<String>,
    current_data: Vec<u8>,
    current_data_truncated: bool,
    event_counts: BTreeMap<String, u64>,
    untracked_event_types: u64,
    oversized_lines: u64,
    response_id: Option<String>,
    terminal: Option<SseTerminalEvent>,
    retry_safe_prefix: bool,
    finished: bool,
}

#[derive(Clone, Default)]
struct SseTerminalEvent {
    event_type: String,
    response_id: Option<String>,
    status: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
    incomplete_reason: Option<String>,
    details_available: bool,
}

#[derive(Clone)]
struct SseEventSnapshot {
    event_counts: BTreeMap<String, u64>,
    untracked_event_types: u64,
    oversized_lines: u64,
    terminal: Option<SseTerminalEvent>,
}

impl SseEventCapture {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SseEventState {
                retry_safe_prefix: true,
                ..SseEventState::default()
            })),
        }
    }

    fn push(&self, chunk: &[u8]) {
        let mut state = self.inner.lock().expect("SSE event capture poisoned");
        if state.finished {
            return;
        }
        push_sse_bytes(&mut state, chunk);
    }

    fn finish(&self) {
        let mut state = self.inner.lock().expect("SSE event capture poisoned");
        if state.finished {
            return;
        }
        state.finished = true;
        if !state.discarding_line && !state.pending_line.is_empty() {
            let line = std::mem::take(&mut state.pending_line);
            process_sse_line(&mut state, &line);
        }
        finish_sse_event(&mut state);
    }

    fn snapshot(&self) -> SseEventSnapshot {
        let state = self.inner.lock().expect("SSE event capture poisoned");
        SseEventSnapshot {
            event_counts: state.event_counts.clone(),
            untracked_event_types: state.untracked_event_types,
            oversized_lines: state.oversized_lines,
            terminal: state.terminal.clone(),
        }
    }

    fn terminal_type(&self) -> Option<String> {
        self.inner
            .lock()
            .expect("SSE event capture poisoned")
            .terminal
            .as_ref()
            .map(|terminal| terminal.event_type.clone())
    }

    fn retry_safe_prefix(&self) -> bool {
        self.inner
            .lock()
            .expect("SSE event capture poisoned")
            .retry_safe_prefix
    }
}

fn push_sse_bytes(state: &mut SseEventState, chunk: &[u8]) {
    let mut offset = 0;
    while offset < chunk.len() {
        let remaining = &chunk[offset..];
        let newline = remaining.iter().position(|byte| *byte == b'\n');
        let segment = newline.map_or(remaining, |index| &remaining[..index]);

        if !state.discarding_line {
            if state.pending_line.len().saturating_add(segment.len()) > SSE_LINE_LIMIT {
                state.pending_line.clear();
                state.discarding_line = true;
                state.current_data_truncated = true;
                state.oversized_lines = state.oversized_lines.saturating_add(1);
            } else {
                state.pending_line.extend_from_slice(segment);
            }
        }

        let Some(newline) = newline else {
            break;
        };
        offset = offset.saturating_add(newline + 1);
        if state.discarding_line {
            state.discarding_line = false;
            continue;
        }

        let mut line = std::mem::take(&mut state.pending_line);
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        process_sse_line(state, &line);
    }
}

fn process_sse_line(state: &mut SseEventState, line: &[u8]) {
    if line.is_empty() {
        finish_sse_event(state);
        return;
    }
    if line.starts_with(b":") {
        return;
    }

    let (field, value) =
        line.iter()
            .position(|byte| *byte == b':')
            .map_or((line, &[][..]), |colon| {
                let mut value = &line[colon + 1..];
                if value.first() == Some(&b' ') {
                    value = &value[1..];
                }
                (&line[..colon], value)
            });
    match field {
        b"event" => {
            let event = String::from_utf8_lossy(value).trim().to_string();
            if !event.is_empty() {
                state.current_event = Some(truncate_single_line(event, SSE_EVENT_TYPE_LIMIT));
            }
        }
        b"data" => {
            if !state.current_data.is_empty() && state.current_data.len() < SSE_EVENT_DATA_LIMIT {
                state.current_data.push(b'\n');
            }
            let remaining = SSE_EVENT_DATA_LIMIT.saturating_sub(state.current_data.len());
            let take = remaining.min(value.len());
            state.current_data.extend_from_slice(&value[..take]);
            if take < value.len() {
                state.current_data_truncated = true;
            }
        }
        _ => {}
    }
}

fn finish_sse_event(state: &mut SseEventState) {
    if state.current_event.is_none() && state.current_data.is_empty() {
        state.current_data_truncated = false;
        return;
    }

    let parsed = (!state.current_data.is_empty() && !state.current_data_truncated)
        .then(|| serde_json::from_slice::<Value>(&state.current_data).ok())
        .flatten();
    let event_type = state.current_event.take().or_else(|| {
        parsed
            .as_ref()
            .and_then(|value| value.get("type"))
            .and_then(Value::as_str)
            .map(|value| truncate_single_line(value.to_string(), SSE_EVENT_TYPE_LIMIT))
    });

    if let Some(event_type) = event_type {
        if state.event_counts.contains_key(&event_type)
            || state.event_counts.len() < SSE_DISTINCT_EVENT_LIMIT
        {
            let count = state.event_counts.entry(event_type.clone()).or_default();
            *count = count.saturating_add(1);
        } else {
            state.untracked_event_types = state.untracked_event_types.saturating_add(1);
        }

        let event_response_id = parsed.as_ref().and_then(|value| {
            value
                .pointer("/response/id")
                .or_else(|| value.get("response_id"))
                .and_then(debug_scalar)
        });
        if (event_type == "response.created" && event_response_id.is_some())
            || state.response_id.is_none()
        {
            state.response_id = event_response_id;
        }

        if !is_safe_retry_prelude_event(&event_type, parsed.as_ref(), !state.current_data_truncated)
            && !is_terminal_response_event(&event_type)
        {
            state.retry_safe_prefix = false;
        }

        if is_terminal_response_event(&event_type) {
            let mut terminal =
                extract_terminal_event(event_type, parsed.as_ref(), parsed.is_some());
            if terminal.response_id.is_none() {
                terminal.response_id.clone_from(&state.response_id);
            }
            state.terminal = Some(terminal);
        }
    } else if state.current_data_truncated || !state.current_data.is_empty() {
        state.retry_safe_prefix = false;
    }

    state.current_data.clear();
    state.current_data_truncated = false;
}

fn is_terminal_response_event(event_type: &str) -> bool {
    matches!(
        event_type,
        "response.completed"
            | "response.failed"
            | "response.incomplete"
            | "response.cancelled"
            | "error"
    )
}

fn is_safe_retry_prelude_event(
    event_type: &str,
    value: Option<&Value>,
    details_available: bool,
) -> bool {
    match event_type {
        "response.created" | "response.in_progress" | "response.queued" => true,
        "response.output_item.added" if details_available => value
            .and_then(|value| value.get("item"))
            .is_some_and(is_empty_reasoning_item),
        _ => false,
    }
}

fn is_empty_reasoning_item(item: &Value) -> bool {
    item.get("type").and_then(Value::as_str) == Some("reasoning")
        && value_is_empty(item.get("content"))
        && value_is_empty(item.get("summary"))
}

fn value_is_empty(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null) => true,
        Some(Value::String(value)) => value.is_empty(),
        Some(Value::Array(value)) => value.is_empty(),
        Some(Value::Object(value)) => value.is_empty(),
        Some(Value::Bool(_)) | Some(Value::Number(_)) => false,
    }
}

fn extract_terminal_event(
    event_type: String,
    value: Option<&Value>,
    details_available: bool,
) -> SseTerminalEvent {
    if event_type == "error" {
        return SseTerminalEvent {
            event_type,
            response_id: value
                .and_then(|value| value.get("response_id"))
                .and_then(debug_scalar),
            status: Some("failed".to_string()),
            error_code: value
                .and_then(|value| value.get("code"))
                .and_then(debug_scalar),
            error_message: value
                .and_then(|value| value.get("message"))
                .and_then(debug_scalar),
            incomplete_reason: None,
            details_available,
        };
    }

    let response = value.and_then(|value| value.get("response"));
    let field = |name: &str| {
        response
            .and_then(|value| value.get(name))
            .or_else(|| value.and_then(|value| value.get(name)))
    };
    let error = field("error");
    let incomplete = field("incomplete_details");

    let inferred_status = event_type.strip_prefix("response.").map(str::to_string);
    SseTerminalEvent {
        event_type,
        response_id: field("id").and_then(debug_scalar),
        status: field("status").and_then(debug_scalar).or(inferred_status),
        error_code: error
            .and_then(|value| value.get("code"))
            .and_then(debug_scalar),
        error_message: error
            .and_then(|value| value.get("message"))
            .and_then(debug_scalar),
        incomplete_reason: incomplete
            .and_then(|value| value.get("reason"))
            .and_then(debug_scalar),
        details_available,
    }
}

fn truncate_single_line(value: String, limit: usize) -> String {
    let value = value
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    if value.len() <= limit {
        return value;
    }
    let mut boundary = limit;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    format!("{}…", &value[..boundary])
}

#[derive(Clone)]
struct ProxyRequestMetadata {
    started: Instant,
    log_path: String,
    request_capture: Option<BodyCapture>,
    client_headers: Option<String>,
    upstream_request_structure: Option<String>,
    route: Arc<RouteTarget>,
    session: SessionDecision,
    safe_retry_body: Option<Bytes>,
}

#[derive(Clone)]
struct ReplayableUpstreamRequest {
    client: reqwest::Client,
    method: axum::http::Method,
    url: url::Url,
    headers: HeaderMap,
    body: Bytes,
}

impl ReplayableUpstreamRequest {
    fn builder(&self) -> reqwest::RequestBuilder {
        self.client
            .request(self.method.clone(), self.url.clone())
            .headers(self.headers.clone())
            .body(self.body.clone())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SessionSource {
    None,
    RabSessionId,
    PromptCacheHeader,
    PromptCacheBody,
}

impl SessionSource {
    fn label(self) -> &'static str {
        match self {
            Self::None => "无",
            Self::RabSessionId => "X-RAB-Session-Id",
            Self::PromptCacheHeader => "X-Prompt-Cache-Id",
            Self::PromptCacheBody => "正文 prompt_cache_key",
        }
    }
}

#[derive(Debug, Clone)]
struct SessionSignals {
    user_id: Option<String>,
    preferred_session_id: Option<String>,
    source: SessionSource,
}

#[derive(Debug, Clone)]
struct SessionDecision {
    prompt_cache_key: Option<String>,
    upstream_session_id: Option<String>,
    tag: Option<String>,
    source_label: String,
}

impl SessionDecision {
    fn debug_summary(&self) -> String {
        format!(
            "会话来源: {}\n匿名会话标签: {}\n注入 prompt_cache_key: {}\n设置 Session-Id: {}",
            self.source_label,
            self.tag.as_deref().unwrap_or("<无>"),
            if self.prompt_cache_key.is_some() {
                "是"
            } else {
                "否"
            },
            if self.upstream_session_id.is_some() {
                "是"
            } else {
                "否"
            },
        )
    }
}

#[derive(Debug)]
struct RequestValidationError {
    code: &'static str,
    message: String,
}

impl RequestValidationError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

fn parse_session_signals(headers: &HeaderMap) -> Result<SessionSignals, RequestValidationError> {
    let user_id = validated_session_header(headers, "x-rab-user-id", "X-RAB-User-Id")?;
    let rab_session = validated_session_header(headers, "x-rab-session-id", "X-RAB-Session-Id")?;
    let prompt_cache_header =
        validated_session_header(headers, "x-prompt-cache-id", "X-Prompt-Cache-Id")?;
    let (preferred_session_id, source) = if let Some(value) = rab_session {
        (Some(value), SessionSource::RabSessionId)
    } else if let Some(value) = prompt_cache_header {
        (Some(value), SessionSource::PromptCacheHeader)
    } else {
        (None, SessionSource::None)
    };
    Ok(SessionSignals {
        user_id,
        preferred_session_id,
        source,
    })
}

fn validated_session_header(
    headers: &HeaderMap,
    name: &'static str,
    label: &'static str,
) -> Result<Option<String>, RequestValidationError> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(RequestValidationError::new(
            "invalid_session_id",
            format!("{label} 只能提供一个值"),
        ));
    }
    let value = value.to_str().map_err(|_| {
        RequestValidationError::new("invalid_session_id", format!("{label} 必须是有效文本"))
    })?;
    validate_session_value(value, label, SESSION_FIELD_MAX_CHARS).map(Some)
}

fn validate_session_value(
    value: &str,
    label: &'static str,
    max_chars: usize,
) -> Result<String, RequestValidationError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(RequestValidationError::new(
            "invalid_session_id",
            format!("{label} 不能为空"),
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(RequestValidationError::new(
            "invalid_session_id",
            format!("{label} 不能包含控制字符"),
        ));
    }
    if value.chars().count() > max_chars {
        return Err(RequestValidationError::new(
            "invalid_session_id",
            format!("{label} 不能超过 {max_chars} 个字符"),
        ));
    }
    Ok(value.to_string())
}

fn decide_session(
    session_hmac_key: &[u8],
    route: &RouteTarget,
    signals: &SessionSignals,
    original_prompt_cache_key: Option<&str>,
    is_responses: bool,
) -> SessionDecision {
    let has_stable_session =
        signals.preferred_session_id.is_some() || original_prompt_cache_key.is_some();
    if has_stable_session {
        let session_id = derive_session_id(
            session_hmac_key,
            route.id,
            signals.user_id.as_deref(),
            signals.preferred_session_id.as_deref(),
            original_prompt_cache_key,
        );
        let source = if signals.source != SessionSource::None && original_prompt_cache_key.is_some()
        {
            format!("{} + 正文 prompt_cache_key", signals.source.label())
        } else if signals.source != SessionSource::None {
            signals.source.label().to_string()
        } else {
            SessionSource::PromptCacheBody.label().to_string()
        };
        let source_label = if signals.user_id.is_some() {
            format!("{source}（含 X-RAB-User-Id 命名空间）")
        } else {
            source
        };
        return SessionDecision {
            prompt_cache_key: is_responses.then(|| session_id.clone()),
            upstream_session_id: (route.kind == UpstreamKind::CliProxyApi)
                .then(|| session_id.clone()),
            tag: Some(short_session_tag(&session_id)),
            source_label,
        };
    }

    if route.kind == UpstreamKind::CliProxyApi {
        let session_id = ephemeral_session_id();
        SessionDecision {
            prompt_cache_key: None,
            upstream_session_id: Some(session_id.clone()),
            tag: Some(short_session_tag(&session_id)),
            source_label: if signals.user_id.is_some() {
                "无稳定会话标识（仅 X-RAB-User-Id，不用于会话）".to_string()
            } else {
                "无稳定会话标识（使用一次性随机 Session-Id）".to_string()
            },
        }
    } else {
        SessionDecision {
            prompt_cache_key: None,
            upstream_session_id: None,
            tag: None,
            source_label: if signals.user_id.is_some() {
                "无稳定会话标识（仅 X-RAB-User-Id，不用于会话）".to_string()
            } else {
                "无".to_string()
            },
        }
    }
}

fn derive_session_id(
    session_hmac_key: &[u8],
    upstream_id: uuid::Uuid,
    user_id: Option<&str>,
    preferred_session_id: Option<&str>,
    original_prompt_cache_key: Option<&str>,
) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(session_hmac_key).expect("HMAC accepts keys of any length");
    mac.update(b"rust-ai-bridge/session/v1");
    update_hmac_field(&mut mac, b"upstream", Some(upstream_id.as_bytes()));
    update_hmac_field(&mut mac, b"user", user_id.map(str::as_bytes));
    update_hmac_field(
        &mut mac,
        b"session",
        preferred_session_id.map(str::as_bytes),
    );
    update_hmac_field(
        &mut mac,
        b"prompt_cache_key",
        original_prompt_cache_key.map(str::as_bytes),
    );
    format!(
        "rabs_{}",
        URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
    )
}

fn update_hmac_field(mac: &mut Hmac<Sha256>, label: &[u8], value: Option<&[u8]>) {
    mac.update(&(label.len() as u32).to_be_bytes());
    mac.update(label);
    match value {
        Some(value) => {
            mac.update(&[1]);
            mac.update(&(value.len() as u32).to_be_bytes());
            mac.update(value);
        }
        None => mac.update(&[0]),
    }
}

fn ephemeral_session_id() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    format!("rabe_{}", URL_SAFE_NO_PAD.encode(bytes))
}

fn short_session_tag(session_id: &str) -> String {
    session_id.chars().take(SESSION_TAG_CHARS).collect()
}

#[derive(Debug, Default)]
pub struct ProxyMetrics {
    active: AtomicU64,
    success: AtomicU64,
    failed: AtomicU64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub active: u64,
    pub success: u64,
    pub failed: u64,
}

impl ProxyMetrics {
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            active: self.active.load(Ordering::Relaxed),
            success: self.success.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.active.store(0, Ordering::Relaxed);
        self.success.store(0, Ordering::Relaxed);
        self.failed.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct GatewayAuth {
    key: String,
}

#[derive(Debug)]
struct RouteTarget {
    id: uuid::Uuid,
    name: String,
    kind: UpstreamKind,
    base_url: String,
    api_key: String,
    cancellation: CancellationToken,
}

impl From<&UpstreamProfile> for RouteTarget {
    fn from(profile: &UpstreamProfile) -> Self {
        Self {
            id: profile.id,
            name: profile.name.clone(),
            kind: profile.kind,
            base_url: profile.base_url.clone(),
            api_key: profile.api_key.trim().to_string(),
            cancellation: CancellationToken::new(),
        }
    }
}

pub struct ProxyShared {
    auth: ArcSwap<GatewayAuth>,
    route: ArcSwap<RouteTarget>,
    client: reqwest::Client,
    session_hmac_key: Vec<u8>,
    pub metrics: Arc<ProxyMetrics>,
    logger: AppLogger,
    running: Arc<AtomicBool>,
}

impl ProxyShared {
    pub fn new(
        gateway_key: String,
        session_secret: String,
        upstream: &UpstreamProfile,
        logger: AppLogger,
        metrics: Arc<ProxyMetrics>,
        running: Arc<AtomicBool>,
    ) -> Result<Arc<Self>> {
        let session_hmac_key = URL_SAFE_NO_PAD
            .decode(session_secret.trim())
            .context("会话密钥格式无效")?;
        anyhow::ensure!(session_hmac_key.len() == 32, "会话密钥长度无效");
        let client = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(15))
            .pool_idle_timeout(std::time::Duration::from_secs(90))
            .tcp_keepalive(std::time::Duration::from_secs(30))
            .build()
            .context("无法创建上游 HTTP 客户端")?;
        Ok(Arc::new(Self {
            auth: ArcSwap::from_pointee(GatewayAuth { key: gateway_key }),
            route: ArcSwap::from_pointee(RouteTarget::from(upstream)),
            client,
            session_hmac_key,
            metrics,
            logger,
            running,
        }))
    }

    pub fn switch_upstream(&self, profile: &UpstreamProfile) {
        self.route.load().cancellation.cancel();
        self.route.store(Arc::new(RouteTarget::from(profile)));
        self.logger.emit(LogEvent::system(
            LogLevel::Info,
            format!("当前上游已切换为 {}，旧请求已取消", profile.name),
        ));
    }

    pub fn update_gateway_key(&self, gateway_key: String) {
        self.auth.store(Arc::new(GatewayAuth { key: gateway_key }));
        self.logger
            .emit(LogEvent::system(LogLevel::Info, "中转 Key 已更新"));
    }

    pub fn cancel_active(&self) {
        self.route.load().cancellation.cancel();
    }
}

pub struct ProxyServer {
    pub shared: Arc<ProxyShared>,
    pub local_addr: SocketAddr,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl ProxyServer {
    pub async fn start(
        address: SocketAddr,
        gateway_key: String,
        session_secret: String,
        upstream: &UpstreamProfile,
        logger: AppLogger,
        metrics: Arc<ProxyMetrics>,
        running: Arc<AtomicBool>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(address)
            .await
            .with_context(|| format!("无法监听 {address}"))?;
        let local_addr = listener.local_addr().context("无法读取监听地址")?;
        let shared = ProxyShared::new(
            gateway_key,
            session_secret,
            upstream,
            logger.clone(),
            metrics,
            running.clone(),
        )?;
        let app = Router::new()
            .route("/health", get(health))
            .route("/{effort}/responses", post(reasoning_responses))
            .route("/v1/responses", post(responses))
            .route("/v1", any(proxy_request))
            .route("/v1/{*path}", any(proxy_request))
            .with_state(shared.clone());
        let stop = CancellationToken::new();
        let shutdown = stop.clone();
        running.store(true, Ordering::Release);
        logger.emit(LogEvent::system(
            LogLevel::Info,
            format!("代理已启动，监听 {local_addr}"),
        ));
        let task_logger = logger.clone();
        let task = tokio::spawn(async move {
            let result = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(shutdown.cancelled_owned())
            .await;
            running.store(false, Ordering::Release);
            if let Err(error) = result {
                task_logger.emit(LogEvent::system(
                    LogLevel::Error,
                    format!("代理服务异常退出: {error}"),
                ));
            }
        });
        Ok(Self {
            shared,
            local_addr,
            stop,
            task,
        })
    }

    pub async fn stop(self) {
        self.shared.cancel_active();
        self.stop.cancel();
        let _ = timeout(std::time::Duration::from_secs(3), self.task).await;
        self.shared.running.store(false, Ordering::Release);
        self.shared
            .logger
            .emit(LogEvent::system(LogLevel::Info, "代理已停止"));
    }
}

async fn health(State(shared): State<Arc<ProxyShared>>) -> impl IntoResponse {
    let route = shared.route.load();
    axum::Json(json!({
        "status": "ok",
        "service": "rust-ai-bridge",
        "upstream": route.name,
    }))
}

async fn reasoning_responses(
    State(shared): State<Arc<ProxyShared>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    Path(effort): Path<String>,
    request: Request,
) -> Response {
    normalize_responses_request(shared, peer, request, Some(effort)).await
}

async fn responses(
    State(shared): State<Arc<ProxyShared>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    normalize_responses_request(shared, peer, request, None).await
}

async fn normalize_responses_request(
    shared: Arc<ProxyShared>,
    peer: SocketAddr,
    request: Request,
    effort: Option<String>,
) -> Response {
    let started = Instant::now();
    let client_ip = peer.ip().to_string();
    let method = request.method().clone();
    let log_path = request.uri().path().to_string();

    if is_websocket_upgrade(request.headers()) {
        return logged_error(
            &shared,
            StatusCode::BAD_REQUEST,
            "unsupported_feature",
            "不支持 WebSocket/Realtime 请求",
            &client_ip,
            method.as_str(),
            &log_path,
            started,
            None,
        );
    }

    if !authorized(request.headers(), &shared.auth.load().key) {
        return logged_error(
            &shared,
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "无效的中转 API Key",
            &client_ip,
            method.as_str(),
            &log_path,
            started,
            None,
        );
    }

    let session_signals = match parse_session_signals(request.headers()) {
        Ok(signals) => signals,
        Err(error) => {
            return logged_error(
                &shared,
                StatusCode::BAD_REQUEST,
                error.code,
                &error.message,
                &client_ip,
                method.as_str(),
                &log_path,
                started,
                None,
            );
        }
    };
    let route = shared.route.load_full();

    if effort
        .as_deref()
        .is_some_and(|effort| !is_supported_reasoning_effort(effort))
    {
        return logged_error(
            &shared,
            StatusCode::BAD_REQUEST,
            "invalid_reasoning_effort",
            "URL 中的思考等级仅支持 low、medium、high、xhigh、max",
            &client_ip,
            method.as_str(),
            &log_path,
            started,
            None,
        );
    }

    let capture_enabled = shared.logger.debug_capture_enabled();
    let request_content_type = content_type(request.headers());
    let client_headers = capture_enabled.then(|| render_debug_headers(request.headers()));
    let query = request.uri().query().map(str::to_owned);
    let (mut parts, body) = request.into_parts();
    let original_body = match to_bytes(body, REASONING_REQUEST_LIMIT).await {
        Ok(body) => body,
        Err(error) => {
            return logged_error(
                &shared,
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                &format!("请求正文超过 4 MiB 限制或读取失败: {error}"),
                &client_ip,
                method.as_str(),
                &log_path,
                started,
                None,
            );
        }
    };

    let (forwarded_body, parameters, upstream_request_structure, session) =
        match normalize_responses_body(
            &original_body,
            effort.as_deref(),
            &route,
            &session_signals,
            &shared.session_hmac_key,
        ) {
            Ok(result) => result,
            Err(error) => {
                let debug = shared
                    .logger
                    .debug_capture_enabled()
                    .then(|| RequestDebugDetails {
                        client_headers: client_headers
                            .clone()
                            .unwrap_or_else(|| "<未捕获客户端请求头>".to_string()),
                        upstream_headers: "<未访问上游>".to_string(),
                        parameters: effort.as_deref().map_or_else(
                            || "自动忽略参数: max_output_tokens".to_string(),
                            |effort| format!("URL 思考等级: {effort}"),
                        ),
                        upstream_request_structure: "<未生成：Responses 请求正文不是有效 JSON>"
                            .to_string(),
                        request_body: render_complete_body(
                            request_content_type.as_deref(),
                            &original_body,
                        ),
                        response_events: "<未访问上游>".to_string(),
                        response_body: "<未访问上游>".to_string(),
                    });
                return logged_error_with_debug(
                    &shared,
                    StatusCode::BAD_REQUEST,
                    error.code,
                    &error.message,
                    &client_ip,
                    method.as_str(),
                    &log_path,
                    started,
                    None,
                    debug,
                );
            }
        };

    let request_capture = capture_enabled.then(|| {
        let parameters = effort.as_deref().map_or(parameters.clone(), |effort| {
            format!("URL 思考等级: {effort}\n{parameters}")
        });
        BodyCapture::from_complete_body(request_content_type, &original_body, Some(parameters))
    });
    if effort.is_some() {
        parts.uri = reasoning_target_uri(query.as_deref());
    }
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    let forwarded_body = Bytes::from(forwarded_body);
    parts.extensions.insert(ProxyRequestMetadata {
        started,
        log_path,
        request_capture,
        client_headers,
        upstream_request_structure: capture_enabled.then_some(upstream_request_structure),
        route,
        session,
        safe_retry_body: Some(forwarded_body.clone()),
    });
    proxy_request(
        State(shared),
        ConnectInfo(peer),
        Request::from_parts(parts, Body::from(forwarded_body)),
    )
    .await
}

async fn proxy_request(
    State(shared): State<Arc<ProxyShared>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    let metadata = request.extensions().get::<ProxyRequestMetadata>().cloned();
    let started = metadata
        .as_ref()
        .map_or_else(Instant::now, |metadata| metadata.started);
    let method = request.method().clone();
    let upstream_path = request.uri().path().to_string();
    let path = metadata.as_ref().map_or_else(
        || upstream_path.clone(),
        |metadata| metadata.log_path.clone(),
    );
    let client_ip = peer.ip().to_string();
    let data_only_sse_client = requires_data_only_sse(request.headers());

    if is_websocket_upgrade(request.headers()) {
        return logged_error(
            &shared,
            StatusCode::BAD_REQUEST,
            "unsupported_feature",
            "不支持 WebSocket/Realtime 请求",
            &client_ip,
            method.as_str(),
            &path,
            started,
            None,
        );
    }

    if !authorized(request.headers(), &shared.auth.load().key) {
        return logged_error(
            &shared,
            StatusCode::UNAUTHORIZED,
            "invalid_api_key",
            "无效的中转 API Key",
            &client_ip,
            method.as_str(),
            &path,
            started,
            None,
        );
    }

    let (route, session) = if let Some(metadata) = &metadata {
        (metadata.route.clone(), metadata.session.clone())
    } else {
        let signals = match parse_session_signals(request.headers()) {
            Ok(signals) => signals,
            Err(error) => {
                return logged_error(
                    &shared,
                    StatusCode::BAD_REQUEST,
                    error.code,
                    &error.message,
                    &client_ip,
                    method.as_str(),
                    &path,
                    started,
                    None,
                );
            }
        };
        let route = shared.route.load_full();
        let session = decide_session(&shared.session_hmac_key, &route, &signals, None, false);
        (route, session)
    };
    let target_url =
        match build_upstream_url(&route.base_url, &upstream_path, request.uri().query()) {
            Ok(url) => url,
            Err(error) => {
                return logged_error_with_context(
                    &shared,
                    StatusCode::BAD_GATEWAY,
                    "invalid_upstream_url",
                    &format!("上游地址无效: {error}"),
                    &client_ip,
                    method.as_str(),
                    &path,
                    started,
                    Some(route.name.clone()),
                    None,
                    session.tag.clone(),
                    None,
                    None,
                );
            }
        };

    shared.metrics.active.fetch_add(1, Ordering::Relaxed);
    let mut upstream_headers = filtered_request_headers(request.headers());
    if data_only_sse_client {
        upstream_headers.remove(header::ACCEPT_ENCODING);
    }
    let authorization = match HeaderValue::from_str(&format!("Bearer {}", route.api_key)) {
        Ok(value) => value,
        Err(_) => {
            shared.metrics.active.fetch_sub(1, Ordering::Relaxed);
            return logged_error_with_context(
                &shared,
                StatusCode::BAD_GATEWAY,
                "invalid_upstream_api_key",
                "上游 API Key 包含无效字符",
                &client_ip,
                method.as_str(),
                &path,
                started,
                Some(route.name.clone()),
                None,
                session.tag.clone(),
                None,
                None,
            );
        }
    };
    upstream_headers.insert(header::AUTHORIZATION, authorization);
    if let Some(session_id) = &session.upstream_session_id {
        upstream_headers.insert(
            HeaderName::from_static("session-id"),
            HeaderValue::from_str(session_id).expect("generated session ID is a valid header"),
        );
    }
    if let Ok(value) = HeaderValue::from_str(&client_ip) {
        upstream_headers.insert(HeaderName::from_static("x-real-ip"), value.clone());
        upstream_headers.insert(HeaderName::from_static("x-forwarded-for"), value);
    }
    upstream_headers.insert(
        HeaderName::from_static("x-forwarded-proto"),
        HeaderValue::from_static("http"),
    );

    let capture_enabled = shared.logger.debug_capture_enabled();
    let client_headers = capture_enabled.then(|| {
        metadata
            .as_ref()
            .and_then(|metadata| metadata.client_headers.clone())
            .unwrap_or_else(|| render_debug_headers(request.headers()))
    });
    let upstream_headers_debug = capture_enabled.then(|| render_debug_headers(&upstream_headers));
    let upstream_request_structure = capture_enabled.then(|| {
        metadata
            .as_ref()
            .and_then(|metadata| metadata.upstream_request_structure.clone())
            .unwrap_or_else(|| "<透明转发：请求正文未改写>".to_string())
    });
    let session_debug = capture_enabled.then(|| {
        format!(
            "{}\nOpenAI Go SSE 兼容模式: {}",
            session.debug_summary(),
            if data_only_sse_client { "是" } else { "否" },
        )
    });
    let existing_request_capture = metadata
        .as_ref()
        .and_then(|metadata| metadata.request_capture.clone());
    let capture_streamed_request = capture_enabled && existing_request_capture.is_none();
    let request_capture = if capture_enabled {
        existing_request_capture.or_else(|| Some(BodyCapture::new(content_type(request.headers()))))
    } else {
        None
    };
    let replayable_body = metadata
        .as_ref()
        .and_then(|metadata| metadata.safe_retry_body.clone());
    let safe_retry_body = replayable_body.clone().filter(|body| {
        route.kind == UpstreamKind::Sub2Api
            && upstream_path == "/v1/responses"
            && responses_request_allows_safe_retry(body)
    });
    let (_, body) = request.into_parts();
    let upstream_body = if let Some(body) = replayable_body {
        reqwest::Body::from(body)
    } else {
        let body_stream = body.into_data_stream();
        match (request_capture.clone(), capture_streamed_request) {
            (Some(capture), true) => reqwest::Body::wrap_stream(body_stream.map(move |result| {
                if let Ok(chunk) = &result {
                    capture.push(chunk);
                }
                result
            })),
            _ => reqwest::Body::wrap_stream(body_stream),
        }
    };
    let replay_request = safe_retry_body.map(|body| ReplayableUpstreamRequest {
        client: shared.client.clone(),
        method: method.clone(),
        url: target_url.clone(),
        headers: upstream_headers.clone(),
        body,
    });
    let upstream_request = shared
        .client
        .request(method.clone(), target_url)
        .headers(upstream_headers)
        .body(upstream_body);

    let upstream_response = tokio::select! {
        response = upstream_request.send() => response,
        _ = route.cancellation.cancelled() => {
            shared.metrics.active.fetch_sub(1, Ordering::Relaxed);
            return logged_error_with_context(
                &shared,
                StatusCode::SERVICE_UNAVAILABLE,
                "upstream_switched",
                "上游已切换，请重试请求",
                &client_ip,
                method.as_str(),
                &path,
                started,
                Some(route.name.clone()),
                debug_details(
                    client_headers.as_deref(),
                    upstream_headers_debug.as_deref(),
                    upstream_request_structure.as_deref(),
                    session_debug.as_deref(),
                    request_capture.as_ref(),
                    None,
                ),
                session.tag.clone(),
                None,
                None,
            );
        }
    };

    let upstream_response = match upstream_response {
        Ok(response) => response,
        Err(error) => {
            shared.metrics.active.fetch_sub(1, Ordering::Relaxed);
            return logged_error_with_context(
                &shared,
                StatusCode::BAD_GATEWAY,
                "upstream_error",
                &format!("连接上游失败: {error}"),
                &client_ip,
                method.as_str(),
                &path,
                started,
                Some(route.name.clone()),
                debug_details(
                    client_headers.as_deref(),
                    upstream_headers_debug.as_deref(),
                    upstream_request_structure.as_deref(),
                    session_debug.as_deref(),
                    request_capture.as_ref(),
                    None,
                ),
                session.tag.clone(),
                None,
                None,
            );
        }
    };
    let upstream_headers_seconds = started.elapsed().as_secs_f64();

    let status = upstream_response.status();
    let mut response_headers = filtered_response_headers(upstream_response.headers());
    let response_content_type = content_type(upstream_response.headers());
    let response_is_sse = is_sse_content_type(response_content_type.as_deref());
    let response_is_identity_encoded = upstream_response
        .headers()
        .get(header::CONTENT_ENCODING)
        .is_none_or(|value| {
            value
                .to_str()
                .ok()
                .is_some_and(|value| value.trim().eq_ignore_ascii_case("identity"))
        });
    let data_only_sse_filter_enabled =
        data_only_sse_client && response_is_sse && response_is_identity_encoded;
    let expects_responses_terminal = upstream_path == "/v1/responses" && response_is_sse;
    let response_capture = (capture_enabled || expects_responses_terminal)
        .then(|| ResponseCapture::new(response_content_type, capture_enabled, response_is_sse));
    let safe_retry_enabled =
        replay_request.is_some() && status.is_success() && expects_responses_terminal;
    if safe_retry_enabled || data_only_sse_filter_enabled {
        response_headers.remove(header::CONTENT_LENGTH);
    }
    let lifecycle = RequestLifecycle::new(
        shared.metrics.clone(),
        shared.logger.clone(),
        started,
        client_ip,
        method.to_string(),
        path,
        route.name.clone(),
        status.as_u16(),
        client_headers,
        upstream_headers_debug,
        upstream_request_structure,
        session_debug,
        session.tag.clone(),
        request_capture,
        response_capture.clone(),
        expects_responses_terminal,
        upstream_headers_seconds,
    );
    let cancellation = route.cancellation.clone();
    let response_stream = stream! {
        let mut lifecycle = lifecycle;
        let mut current_response = Some(upstream_response);
        let mut retries_remaining = if safe_retry_enabled {
            SAFE_RETRY_MAX_ATTEMPTS
        } else {
            0
        };
        let mut retry_gate_active = safe_retry_enabled;
        let mut downstream_filter =
            data_only_sse_filter_enabled.then(SseDataEventFilter::default);
        let mut heartbeat = interval_at(
            tokio::time::Instant::now() + SSE_HEARTBEAT_INTERVAL,
            SSE_HEARTBEAT_INTERVAL,
        );
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        'attempts: loop {
            let response = current_response
                .take()
                .expect("safe retry response must be available");
            let mut upstream_stream = std::pin::pin!(response.bytes_stream());
            let retry_probe = retry_gate_active.then(SseEventCapture::new);
            let mut buffered = Vec::new();

            let attempt_end = loop {
                enum PollResult {
                    Upstream(Option<Result<Bytes, reqwest::Error>>),
                    Heartbeat,
                    Cancelled,
                }

                let next = tokio::select! {
                    value = upstream_stream.next() => PollResult::Upstream(value),
                    _ = cancellation.cancelled() => PollResult::Cancelled,
                    _ = heartbeat.tick(), if retry_gate_active && !data_only_sse_client => PollResult::Heartbeat,
                };
                match next {
                    PollResult::Heartbeat => {
                        yield Ok(Bytes::from_static(SSE_HEARTBEAT));
                    }
                    PollResult::Cancelled => {
                        lifecycle.finish(Some("上游切换或服务停止".to_string()));
                        yield Err(io::Error::new(
                            io::ErrorKind::Interrupted,
                            "upstream switched",
                        ));
                        break 'attempts;
                    }
                    PollResult::Upstream(Some(Ok(chunk))) => {
                        lifecycle.mark_first_chunk();
                        if retry_gate_active {
                            buffered.extend_from_slice(&chunk);
                            if let Some(probe) = &retry_probe {
                                probe.push(&chunk);
                            }
                            let must_commit = buffered.len() > SAFE_RETRY_BUFFER_LIMIT
                                || retry_probe.as_ref().is_some_and(|probe| {
                                    !probe.retry_safe_prefix() || probe.terminal_type().is_some()
                                });
                            if must_commit {
                                let committed = Bytes::from(std::mem::take(&mut buffered));
                                if let Some(committed) = prepare_downstream_chunk(
                                    response_capture.as_ref(),
                                    &mut downstream_filter,
                                    committed,
                                ) {
                                    yield Ok(committed);
                                }
                                retry_gate_active = false;
                                retries_remaining = 0;
                            }
                        } else {
                            if let Some(chunk) = prepare_downstream_chunk(
                                response_capture.as_ref(),
                                &mut downstream_filter,
                                chunk,
                            ) {
                                yield Ok(chunk);
                            }
                        }
                    }
                    PollResult::Upstream(Some(Err(error))) => {
                        break Some(error.to_string());
                    }
                    PollResult::Upstream(None) => {
                        break None;
                    }
                }
            };

            if let Some(probe) = &retry_probe {
                probe.finish();
            }
            let can_retry = retry_gate_active
                && retries_remaining > 0
                && retry_probe
                    .as_ref()
                    .is_some_and(|probe| probe.retry_safe_prefix() && probe.terminal_type().is_none());

            if can_retry {
                let reason = attempt_end.as_ref().map_or_else(
                    || "上游 SSE 未返回终止事件便结束".to_string(),
                    |error| format!("上游 SSE 传输错误: {error}"),
                );
                if !data_only_sse_client {
                    yield Ok(Bytes::from_static(SSE_HEARTBEAT));
                }
                lifecycle.mark_safe_retry_started(reason);
                let retry = replay_request
                    .as_ref()
                    .expect("safe retry request must be replayable")
                    .builder()
                    .send();
                let mut retry = std::pin::pin!(retry);
                let retry_response = loop {
                    enum RetryPoll {
                        Response(Result<reqwest::Response, reqwest::Error>),
                        Heartbeat,
                        Cancelled,
                    }
                    let retry_poll = tokio::select! {
                        response = &mut retry => RetryPoll::Response(response),
                        _ = heartbeat.tick(), if !data_only_sse_client => RetryPoll::Heartbeat,
                        _ = cancellation.cancelled() => RetryPoll::Cancelled,
                    };
                    match retry_poll {
                        RetryPoll::Response(response) => break response,
                        RetryPoll::Heartbeat => {
                            yield Ok(Bytes::from_static(SSE_HEARTBEAT));
                        }
                        RetryPoll::Cancelled => {
                            lifecycle.finish(Some("上游切换或服务停止".to_string()));
                            yield Err(io::Error::new(
                                io::ErrorKind::Interrupted,
                                "upstream switched",
                            ));
                            break 'attempts;
                        }
                    }
                };
                match retry_response {
                    Ok(response)
                        if response.status().is_success()
                            && is_sse_content_type(content_type(response.headers()).as_deref()) =>
                    {
                        retries_remaining = retries_remaining.saturating_sub(1);
                        lifecycle.mark_safe_retry_outcome(format!(
                            "已连接，HTTP {}，SSE 响应",
                            response.status()
                        ));
                        current_response = Some(response);
                        continue 'attempts;
                    }
                    Ok(response) => {
                        let error = format!(
                            "安全重试返回 HTTP {}{}",
                            response.status(),
                            if is_sse_content_type(content_type(response.headers()).as_deref()) {
                                ""
                            } else {
                                "，响应不是 SSE"
                            }
                        );
                        lifecycle.mark_safe_retry_outcome(error.clone());
                        if !buffered.is_empty() {
                            let committed = Bytes::from(std::mem::take(&mut buffered));
                            if let Some(committed) = prepare_downstream_chunk(
                                response_capture.as_ref(),
                                &mut downstream_filter,
                                committed,
                            ) {
                                yield Ok(committed);
                            }
                        }
                        if let Some(pending) = downstream_filter
                            .as_mut()
                            .and_then(SseDataEventFilter::finish)
                        {
                            yield Ok(pending);
                        }
                        lifecycle.finish(Some(error.clone()));
                        yield Err(io::Error::new(io::ErrorKind::ConnectionAborted, error));
                        break 'attempts;
                    }
                    Err(error) => {
                        let error = format!("安全重试连接上游失败: {error}");
                        lifecycle.mark_safe_retry_outcome(error.clone());
                        if !buffered.is_empty() {
                            let committed = Bytes::from(std::mem::take(&mut buffered));
                            if let Some(committed) = prepare_downstream_chunk(
                                response_capture.as_ref(),
                                &mut downstream_filter,
                                committed,
                            ) {
                                yield Ok(committed);
                            }
                        }
                        if let Some(pending) = downstream_filter
                            .as_mut()
                            .and_then(SseDataEventFilter::finish)
                        {
                            yield Ok(pending);
                        }
                        lifecycle.finish(Some(error.clone()));
                        yield Err(io::Error::new(io::ErrorKind::ConnectionAborted, error));
                        break 'attempts;
                    }
                }
            }

            if !buffered.is_empty() {
                let committed = Bytes::from(std::mem::take(&mut buffered));
                if let Some(committed) = prepare_downstream_chunk(
                    response_capture.as_ref(),
                    &mut downstream_filter,
                    committed,
                ) {
                    yield Ok(committed);
                }
            }
            if let Some(pending) = downstream_filter
                .as_mut()
                .and_then(SseDataEventFilter::finish)
            {
                yield Ok(pending);
            }
            match attempt_end {
                Some(error) => {
                    lifecycle.finish(Some(error.clone()));
                    yield Err(io::Error::new(io::ErrorKind::ConnectionAborted, error));
                }
                None => lifecycle.finish(None),
            }
            break 'attempts;
        }
    };

    let mut response = Response::new(Body::from_stream(response_stream));
    *response.status_mut() = status;
    *response.headers_mut() = response_headers;
    response
}

struct RequestLifecycle {
    metrics: Arc<ProxyMetrics>,
    logger: AppLogger,
    started: Instant,
    client_ip: String,
    method: String,
    path: String,
    upstream: String,
    status: u16,
    client_headers: Option<String>,
    upstream_headers: Option<String>,
    upstream_request_structure: Option<String>,
    session_debug: Option<String>,
    session_tag: Option<String>,
    request_capture: Option<BodyCapture>,
    response_capture: Option<ResponseCapture>,
    expects_responses_terminal: bool,
    upstream_headers_seconds: f64,
    first_chunk_seconds: Option<f64>,
    safe_retry_attempts: u8,
    finished: bool,
}

impl RequestLifecycle {
    #[allow(clippy::too_many_arguments)]
    fn new(
        metrics: Arc<ProxyMetrics>,
        logger: AppLogger,
        started: Instant,
        client_ip: String,
        method: String,
        path: String,
        upstream: String,
        status: u16,
        client_headers: Option<String>,
        upstream_headers: Option<String>,
        upstream_request_structure: Option<String>,
        session_debug: Option<String>,
        session_tag: Option<String>,
        request_capture: Option<BodyCapture>,
        response_capture: Option<ResponseCapture>,
        expects_responses_terminal: bool,
        upstream_headers_seconds: f64,
    ) -> Self {
        Self {
            metrics,
            logger,
            started,
            client_ip,
            method,
            path,
            upstream,
            status,
            client_headers,
            upstream_headers,
            upstream_request_structure,
            session_debug,
            session_tag,
            request_capture,
            response_capture,
            expects_responses_terminal,
            upstream_headers_seconds,
            first_chunk_seconds: None,
            safe_retry_attempts: 0,
            finished: false,
        }
    }

    fn mark_first_chunk(&mut self) {
        if self.first_chunk_seconds.is_none() {
            self.first_chunk_seconds = Some(self.started.elapsed().as_secs_f64());
        }
    }

    fn mark_safe_retry_started(&mut self, reason: String) {
        self.safe_retry_attempts = self.safe_retry_attempts.saturating_add(1);
        if let Some(response_capture) = &self.response_capture {
            response_capture.mark_retry_started(reason);
        }
    }

    fn mark_safe_retry_outcome(&mut self, outcome: String) {
        if let Some(response_capture) = &self.response_capture {
            response_capture.mark_retry_outcome(outcome);
        }
    }

    fn finish(&mut self, transport_error: Option<String>) {
        if self.finished {
            return;
        }
        self.finished = true;
        if let Some(response_capture) = &self.response_capture {
            response_capture.finish();
        }
        let terminal_error = transport_error
            .is_none()
            .then(|| {
                self.response_capture
                    .as_ref()
                    .and_then(ResponseCapture::terminal_error_message)
            })
            .flatten();
        let missing_terminal = transport_error.is_none()
            && terminal_error.is_none()
            && self.status < 400
            && self.expects_responses_terminal
            && self
                .response_capture
                .as_ref()
                .is_none_or(|capture| capture.terminal_type().is_none());
        let completion_error = transport_error.or(terminal_error).or_else(|| {
            missing_terminal.then(|| {
                if self.safe_retry_attempts > 0 {
                    "安全重试后 Responses SSE 仍缺少终止事件".to_string()
                } else {
                    "Responses SSE 缺少终止事件，上游流提前结束".to_string()
                }
            })
        });
        self.metrics.active.fetch_sub(1, Ordering::Relaxed);
        let failed = completion_error.is_some() || self.status >= 400;
        if failed {
            self.metrics.failed.fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics.success.fetch_add(1, Ordering::Relaxed);
        }
        let message = completion_error.unwrap_or_else(|| {
            if self.safe_retry_attempts > 0 {
                format!("请求完成（安全重试 {} 次）", self.safe_retry_attempts)
            } else {
                "请求完成".to_string()
            }
        });
        let level = if failed || self.status >= 500 {
            LogLevel::Error
        } else if self.status >= 400 {
            LogLevel::Warn
        } else {
            LogLevel::Info
        };
        let debug = if self.logger.debug_capture_enabled() {
            debug_details(
                self.client_headers.as_deref(),
                self.upstream_headers.as_deref(),
                self.upstream_request_structure.as_deref(),
                self.session_debug.as_deref(),
                self.request_capture.as_ref(),
                self.response_capture.as_ref(),
            )
        } else {
            None
        };
        self.logger.emit(
            LogEvent::request(
                level,
                message,
                self.client_ip.clone(),
                self.method.clone(),
                self.path.clone(),
                Some(self.upstream.clone()),
                self.status,
                self.started.elapsed().as_secs_f64(),
            )
            .with_request_timings(
                Some(self.upstream_headers_seconds),
                self.first_chunk_seconds,
            )
            .with_session_tag(self.session_tag.clone())
            .with_debug(debug),
        );
    }
}

impl Drop for RequestLifecycle {
    fn drop(&mut self) {
        if !self.finished {
            self.finish(Some("客户端断开连接".to_string()));
        }
    }
}

fn is_supported_reasoning_effort(effort: &str) -> bool {
    SUPPORTED_REASONING_EFFORTS.contains(&effort)
}

fn reasoning_target_uri(query: Option<&str>) -> Uri {
    let value = match query {
        Some(query) => format!("/v1/responses?{query}"),
        None => "/v1/responses".to_string(),
    };
    value
        .parse()
        .expect("existing request query must form a URI")
}

fn normalize_responses_body(
    body: &[u8],
    effort: Option<&str>,
    route: &RouteTarget,
    session_signals: &SessionSignals,
    session_hmac_key: &[u8],
) -> Result<(Vec<u8>, String, String, SessionDecision), RequestValidationError> {
    let mut value: Value = serde_json::from_slice(body).map_err(|error| {
        RequestValidationError::new(
            "invalid_json",
            format!("Responses 请求正文必须是有效 JSON: {error}"),
        )
    })?;
    let object = value.as_object_mut().ok_or_else(|| {
        RequestValidationError::new("invalid_json", "Responses 请求正文必须是 JSON 对象")
    })?;

    let original_prompt_cache_key = match object.get("prompt_cache_key") {
        Some(Value::String(value)) => Some(validate_session_value(
            value,
            "prompt_cache_key",
            PROMPT_CACHE_KEY_MAX_CHARS,
        )?),
        Some(_) => {
            return Err(RequestValidationError::new(
                "invalid_session_id",
                "prompt_cache_key 必须是字符串",
            ));
        }
        None => None,
    };
    let session = decide_session(
        session_hmac_key,
        route,
        session_signals,
        original_prompt_cache_key.as_deref(),
        true,
    );

    let ignored_max_output_tokens = object.remove("max_output_tokens").is_some();

    if let Some(prompt_cache_key) = &session.prompt_cache_key {
        object.insert(
            "prompt_cache_key".to_string(),
            Value::String(prompt_cache_key.clone()),
        );
    }

    if let Some(effort) = effort {
        match object.get_mut("reasoning") {
            Some(Value::Object(reasoning)) => {
                reasoning.insert("effort".to_string(), Value::String(effort.to_string()));
            }
            _ => {
                object.insert("reasoning".to_string(), json!({ "effort": effort }));
            }
        }
    }

    let mut parameters = extract_request_parameters(&value);
    if ignored_max_output_tokens {
        parameters = format!("已忽略参数: max_output_tokens\n{parameters}");
    }
    let structure = summarize_upstream_request_structure(&value);
    let body = serde_json::to_vec(&value).map_err(|error| {
        RequestValidationError::new(
            "invalid_json",
            format!("无法生成上游 Responses 请求: {error}"),
        )
    })?;
    Ok((body, parameters, structure, session))
}

fn summarize_upstream_request_structure(value: &Value) -> String {
    let Some(object) = value.as_object() else {
        return "<实际上游请求不是 JSON 对象>".to_string();
    };

    let mut top_level_fields = object.keys().map(String::as_str).collect::<Vec<_>>();
    top_level_fields.sort_unstable();
    let mut lines = vec![format!(
        "顶层字段 ({}): {}",
        top_level_fields.len(),
        top_level_fields.join(", ")
    )];

    match object.get("input") {
        Some(Value::Array(items)) => {
            lines.push(format!("input: 数组，共 {} 项", items.len()));
            for (index, item) in items.iter().enumerate() {
                summarize_input_item(index, item, &mut lines);
            }
        }
        Some(Value::String(_)) => lines.push("input: 字符串，共 1 项（内容已隐藏）".to_string()),
        Some(Value::Object(_)) => {
            lines.push("input: 单个对象，共 1 项".to_string());
            summarize_input_item(0, object.get("input").expect("input exists"), &mut lines);
        }
        Some(value) => lines.push(format!("input: {}", json_value_kind(value))),
        None => lines.push("input: <未提供>".to_string()),
    }

    match object.get("tools") {
        Some(Value::Array(tools)) => {
            lines.push(format!("tools: 数组，共 {} 项", tools.len()));
            for (index, tool) in tools.iter().enumerate() {
                let tool_type = tool
                    .get("type")
                    .and_then(debug_scalar)
                    .unwrap_or_else(|| json_value_kind(tool).to_string());
                lines.push(format!("  [{index}] type={tool_type}"));
            }
        }
        Some(value) => lines.push(format!("tools: {}", json_value_kind(value))),
        None => lines.push("tools: <未提供>".to_string()),
    }

    truncate_utf8(
        lines.join("\r\n"),
        DEBUG_STRUCTURE_LIMIT,
        "实际上游请求结构",
    )
}

fn summarize_input_item(index: usize, item: &Value, lines: &mut Vec<String>) {
    let Some(object) = item.as_object() else {
        lines.push(format!(
            "  [{index}] type={}（内容已隐藏）",
            json_value_kind(item)
        ));
        return;
    };

    let item_type = object
        .get("type")
        .and_then(debug_scalar)
        .unwrap_or_else(|| "<未提供>".to_string());
    let role = object
        .get("role")
        .and_then(debug_scalar)
        .unwrap_or_else(|| "<未提供>".to_string());
    let status = object
        .get("status")
        .and_then(debug_scalar)
        .unwrap_or_else(|| "<未提供>".to_string());
    let id = object
        .get("id")
        .and_then(debug_scalar)
        .unwrap_or_else(|| "<未提供>".to_string());
    lines.push(format!(
        "  [{index}] type={item_type}, role={role}, status={status}, id={id}"
    ));

    if let Some(content) = object.get("content") {
        let content_types = match content {
            Value::Array(items) => items
                .iter()
                .map(|item| {
                    item.get("type")
                        .and_then(debug_scalar)
                        .unwrap_or_else(|| json_value_kind(item).to_string())
                })
                .collect::<Vec<_>>(),
            Value::Object(item) => vec![
                item.get("type")
                    .and_then(debug_scalar)
                    .unwrap_or_else(|| "object".to_string()),
            ],
            value => vec![json_value_kind(value).to_string()],
        };
        lines.push(format!(
            "       content 类型 ({}): {}",
            content_types.len(),
            content_types.join(", ")
        ));
    }
}

fn json_value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn debug_scalar(value: &Value) -> Option<String> {
    let rendered = match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(_) | Value::Object(_) => return None,
    };
    Some(truncate_single_line(rendered, 256))
}

fn content_type(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
}

fn is_sse_content_type(content_type: Option<&str>) -> bool {
    content_type.is_some_and(|content_type| {
        content_type
            .split(';')
            .next()
            .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("text/event-stream"))
    })
}

fn requires_data_only_sse(headers: &HeaderMap) -> bool {
    headers
        .get("x-stainless-lang")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("go"))
        || headers
            .get(header::USER_AGENT)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .trim_start()
                    .to_ascii_lowercase()
                    .starts_with("openai/go")
            })
}

fn responses_request_allows_safe_retry(body: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    let Some(object) = value.as_object() else {
        return false;
    };
    if object.get("stream").and_then(Value::as_bool) != Some(true)
        || object.get("background").and_then(Value::as_bool) == Some(true)
        || object.contains_key("conversation")
        || object.contains_key("previous_response_id")
    {
        return false;
    }

    match object.get("tools") {
        None => true,
        Some(Value::Array(tools)) => tools
            .iter()
            .all(|tool| tool.get("type").and_then(Value::as_str) == Some("function")),
        Some(_) => false,
    }
}

fn render_debug_headers(headers: &HeaderMap) -> String {
    let mut lines = headers
        .iter()
        .map(|(name, value)| {
            let rendered = if is_sensitive_debug_header(name) {
                "<已隐藏>".to_string()
            } else {
                value
                    .to_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|_| format!("<非 UTF-8，{} 字节>", value.as_bytes().len()))
            };
            format!("{}: {rendered}", name.as_str())
        })
        .collect::<Vec<_>>();
    lines.sort_unstable();
    if lines.is_empty() {
        return "<无请求头>".to_string();
    }
    truncate_utf8(lines.join("\r\n"), DEBUG_HEADERS_LIMIT, "请求头")
}

fn is_sensitive_debug_header(name: &HeaderName) -> bool {
    let name = name.as_str().to_ascii_lowercase();
    matches!(
        name.as_str(),
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "set-cookie"
            | "sec-websocket-protocol"
            | "session-id"
            | "x-auth-token"
            | "x-access-token"
            | "x-prompt-cache-id"
            | "x-rab-session-id"
            | "x-rab-user-id"
    ) || name.contains("api-key")
        || name.contains("apikey")
        || name.contains("auth")
        || name.contains("secret")
        || name.ends_with("-token")
}

fn debug_details(
    client_headers: Option<&str>,
    upstream_headers: Option<&str>,
    upstream_request_structure: Option<&str>,
    session_debug: Option<&str>,
    request_capture: Option<&BodyCapture>,
    response_capture: Option<&ResponseCapture>,
) -> Option<RequestDebugDetails> {
    if client_headers.is_none()
        && upstream_headers.is_none()
        && upstream_request_structure.is_none()
        && session_debug.is_none()
        && request_capture.is_none()
        && response_capture.is_none()
    {
        return None;
    }

    let request = request_capture.map(BodyCapture::snapshot);
    let response =
        response_capture.and_then(|capture| capture.body.as_ref().map(BodyCapture::snapshot));
    let mut parameters = request
        .as_ref()
        .and_then(|snapshot| snapshot.parameters.clone())
        .unwrap_or_else(|| {
            request
                .as_ref()
                .map(extract_parameters_from_snapshot)
                .unwrap_or_else(|| "<未捕获请求参数>".to_string())
        });
    if let Some(session_debug) = session_debug {
        parameters = format!("{session_debug}\n{parameters}");
    }
    if let Some(response_capture) = response_capture {
        let retry = response_capture.retry_snapshot();
        if retry.attempts > 0 {
            parameters = format!(
                "安全重试次数: {}\n首次重试原因: {}\n最近重试结果: {}\n{}",
                retry.attempts,
                retry.reason.as_deref().unwrap_or("<未记录>"),
                retry.outcome.as_deref().unwrap_or("<未记录>"),
                parameters,
            );
        }
    }
    let request_body = request
        .as_ref()
        .map(render_snapshot)
        .unwrap_or_else(|| "<未捕获请求正文>".to_string());
    let response_body = response
        .as_ref()
        .map(render_snapshot)
        .unwrap_or_else(|| "<未收到上游响应正文>".to_string());
    let response_events = response_capture
        .and_then(|capture| capture.events.as_ref())
        .map(render_sse_event_summary)
        .unwrap_or_else(|| "<非 SSE 响应>".to_string());

    Some(RequestDebugDetails {
        client_headers: client_headers.unwrap_or("<未捕获客户端请求头>").to_string(),
        upstream_headers: upstream_headers.unwrap_or("<未捕获上游请求头>").to_string(),
        parameters,
        upstream_request_structure: upstream_request_structure
            .unwrap_or("<透明转发：请求正文未改写>")
            .to_string(),
        request_body,
        response_events,
        response_body,
    })
}

fn extract_parameters_from_snapshot(snapshot: &BodyCaptureSnapshot) -> String {
    snapshot.complete_bytes().map_or_else(
        || "<请求正文超过 128 KiB，无法从截断内容可靠提取参数>".to_string(),
        |bytes| extract_parameters_from_bytes(&bytes),
    )
}

fn render_sse_event_summary(capture: &SseEventCapture) -> String {
    let snapshot = capture.snapshot();
    let mut lines = Vec::new();
    if snapshot.event_counts.is_empty() {
        lines.push("<未解析到 SSE 事件>".to_string());
    } else {
        lines.push("事件类型计数：".to_string());
        for (event_type, count) in snapshot.event_counts {
            lines.push(format!("  {event_type}: {count}"));
        }
    }
    if snapshot.untracked_event_types > 0 {
        lines.push(format!(
            "未统计的新事件类型: {} 个事件（事件类型数量超过限制）",
            snapshot.untracked_event_types
        ));
    }
    if snapshot.oversized_lines > 0 {
        lines.push(format!(
            "超长 SSE 行: {} 行（详情已跳过）",
            snapshot.oversized_lines
        ));
    }

    match snapshot.terminal {
        Some(terminal) => {
            lines.push(format!("终止事件: {}", terminal.event_type));
            if let Some(value) = terminal.response_id {
                lines.push(format!("Response ID: {value}"));
            }
            if let Some(value) = terminal.status {
                lines.push(format!("status: {value}"));
            }
            if let Some(value) = terminal.error_code {
                lines.push(format!("error.code: {value}"));
            }
            if let Some(value) = terminal.error_message {
                lines.push(format!("error.message: {value}"));
            }
            if let Some(value) = terminal.incomplete_reason {
                lines.push(format!("incomplete_details.reason: {value}"));
            }
            if !terminal.details_available {
                lines.push("终止事件数据过长，详细字段可能不完整".to_string());
            }
        }
        None => lines.push("终止事件: <缺失>".to_string()),
    }

    truncate_utf8(lines.join("\r\n"), DEBUG_EVENTS_LIMIT, "SSE 事件摘要")
}

fn extract_parameters_from_bytes(body: &[u8]) -> String {
    match serde_json::from_slice::<Value>(body) {
        Ok(value) => extract_request_parameters(&value),
        Err(_) => "<请求正文不是可解析的 JSON>".to_string(),
    }
}

fn extract_request_parameters(value: &Value) -> String {
    const DIRECT_FIELDS: [&str; 15] = [
        "model",
        "stream",
        "reasoning_effort",
        "temperature",
        "top_p",
        "max_output_tokens",
        "max_completion_tokens",
        "max_tokens",
        "service_tier",
        "tool_choice",
        "parallel_tool_calls",
        "previous_response_id",
        "store",
        "background",
        "truncation",
    ];

    let Some(object) = value.as_object() else {
        return "<请求正文不是 JSON 对象>".to_string();
    };
    let mut parameters = Map::new();
    for field in DIRECT_FIELDS {
        if let Some(value) = object.get(field) {
            parameters.insert(field.to_string(), value.clone());
        }
    }
    if let Some(reasoning) = object.get("reasoning").and_then(Value::as_object) {
        if let Some(effort) = reasoning.get("effort") {
            parameters.insert("reasoning.effort".to_string(), effort.clone());
        }
        if let Some(mode) = reasoning.get("mode") {
            parameters.insert("reasoning.mode".to_string(), mode.clone());
        }
    }

    if parameters.is_empty() {
        return "<未识别到常用请求参数>".to_string();
    }
    let rendered = serde_json::to_string_pretty(&Value::Object(parameters))
        .unwrap_or_else(|_| "<请求参数格式化失败>".to_string());
    truncate_utf8(rendered, DEBUG_PARAMETERS_LIMIT, "请求参数")
}

fn truncate_utf8(mut value: String, limit: usize, label: &str) -> String {
    if value.len() <= limit {
        return value;
    }
    let mut boundary = limit;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    let total = value.len();
    value.truncate(boundary);
    value.push_str(&format!(
        "\r\n\r\n<已截断：{label}共 {total} 字节，仅保留前 {limit} 字节>"
    ));
    value
}

fn render_snapshot(snapshot: &BodyCaptureSnapshot) -> String {
    if let Some(bytes) = snapshot.complete_bytes() {
        return render_complete_content(snapshot.content_type.as_deref(), &bytes);
    }
    render_body_head_tail(
        snapshot.content_type.as_deref(),
        &snapshot.head,
        &snapshot.tail,
        snapshot.total_bytes,
    )
}

fn render_complete_body(content_type: Option<&str>, body: &[u8]) -> String {
    if body.len() <= DEBUG_PREVIEW_LIMIT * 2 {
        return render_complete_content(content_type, body);
    }
    render_body_head_tail(
        content_type,
        &body[..DEBUG_PREVIEW_LIMIT],
        &body[body.len() - DEBUG_PREVIEW_LIMIT..],
        body.len(),
    )
}

fn render_complete_content(content_type: Option<&str>, bytes: &[u8]) -> String {
    let media_type = content_type
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let is_json = media_type == "application/json" || media_type.ends_with("+json");
    let is_text = media_type.starts_with("text/")
        || is_json
        || media_type == "application/x-ndjson"
        || media_type == "application/xml"
        || media_type.ends_with("+xml")
        || media_type == "application/javascript"
        || media_type == "application/x-www-form-urlencoded"
        || media_type.is_empty();

    if is_json {
        serde_json::from_slice::<Value>(bytes)
            .ok()
            .and_then(|value| serde_json::to_string_pretty(&value).ok())
            .unwrap_or_else(|| String::from_utf8_lossy(bytes).into_owned())
    } else if is_text {
        String::from_utf8_lossy(bytes).into_owned()
    } else {
        let hex = bytes
            .iter()
            .take(64)
            .map(|byte| format!("{byte:02X}"))
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "<二进制正文，Content-Type: {}，共 {} 字节>\n前 {} 字节十六进制：{}",
            content_type.unwrap_or("未知"),
            bytes.len(),
            bytes.len().min(64),
            hex
        )
    }
}

fn render_body_head_tail(
    content_type: Option<&str>,
    head: &[u8],
    tail: &[u8],
    total_bytes: usize,
) -> String {
    let media_type = content_type
        .unwrap_or("")
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    let is_text = media_type.starts_with("text/")
        || media_type == "application/json"
        || media_type.ends_with("+json")
        || media_type == "application/x-ndjson"
        || media_type == "application/xml"
        || media_type.ends_with("+xml")
        || media_type == "application/javascript"
        || media_type == "application/x-www-form-urlencoded"
        || media_type.is_empty();
    let omitted = total_bytes
        .saturating_sub(head.len())
        .saturating_sub(tail.len());

    if is_text {
        format!(
            "{}\r\n\r\n<中间已省略 {omitted} 字节；以下为末尾 {} 字节>\r\n\r\n{}",
            String::from_utf8_lossy(head),
            tail.len(),
            String::from_utf8_lossy(tail)
        )
    } else {
        let head_hex = head
            .iter()
            .take(64)
            .map(|byte| format!("{byte:02X}"))
            .collect::<Vec<_>>()
            .join(" ");
        let tail_hex = tail
            .iter()
            .rev()
            .take(64)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .map(|byte| format!("{byte:02X}"))
            .collect::<Vec<_>>()
            .join(" ");
        format!(
            "<二进制正文，Content-Type: {}，共 {total_bytes} 字节>\r\n前 {} 字节中的前 64 字节：{head_hex}\r\n<中间已省略 {omitted} 字节>\r\n末尾 {} 字节中的后 64 字节：{tail_hex}",
            content_type.unwrap_or("未知"),
            head.len(),
            tail.len(),
        )
    }
}

fn authorized(headers: &HeaderMap, expected: &str) -> bool {
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some(provided) = value
        .strip_prefix("Bearer ")
        .or_else(|| value.strip_prefix("bearer "))
    else {
        return false;
    };
    provided.as_bytes().ct_eq(expected.as_bytes()).into()
}

fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    headers.contains_key(header::UPGRADE)
        || headers
            .get(header::CONNECTION)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.to_ascii_lowercase().contains("upgrade"))
}

fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str().to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn filtered_request_headers(headers: &HeaderMap) -> HeaderMap {
    let mut filtered = HeaderMap::new();
    for (name, value) in headers {
        if is_hop_by_hop(name)
            || *name == header::HOST
            || *name == header::AUTHORIZATION
            || name.as_str().eq_ignore_ascii_case("x-api-key")
            || name.as_str().eq_ignore_ascii_case("api-key")
            || name.as_str().eq_ignore_ascii_case("x-forwarded-for")
            || name.as_str().eq_ignore_ascii_case("x-real-ip")
            || name.as_str().eq_ignore_ascii_case("x-forwarded-proto")
            || name
                .as_str()
                .get(..6)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("x-rab-"))
            || name.as_str().eq_ignore_ascii_case("session-id")
        {
            continue;
        }
        filtered.append(name.clone(), value.clone());
    }
    filtered
}

fn filtered_response_headers(headers: &HeaderMap) -> HeaderMap {
    let mut filtered = HeaderMap::new();
    for (name, value) in headers {
        if !is_hop_by_hop(name) {
            filtered.append(name.clone(), value.clone());
        }
    }
    filtered
}

#[allow(clippy::too_many_arguments)]
fn logged_error(
    shared: &ProxyShared,
    status: StatusCode,
    code: &str,
    message: &str,
    client_ip: &str,
    method: &str,
    path: &str,
    started: Instant,
    upstream: Option<String>,
) -> Response {
    logged_error_with_debug(
        shared, status, code, message, client_ip, method, path, started, upstream, None,
    )
}

#[allow(clippy::too_many_arguments)]
fn logged_error_with_debug(
    shared: &ProxyShared,
    status: StatusCode,
    code: &str,
    message: &str,
    client_ip: &str,
    method: &str,
    path: &str,
    started: Instant,
    upstream: Option<String>,
    debug: Option<RequestDebugDetails>,
) -> Response {
    logged_error_with_context(
        shared, status, code, message, client_ip, method, path, started, upstream, debug, None,
        None, None,
    )
}

#[allow(clippy::too_many_arguments)]
fn logged_error_with_context(
    shared: &ProxyShared,
    status: StatusCode,
    code: &str,
    message: &str,
    client_ip: &str,
    method: &str,
    path: &str,
    started: Instant,
    upstream: Option<String>,
    debug: Option<RequestDebugDetails>,
    session_tag: Option<String>,
    upstream_headers_seconds: Option<f64>,
    first_chunk_seconds: Option<f64>,
) -> Response {
    shared.metrics.failed.fetch_add(1, Ordering::Relaxed);
    let level = if status.is_server_error() {
        LogLevel::Error
    } else {
        LogLevel::Warn
    };
    shared.logger.emit(
        LogEvent::request(
            level,
            message,
            client_ip,
            method,
            path,
            upstream,
            status.as_u16(),
            started.elapsed().as_secs_f64(),
        )
        .with_request_timings(upstream_headers_seconds, first_chunk_seconds)
        .with_session_tag(session_tag)
        .with_debug(debug),
    );
    openai_error(status, code, message)
}

pub fn openai_error(status: StatusCode, code: &str, message: &str) -> Response {
    (
        status,
        axum::Json(json!({
            "error": {
                "message": message,
                "type": "invalid_request_error",
                "param": null,
                "code": code
            }
        })),
    )
        .into_response()
}

pub async fn test_upstream(profile: &UpstreamProfile) -> Result<String> {
    profile.validate()?;
    let url = build_upstream_url(&profile.base_url, "/v1/models", None)?;
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(15))
        .build()?;
    let response = client
        .get(url)
        .bearer_auth(profile.api_key.trim())
        .send()
        .await
        .context("连接上游失败")?;
    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let body = compact_error_body(&body, 2048);
        let detail = if body.is_empty() {
            String::new()
        } else {
            format!("\n上游响应：{body}")
        };
        anyhow::bail!("上游返回 HTTP {status}{detail}");
    }
    Ok(format!("连接成功，HTTP {status}"))
}

fn compact_error_body(body: &str, limit: usize) -> String {
    let compact = body
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>();
    truncate_utf8(compact.trim().to_string(), limit, "上游错误正文")
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SESSION_KEY: [u8; 32] = [7; 32];

    fn test_route(kind: UpstreamKind, id: u128) -> RouteTarget {
        RouteTarget {
            id: uuid::Uuid::from_u128(id),
            name: "test".to_string(),
            kind,
            base_url: "http://127.0.0.1:8080/v1".to_string(),
            api_key: "upstream-key".to_string(),
            cancellation: CancellationToken::new(),
        }
    }

    fn no_session_signals() -> SessionSignals {
        SessionSignals {
            user_id: None,
            preferred_session_id: None,
            source: SessionSource::None,
        }
    }

    fn normalize_for_test(
        body: &[u8],
        effort: Option<&str>,
    ) -> (Vec<u8>, String, String, SessionDecision) {
        normalize_responses_body(
            body,
            effort,
            &test_route(UpstreamKind::Sub2Api, 1),
            &no_session_signals(),
            &TEST_SESSION_KEY,
        )
        .unwrap()
    }

    #[test]
    fn authenticates_bearer_key() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer rab_secret"),
        );
        assert!(authorized(&headers, "rab_secret"));
        assert!(!authorized(&headers, "rab_other"));
    }

    #[test]
    fn strips_sensitive_and_hop_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer user-key"),
        );
        headers.insert(header::HOST, HeaderValue::from_static("localhost"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert("x-api-key", HeaderValue::from_static("secret"));
        headers.insert("x-rab-user-id", HeaderValue::from_static("user-secret"));
        headers.insert(
            "x-rab-session-id",
            HeaderValue::from_static("session-secret"),
        );
        headers.insert("x-rab-future", HeaderValue::from_static("private"));
        headers.insert("session-id", HeaderValue::from_static("client-session"));
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        let filtered = filtered_request_headers(&headers);
        assert_eq!(filtered.len(), 1);
        assert_eq!(
            filtered.get(header::CONTENT_TYPE).unwrap(),
            "application/json"
        );
    }

    #[test]
    fn detects_openai_go_clients_that_require_data_only_sse() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::USER_AGENT,
            HeaderValue::from_static("OpenAI/Go 2.6.0"),
        );
        assert!(requires_data_only_sse(&headers));

        headers.clear();
        headers.insert("x-stainless-lang", HeaderValue::from_static("go"));
        assert!(requires_data_only_sse(&headers));

        headers.clear();
        headers.insert(
            header::USER_AGENT,
            HeaderValue::from_static("OpenAI/Python 2.0.0"),
        );
        assert!(!requires_data_only_sse(&headers));
    }

    #[test]
    fn data_only_sse_filter_drops_empty_events_across_chunks() {
        let input = concat!(
            ": upstream-keepalive\r\n\r\n",
            "retry: 2000\n\n",
            "event: response.created\n",
            "data: {\"type\":\"response.created\"}\n\n",
            "data:\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\"}"
        );
        let mut filter = SseDataEventFilter::default();
        let mut output = Vec::new();
        for chunk in input.as_bytes().chunks(7) {
            if let Some(filtered) = filter.push(chunk) {
                output.extend_from_slice(&filtered);
            }
        }
        if let Some(filtered) = filter.finish() {
            output.extend_from_slice(&filtered);
        }
        let output = String::from_utf8(output).unwrap();
        assert!(!output.contains("upstream-keepalive"));
        assert!(!output.contains("retry:"));
        assert!(!output.contains("data:\n\n"));
        assert!(output.contains("response.created"));
        assert!(output.contains("response.completed"));
    }

    #[test]
    fn stable_session_ids_are_deterministic_and_scoped() {
        let upstream_a = uuid::Uuid::from_u128(1);
        let upstream_b = uuid::Uuid::from_u128(2);
        let first = derive_session_id(
            &TEST_SESSION_KEY,
            upstream_a,
            Some("user-a"),
            Some("conversation-1"),
            Some("client-cache-group"),
        );
        let repeated = derive_session_id(
            &TEST_SESSION_KEY,
            upstream_a,
            Some("user-a"),
            Some("conversation-1"),
            Some("client-cache-group"),
        );
        assert_eq!(first, repeated);
        assert!(first.starts_with("rabs_"));
        assert_eq!(first.chars().count(), 48);
        assert!(first.chars().count() <= PROMPT_CACHE_KEY_MAX_CHARS);
        assert_ne!(
            first,
            derive_session_id(
                &TEST_SESSION_KEY,
                upstream_a,
                Some("user-b"),
                Some("conversation-1"),
                Some("client-cache-group"),
            )
        );
        assert_ne!(
            first,
            derive_session_id(
                &TEST_SESSION_KEY,
                upstream_a,
                Some("user-a"),
                Some("conversation-1"),
                Some("other-cache-group"),
            )
        );
        assert_ne!(
            first,
            derive_session_id(
                &TEST_SESSION_KEY,
                upstream_a,
                Some("user-a"),
                Some("conversation-2"),
                Some("client-cache-group"),
            )
        );
        assert_ne!(
            first,
            derive_session_id(
                &TEST_SESSION_KEY,
                upstream_b,
                Some("user-a"),
                Some("conversation-1"),
                Some("client-cache-group"),
            )
        );
    }

    #[test]
    fn session_signal_priority_and_validation_are_enforced() {
        let mut headers = HeaderMap::new();
        headers.insert("x-rab-user-id", HeaderValue::from_static("user-a"));
        headers.insert("x-rab-session-id", HeaderValue::from_static("rab-session"));
        headers.insert(
            "x-prompt-cache-id",
            HeaderValue::from_static("prompt-cache-session"),
        );
        let signals = parse_session_signals(&headers).unwrap();
        assert_eq!(signals.user_id.as_deref(), Some("user-a"));
        assert_eq!(signals.preferred_session_id.as_deref(), Some("rab-session"));
        assert_eq!(signals.source, SessionSource::RabSessionId);

        let mut oversized = HeaderMap::new();
        oversized.insert(
            "x-rab-session-id",
            HeaderValue::from_str(&"s".repeat(SESSION_FIELD_MAX_CHARS + 1)).unwrap(),
        );
        let error = parse_session_signals(&oversized).unwrap_err();
        assert_eq!(error.code, "invalid_session_id");
    }

    #[test]
    fn responses_session_injection_respects_upstream_behavior() {
        let signals = SessionSignals {
            user_id: Some("user-a".to_string()),
            preferred_session_id: Some("conversation-1".to_string()),
            source: SessionSource::RabSessionId,
        };
        let body = br#"{"model":"gpt-test","input":"hello"}"#;
        let (sub_body, _, _, sub_session) = normalize_responses_body(
            body,
            None,
            &test_route(UpstreamKind::Sub2Api, 1),
            &signals,
            &TEST_SESSION_KEY,
        )
        .unwrap();
        let sub_body: Value = serde_json::from_slice(&sub_body).unwrap();
        assert_eq!(
            sub_body["prompt_cache_key"].as_str(),
            sub_session.prompt_cache_key.as_deref()
        );
        assert!(sub_session.upstream_session_id.is_none());

        let (cli_body, _, _, cli_session) = normalize_responses_body(
            body,
            None,
            &test_route(UpstreamKind::CliProxyApi, 1),
            &signals,
            &TEST_SESSION_KEY,
        )
        .unwrap();
        let cli_body: Value = serde_json::from_slice(&cli_body).unwrap();
        assert_eq!(
            cli_body["prompt_cache_key"].as_str(),
            cli_session.upstream_session_id.as_deref()
        );
    }

    #[test]
    fn no_signal_requests_prefer_isolation() {
        let body = br#"{"model":"gpt-test","input":"hello"}"#;
        let (sub_body, _, _, sub_session) = normalize_responses_body(
            body,
            None,
            &test_route(UpstreamKind::Sub2Api, 1),
            &no_session_signals(),
            &TEST_SESSION_KEY,
        )
        .unwrap();
        let sub_body: Value = serde_json::from_slice(&sub_body).unwrap();
        assert!(sub_body.get("prompt_cache_key").is_none());
        assert!(sub_session.upstream_session_id.is_none());

        let route = test_route(UpstreamKind::CliProxyApi, 1);
        let first = decide_session(&TEST_SESSION_KEY, &route, &no_session_signals(), None, true);
        let second = decide_session(&TEST_SESSION_KEY, &route, &no_session_signals(), None, true);
        assert!(first.prompt_cache_key.is_none());
        assert!(second.prompt_cache_key.is_none());
        assert_ne!(first.upstream_session_id, second.upstream_session_id);
    }

    #[test]
    fn invalid_body_prompt_cache_key_is_rejected() {
        let body = serde_json::json!({
            "model": "gpt-test",
            "input": "hello",
            "prompt_cache_key": "x".repeat(PROMPT_CACHE_KEY_MAX_CHARS + 1)
        })
        .to_string();
        let error = normalize_responses_body(
            body.as_bytes(),
            None,
            &test_route(UpstreamKind::Sub2Api, 1),
            &no_session_signals(),
            &TEST_SESSION_KEY,
        )
        .unwrap_err();
        assert_eq!(error.code, "invalid_session_id");
    }

    #[test]
    fn responses_body_ignores_max_output_tokens_and_overrides_reasoning_effort() {
        let body = br#"{
            "model": "gpt-test",
            "reasoning": {"effort": "low", "mode": "standard"},
            "stream": true,
            "max_output_tokens": 131072
        }"#;
        let (rewritten, parameters, structure, _) = normalize_for_test(body, Some("high"));
        let value: Value = serde_json::from_slice(&rewritten).unwrap();
        assert_eq!(value["reasoning"]["effort"], "high");
        assert_eq!(value["reasoning"]["mode"], "standard");
        assert!(value.get("max_output_tokens").is_none());
        assert!(parameters.contains("已忽略参数: max_output_tokens"));
        assert!(parameters.contains("reasoning.effort"));
        assert!(parameters.contains("high"));
        assert!(!structure.contains("max_output_tokens"));
        assert!(structure.contains("reasoning"));
    }

    #[test]
    fn direct_responses_body_only_removes_max_output_tokens() {
        let body = br#"{
            "model": "gpt-test",
            "reasoning": {"effort": "medium"},
            "max_output_tokens": 32768
        }"#;
        let (rewritten, parameters, structure, _) = normalize_for_test(body, None);
        let value: Value = serde_json::from_slice(&rewritten).unwrap();
        assert_eq!(value["reasoning"]["effort"], "medium");
        assert!(value.get("max_output_tokens").is_none());
        assert!(parameters.contains("已忽略参数: max_output_tokens"));
        assert!(!structure.contains("max_output_tokens"));
    }

    #[test]
    fn only_requested_reasoning_efforts_are_supported() {
        for effort in SUPPORTED_REASONING_EFFORTS {
            assert!(is_supported_reasoning_effort(effort));
        }
        for effort in ["none", "minimal", "LOW", "ultra", ""] {
            assert!(!is_supported_reasoning_effort(effort));
        }
    }

    #[test]
    fn reasoning_target_keeps_query_parameters() {
        assert_eq!(
            reasoning_target_uri(Some("include=usage&test=1")).to_string(),
            "/v1/responses?include=usage&test=1"
        );
        assert_eq!(reasoning_target_uri(None).to_string(), "/v1/responses");
    }

    #[test]
    fn debug_preview_extracts_parameters_and_keeps_head_and_tail() {
        let value = json!({
            "model": "gpt-test",
            "stream": true,
            "reasoning": {"effort": "xhigh"},
            "temperature": 0.3
        });
        let parameters = extract_request_parameters(&value);
        assert!(parameters.contains("gpt-test"));
        assert!(parameters.contains("reasoning.effort"));
        assert!(parameters.contains("xhigh"));

        let mut body = vec![b'a'; DEBUG_PREVIEW_LIMIT];
        body.extend_from_slice(&[b'b'; 17]);
        body.extend_from_slice(&vec![b'c'; DEBUG_PREVIEW_LIMIT]);
        let capture = BodyCapture::new(Some("text/plain".to_string()));
        for chunk in body.chunks(7777) {
            capture.push(chunk);
        }
        let snapshot = capture.snapshot();
        assert_eq!(snapshot.head, vec![b'a'; DEBUG_PREVIEW_LIMIT]);
        assert_eq!(snapshot.tail, vec![b'c'; DEBUG_PREVIEW_LIMIT]);
        let rendered = render_snapshot(&snapshot);
        assert!(rendered.contains("中间已省略 17 字节"));
        assert!(rendered.ends_with(&"c".repeat(DEBUG_PREVIEW_LIMIT)));

        let small_body = vec![b'x'; DEBUG_PREVIEW_LIMIT + 17];
        let small_capture = BodyCapture::new(Some("text/plain".to_string()));
        for chunk in small_body.chunks(4093) {
            small_capture.push(chunk);
        }
        assert_eq!(
            small_capture.snapshot().complete_bytes().unwrap(),
            small_body
        );
    }

    #[test]
    fn sse_capture_counts_events_and_extracts_terminal_details_across_chunks() {
        let capture = SseEventCapture::new();
        let body = concat!(
            "event: response.created\n",
            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_123\",\"status\":\"in_progress\"}}\n\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"secret delta\"}\n\n",
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
        );
        for chunk in body.as_bytes().chunks(13) {
            capture.push(chunk);
        }
        capture.finish();

        let snapshot = capture.snapshot();
        assert_eq!(snapshot.event_counts["response.created"], 1);
        assert_eq!(snapshot.event_counts["response.output_text.delta"], 1);
        assert_eq!(snapshot.event_counts["response.completed"], 1);
        let terminal = snapshot.terminal.unwrap();
        assert_eq!(terminal.event_type, "response.completed");
        assert_eq!(terminal.response_id.as_deref(), Some("resp_123"));
        assert_eq!(terminal.status.as_deref(), Some("completed"));
        let summary = render_sse_event_summary(&capture);
        assert!(!summary.contains("secret delta"));
    }

    #[test]
    fn sse_capture_extracts_failed_and_incomplete_errors() {
        let failed = SseEventCapture::new();
        failed.push(b"data: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_failed\",\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"upstream broke\"}}}\n\n");
        failed.finish();
        let summary = render_sse_event_summary(&failed);
        assert!(summary.contains("response.failed"));
        assert!(summary.contains("server_error"));
        assert!(summary.contains("upstream broke"));

        let incomplete = SseEventCapture::new();
        incomplete.push(b"event: response.incomplete\ndata: {\"response\":{\"status\":\"incomplete\",\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n");
        incomplete.finish();
        let summary = render_sse_event_summary(&incomplete);
        assert!(summary.contains("response.incomplete"));
        assert!(summary.contains("max_output_tokens"));
    }

    #[test]
    fn safe_retry_prelude_accepts_only_non_visible_response_events() {
        for event_type in [
            "response.created",
            "response.in_progress",
            "response.queued",
        ] {
            assert!(is_safe_retry_prelude_event(event_type, None, false));
        }

        let empty_reasoning = json!({
            "item": {
                "type": "reasoning",
                "content": [],
                "summary": [],
                "encrypted_content": "opaque"
            }
        });
        assert!(is_safe_retry_prelude_event(
            "response.output_item.added",
            Some(&empty_reasoning),
            true
        ));

        let visible_reasoning = json!({
            "item": {
                "type": "reasoning",
                "content": [],
                "summary": [{"type": "summary_text", "text": "visible"}]
            }
        });
        assert!(!is_safe_retry_prelude_event(
            "response.output_item.added",
            Some(&visible_reasoning),
            true
        ));
        assert!(!is_safe_retry_prelude_event(
            "response.output_item.added",
            Some(&empty_reasoning),
            false
        ));

        for event_type in [
            "response.output_text.delta",
            "response.refusal.delta",
            "response.function_call_arguments.delta",
            "response.custom_unknown_event",
        ] {
            assert!(!is_safe_retry_prelude_event(event_type, None, true));
        }
    }

    #[test]
    fn safe_retry_request_eligibility_rejects_stateful_or_builtin_tool_requests() {
        assert!(responses_request_allows_safe_retry(
            br#"{"stream":true,"model":"gpt-test"}"#
        ));
        assert!(responses_request_allows_safe_retry(
            br#"{"stream":true,"tools":[{"type":"function","name":"lookup"}]}"#
        ));

        for body in [
            br#"{"stream":false}"#.as_slice(),
            br#"{"model":"gpt-test"}"#.as_slice(),
            br#"{"stream":true,"background":true}"#.as_slice(),
            br#"{"stream":true,"conversation":null}"#.as_slice(),
            br#"{"stream":true,"previous_response_id":null}"#.as_slice(),
            br#"{"stream":true,"tools":[{"type":"web_search_preview"}]}"#.as_slice(),
            br#"{"stream":true,"tools":{"type":"function"}}"#.as_slice(),
            br#"not-json"#.as_slice(),
        ] {
            assert!(!responses_request_allows_safe_retry(body));
        }
    }

    #[test]
    fn upstream_request_structure_is_safe_and_describes_items() {
        let body = br#"{
            "model":"gpt-test",
            "instructions":"TOP SECRET PROMPT",
            "max_output_tokens":131072,
            "input":[
                {"type":"message","role":"developer","status":"completed","id":"msg_1","content":[{"type":"input_text","text":"PRIVATE CONTENT"}]},
                {"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"PRIVATE REASONING"}]}
            ],
            "tools":[{"type":"function","name":"lookup","description":"PRIVATE DESCRIPTION","parameters":{"type":"object","properties":{"secret_schema":{"type":"string"}}}}]
        }"#;
        let (_, _, structure, _) = normalize_for_test(body, Some("high"));
        assert!(structure.contains("input: 数组，共 2 项"));
        assert!(structure.contains("type=message"));
        assert!(structure.contains("content 类型 (1): input_text"));
        assert!(structure.contains("tools: 数组，共 1 项"));
        assert!(structure.contains("type=function"));
        assert!(!structure.contains("max_output_tokens"));
        for secret in [
            "TOP SECRET PROMPT",
            "PRIVATE CONTENT",
            "PRIVATE REASONING",
            "PRIVATE DESCRIPTION",
            "secret_schema",
        ] {
            assert!(!structure.contains(secret));
        }
    }

    #[test]
    fn upstream_error_body_is_compact_and_limited() {
        assert_eq!(compact_error_body(" bad\r\nkey ", 128), "bad  key");
        let rendered = compact_error_body(&"x".repeat(256), 32);
        assert!(rendered.contains("已截断"));
        assert!(rendered.contains("256"));
    }

    #[test]
    fn debug_headers_hide_credentials_but_keep_diagnostic_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer client-secret"),
        );
        headers.insert("x-api-key", HeaderValue::from_static("api-secret"));
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        headers.insert(header::USER_AGENT, HeaderValue::from_static("test-sdk/1.0"));

        let rendered = render_debug_headers(&headers);
        assert!(!rendered.contains("client-secret"));
        assert!(!rendered.contains("api-secret"));
        assert!(rendered.contains("authorization: <已隐藏>"));
        assert!(rendered.contains("x-api-key: <已隐藏>"));
        assert!(rendered.contains("content-type: application/json"));
        assert!(rendered.contains("user-agent: test-sdk/1.0"));
    }

    #[tokio::test]
    async fn openai_errors_have_expected_shape() {
        use axum::body::to_bytes;
        let response = openai_error(StatusCode::UNAUTHORIZED, "invalid_api_key", "bad key");
        let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["error"]["code"], "invalid_api_key");
        assert_eq!(value["error"]["message"], "bad key");
    }
}
