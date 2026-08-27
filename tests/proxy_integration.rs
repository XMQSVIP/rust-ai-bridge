use std::{
    net::{SocketAddr, TcpListener as StdTcpListener},
    sync::{Arc, Mutex, atomic::AtomicBool},
    time::{Duration, Instant},
};

use async_stream::stream;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    response::Response,
    routing::any,
};
use bytes::Bytes;
use futures_util::StreamExt;
use rust_ai_bridge::{
    config::{UpstreamKind, UpstreamProfile},
    logger::{AppLogger, LogLevel},
    proxy::{ProxyMetrics, ProxyServer},
};
use tempfile::TempDir;
use tokio::net::TcpListener;
use uuid::Uuid;

#[derive(Debug, Clone)]
struct RequestRecord {
    method: String,
    uri: String,
    authorization: Option<String>,
    x_api_key: Option<String>,
    session_id: Option<String>,
    x_rab_user_id: Option<String>,
    x_rab_session_id: Option<String>,
    x_prompt_cache_id: Option<String>,
    x_client_request_id: Option<String>,
    body: Vec<u8>,
}

#[derive(Clone)]
struct MockState {
    name: String,
    records: Arc<Mutex<Vec<RequestRecord>>>,
}

async fn mock_handler(State(state): State<MockState>, request: Request) -> Response {
    let method = request.method().to_string();
    let uri = request.uri().to_string();
    let path = request.uri().path().to_string();
    let headers = request.headers().clone();
    let body = to_bytes(request.into_body(), 2 * 1024 * 1024)
        .await
        .unwrap()
        .to_vec();
    state.records.lock().unwrap().push(RequestRecord {
        method,
        uri,
        authorization: header_text(&headers, header::AUTHORIZATION.as_str()),
        x_api_key: header_text(&headers, "x-api-key"),
        session_id: header_text(&headers, "session-id"),
        x_rab_user_id: header_text(&headers, "x-rab-user-id"),
        x_rab_session_id: header_text(&headers, "x-rab-session-id"),
        x_prompt_cache_id: header_text(&headers, "x-prompt-cache-id"),
        x_client_request_id: header_text(&headers, "x-client-request-id"),
        body,
    });

    let bridge_test = state
        .records
        .lock()
        .unwrap()
        .last()
        .and_then(|record| serde_json::from_slice::<serde_json::Value>(&record.body).ok())
        .and_then(|value| {
            value
                .get("bridge_test")
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        });
    let bridge_attempt = bridge_test.as_ref().map_or(1, |marker| {
        state
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|record| {
                serde_json::from_slice::<serde_json::Value>(&record.body)
                    .ok()
                    .and_then(|value| {
                        value
                            .get("bridge_test")
                            .and_then(|value| value.as_str())
                            .map(str::to_owned)
                    })
                    .as_deref()
                    == Some(marker.as_str())
            })
            .count()
    });

    if path == "/v1/responses" {
        if let Some(marker) = bridge_test
            .as_deref()
            .and_then(|value| value.strip_prefix("concurrent-"))
        {
            let marker = marker.to_string();
            let body = stream! {
                yield Ok::<Bytes, std::io::Error>(Bytes::from(format!(
                    "event: response.created\ndata: {{\"type\":\"response.created\",\"response\":{{\"id\":\"resp_{marker}\",\"status\":\"in_progress\"}}}}\n\n"
                )));
                tokio::time::sleep(Duration::from_millis(40)).await;
                yield Ok::<Bytes, std::io::Error>(Bytes::from(format!(
                    "event: response.output_text.delta\ndata: {{\"type\":\"response.output_text.delta\",\"delta\":\"{marker}\"}}\n\n"
                )));
                yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
                    b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n",
                ));
            };
            let mut response = Response::new(Body::from_stream(body));
            response
                .headers_mut()
                .insert(header::CONTENT_TYPE, "text/event-stream".parse().unwrap());
            return response;
        }
        if bridge_test.as_deref() == Some("slow_comments_complete") {
            let body = stream! {
                yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
                    b": upstream-keepalive\n\nretry: 1000\n\n",
                ));
                tokio::time::sleep(Duration::from_millis(5_150)).await;
                yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
                    b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_slow\",\"status\":\"in_progress\"}}\n\n",
                ));
                yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
                    b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\n",
                ));
                yield Ok::<Bytes, std::io::Error>(Bytes::from_static(
                    b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_slow\",\"status\":\"completed\"}}\n\n",
                ));
            };
            let mut response = Response::new(Body::from_stream(body));
            response
                .headers_mut()
                .insert(header::CONTENT_TYPE, "text/event-stream".parse().unwrap());
            return response;
        }
        if bridge_test.as_deref() == Some("retry_then_http_error") && bridge_attempt > 1 {
            let mut response = Response::new(Body::from(r#"{"error":"retry failed"}"#));
            *response.status_mut() = StatusCode::BAD_GATEWAY;
            response
                .headers_mut()
                .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
            return response;
        }
        let events = match bridge_test.as_deref() {
            Some("complete") => Some(vec![
                Bytes::from_static(b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_complete\",\"status\":\"in_progress\"}}\n\n"),
                Bytes::from_static(b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n"),
                Bytes::from_static(b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"),
            ]),
            Some("missing_terminal") => Some(vec![Bytes::from_static(
                b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_missing\",\"status\":\"in_progress\"}}\n\n",
            )]),
            Some("retry_then_complete") if bridge_attempt == 1 => Some(vec![
                Bytes::from_static(b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_retry_1\",\"status\":\"in_progress\"}}\n\n"),
                Bytes::from_static(b"event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp_retry_1\",\"status\":\"in_progress\"}}\n\n"),
                Bytes::from_static(b"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"id\":\"rs_retry_1\",\"type\":\"reasoning\",\"content\":[],\"summary\":[],\"encrypted_content\":\"opaque\"},\"output_index\":0}\n\n"),
            ]),
            Some("retry_then_http_error") => Some(vec![Bytes::from_static(
                b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_retry_error\",\"status\":\"in_progress\"}}\n\n",
            )]),
            Some("retry_then_complete") => Some(vec![
                Bytes::from_static(b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_retry_2\",\"status\":\"in_progress\"}}\n\n"),
                Bytes::from_static(b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"retried\"}\n\n"),
                Bytes::from_static(b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_retry_2\",\"status\":\"completed\"}}\n\n"),
            ]),
            Some("unsafe_missing_terminal") => Some(vec![
                Bytes::from_static(b"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_unsafe\",\"status\":\"in_progress\"}}\n\n"),
                Bytes::from_static(b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"visible\"}\n\n"),
            ]),
            _ => None,
        };
        if let Some(events) = events {
            let response_length = events.iter().map(Bytes::len).sum::<usize>();
            let body = stream! {
                for event in events {
                    yield Ok::<Bytes, std::io::Error>(event);
                }
            };
            let mut response = Response::new(Body::from_stream(body));
            response
                .headers_mut()
                .insert(header::CONTENT_TYPE, "text/event-stream".parse().unwrap());
            if bridge_test.as_deref() == Some("retry_then_complete") {
                response.headers_mut().insert(
                    header::CONTENT_LENGTH,
                    response_length.to_string().parse().unwrap(),
                );
            }
            return response;
        }
    }

    if path == "/v1/error" {
        let mut response = Response::new(Body::from("rate limited"));
        *response.status_mut() = StatusCode::TOO_MANY_REQUESTS;
        response
            .headers_mut()
            .insert("x-upstream-error", "yes".parse().unwrap());
        return response;
    }

    if path == "/v1/stream" || path == "/v1/slow" {
        let delay = if path == "/v1/slow" {
            Duration::from_secs(5)
        } else {
            Duration::from_millis(450)
        };
        let body = stream! {
            yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data: first\n\n"));
            tokio::time::sleep(delay).await;
            yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"data: second\n\n"));
        };
        let mut response = Response::new(Body::from_stream(body));
        response
            .headers_mut()
            .insert(header::CONTENT_TYPE, "text/event-stream".parse().unwrap());
        return response;
    }

    Response::new(Body::from(format!(r#"{{"server":"{}"}}"#, state.name)))
}

fn header_text(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned)
}

fn record_json(record: &RequestRecord) -> serde_json::Value {
    serde_json::from_slice(&record.body).expect("mock request body must be JSON")
}

fn record_prompt_cache_key(record: &RequestRecord) -> Option<String> {
    record_json(record)
        .get("prompt_cache_key")
        .and_then(|value| value.as_str())
        .map(str::to_owned)
}

async fn spawn_mock(
    name: &str,
) -> (
    SocketAddr,
    Arc<Mutex<Vec<RequestRecord>>>,
    tokio::task::JoinHandle<()>,
) {
    let records = Arc::new(Mutex::new(Vec::new()));
    let state = MockState {
        name: name.to_string(),
        records: records.clone(),
    };
    let app = Router::new()
        .route("/v1", any(mock_handler))
        .route("/v1/{*path}", any(mock_handler))
        .with_state(state);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (address, records, task)
}

fn profile(name: &str, address: SocketAddr, key: &str) -> UpstreamProfile {
    profile_with_kind(name, address, key, UpstreamKind::Sub2Api)
}

fn profile_with_kind(
    name: &str,
    address: SocketAddr,
    key: &str,
    kind: UpstreamKind,
) -> UpstreamProfile {
    UpstreamProfile {
        id: Uuid::new_v4(),
        name: name.to_string(),
        kind,
        base_url: format!("http://{address}/v1"),
        encrypted_api_key: String::new(),
        api_key: key.to_string(),
    }
}

fn test_logger() -> (TempDir, AppLogger) {
    let directory = tempfile::tempdir().unwrap();
    let logger = AppLogger::new(directory.path().to_path_buf(), LogLevel::Trace).unwrap();
    (directory, logger)
}

async fn start_bridge(profile: &UpstreamProfile) -> (TempDir, AppLogger, ProxyServer) {
    let (directory, logger) = test_logger();
    let server = ProxyServer::start(
        "127.0.0.1:0".parse().unwrap(),
        "rab_client".to_string(),
        "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc".to_string(),
        profile,
        logger.clone(),
        Arc::new(ProxyMetrics::default()),
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    (directory, logger, server)
}

#[tokio::test]
async fn proxies_methods_queries_bodies_headers_and_errors() {
    let (upstream_address, records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, _logger, bridge) = start_bridge(&upstream).await;
    let client = reqwest::Client::new();

    let health = client
        .get(format!("http://{}/health", bridge.local_addr))
        .send()
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);
    let health: serde_json::Value = serde_json::from_str(&health.text().await.unwrap()).unwrap();
    assert_eq!(health["status"], "ok");
    assert_eq!(health["service"], "rust-ai-bridge");

    let response = client
        .patch(format!(
            "http://{}/v1/custom/path?mode=test",
            bridge.local_addr
        ))
        .bearer_auth("rab_client")
        .header("x-api-key", "must-not-leak")
        .header("x-rab-user-id", "private-user")
        .header("x-rab-session-id", "private-session")
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"hello":"world"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let records_snapshot = records.lock().unwrap().clone();
    assert_eq!(records_snapshot.len(), 1);
    let record = &records_snapshot[0];
    assert_eq!(record.method, "PATCH");
    assert_eq!(record.uri, "/v1/custom/path?mode=test");
    assert_eq!(
        record.authorization.as_deref(),
        Some("Bearer upstream-secret")
    );
    assert_eq!(record.x_api_key, None);
    assert_eq!(record.x_rab_user_id, None);
    assert_eq!(record.x_rab_session_id, None);
    assert_eq!(record.session_id, None);
    assert_eq!(record.body, br#"{"hello":"world"}"#);

    let error_response = client
        .get(format!("http://{}/v1/error", bridge.local_addr))
        .bearer_auth("rab_client")
        .send()
        .await
        .unwrap();
    assert_eq!(error_response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        error_response.headers().get("x-upstream-error").unwrap(),
        "yes"
    );
    assert_eq!(error_response.text().await.unwrap(), "rate limited");

    let before_unauthorized = records.lock().unwrap().len();
    let unauthorized = client
        .get(format!("http://{}/v1/models", bridge.local_addr))
        .bearer_auth("wrong-key")
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    let body: serde_json::Value =
        serde_json::from_str(&unauthorized.text().await.unwrap()).unwrap();
    assert_eq!(body["error"]["code"], "invalid_api_key");
    assert_eq!(records.lock().unwrap().len(), before_unauthorized);

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn reasoning_url_routes_to_responses_and_overrides_effort() {
    let (upstream_address, records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, _logger, bridge) = start_bridge(&upstream).await;
    let client = reqwest::Client::new();

    let response = client
        .post(format!(
            "http://{}/high/responses?include=usage",
            bridge.local_addr
        ))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::json!({
                "model": "gpt-test",
                "input": "hello",
                "stream": true,
                "max_output_tokens": 131072,
                "reasoning": {
                    "effort": "low",
                    "mode": "standard"
                }
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let records_snapshot = records.lock().unwrap().clone();
    assert_eq!(records_snapshot.len(), 1);
    let record = &records_snapshot[0];
    assert_eq!(record.method, "POST");
    assert_eq!(record.uri, "/v1/responses?include=usage");
    assert_eq!(
        record.authorization.as_deref(),
        Some("Bearer upstream-secret")
    );
    let body: serde_json::Value = serde_json::from_slice(&record.body).unwrap();
    assert_eq!(body["reasoning"]["effort"], "high");
    assert_eq!(body["reasoning"]["mode"], "standard");
    assert!(body.get("max_output_tokens").is_none());

    let before_rejected = records.lock().unwrap().len();
    let rejected = client
        .post(format!("http://{}/minimal/responses", bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"model":"gpt-test","input":"hello"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    let error: serde_json::Value = serde_json::from_str(&rejected.text().await.unwrap()).unwrap();
    assert_eq!(error["error"]["code"], "invalid_reasoning_effort");
    assert_eq!(records.lock().unwrap().len(), before_rejected);

    let unauthorized = client
        .post(format!("http://{}/max/responses", bridge.local_addr))
        .bearer_auth("wrong-key")
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"model":"gpt-test","input":"hello"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(records.lock().unwrap().len(), before_rejected);

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn direct_responses_ignores_max_output_tokens_without_affecting_other_routes() {
    let (upstream_address, records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, _logger, bridge) = start_bridge(&upstream).await;
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{}/v1/responses", bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"model":"gpt-test","input":"hello","max_output_tokens":131072}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = client
        .post(format!("http://{}/v1/chat/completions", bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"model":"gpt-test","messages":[],"max_output_tokens":77}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let records_snapshot = records.lock().unwrap().clone();
    assert_eq!(records_snapshot.len(), 2);
    let responses_body: serde_json::Value =
        serde_json::from_slice(&records_snapshot[0].body).unwrap();
    assert!(responses_body.get("max_output_tokens").is_none());
    assert!(responses_body.get("prompt_cache_key").is_none());
    let chat_body: serde_json::Value = serde_json::from_slice(&records_snapshot[1].body).unwrap();
    assert_eq!(chat_body["max_output_tokens"], 77);

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn streams_first_sse_chunk_without_buffering_entire_response() {
    let (upstream_address, _records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, logger, bridge) = start_bridge(&upstream).await;
    logger.set_debug_capture(true);
    let client = reqwest::Client::new();

    let response = client
        .get(format!("http://{}/v1/stream", bridge.local_addr))
        .bearer_auth("rab_client")
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    let started = Instant::now();
    let mut stream = response.bytes_stream();
    let first = tokio::time::timeout(Duration::from_millis(250), stream.next())
        .await
        .expect("first SSE chunk was buffered")
        .unwrap()
        .unwrap();
    assert_eq!(first, Bytes::from_static(b"data: first\n\n"));
    assert!(started.elapsed() < Duration::from_millis(300));
    let second = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second, Bytes::from_static(b"data: second\n\n"));
    assert!(stream.next().await.is_none());

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if logger.snapshot().iter().any(|event| {
                event.debug.as_ref().is_some_and(|debug| {
                    debug.response_body.contains("data: first")
                        && debug.response_body.contains("data: second")
                })
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("SSE debug capture was not completed");
    let event = logger
        .snapshot()
        .into_iter()
        .find(|event| event.path.as_deref() == Some("/v1/stream"))
        .expect("stream request log was not emitted");
    let upstream_headers_seconds = event
        .upstream_headers_seconds
        .expect("upstream response-header timing missing");
    let first_chunk_seconds = event
        .first_chunk_seconds
        .expect("first response-chunk timing missing");
    assert!(first_chunk_seconds >= upstream_headers_seconds);
    assert!(event.duration_seconds.unwrap() >= first_chunk_seconds);
    assert_eq!(bridge.shared.metrics.snapshot().success, 1);
    assert_eq!(bridge.shared.metrics.snapshot().failed, 0);

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn responses_sse_logs_structure_events_and_terminal_status() {
    let (upstream_address, records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, logger, bridge) = start_bridge(&upstream).await;
    logger.set_debug_capture(true);
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{}/high/responses", bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::json!({
                "model": "gpt-test",
                "bridge_test": "complete",
                "max_output_tokens": 131072,
                "input": [{
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "do not log this prompt"}]
                }]
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    assert!(body.contains("response.completed"));

    let event = logger
        .snapshot()
        .into_iter()
        .find(|event| event.path.as_deref() == Some("/high/responses"))
        .expect("Responses request log was not emitted");
    assert_eq!(event.level, LogLevel::Info);
    let debug = event.debug.expect("debug details missing");
    assert!(
        debug
            .upstream_request_structure
            .contains("content 类型 (1): input_text")
    );
    assert!(
        !debug
            .upstream_request_structure
            .contains("do not log this prompt")
    );
    assert!(
        !debug
            .upstream_request_structure
            .contains("max_output_tokens")
    );
    assert!(debug.response_events.contains("response.created: 1"));
    assert!(debug.response_events.contains("response.completed: 1"));
    assert!(debug.response_events.contains("Response ID: resp_complete"));
    assert!(debug.response_events.contains("status: completed"));
    assert_eq!(bridge.shared.metrics.snapshot().success, 1);

    let upstream_body: serde_json::Value =
        serde_json::from_slice(&records.lock().unwrap()[0].body).unwrap();
    assert!(upstream_body.get("max_output_tokens").is_none());

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn openai_go_clients_receive_only_data_bearing_sse_events() {
    let (upstream_address, _records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, logger, bridge) = start_bridge(&upstream).await;
    logger.set_debug_capture(true);
    let client = reqwest::Client::new();

    let go_request = client
        .post(format!("http://{}/high/responses", bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::USER_AGENT, "OpenAI/Go 2.6.0")
        .header("x-stainless-lang", "go")
        .body(
            serde_json::json!({
                "model": "gpt-test",
                "bridge_test": "slow_comments_complete",
                "stream": true,
                "input": "hello"
            })
            .to_string(),
        )
        .send();
    let generic_request = client
        .post(format!("http://{}/high/responses", bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::USER_AGENT, "test-client/1.0")
        .body(
            serde_json::json!({
                "model": "gpt-test",
                "bridge_test": "slow_comments_complete",
                "stream": true,
                "input": "hello"
            })
            .to_string(),
        )
        .send();
    let (go_response, generic_response) = tokio::join!(go_request, generic_request);
    let go_response = go_response.unwrap();
    let generic_response = generic_response.unwrap();
    assert_eq!(go_response.status(), StatusCode::OK);
    assert_eq!(generic_response.status(), StatusCode::OK);
    let (go_body, generic_body) = tokio::join!(go_response.text(), generic_response.text());
    let go_body = go_body.unwrap();
    let generic_body = generic_body.unwrap();
    assert!(!go_body.contains("rab-keepalive"));
    assert!(!go_body.contains("upstream-keepalive"));
    assert!(!go_body.contains("retry:"));
    assert!(go_body.contains("response.output_text.delta"));
    assert!(go_body.contains("response.completed"));
    assert!(generic_body.contains("rab-keepalive"));
    assert!(generic_body.contains("upstream-keepalive"));
    assert!(generic_body.contains("retry:"));

    let event = logger
        .snapshot()
        .into_iter()
        .find(|event| {
            event.path.as_deref() == Some("/high/responses")
                && event
                    .debug
                    .as_ref()
                    .is_some_and(|debug| debug.parameters.contains("OpenAI Go SSE 兼容模式: 是"))
        })
        .expect("OpenAI Go request log was not emitted");
    assert_eq!(event.level, LogLevel::Info);
    assert!(
        event
            .debug
            .expect("debug details missing")
            .parameters
            .contains("OpenAI Go SSE 兼容模式: 是")
    );

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn responses_sse_without_terminal_event_is_logged_as_failure() {
    let (upstream_address, records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, logger, bridge) = start_bridge(&upstream).await;
    logger.set_debug_capture(true);
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{}/v1/responses", bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::json!({
                "model": "gpt-test",
                "bridge_test": "missing_terminal",
                "stream": true
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.text().await.unwrap().contains("response.created"));

    let event = logger
        .snapshot()
        .into_iter()
        .find(|event| event.path.as_deref() == Some("/v1/responses"))
        .expect("Responses request log was not emitted");
    assert_eq!(event.level, LogLevel::Error);
    assert_eq!(event.message, "安全重试后 Responses SSE 仍缺少终止事件");
    let debug = event.debug.expect("debug details missing");
    assert!(debug.parameters.contains("安全重试次数: 1"));
    assert!(debug.response_events.contains("response.created: 1"));
    assert!(debug.response_events.contains("终止事件: <缺失>"));
    assert_eq!(records.lock().unwrap().len(), 2);
    assert_eq!(bridge.shared.metrics.snapshot().success, 0);
    assert_eq!(bridge.shared.metrics.snapshot().failed, 1);

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn responses_safe_retry_recovers_before_meaningful_output() {
    let (upstream_address, records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, logger, bridge) = start_bridge(&upstream).await;
    logger.set_debug_capture(true);
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{}/high/responses", bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::json!({
                "model": "gpt-test",
                "bridge_test": "retry_then_complete",
                "stream": true,
                "input": "hello"
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get(header::CONTENT_LENGTH).is_none());
    let body = response.text().await.unwrap();
    assert!(body.contains("retried"));
    assert!(body.contains("response.completed"));
    assert!(!body.contains("resp_retry_1"));
    assert_eq!(body.matches("response.created").count(), 2);

    let event = logger
        .snapshot()
        .into_iter()
        .find(|event| event.path.as_deref() == Some("/high/responses"))
        .expect("safe retry request log was not emitted");
    assert_eq!(event.level, LogLevel::Info);
    assert_eq!(event.message, "请求完成（安全重试 1 次）");
    let debug = event.debug.expect("debug details missing");
    assert!(debug.parameters.contains("安全重试次数: 1"));
    assert!(debug.response_events.contains("response.created: 1"));
    assert!(debug.response_events.contains("Response ID: resp_retry_2"));
    {
        let records = records.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].body, records[1].body);
        assert_eq!(records[0].authorization, records[1].authorization);
        assert_eq!(records[0].session_id, records[1].session_id);
    }
    assert_eq!(bridge.shared.metrics.snapshot().success, 1);
    assert_eq!(bridge.shared.metrics.snapshot().failed, 0);

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn responses_safe_retry_failure_is_visible_in_debug_details() {
    let (upstream_address, records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, logger, bridge) = start_bridge(&upstream).await;
    logger.set_debug_capture(true);
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{}/v1/responses", bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::json!({
                "model": "gpt-test",
                "bridge_test": "retry_then_http_error",
                "stream": true,
                "input": "hello"
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = response.bytes().await;

    let event = logger
        .snapshot()
        .into_iter()
        .find(|event| event.path.as_deref() == Some("/v1/responses"))
        .expect("failed safe retry request log was not emitted");
    assert_eq!(event.level, LogLevel::Error);
    assert!(event.message.contains("安全重试返回 HTTP 502"));
    let debug = event.debug.expect("debug details missing");
    assert!(debug.parameters.contains("安全重试次数: 1"));
    assert!(
        debug
            .parameters
            .contains("最近重试结果: 安全重试返回 HTTP 502")
    );
    assert_eq!(records.lock().unwrap().len(), 2);

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn responses_safe_retry_stops_after_visible_output() {
    let (upstream_address, records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, logger, bridge) = start_bridge(&upstream).await;
    logger.set_debug_capture(true);
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{}/v1/responses", bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::json!({
                "model": "gpt-test",
                "bridge_test": "unsafe_missing_terminal",
                "stream": true,
                "input": "hello"
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.text().await.unwrap();
    assert!(body.contains("visible"));
    assert_eq!(records.lock().unwrap().len(), 1);

    let event = logger
        .snapshot()
        .into_iter()
        .find(|event| event.path.as_deref() == Some("/v1/responses"))
        .expect("unsafe stream request log was not emitted");
    assert_eq!(event.level, LogLevel::Error);
    assert_eq!(event.message, "Responses SSE 缺少终止事件，上游流提前结束");
    assert!(
        !event
            .debug
            .expect("debug details missing")
            .parameters
            .contains("安全重试次数")
    );

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn responses_safe_retry_is_disabled_for_cli_and_builtin_tools() {
    let (cli_address, cli_records, cli_task) = spawn_mock("CLI").await;
    let cli = profile_with_kind(
        "CLI",
        cli_address,
        "cliproxy-secret",
        UpstreamKind::CliProxyApi,
    );
    let (_cli_logs, _cli_logger, cli_bridge) = start_bridge(&cli).await;
    let client = reqwest::Client::new();

    let response = client
        .post(format!("http://{}/v1/responses", cli_bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::json!({
                "model": "gpt-test",
                "bridge_test": "missing_terminal",
                "stream": true,
                "input": "hello"
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    let _ = response.text().await.unwrap();
    assert_eq!(cli_records.lock().unwrap().len(), 1);

    cli_bridge.stop().await;
    cli_task.abort();

    let (sub_address, sub_records, sub_task) = spawn_mock("Sub").await;
    let sub = profile("Sub", sub_address, "sub-secret");
    let (_sub_logs, _sub_logger, sub_bridge) = start_bridge(&sub).await;
    let response = client
        .post(format!("http://{}/v1/responses", sub_bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::json!({
                "model": "gpt-test",
                "bridge_test": "missing_terminal",
                "stream": true,
                "input": "hello",
                "tools": [{"type": "web_search"}]
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    let _ = response.text().await.unwrap();
    assert_eq!(sub_records.lock().unwrap().len(), 1);

    sub_bridge.stop().await;
    sub_task.abort();
}

#[tokio::test]
async fn switching_upstream_cancels_old_stream_and_routes_new_requests() {
    let (address_a, _records_a, task_a) = spawn_mock("A").await;
    let (address_b, records_b, task_b) = spawn_mock("B").await;
    let profile_a = profile("A", address_a, "key-a");
    let profile_b = profile("B", address_b, "key-b");
    let (_logs, _logger, bridge) = start_bridge(&profile_a).await;
    let client = reqwest::Client::new();

    let response = client
        .get(format!("http://{}/v1/slow", bridge.local_addr))
        .bearer_auth("rab_client")
        .send()
        .await
        .unwrap();
    let mut old_stream = response.bytes_stream();
    assert!(old_stream.next().await.unwrap().is_ok());
    bridge.shared.switch_upstream(&profile_b);
    let cancelled = tokio::time::timeout(Duration::from_secs(1), old_stream.next())
        .await
        .expect("old stream was not cancelled promptly");
    assert!(cancelled.is_none() || cancelled.unwrap().is_err());

    let response = client
        .get(format!("http://{}/v1/models", bridge.local_addr))
        .bearer_auth("rab_client")
        .send()
        .await
        .unwrap();
    assert_eq!(response.text().await.unwrap(), r#"{"server":"B"}"#);
    {
        let records = records_b.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].authorization.as_deref(), Some("Bearer key-b"));
    }

    bridge.stop().await;
    task_a.abort();
    task_b.abort();
}

#[tokio::test]
async fn client_disconnect_during_stream_releases_active_request() {
    let (upstream_address, _records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, _logger, bridge) = start_bridge(&upstream).await;
    let client = reqwest::Client::new();

    let response = client
        .get(format!("http://{}/v1/slow", bridge.local_addr))
        .bearer_auth("rab_client")
        .send()
        .await
        .unwrap();
    let mut stream = response.bytes_stream();
    assert!(stream.next().await.unwrap().is_ok());
    assert_eq!(bridge.shared.metrics.snapshot().active, 1);
    drop(stream);

    tokio::time::timeout(Duration::from_secs(1), async {
        while bridge.shared.metrics.snapshot().active != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("active request was not released after client disconnect");
    assert_eq!(bridge.shared.metrics.snapshot().failed, 1);

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn stopped_server_can_rebind_the_same_port() {
    let (upstream_address, _records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let port_probe = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let address = port_probe.local_addr().unwrap();
    drop(port_probe);

    let (_logs_a, logger_a) = test_logger();
    let first = ProxyServer::start(
        address,
        "rab_client".to_string(),
        "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc".to_string(),
        &upstream,
        logger_a,
        Arc::new(ProxyMetrics::default()),
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    first.stop().await;

    let (_logs_b, logger_b) = test_logger();
    let second = ProxyServer::start(
        address,
        "rab_client".to_string(),
        "BwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwcHBwc".to_string(),
        &upstream,
        logger_b,
        Arc::new(ProxyMetrics::default()),
        Arc::new(AtomicBool::new(false)),
    )
    .await
    .unwrap();
    second.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn prompt_cache_header_maps_to_stable_user_scoped_responses_key() {
    let (upstream_address, records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, _logger, bridge) = start_bridge(&upstream).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/responses", bridge.local_addr);

    for user in ["user-a", "user-a", "user-b"] {
        let response = client
            .post(&url)
            .bearer_auth("rab_client")
            .header("x-rab-user-id", user)
            .header("x-prompt-cache-id", "conversation-1")
            .header(header::CONTENT_TYPE, "application/json")
            .body(r#"{"model":"gpt-test","input":"hello"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    let other_upstream = profile("B", upstream_address, "upstream-secret-b");
    bridge.shared.switch_upstream(&other_upstream);
    let response = client
        .post(&url)
        .bearer_auth("rab_client")
        .header("x-rab-user-id", "user-a")
        .header("x-prompt-cache-id", "conversation-1")
        .header(header::CONTENT_TYPE, "application/json")
        .body(r#"{"model":"gpt-test","input":"hello"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    {
        let records = records.lock().unwrap();
        assert_eq!(records.len(), 4);
        let first = record_prompt_cache_key(&records[0]).unwrap();
        let repeated = record_prompt_cache_key(&records[1]).unwrap();
        let other_user = record_prompt_cache_key(&records[2]).unwrap();
        let other_upstream = record_prompt_cache_key(&records[3]).unwrap();
        assert_eq!(first, repeated);
        assert_ne!(first, other_user);
        assert_ne!(first, other_upstream);
        assert!(first.starts_with("rabs_"));
        assert!(first.len() <= 64);
        for record in records.iter() {
            assert_eq!(record.x_rab_user_id, None);
            assert_eq!(record.x_rab_session_id, None);
            assert_eq!(record.x_prompt_cache_id.as_deref(), Some("conversation-1"));
        }
        assert_eq!(
            records[0].authorization.as_deref(),
            Some("Bearer upstream-secret")
        );
        assert_eq!(
            records[3].authorization.as_deref(),
            Some("Bearer upstream-secret-b")
        );
    }

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn cliproxy_responses_uses_matching_anonymous_body_and_session_ids() {
    let (upstream_address, records, upstream_task) = spawn_mock("CLI").await;
    let upstream = profile_with_kind(
        "CLI",
        upstream_address,
        "cliproxy-secret",
        UpstreamKind::CliProxyApi,
    );
    let (_logs, _logger, bridge) = start_bridge(&upstream).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/responses", bridge.local_addr);

    for session in ["conversation-1", "conversation-1", "conversation-2"] {
        let response = client
            .post(&url)
            .bearer_auth("rab_client")
            .header("x-rab-user-id", "user-a")
            .header("x-rab-session-id", session)
            .header("session-id", "client-must-not-control-this")
            .header("x-client-request-id", "request-observer")
            .header(header::CONTENT_TYPE, "application/json")
            .body(r#"{"model":"gpt-test","input":"hello","prompt_cache_key":"client-cache-group"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    {
        let records = records.lock().unwrap();
        assert_eq!(records.len(), 3);
        let first = record_prompt_cache_key(&records[0]).unwrap();
        assert_eq!(records[0].session_id.as_deref(), Some(first.as_str()));
        assert_eq!(
            record_prompt_cache_key(&records[1]).as_deref(),
            Some(first.as_str())
        );
        assert_eq!(records[1].session_id.as_deref(), Some(first.as_str()));
        assert_ne!(
            record_prompt_cache_key(&records[2]).as_deref(),
            Some(first.as_str())
        );
        assert_ne!(records[2].session_id.as_deref(), Some(first.as_str()));
        for record in records.iter() {
            assert_eq!(
                record.authorization.as_deref(),
                Some("Bearer cliproxy-secret")
            );
            assert_eq!(record.x_rab_user_id, None);
            assert_eq!(record.x_rab_session_id, None);
            assert_eq!(
                record.x_client_request_id.as_deref(),
                Some("request-observer")
            );
            assert_ne!(
                record.session_id.as_deref(),
                Some("client-must-not-control-this")
            );
            assert_eq!(record.session_id, record_prompt_cache_key(record));
        }
    }

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn cliproxy_without_session_signal_uses_unique_ephemeral_ids_only() {
    let (upstream_address, records, upstream_task) = spawn_mock("CLI").await;
    let upstream = profile_with_kind(
        "CLI",
        upstream_address,
        "cliproxy-secret",
        UpstreamKind::CliProxyApi,
    );
    let (_logs, _logger, bridge) = start_bridge(&upstream).await;
    let client = reqwest::Client::new();

    for _ in 0..2 {
        let response = client
            .post(format!("http://{}/v1/responses", bridge.local_addr))
            .bearer_auth("rab_client")
            .header(header::CONTENT_TYPE, "application/json")
            .body(r#"{"model":"gpt-test","input":"hello"}"#)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    {
        let records = records.lock().unwrap();
        assert_eq!(records.len(), 2);
        let first = records[0].session_id.as_deref().unwrap();
        let second = records[1].session_id.as_deref().unwrap();
        assert!(first.starts_with("rabe_"));
        assert!(second.starts_with("rabe_"));
        assert_ne!(first, second);
        assert!(record_prompt_cache_key(&records[0]).is_none());
        assert!(record_prompt_cache_key(&records[1]).is_none());
    }

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn invalid_session_fields_return_400_without_reaching_upstream() {
    let (upstream_address, records, upstream_task) = spawn_mock("A").await;
    let upstream = profile("A", upstream_address, "upstream-secret");
    let (_logs, _logger, bridge) = start_bridge(&upstream).await;
    let client = reqwest::Client::new();

    let response = client
        .get(format!("http://{}/v1/models", bridge.local_addr))
        .bearer_auth("rab_client")
        .header("x-rab-session-id", "x".repeat(257))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: serde_json::Value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    assert_eq!(error["error"]["code"], "invalid_session_id");

    let response = client
        .post(format!("http://{}/v1/responses", bridge.local_addr))
        .bearer_auth("rab_client")
        .header(header::CONTENT_TYPE, "application/json")
        .body(
            serde_json::json!({
                "model": "gpt-test",
                "input": "hello",
                "prompt_cache_key": "x".repeat(65)
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: serde_json::Value = serde_json::from_str(&response.text().await.unwrap()).unwrap();
    assert_eq!(error["error"]["code"], "invalid_session_id");
    assert!(records.lock().unwrap().is_empty());

    bridge.stop().await;
    upstream_task.abort();
}

#[tokio::test]
async fn concurrent_shared_key_streams_keep_sessions_and_responses_separate() {
    let (upstream_address, records, upstream_task) = spawn_mock("CLI").await;
    let upstream = profile_with_kind(
        "CLI",
        upstream_address,
        "cliproxy-secret",
        UpstreamKind::CliProxyApi,
    );
    let (_logs, _logger, bridge) = start_bridge(&upstream).await;
    let client = reqwest::Client::new();
    let url = format!("http://{}/v1/responses", bridge.local_addr);

    let request = |session: &'static str, marker: &'static str| {
        let client = client.clone();
        let url = url.clone();
        async move {
            client
                .post(url)
                .bearer_auth("rab_client")
                .header("x-rab-user-id", session)
                .header("x-rab-session-id", session)
                .header(header::CONTENT_TYPE, "application/json")
                .body(
                    serde_json::json!({
                        "model": "gpt-test",
                        "bridge_test": format!("concurrent-{marker}"),
                        "stream": true,
                        "input": marker
                    })
                    .to_string(),
                )
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap()
        }
    };
    let (body_a, body_b) = tokio::join!(request("user-a", "alpha"), request("user-b", "beta"));
    assert!(body_a.contains("alpha"));
    assert!(!body_a.contains("beta"));
    assert!(body_b.contains("beta"));
    assert!(!body_b.contains("alpha"));

    {
        let records = records.lock().unwrap();
        assert_eq!(records.len(), 2);
        for record in records.iter() {
            assert_eq!(record.session_id, record_prompt_cache_key(record));
            assert_eq!(record.x_rab_user_id, None);
            assert_eq!(record.x_rab_session_id, None);
        }
        assert_ne!(records[0].session_id, records[1].session_id);
    }

    bridge.stop().await;
    upstream_task.abort();
}
