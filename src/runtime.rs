use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
        mpsc,
    },
    thread,
};

use crate::{
    config::UpstreamProfile,
    logger::{AppLogger, LogEvent, LogLevel},
    proxy::{MetricsSnapshot, ProxyMetrics, ProxyServer, test_upstream},
};

#[derive(Debug, Clone)]
pub enum RuntimeEvent {
    Started(SocketAddr),
    Stopped,
    Error(String),
    UpstreamSwitched(String),
    GatewayKeyUpdated,
    TestFinished {
        profile_id: uuid::Uuid,
        result: Result<String, String>,
    },
}

enum RuntimeCommand {
    Start {
        address: SocketAddr,
        gateway_key: String,
        session_secret: String,
        upstream: UpstreamProfile,
    },
    Stop,
    SwitchUpstream(UpstreamProfile),
    UpdateGatewayKey(String),
    TestUpstream(UpstreamProfile),
    Shutdown,
}

pub struct RuntimeController {
    commands: tokio::sync::mpsc::UnboundedSender<RuntimeCommand>,
    events: Mutex<mpsc::Receiver<RuntimeEvent>>,
    thread: Mutex<Option<thread::JoinHandle<()>>>,
    running: Arc<AtomicBool>,
    metrics: Arc<ProxyMetrics>,
}

impl RuntimeController {
    pub fn spawn(logger: AppLogger) -> anyhow::Result<Arc<Self>> {
        let (command_sender, command_receiver) = tokio::sync::mpsc::unbounded_channel();
        let (event_sender, event_receiver) = mpsc::channel();
        let running = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(ProxyMetrics::default());
        let thread_running = running.clone();
        let thread_metrics = metrics.clone();
        let thread = thread::Builder::new()
            .name("rust-ai-bridge-runtime".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .thread_name("rust-ai-bridge-worker")
                    .build();
                match runtime {
                    Ok(runtime) => runtime.block_on(runtime_loop(
                        command_receiver,
                        event_sender,
                        logger,
                        thread_running,
                        thread_metrics,
                    )),
                    Err(error) => {
                        let _ = event_sender
                            .send(RuntimeEvent::Error(format!("无法创建异步运行时: {error}")));
                    }
                }
            })?;
        Ok(Arc::new(Self {
            commands: command_sender,
            events: Mutex::new(event_receiver),
            thread: Mutex::new(Some(thread)),
            running,
            metrics,
        }))
    }

    pub fn start(
        &self,
        address: SocketAddr,
        gateway_key: String,
        session_secret: String,
        upstream: UpstreamProfile,
    ) {
        let _ = self.commands.send(RuntimeCommand::Start {
            address,
            gateway_key,
            session_secret,
            upstream,
        });
    }

    pub fn stop(&self) {
        let _ = self.commands.send(RuntimeCommand::Stop);
    }

    pub fn switch_upstream(&self, upstream: UpstreamProfile) {
        let _ = self.commands.send(RuntimeCommand::SwitchUpstream(upstream));
    }

    pub fn update_gateway_key(&self, gateway_key: String) {
        let _ = self
            .commands
            .send(RuntimeCommand::UpdateGatewayKey(gateway_key));
    }

    pub fn test_upstream(&self, upstream: UpstreamProfile) {
        let _ = self.commands.send(RuntimeCommand::TestUpstream(upstream));
    }

    pub fn try_events(&self) -> Vec<RuntimeEvent> {
        let receiver = self.events.lock().expect("runtime event lock poisoned");
        receiver.try_iter().collect()
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    pub fn shutdown(&self) {
        let _ = self.commands.send(RuntimeCommand::Shutdown);
        if let Some(thread) = self
            .thread
            .lock()
            .expect("runtime thread lock poisoned")
            .take()
        {
            let _ = thread.join();
        }
    }
}

async fn runtime_loop(
    mut commands: tokio::sync::mpsc::UnboundedReceiver<RuntimeCommand>,
    events: mpsc::Sender<RuntimeEvent>,
    logger: AppLogger,
    running: Arc<AtomicBool>,
    metrics: Arc<ProxyMetrics>,
) {
    let mut server: Option<ProxyServer> = None;
    while let Some(command) = commands.recv().await {
        match command {
            RuntimeCommand::Start {
                address,
                gateway_key,
                session_secret,
                upstream,
            } => {
                if server.is_some() {
                    let _ = events.send(RuntimeEvent::Error("代理已经在运行".to_string()));
                    continue;
                }
                metrics.reset();
                match ProxyServer::start(
                    address,
                    gateway_key,
                    session_secret,
                    &upstream,
                    logger.clone(),
                    metrics.clone(),
                    running.clone(),
                )
                .await
                {
                    Ok(instance) => {
                        let local_addr = instance.local_addr;
                        server = Some(instance);
                        let _ = events.send(RuntimeEvent::Started(local_addr));
                    }
                    Err(error) => {
                        running.store(false, Ordering::Release);
                        logger.emit(LogEvent::system(LogLevel::Error, error.to_string()));
                        let _ = events.send(RuntimeEvent::Error(error.to_string()));
                    }
                }
            }
            RuntimeCommand::Stop => {
                if let Some(instance) = server.take() {
                    instance.stop().await;
                }
                running.store(false, Ordering::Release);
                let _ = events.send(RuntimeEvent::Stopped);
            }
            RuntimeCommand::SwitchUpstream(upstream) => {
                if let Some(instance) = &server {
                    instance.shared.switch_upstream(&upstream);
                }
                let _ = events.send(RuntimeEvent::UpstreamSwitched(upstream.name));
            }
            RuntimeCommand::UpdateGatewayKey(key) => {
                if let Some(instance) = &server {
                    instance.shared.update_gateway_key(key);
                }
                let _ = events.send(RuntimeEvent::GatewayKeyUpdated);
            }
            RuntimeCommand::TestUpstream(profile) => {
                let events = events.clone();
                tokio::spawn(async move {
                    let id = profile.id;
                    let result = test_upstream(&profile)
                        .await
                        .map_err(|error| error.to_string());
                    let _ = events.send(RuntimeEvent::TestFinished {
                        profile_id: id,
                        result,
                    });
                });
            }
            RuntimeCommand::Shutdown => {
                if let Some(instance) = server.take() {
                    instance.stop().await;
                }
                running.store(false, Ordering::Release);
                break;
            }
        }
    }
}
