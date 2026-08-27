use std::{
    env, fs,
    io::Write,
    net::IpAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::{crypto, logger::LogLevel};

pub const APP_NAME: &str = "Rust AI Bridge";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UpstreamKind {
    Sub2Api,
    CliProxyApi,
}

impl UpstreamKind {
    pub const ALL: [Self; 2] = [Self::Sub2Api, Self::CliProxyApi];

    pub fn label(self) -> &'static str {
        match self {
            Self::Sub2Api => "Sub2API",
            Self::CliProxyApi => "CLIProxyAPI",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamProfile {
    pub id: Uuid,
    pub name: String,
    pub kind: UpstreamKind,
    pub base_url: String,
    pub encrypted_api_key: String,
    #[serde(skip)]
    pub api_key: String,
}

impl UpstreamProfile {
    pub fn new(kind: UpstreamKind) -> Self {
        Self {
            id: Uuid::new_v4(),
            name: kind.label().to_string(),
            kind,
            base_url: String::new(),
            encrypted_api_key: String::new(),
            api_key: String::new(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.name.trim().is_empty() {
            bail!("上游名称不能为空");
        }
        normalize_base_url(&self.base_url)?;
        if self.api_key.trim().is_empty() {
            bail!("上游 API Key 不能为空");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppConfig {
    pub listen_address: String,
    pub port: u16,
    pub log_level: LogLevel,
    pub active_upstream_id: Option<Uuid>,
    pub upstreams: Vec<UpstreamProfile>,
    pub encrypted_gateway_key: String,
    #[serde(skip)]
    pub gateway_key: String,
    pub encrypted_session_secret: String,
    #[serde(skip)]
    pub session_secret: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            listen_address: "0.0.0.0".to_string(),
            port: 8317,
            log_level: LogLevel::Info,
            active_upstream_id: None,
            upstreams: Vec::new(),
            encrypted_gateway_key: String::new(),
            gateway_key: generate_gateway_key(),
            encrypted_session_secret: String::new(),
            session_secret: generate_session_secret(),
        }
    }
}

impl AppConfig {
    pub fn active_upstream(&self) -> Option<&UpstreamProfile> {
        let id = self.active_upstream_id?;
        self.upstreams.iter().find(|profile| profile.id == id)
    }

    pub fn validate_listener(&self) -> Result<()> {
        self.listen_address
            .parse::<IpAddr>()
            .with_context(|| "监听地址必须是有效的 IPv4 或 IPv6 地址")?;
        if self.port == 0 {
            bail!("端口必须在 1 到 65535 之间");
        }
        if self.gateway_key.trim().is_empty() {
            bail!("中转 Key 不能为空");
        }
        Ok(())
    }

    pub fn validate_for_start(&self) -> Result<()> {
        self.validate_listener()?;
        self.active_upstream()
            .context("请先添加并启用一个上游")?
            .validate()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct AppPaths {
    pub root: PathBuf,
    pub config_file: PathBuf,
    pub log_dir: PathBuf,
}

impl AppPaths {
    pub fn discover() -> Result<Self> {
        let local_app_data = env::var_os("LOCALAPPDATA").context("无法确定 LocalAppData 路径")?;
        let root = PathBuf::from(local_app_data).join("RustAIBridge");
        Ok(Self {
            config_file: root.join("config.json"),
            log_dir: root.join("logs"),
            root,
        })
    }

    pub fn ensure(&self) -> Result<()> {
        fs::create_dir_all(&self.root)
            .with_context(|| format!("无法创建配置目录 {}", self.root.display()))?;
        fs::create_dir_all(&self.log_dir)
            .with_context(|| format!("无法创建日志目录 {}", self.log_dir.display()))?;
        Ok(())
    }
}

pub fn load_config(path: &Path) -> Result<AppConfig> {
    if !path.exists() {
        return Ok(AppConfig::default());
    }
    let bytes = fs::read(path).with_context(|| format!("无法读取 {}", path.display()))?;
    let mut config: AppConfig = serde_json::from_slice(&bytes)
        .map_err(|error| anyhow::anyhow!("配置文件格式无效或不受支持: {error}"))?;
    if config.encrypted_gateway_key.is_empty() {
        bail!("配置文件中的 encrypted_gateway_key 不能为空");
    }
    if config.encrypted_session_secret.is_empty() {
        bail!("配置文件中的 encrypted_session_secret 不能为空");
    }
    config.gateway_key = crypto::unprotect_string(&config.encrypted_gateway_key)
        .context("无法解密中转 Key；配置可能由其他 Windows 用户创建")?;
    config.session_secret = crypto::unprotect_string(&config.encrypted_session_secret)
        .context("无法解密会话密钥；配置可能由其他 Windows 用户创建")?;
    for profile in &mut config.upstreams {
        if profile.encrypted_api_key.is_empty() {
            bail!("上游 {} 的 encrypted_api_key 不能为空", profile.name);
        }
        profile.api_key = crypto::unprotect_string(&profile.encrypted_api_key)
            .with_context(|| format!("无法解密上游 {} 的 API Key", profile.name))?
            .trim()
            .to_string();
    }
    Ok(config)
}

pub fn save_config(path: &Path, config: &AppConfig) -> Result<()> {
    let mut stored = config.clone();
    stored.encrypted_gateway_key = crypto::protect_string(&stored.gateway_key)?;
    stored.gateway_key.clear();
    stored.encrypted_session_secret = crypto::protect_string(&stored.session_secret)?;
    stored.session_secret.clear();
    for profile in &mut stored.upstreams {
        profile.api_key = profile.api_key.trim().to_string();
        profile.encrypted_api_key = crypto::protect_string(&profile.api_key)?;
        profile.api_key.clear();
        profile.base_url = normalize_base_url(&profile.base_url)?;
        profile.name = profile.name.trim().to_string();
    }

    let parent = path.parent().context("配置文件路径缺少父目录")?;
    fs::create_dir_all(parent)?;
    let temp_path = path.with_extension("json.tmp");
    let bytes = serde_json::to_vec_pretty(&stored)?;
    {
        let mut file = fs::File::create(&temp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    if path.exists() {
        fs::remove_file(path)?;
    }
    fs::rename(&temp_path, path)?;
    Ok(())
}

pub fn generate_gateway_key() -> String {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    format!("rab_{}", URL_SAFE_NO_PAD.encode(bytes))
}

pub fn generate_session_secret() -> String {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn normalize_base_url(input: &str) -> Result<String> {
    let mut url = Url::parse(input.trim()).context("上游 Base URL 无效")?;
    if !matches!(url.scheme(), "http" | "https") {
        bail!("上游 Base URL 只支持 http 或 https");
    }
    if url.host_str().is_none() {
        bail!("上游 Base URL 缺少主机名");
    }
    if url.query().is_some() || url.fragment().is_some() {
        bail!("上游 Base URL 不能包含查询参数或片段");
    }
    let trimmed = url.path().trim_end_matches('/').to_string();
    url.set_path(if trimmed.is_empty() { "/" } else { &trimmed });
    Ok(url.to_string().trim_end_matches('/').to_string())
}

pub fn build_upstream_url(base_url: &str, incoming_path: &str, query: Option<&str>) -> Result<Url> {
    let normalized = normalize_base_url(base_url)?;
    let mut url = Url::parse(&normalized)?;
    let base_path = url.path().trim_end_matches('/');
    let suffix = if base_path.ends_with("/v1") {
        incoming_path.strip_prefix("/v1").unwrap_or(incoming_path)
    } else {
        incoming_path
    };
    let path = format!(
        "{}{}",
        base_path,
        if suffix.starts_with('/') {
            suffix.to_string()
        } else {
            format!("/{suffix}")
        }
    );
    url.set_path(&path);
    url.set_query(query);
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use serde_json::Value;

    #[test]
    fn normalizes_base_urls() {
        assert_eq!(
            normalize_base_url("http://127.0.0.1:8317/").unwrap(),
            "http://127.0.0.1:8317"
        );
        assert_eq!(
            normalize_base_url("https://api.example.com/openai/v1/").unwrap(),
            "https://api.example.com/openai/v1"
        );
    }

    #[test]
    fn joins_root_and_v1_bases_without_duplication() {
        let root = build_upstream_url("http://localhost:8080", "/v1/chat/completions", Some("a=1"))
            .unwrap();
        assert_eq!(
            root.as_str(),
            "http://localhost:8080/v1/chat/completions?a=1"
        );

        let v1 = build_upstream_url("http://localhost:8080/v1", "/v1/responses", None).unwrap();
        assert_eq!(v1.as_str(), "http://localhost:8080/v1/responses");

        let prefix =
            build_upstream_url("https://example.com/gateway/v1/", "/v1/models", None).unwrap();
        assert_eq!(prefix.as_str(), "https://example.com/gateway/v1/models");
    }

    #[test]
    fn session_secret_is_random_key_material() {
        let secret = generate_session_secret();
        assert_eq!(URL_SAFE_NO_PAD.decode(secret).unwrap().len(), 32);
    }

    #[test]
    fn config_round_trip_encrypts_runtime_secrets() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.json");
        let config = AppConfig::default();
        save_config(&path, &config).unwrap();

        let stored: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        assert!(
            stored["encrypted_session_secret"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        assert!(stored.get("session_secret").is_none());
        let loaded_again = load_config(&path).unwrap();
        assert_eq!(loaded_again.session_secret, config.session_secret);
        assert_eq!(loaded_again.gateway_key, config.gateway_key);
    }

    #[test]
    fn incomplete_config_schema_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.json");
        fs::write(&path, b"{}").unwrap();

        let error = load_config(&path).unwrap_err();
        assert!(error.to_string().contains("配置文件格式无效"));
    }

    #[test]
    fn unknown_or_empty_persisted_fields_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("config.json");
        let mut value = serde_json::to_value(AppConfig::default()).unwrap();
        value["obsolete_field"] = Value::Bool(true);
        fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(
            load_config(&path)
                .unwrap_err()
                .to_string()
                .contains("配置文件格式无效")
        );

        value.as_object_mut().unwrap().remove("obsolete_field");
        value["gateway_key"] = Value::String("legacy-plain-text-key".to_string());
        fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(
            load_config(&path)
                .unwrap_err()
                .to_string()
                .contains("配置文件格式无效")
        );

        value.as_object_mut().unwrap().remove("gateway_key");
        fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(
            load_config(&path)
                .unwrap_err()
                .to_string()
                .contains("encrypted_gateway_key 不能为空")
        );
    }
}
