use std::{
    cell::RefCell,
    net::{IpAddr, SocketAddr},
    process::Command,
    str::FromStr,
    sync::Arc,
};

use native_windows_derive::NwgUi;
use native_windows_gui as nwg;
use nwg::NativeUi;
use uuid::Uuid;
use winapi::um::winuser::{SW_HIDE, ShowWindow, WM_CLOSE};

use rust_ai_bridge::{
    config::{
        APP_NAME, AppConfig, AppPaths, UpstreamKind, UpstreamProfile, generate_gateway_key,
        save_config,
    },
    logger::{AppLogger, LogEvent, LogLevel, format_duration_seconds},
    runtime::{RuntimeController, RuntimeEvent},
};

struct Services {
    config: AppConfig,
    paths: AppPaths,
    runtime: Arc<RuntimeController>,
    logger: AppLogger,
    editor_id: Option<Uuid>,
    last_log_version: u64,
    visible_logs: Vec<LogEvent>,
}

#[derive(Default, NwgUi)]
pub struct BridgeApp {
    services: RefCell<Option<Services>>,
    close_handlers: RefCell<Vec<nwg::RawEventHandler>>,

    #[nwg_resource(source_bin: Some(include_bytes!(concat!(env!("OUT_DIR"), "/rust-ai-bridge.ico"))))]
    icon: nwg::Icon,

    #[nwg_control(
        size: (920, 680),
        center: true,
        title: "Rust AI Bridge",
        icon: Some(&data.icon),
        flags: "MAIN_WINDOW|VISIBLE"
    )]
    #[nwg_events(OnInit: [BridgeApp::initialize])]
    window: nwg::Window,

    #[nwg_layout(parent: window, min_size: [780, 560], margin: [8, 8, 8, 8])]
    window_layout: nwg::GridLayout,

    #[nwg_control(parent: window)]
    #[nwg_layout_item(layout: window_layout, col: 0, row: 0)]
    tabs: nwg::TabsContainer,

    #[nwg_control(parent: tabs, text: "总览")]
    overview_tab: nwg::Tab,

    #[nwg_layout(parent: overview_tab, spacing: 8, margin: [14, 14, 14, 14])]
    overview_layout: nwg::GridLayout,

    #[nwg_control(parent: overview_tab, text: "运行状态")]
    #[nwg_layout_item(layout: overview_layout, col: 0, row: 0, col_span: 3)]
    status_title: nwg::Label,

    #[nwg_control(parent: overview_tab, text: "已停止")]
    #[nwg_layout_item(layout: overview_layout, col: 3, row: 0, col_span: 5)]
    status_value: nwg::Label,

    #[nwg_control(parent: overview_tab, text: "启动代理")]
    #[nwg_events(OnButtonClick: [BridgeApp::toggle_proxy])]
    #[nwg_layout_item(layout: overview_layout, col: 9, row: 0, col_span: 3)]
    start_stop_button: nwg::Button,

    #[nwg_control(parent: overview_tab, text: "客户端 Base URL")]
    #[nwg_layout_item(layout: overview_layout, col: 0, row: 1, col_span: 12)]
    client_url_title: nwg::Label,

    #[nwg_control(parent: overview_tab, readonly: true)]
    #[nwg_layout_item(layout: overview_layout, col: 0, row: 2, col_span: 10)]
    client_url_value: nwg::TextInput,

    #[nwg_control(parent: overview_tab, text: "复制")]
    #[nwg_events(OnButtonClick: [BridgeApp::copy_client_url])]
    #[nwg_layout_item(layout: overview_layout, col: 10, row: 2, col_span: 2)]
    copy_url_button: nwg::Button,

    #[nwg_control(parent: overview_tab, text: "当前上游")]
    #[nwg_layout_item(layout: overview_layout, col: 0, row: 3, col_span: 3)]
    upstream_title: nwg::Label,

    #[nwg_control(parent: overview_tab, text: "未配置")]
    #[nwg_layout_item(layout: overview_layout, col: 3, row: 3, col_span: 9)]
    upstream_value: nwg::Label,

    #[nwg_control(parent: overview_tab, text: "活动请求\r\n0")]
    #[nwg_layout_item(layout: overview_layout, col: 0, row: 5, col_span: 4, row_span: 2)]
    active_metric: nwg::Label,

    #[nwg_control(parent: overview_tab, text: "成功请求\r\n0")]
    #[nwg_layout_item(layout: overview_layout, col: 4, row: 5, col_span: 4, row_span: 2)]
    success_metric: nwg::Label,

    #[nwg_control(parent: overview_tab, text: "失败请求\r\n0")]
    #[nwg_layout_item(layout: overview_layout, col: 8, row: 5, col_span: 4, row_span: 2)]
    failed_metric: nwg::Label,

    #[nwg_control(parent: overview_tab, text: "提示：公网部署请在本程序前使用 IIS、Caddy 或 Nginx 提供 HTTPS。")]
    #[nwg_layout_item(layout: overview_layout, col: 0, row: 8, col_span: 12)]
    tls_warning: nwg::Label,

    #[nwg_control(parent: tabs, text: "上游")]
    upstreams_tab: nwg::Tab,

    #[nwg_layout(parent: upstreams_tab, spacing: 8, margin: [12, 12, 12, 12])]
    upstreams_layout: nwg::GridLayout,

    #[nwg_control(
        parent: upstreams_tab,
        list_style: nwg::ListViewStyle::Detailed,
        ex_flags: nwg::ListViewExFlags::GRID | nwg::ListViewExFlags::FULL_ROW_SELECT,
        focus: true
    )]
    #[nwg_events(OnListViewDoubleClick: [BridgeApp::edit_upstream])]
    #[nwg_layout_item(layout: upstreams_layout, col: 0, row: 0, col_span: 12, row_span: 7)]
    upstream_list: nwg::ListView,

    #[nwg_control(parent: upstreams_tab, text: "新增")]
    #[nwg_events(OnButtonClick: [BridgeApp::add_upstream])]
    #[nwg_layout_item(layout: upstreams_layout, col: 0, row: 7, col_span: 2)]
    add_upstream_button: nwg::Button,

    #[nwg_control(parent: upstreams_tab, text: "编辑")]
    #[nwg_events(OnButtonClick: [BridgeApp::edit_upstream])]
    #[nwg_layout_item(layout: upstreams_layout, col: 2, row: 7, col_span: 2)]
    edit_upstream_button: nwg::Button,

    #[nwg_control(parent: upstreams_tab, text: "删除")]
    #[nwg_events(OnButtonClick: [BridgeApp::delete_upstream])]
    #[nwg_layout_item(layout: upstreams_layout, col: 4, row: 7, col_span: 2)]
    delete_upstream_button: nwg::Button,

    #[nwg_control(parent: upstreams_tab, text: "测试连接")]
    #[nwg_events(OnButtonClick: [BridgeApp::test_selected_upstream])]
    #[nwg_layout_item(layout: upstreams_layout, col: 7, row: 7, col_span: 2)]
    test_upstream_button: nwg::Button,

    #[nwg_control(parent: upstreams_tab, text: "设为当前")]
    #[nwg_events(OnButtonClick: [BridgeApp::activate_upstream])]
    #[nwg_layout_item(layout: upstreams_layout, col: 9, row: 7, col_span: 3)]
    activate_upstream_button: nwg::Button,

    #[nwg_control(parent: tabs, text: "设置")]
    settings_tab: nwg::Tab,

    #[nwg_layout(parent: settings_tab, spacing: 8, margin: [16, 16, 16, 16])]
    settings_layout: nwg::GridLayout,

    #[nwg_control(parent: settings_tab, text: "监听地址")]
    #[nwg_layout_item(layout: settings_layout, col: 0, row: 0, col_span: 3)]
    listen_address_label: nwg::Label,

    #[nwg_control(parent: settings_tab)]
    #[nwg_layout_item(layout: settings_layout, col: 3, row: 0, col_span: 9)]
    listen_address_input: nwg::TextInput,

    #[nwg_control(parent: settings_tab, text: "端口")]
    #[nwg_layout_item(layout: settings_layout, col: 0, row: 1, col_span: 3)]
    port_label: nwg::Label,

    #[nwg_control(parent: settings_tab, limit: 5)]
    #[nwg_layout_item(layout: settings_layout, col: 3, row: 1, col_span: 4)]
    port_input: nwg::TextInput,

    #[nwg_control(parent: settings_tab, text: "中转 Key")]
    #[nwg_layout_item(layout: settings_layout, col: 0, row: 2, col_span: 3)]
    gateway_key_label: nwg::Label,

    #[nwg_control(parent: settings_tab, readonly: true, password: Some('●'))]
    #[nwg_layout_item(layout: settings_layout, col: 3, row: 2, col_span: 5)]
    gateway_key_input: nwg::TextInput,

    #[nwg_control(parent: settings_tab, text: "复制")]
    #[nwg_events(OnButtonClick: [BridgeApp::copy_gateway_key])]
    #[nwg_layout_item(layout: settings_layout, col: 8, row: 2, col_span: 2)]
    copy_key_button: nwg::Button,

    #[nwg_control(parent: settings_tab, text: "重新生成")]
    #[nwg_events(OnButtonClick: [BridgeApp::regenerate_gateway_key])]
    #[nwg_layout_item(layout: settings_layout, col: 10, row: 2, col_span: 2)]
    regenerate_key_button: nwg::Button,

    #[nwg_control(parent: settings_tab, text: "日志等级")]
    #[nwg_layout_item(layout: settings_layout, col: 0, row: 3, col_span: 3)]
    log_level_label: nwg::Label,

    #[nwg_control(parent: settings_tab, collection: vec![
        "Off".to_string(), "Error".to_string(), "Warn".to_string(),
        "Info".to_string(), "Debug".to_string(), "Trace".to_string()
    ])]
    #[nwg_events(OnComboxBoxSelection: [BridgeApp::change_log_level])]
    #[nwg_layout_item(layout: settings_layout, col: 3, row: 3, col_span: 4)]
    log_level_combo: nwg::ComboBox<String>,

    #[nwg_control(parent: settings_tab, text: "保存设置")]
    #[nwg_events(OnButtonClick: [BridgeApp::save_settings])]
    #[nwg_layout_item(layout: settings_layout, col: 7, row: 5, col_span: 3)]
    save_settings_button: nwg::Button,

    #[nwg_control(parent: settings_tab, text: "打开日志目录")]
    #[nwg_events(OnButtonClick: [BridgeApp::open_log_directory])]
    #[nwg_layout_item(layout: settings_layout, col: 10, row: 5, col_span: 2)]
    open_logs_button: nwg::Button,

    #[nwg_control(parent: settings_tab, text: "监听地址和端口只能在代理停止时修改。配置与密钥保存在当前用户的 LocalAppData，密钥使用 Windows DPAPI 加密。")]
    #[nwg_layout_item(layout: settings_layout, col: 0, row: 7, col_span: 12)]
    settings_help: nwg::Label,

    #[nwg_control(parent: tabs, text: "日志")]
    logs_tab: nwg::Tab,

    #[nwg_layout(parent: logs_tab, spacing: 8, margin: [12, 12, 12, 12])]
    logs_layout: nwg::GridLayout,

    #[nwg_control(parent: logs_tab, text: "显示等级")]
    #[nwg_layout_item(layout: logs_layout, col: 0, row: 0, col_span: 2)]
    log_filter_label: nwg::Label,

    #[nwg_control(parent: logs_tab, collection: vec![
        "全部".to_string(), "Error".to_string(), "Warn".to_string(),
        "Info".to_string(), "Debug".to_string(), "Trace".to_string()
    ], selected_index: Some(0))]
    #[nwg_events(OnComboxBoxSelection: [BridgeApp::refresh_logs])]
    #[nwg_layout_item(layout: logs_layout, col: 2, row: 0, col_span: 3)]
    log_filter_combo: nwg::ComboBox<String>,

    #[nwg_control(parent: logs_tab, text: "清空界面")]
    #[nwg_events(OnButtonClick: [BridgeApp::clear_logs])]
    #[nwg_layout_item(layout: logs_layout, col: 10, row: 0, col_span: 2)]
    clear_logs_button: nwg::Button,

    #[nwg_control(parent: logs_tab, text: "捕获请求/响应正文（敏感）")]
    #[nwg_events(OnButtonClick: [BridgeApp::toggle_debug_capture])]
    #[nwg_layout_item(layout: logs_layout, col: 5, row: 0, col_span: 3)]
    debug_capture_checkbox: nwg::CheckBox,

    #[nwg_control(parent: logs_tab, text: "查看详情")]
    #[nwg_events(OnButtonClick: [BridgeApp::show_log_details])]
    #[nwg_layout_item(layout: logs_layout, col: 8, row: 0, col_span: 2)]
    view_log_details_button: nwg::Button,

    #[nwg_control(
        parent: logs_tab,
        list_style: nwg::ListViewStyle::Detailed,
        ex_flags: nwg::ListViewExFlags::GRID | nwg::ListViewExFlags::FULL_ROW_SELECT
    )]
    #[nwg_events(OnListViewDoubleClick: [BridgeApp::show_log_details])]
    #[nwg_layout_item(layout: logs_layout, col: 0, row: 1, col_span: 12, row_span: 8)]
    log_list: nwg::ListView,

    #[allow(deprecated)]
    #[nwg_control(parent: window, interval: 500, stopped: false)]
    #[nwg_events(OnTimerTick: [BridgeApp::poll_runtime])]
    refresh_timer: nwg::Timer,

    #[nwg_control(parent: window, icon: Some(&data.icon), tip: Some("Rust AI Bridge"))]
    #[nwg_events(MousePressLeftUp: [BridgeApp::show_window], OnContextMenu: [BridgeApp::show_tray_menu])]
    tray: nwg::TrayNotification,

    #[nwg_control(parent: window, popup: true)]
    tray_menu: nwg::Menu,

    #[nwg_control(parent: tray_menu, text: "显示窗口")]
    #[nwg_events(OnMenuItemSelected: [BridgeApp::show_window])]
    tray_show_item: nwg::MenuItem,

    #[nwg_control(parent: tray_menu, text: "启动/停止代理")]
    #[nwg_events(OnMenuItemSelected: [BridgeApp::toggle_proxy])]
    tray_start_stop_item: nwg::MenuItem,

    #[nwg_control(parent: tray_menu, text: "退出")]
    #[nwg_events(OnMenuItemSelected: [BridgeApp::exit_application])]
    tray_exit_item: nwg::MenuItem,

    #[nwg_control(
        size: (520, 300),
        center: true,
        title: "上游配置",
        icon: Some(&data.icon),
        parent: Some(&data.window),
        flags: "WINDOW"
    )]
    editor_window: nwg::Window,

    #[nwg_layout(parent: editor_window, spacing: 8, margin: [12, 12, 12, 12])]
    editor_layout: nwg::GridLayout,

    #[nwg_control(parent: editor_window, text: "名称")]
    #[nwg_layout_item(layout: editor_layout, col: 0, row: 0, col_span: 3)]
    editor_name_label: nwg::Label,

    #[nwg_control(parent: editor_window)]
    #[nwg_layout_item(layout: editor_layout, col: 3, row: 0, col_span: 9)]
    editor_name_input: nwg::TextInput,

    #[nwg_control(parent: editor_window, text: "类型")]
    #[nwg_layout_item(layout: editor_layout, col: 0, row: 1, col_span: 3)]
    editor_kind_label: nwg::Label,

    #[nwg_control(parent: editor_window, collection: vec!["Sub2API".to_string(), "CLIProxyAPI".to_string()], selected_index: Some(0))]
    #[nwg_layout_item(layout: editor_layout, col: 3, row: 1, col_span: 9)]
    editor_kind_combo: nwg::ComboBox<String>,

    #[nwg_control(parent: editor_window, text: "Base URL")]
    #[nwg_layout_item(layout: editor_layout, col: 0, row: 2, col_span: 3)]
    editor_url_label: nwg::Label,

    #[nwg_control(parent: editor_window, placeholder_text: Some("例如 http://127.0.0.1:8080/v1"))]
    #[nwg_layout_item(layout: editor_layout, col: 3, row: 2, col_span: 9)]
    editor_url_input: nwg::TextInput,

    #[nwg_control(parent: editor_window, text: "API Key")]
    #[nwg_layout_item(layout: editor_layout, col: 0, row: 3, col_span: 3)]
    editor_key_label: nwg::Label,

    #[nwg_control(parent: editor_window, password: Some('●'))]
    #[nwg_layout_item(layout: editor_layout, col: 3, row: 3, col_span: 9)]
    editor_key_input: nwg::TextInput,

    #[nwg_control(parent: editor_window, text: "取消")]
    #[nwg_events(OnButtonClick: [BridgeApp::close_editor])]
    #[nwg_layout_item(layout: editor_layout, col: 8, row: 4, col_span: 2)]
    editor_cancel_button: nwg::Button,

    #[nwg_control(parent: editor_window, text: "保存")]
    #[nwg_events(OnButtonClick: [BridgeApp::save_editor])]
    #[nwg_layout_item(layout: editor_layout, col: 10, row: 4, col_span: 2)]
    editor_save_button: nwg::Button,

    #[nwg_control(
        size: (820, 620),
        center: true,
        title: "请求调试详情",
        icon: Some(&data.icon),
        parent: Some(&data.window),
        flags: "WINDOW"
    )]
    debug_detail_window: nwg::Window,

    #[nwg_layout(parent: debug_detail_window, spacing: 8, margin: [12, 12, 12, 12])]
    debug_detail_layout: nwg::GridLayout,

    #[nwg_control(parent: debug_detail_window, text: "仅显示内存中捕获的数据，不会写入日志文件。")]
    #[nwg_layout_item(layout: debug_detail_layout, col: 0, row: 0, col_span: 12)]
    debug_detail_hint: nwg::Label,

    #[nwg_control(parent: debug_detail_window, readonly: true, limit: 200000)]
    #[nwg_layout_item(layout: debug_detail_layout, col: 0, row: 1, col_span: 12, row_span: 10)]
    debug_detail_text: nwg::TextBox,

    #[nwg_control(parent: debug_detail_window, text: "复制全部")]
    #[nwg_events(OnButtonClick: [BridgeApp::copy_log_details])]
    #[nwg_layout_item(layout: debug_detail_layout, col: 8, row: 11, col_span: 2)]
    debug_detail_copy_button: nwg::Button,

    #[nwg_control(parent: debug_detail_window, text: "关闭")]
    #[nwg_events(OnButtonClick: [BridgeApp::close_log_details])]
    #[nwg_layout_item(layout: debug_detail_layout, col: 10, row: 11, col_span: 2)]
    debug_detail_close_button: nwg::Button,
}

impl BridgeApp {
    pub fn run(
        config: AppConfig,
        paths: AppPaths,
        logger: AppLogger,
        runtime: Arc<RuntimeController>,
    ) -> Result<(), nwg::NwgError> {
        let app = Self {
            services: RefCell::new(Some(Services {
                config,
                paths,
                runtime,
                logger,
                editor_id: None,
                last_log_version: 0,
                visible_logs: Vec::new(),
            })),
            ..Default::default()
        };
        let _ui = Self::build_ui(app)?;
        nwg::dispatch_thread_events();
        Ok(())
    }

    fn initialize(&self) {
        if let Err(error) = self.bind_close_handlers() {
            nwg::modal_error_message(
                &self.window,
                APP_NAME,
                &format!("初始化窗口关闭处理失败：{error}"),
            );
            nwg::stop_thread_dispatch();
            return;
        }

        self.upstream_list.insert_column(nwg::InsertListViewColumn {
            index: Some(0),
            width: Some(70),
            text: Some("当前".to_string()),
            ..Default::default()
        });
        self.upstream_list.insert_column(nwg::InsertListViewColumn {
            index: Some(1),
            width: Some(180),
            text: Some("名称".to_string()),
            ..Default::default()
        });
        self.upstream_list.insert_column(nwg::InsertListViewColumn {
            index: Some(2),
            width: Some(120),
            text: Some("类型".to_string()),
            ..Default::default()
        });
        self.upstream_list.insert_column(nwg::InsertListViewColumn {
            index: Some(3),
            width: Some(460),
            text: Some("Base URL".to_string()),
            ..Default::default()
        });
        self.upstream_list.set_headers_enabled(true);

        for (index, (title, width)) in [
            ("时间", 90),
            ("等级", 70),
            ("请求 / 消息", 450),
            ("上游 / 客户端", 250),
        ]
        .into_iter()
        .enumerate()
        {
            self.log_list.insert_column(nwg::InsertListViewColumn {
                index: Some(index as i32),
                width: Some(width),
                text: Some(title.to_string()),
                ..Default::default()
            });
        }
        self.log_list.set_headers_enabled(true);
        if let Some(services) = self.services.borrow().as_ref() {
            self.listen_address_input
                .set_text(&services.config.listen_address);
            self.port_input.set_text(&services.config.port.to_string());
            self.gateway_key_input
                .set_text(&services.config.gateway_key);
            let level_index = LogLevel::ALL
                .iter()
                .position(|level| *level == services.config.log_level);
            self.log_level_combo.set_selection(level_index);
        }
        self.refresh_upstream_list();
        self.refresh_status();
        self.refresh_logs();
    }

    fn bind_close_handlers(&self) -> Result<(), nwg::NwgError> {
        const MAIN_CLOSE_HANDLER_ID: usize = 0x10001;
        const EDITOR_CLOSE_HANDLER_ID: usize = 0x10002;
        const DEBUG_DETAIL_CLOSE_HANDLER_ID: usize = 0x10003;

        let main_handler = nwg::bind_raw_event_handler(
            &self.window.handle,
            MAIN_CLOSE_HANDLER_ID,
            |hwnd, message, _, _| {
                if message != WM_CLOSE {
                    return None;
                }

                unsafe {
                    ShowWindow(hwnd, SW_HIDE);
                }
                Some(0)
            },
        )?;

        let editor_handler = match nwg::bind_raw_event_handler(
            &self.editor_window.handle,
            EDITOR_CLOSE_HANDLER_ID,
            |hwnd, message, _, _| {
                if message != WM_CLOSE {
                    return None;
                }

                unsafe {
                    ShowWindow(hwnd, SW_HIDE);
                }
                Some(0)
            },
        ) {
            Ok(handler) => handler,
            Err(error) => {
                let _ = nwg::unbind_raw_event_handler(&main_handler);
                return Err(error);
            }
        };

        let debug_detail_handler = match nwg::bind_raw_event_handler(
            &self.debug_detail_window.handle,
            DEBUG_DETAIL_CLOSE_HANDLER_ID,
            |hwnd, message, _, _| {
                if message != WM_CLOSE {
                    return None;
                }

                unsafe {
                    ShowWindow(hwnd, SW_HIDE);
                }
                Some(0)
            },
        ) {
            Ok(handler) => handler,
            Err(error) => {
                let _ = nwg::unbind_raw_event_handler(&main_handler);
                let _ = nwg::unbind_raw_event_handler(&editor_handler);
                return Err(error);
            }
        };

        self.close_handlers.borrow_mut().extend([
            main_handler,
            editor_handler,
            debug_detail_handler,
        ]);
        Ok(())
    }

    fn toggle_proxy(&self) {
        let mut services_ref = self.services.borrow_mut();
        let Some(services) = services_ref.as_mut() else {
            return;
        };
        if services.runtime.is_running() {
            services.runtime.stop();
            self.start_stop_button.set_enabled(false);
            self.tray_start_stop_item.set_enabled(false);
            return;
        }
        if let Err(error) = self.read_settings_into(&mut services.config, false) {
            nwg::modal_error_message(&self.window, APP_NAME, &error);
            return;
        }
        if let Err(error) = services.config.validate_for_start() {
            nwg::modal_error_message(&self.window, APP_NAME, &error.to_string());
            return;
        }
        if let Err(error) = save_config(&services.paths.config_file, &services.config) {
            nwg::modal_error_message(&self.window, APP_NAME, &error.to_string());
            return;
        }
        let address = match socket_address(&services.config) {
            Ok(address) => address,
            Err(error) => {
                nwg::modal_error_message(&self.window, APP_NAME, &error);
                return;
            }
        };
        let upstream = services
            .config
            .active_upstream()
            .cloned()
            .expect("validated upstream");
        services.runtime.start(
            address,
            services.config.gateway_key.clone(),
            services.config.session_secret.clone(),
            upstream,
        );
        self.start_stop_button.set_enabled(false);
        self.tray_start_stop_item.set_enabled(false);
    }

    fn save_settings(&self) {
        let mut services_ref = self.services.borrow_mut();
        let Some(services) = services_ref.as_mut() else {
            return;
        };
        if services.runtime.is_running() {
            nwg::modal_error_message(&self.window, APP_NAME, "请先停止代理，再修改监听设置");
            return;
        }
        if let Err(error) = self.read_settings_into(&mut services.config, true) {
            nwg::modal_error_message(&self.window, APP_NAME, &error);
            return;
        }
        services.logger.set_level(services.config.log_level);
        let save_result = save_config(&services.paths.config_file, &services.config);
        drop(services_ref);
        match save_result {
            Ok(()) => {
                self.refresh_status();
                nwg::modal_info_message(&self.window, APP_NAME, "设置已保存");
            }
            Err(error) => {
                nwg::modal_error_message(&self.window, APP_NAME, &error.to_string());
            }
        };
    }

    fn change_log_level(&self) {
        let selected = self
            .log_level_combo
            .selection_string()
            .unwrap_or_else(|| "Info".to_string());
        let Ok(level) = LogLevel::from_str(&selected) else {
            return;
        };
        let mut services_ref = self.services.borrow_mut();
        let Some(services) = services_ref.as_mut() else {
            return;
        };
        if services.config.log_level == level {
            return;
        }
        services.config.log_level = level;
        services.logger.set_level(level);
        let save_result = save_config(&services.paths.config_file, &services.config);
        drop(services_ref);
        if let Err(error) = save_result {
            nwg::modal_error_message(&self.window, APP_NAME, &error.to_string());
        }
    }

    fn read_settings_into(
        &self,
        config: &mut AppConfig,
        include_log_level: bool,
    ) -> Result<(), String> {
        config.listen_address = self.listen_address_input.text().trim().to_string();
        config.port = self
            .port_input
            .text()
            .trim()
            .parse::<u16>()
            .map_err(|_| "端口必须是 1 到 65535 之间的整数".to_string())?;
        if include_log_level {
            let selected = self
                .log_level_combo
                .selection_string()
                .unwrap_or_else(|| "Info".to_string());
            config.log_level = LogLevel::from_str(&selected).map_err(|error| error.to_string())?;
        }
        config
            .validate_listener()
            .map_err(|error| error.to_string())
    }

    fn regenerate_gateway_key(&self) {
        if !confirm(
            &self.window,
            "重新生成中转 Key 后，旧 Key 会立即失效。是否继续？",
        ) {
            return;
        }
        let mut services_ref = self.services.borrow_mut();
        let Some(services) = services_ref.as_mut() else {
            return;
        };
        services.config.gateway_key = generate_gateway_key();
        self.gateway_key_input
            .set_text(&services.config.gateway_key);
        if let Err(error) = save_config(&services.paths.config_file, &services.config) {
            nwg::modal_error_message(&self.window, APP_NAME, &error.to_string());
            return;
        }
        services
            .runtime
            .update_gateway_key(services.config.gateway_key.clone());
        nwg::Clipboard::set_data_text(&self.window, &services.config.gateway_key);
        nwg::modal_info_message(&self.window, APP_NAME, "新中转 Key 已生成并复制");
    }

    fn add_upstream(&self) {
        if let Some(services) = self.services.borrow_mut().as_mut() {
            services.editor_id = None;
        }
        self.editor_window.set_text("新增上游");
        self.editor_name_input.set_text("");
        self.editor_kind_combo.set_selection(Some(0));
        self.editor_url_input.set_text("");
        self.editor_key_input.set_text("");
        self.editor_window.set_visible(true);
        self.editor_name_input.set_focus();
    }

    fn edit_upstream(&self) {
        let Some(index) = self.upstream_list.selected_item() else {
            nwg::modal_info_message(&self.window, APP_NAME, "请先选择一个上游");
            return;
        };
        let mut services_ref = self.services.borrow_mut();
        let Some(services) = services_ref.as_mut() else {
            return;
        };
        let Some(profile) = services.config.upstreams.get(index).cloned() else {
            return;
        };
        services.editor_id = Some(profile.id);
        self.editor_window.set_text("编辑上游");
        self.editor_name_input.set_text(&profile.name);
        self.editor_kind_combo
            .set_selection(Some(match profile.kind {
                UpstreamKind::Sub2Api => 0,
                UpstreamKind::CliProxyApi => 1,
            }));
        self.editor_url_input.set_text(&profile.base_url);
        self.editor_key_input.set_text(&profile.api_key);
        self.editor_window.set_visible(true);
        self.editor_name_input.set_focus();
    }

    fn save_editor(&self) {
        let kind = match self.editor_kind_combo.selection().unwrap_or(0) {
            0 => UpstreamKind::Sub2Api,
            _ => UpstreamKind::CliProxyApi,
        };
        let mut services_ref = self.services.borrow_mut();
        let Some(services) = services_ref.as_mut() else {
            return;
        };
        let id = services.editor_id.unwrap_or_else(Uuid::new_v4);
        let profile = UpstreamProfile {
            id,
            name: self.editor_name_input.text().trim().to_string(),
            kind,
            base_url: self.editor_url_input.text().trim().to_string(),
            encrypted_api_key: String::new(),
            api_key: self.editor_key_input.text().trim().to_string(),
        };
        if let Err(error) = profile.validate() {
            nwg::modal_error_message(&self.editor_window, APP_NAME, &error.to_string());
            return;
        }
        let is_active = services.config.active_upstream_id == Some(id);
        if is_active
            && services.runtime.is_running()
            && services.runtime.metrics().active > 0
            && !confirm(
                &self.editor_window,
                "保存当前上游会立即断开正在进行的请求。是否继续？",
            )
        {
            return;
        }
        if let Some(existing) = services
            .config
            .upstreams
            .iter_mut()
            .find(|existing| existing.id == id)
        {
            *existing = profile.clone();
        } else {
            services.config.upstreams.push(profile.clone());
            if services.config.active_upstream_id.is_none() {
                services.config.active_upstream_id = Some(id);
            }
        }
        if let Err(error) = save_config(&services.paths.config_file, &services.config) {
            nwg::modal_error_message(&self.editor_window, APP_NAME, &error.to_string());
            return;
        }
        if services.config.active_upstream_id == Some(id) && services.runtime.is_running() {
            services.runtime.switch_upstream(profile);
        }
        self.editor_window.set_visible(false);
        drop(services_ref);
        self.refresh_upstream_list();
        self.refresh_status();
    }

    fn close_editor(&self) {
        self.editor_window.set_visible(false);
    }

    fn delete_upstream(&self) {
        let Some(index) = self.upstream_list.selected_item() else {
            nwg::modal_info_message(&self.window, APP_NAME, "请先选择一个上游");
            return;
        };
        let mut services_ref = self.services.borrow_mut();
        let Some(services) = services_ref.as_mut() else {
            return;
        };
        let Some(profile) = services.config.upstreams.get(index) else {
            return;
        };
        if services.config.active_upstream_id == Some(profile.id) {
            nwg::modal_error_message(&self.window, APP_NAME, "当前上游不能删除，请先启用其他上游");
            return;
        }
        if !confirm(&self.window, &format!("确定删除上游“{}”？", profile.name)) {
            return;
        }
        services.config.upstreams.remove(index);
        if let Err(error) = save_config(&services.paths.config_file, &services.config) {
            nwg::modal_error_message(&self.window, APP_NAME, &error.to_string());
            return;
        }
        drop(services_ref);
        self.refresh_upstream_list();
    }

    fn activate_upstream(&self) {
        let Some(index) = self.upstream_list.selected_item() else {
            nwg::modal_info_message(&self.window, APP_NAME, "请先选择一个上游");
            return;
        };
        let mut services_ref = self.services.borrow_mut();
        let Some(services) = services_ref.as_mut() else {
            return;
        };
        let Some(profile) = services.config.upstreams.get(index).cloned() else {
            return;
        };
        if services.config.active_upstream_id == Some(profile.id) {
            return;
        }
        if services.runtime.is_running()
            && services.runtime.metrics().active > 0
            && !confirm(&self.window, "切换上游会立即断开正在进行的请求。是否继续？")
        {
            return;
        }
        services.config.active_upstream_id = Some(profile.id);
        if let Err(error) = save_config(&services.paths.config_file, &services.config) {
            nwg::modal_error_message(&self.window, APP_NAME, &error.to_string());
            return;
        }
        if services.runtime.is_running() {
            services.runtime.switch_upstream(profile);
        }
        drop(services_ref);
        self.refresh_upstream_list();
        self.refresh_status();
    }

    fn test_selected_upstream(&self) {
        let Some(index) = self.upstream_list.selected_item() else {
            nwg::modal_info_message(&self.window, APP_NAME, "请先选择一个上游");
            return;
        };
        let services_ref = self.services.borrow();
        let Some(services) = services_ref.as_ref() else {
            return;
        };
        let Some(profile) = services.config.upstreams.get(index).cloned() else {
            return;
        };
        services.runtime.test_upstream(profile);
        self.test_upstream_button.set_enabled(false);
        self.test_upstream_button.set_text("测试中...");
    }

    fn refresh_upstream_list(&self) {
        self.upstream_list.clear();
        let services_ref = self.services.borrow();
        let Some(services) = services_ref.as_ref() else {
            return;
        };
        for profile in &services.config.upstreams {
            let active = if services.config.active_upstream_id == Some(profile.id) {
                "是"
            } else {
                ""
            };
            self.upstream_list.insert_items_row(
                None,
                &[
                    active.to_string(),
                    profile.name.clone(),
                    profile.kind.label().to_string(),
                    profile.base_url.clone(),
                ],
            );
        }
    }

    fn poll_runtime(&self) {
        let mut messages = Vec::new();
        if let Some(services) = self.services.borrow().as_ref() {
            messages = services.runtime.try_events();
        }
        for event in messages {
            match event {
                RuntimeEvent::Started(address) => {
                    nwg::modal_info_message(
                        &self.window,
                        APP_NAME,
                        &format!("代理已启动：{address}"),
                    );
                }
                RuntimeEvent::Stopped => {}
                RuntimeEvent::Error(error) => {
                    nwg::modal_error_message(&self.window, APP_NAME, &error);
                }
                RuntimeEvent::UpstreamSwitched(name) => {
                    self.status_value.set_text(&format!("上游已切换：{name}"));
                }
                RuntimeEvent::GatewayKeyUpdated => {}
                RuntimeEvent::TestFinished { profile_id, result } => {
                    self.test_upstream_button.set_enabled(true);
                    self.test_upstream_button.set_text("测试连接");
                    let name = self
                        .services
                        .borrow()
                        .as_ref()
                        .and_then(|services| {
                            services
                                .config
                                .upstreams
                                .iter()
                                .find(|profile| profile.id == profile_id)
                                .map(|profile| profile.name.clone())
                        })
                        .unwrap_or_else(|| "上游".to_string());
                    match result {
                        Ok(message) => nwg::modal_info_message(
                            &self.window,
                            APP_NAME,
                            &format!("{name}：{message}"),
                        ),
                        Err(error) => nwg::modal_error_message(
                            &self.window,
                            APP_NAME,
                            &format!("{name}：{error}"),
                        ),
                    };
                }
            }
        }
        self.refresh_status();
        let should_refresh_logs = self
            .services
            .borrow()
            .as_ref()
            .is_some_and(|services| services.logger.version() != services.last_log_version);
        if should_refresh_logs {
            self.refresh_logs();
        }
    }

    fn refresh_status(&self) {
        let services_ref = self.services.borrow();
        let Some(services) = services_ref.as_ref() else {
            return;
        };
        let running = services.runtime.is_running();
        self.status_value
            .set_text(if running { "运行中" } else { "已停止" });
        self.start_stop_button.set_text(if running {
            "停止代理"
        } else {
            "启动代理"
        });
        self.start_stop_button.set_enabled(true);
        self.tray_start_stop_item.set_enabled(true);
        self.listen_address_input.set_enabled(!running);
        self.port_input.set_enabled(!running);
        self.save_settings_button.set_enabled(!running);
        self.client_url_value.set_text(&format!(
            "http://{}:{}/v1",
            display_host(&services.config.listen_address),
            services.config.port
        ));
        self.upstream_value.set_text(
            services
                .config
                .active_upstream()
                .map(|profile| format!("{} ({})", profile.name, profile.kind.label()))
                .as_deref()
                .unwrap_or("未配置"),
        );
        let metrics = services.runtime.metrics();
        self.active_metric
            .set_text(&format!("活动请求\r\n{}", metrics.active));
        self.success_metric
            .set_text(&format!("成功请求\r\n{}", metrics.success));
        self.failed_metric
            .set_text(&format!("失败请求\r\n{}", metrics.failed));
        self.tls_warning.set_visible(!matches!(
            services.config.listen_address.as_str(),
            "127.0.0.1" | "::1"
        ));
    }

    fn refresh_logs(&self) {
        self.log_list.clear();
        let mut services_ref = self.services.borrow_mut();
        let Some(services) = services_ref.as_mut() else {
            return;
        };
        services.visible_logs.clear();
        let filter = self
            .log_filter_combo
            .selection_string()
            .unwrap_or_else(|| "全部".to_string());
        for event in services.logger.snapshot().into_iter().rev() {
            if filter != "全部" && !event.level.label().eq_ignore_ascii_case(&filter) {
                continue;
            }
            let detail = if let (Some(method), Some(path), Some(status), Some(duration)) = (
                &event.method,
                &event.path,
                event.status,
                event.duration_seconds,
            ) {
                format!(
                    "{method} {path}  {status}  {}",
                    format_duration_seconds(duration)
                )
            } else {
                event.message.clone()
            };
            let context = match (&event.upstream, &event.client_ip) {
                (Some(upstream), Some(client)) => format!("{upstream} / {client}"),
                (Some(upstream), None) => upstream.clone(),
                (None, Some(client)) => client.clone(),
                (None, None) => String::new(),
            };
            self.log_list.insert_items_row(
                None,
                &[
                    event.timestamp.format("%H:%M:%S").to_string(),
                    event.level.label().to_string(),
                    detail,
                    context,
                ],
            );
            services.visible_logs.push(event);
        }
        services.last_log_version = services.logger.version();
    }

    fn toggle_debug_capture(&self) {
        let enabled = self.debug_capture_checkbox.check_state() == nwg::CheckBoxState::Checked;
        if enabled
            && !confirm(
                &self.window,
                "调试捕获可能显示提示词、用户输入、模型输出和其他敏感信息。数据仅保存在内存中，每个请求/响应最多保留 64 KiB，关闭开关或退出程序即清除。是否开启？",
            )
        {
            self.debug_capture_checkbox
                .set_check_state(nwg::CheckBoxState::Unchecked);
            return;
        }

        if let Some(services) = self.services.borrow().as_ref() {
            services.logger.set_debug_capture(enabled);
        }
        if !enabled {
            self.debug_detail_text.clear();
            self.debug_detail_window.set_visible(false);
        }
        self.refresh_logs();
    }

    fn show_log_details(&self) {
        let Some(index) = self.log_list.selected_item() else {
            nwg::modal_info_message(&self.window, APP_NAME, "请先选择一条请求日志");
            return;
        };
        let event = self
            .services
            .borrow()
            .as_ref()
            .and_then(|services| services.visible_logs.get(index).cloned());
        let Some(event) = event else {
            return;
        };
        let Some(debug) = event.debug else {
            nwg::modal_info_message(
                &self.window,
                APP_NAME,
                "这条日志没有调试正文。请先开启“捕获请求/响应正文（敏感）”，再发送新请求。",
            );
            return;
        };

        let summary = format!(
            "时间: {}\n日志等级: {}\n结果: {}\n客户端: {}\n请求: {} {}\n上游: {}\n状态: {}\n总耗时: {}\n上游响应头耗时: {}\n首个响应块耗时: {}\n匿名会话标签: {}\n\n========== 客户端请求头 ==========\n{}\n\n========== 实际上游请求头 ==========\n{}\n\n========== 提取参数 ==========\n{}\n\n========== 实际上游请求结构 ==========\n{}\n\n========== 客户端请求正文 ==========\n{}\n\n========== SSE 事件摘要 ==========\n{}\n\n========== 上游响应正文 ==========\n{}",
            event.timestamp.format("%Y-%m-%d %H:%M:%S%.3f"),
            event.level.label(),
            event.message,
            event.client_ip.as_deref().unwrap_or("-"),
            event.method.as_deref().unwrap_or("-"),
            event.path.as_deref().unwrap_or("-"),
            event.upstream.as_deref().unwrap_or("-"),
            event
                .status
                .map_or_else(|| "-".to_string(), |value| value.to_string()),
            event
                .duration_seconds
                .map_or_else(|| "-".to_string(), format_duration_seconds),
            event
                .upstream_headers_seconds
                .map_or_else(|| "-".to_string(), format_duration_seconds),
            event
                .first_chunk_seconds
                .map_or_else(|| "-".to_string(), format_duration_seconds),
            event.session_tag.as_deref().unwrap_or("-"),
            debug.client_headers,
            debug.upstream_headers,
            debug.parameters,
            debug.upstream_request_structure,
            debug.request_body,
            debug.response_events,
            debug.response_body,
        );
        self.debug_detail_text.set_text_unix2dos(&summary);
        self.debug_detail_text.set_selection(0..0);
        self.debug_detail_window.set_visible(true);
        self.debug_detail_window.set_focus();
    }

    fn copy_log_details(&self) {
        nwg::Clipboard::set_data_text(&self.debug_detail_window, &self.debug_detail_text.text());
    }

    fn close_log_details(&self) {
        self.debug_detail_window.set_visible(false);
    }

    fn clear_logs(&self) {
        if let Some(services) = self.services.borrow().as_ref() {
            services.logger.clear_memory();
        }
        self.refresh_logs();
    }

    fn copy_client_url(&self) {
        nwg::Clipboard::set_data_text(&self.window, &self.client_url_value.text());
    }

    fn copy_gateway_key(&self) {
        if let Some(services) = self.services.borrow().as_ref() {
            nwg::Clipboard::set_data_text(&self.window, &services.config.gateway_key);
        }
    }

    fn open_log_directory(&self) {
        if let Some(services) = self.services.borrow().as_ref()
            && let Err(error) = Command::new("explorer.exe")
                .arg(&services.paths.log_dir)
                .spawn()
        {
            nwg::modal_error_message(&self.window, APP_NAME, &error.to_string());
        }
    }

    fn show_window(&self) {
        self.window.set_visible(true);
        self.window.restore();
        self.window.set_focus();
    }

    fn show_tray_menu(&self) {
        let (x, y) = nwg::GlobalCursor::position();
        self.tray_menu.popup(x, y);
    }

    fn exit_application(&self) {
        let running = self
            .services
            .borrow()
            .as_ref()
            .is_some_and(|services| services.runtime.is_running());
        if running && !confirm(&self.window, "退出会停止代理并断开活动请求。是否继续？")
        {
            return;
        }
        let runtime = {
            let mut services_ref = self.services.borrow_mut();
            let Some(services) = services_ref.as_mut() else {
                return;
            };
            services.runtime.clone()
        };
        runtime.shutdown();
        for handler in self.close_handlers.borrow_mut().drain(..) {
            let _ = nwg::unbind_raw_event_handler(&handler);
        }
        nwg::stop_thread_dispatch();
    }
}

fn socket_address(config: &AppConfig) -> Result<SocketAddr, String> {
    let ip = config
        .listen_address
        .parse::<IpAddr>()
        .map_err(|_| "监听地址无效".to_string())?;
    Ok(SocketAddr::new(ip, config.port))
}

fn display_host(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

fn confirm(parent: &nwg::Window, message: &str) -> bool {
    nwg::modal_message(
        parent,
        &nwg::MessageParams {
            title: APP_NAME,
            content: message,
            buttons: nwg::MessageButtons::YesNo,
            icons: nwg::MessageIcons::Warning,
        },
    ) == nwg::MessageChoice::Yes
}
