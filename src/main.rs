#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod ui;

use native_windows_gui as nwg;
use rust_ai_bridge::{
    config::{APP_NAME, AppPaths, load_config, save_config},
    logger::AppLogger,
    runtime::RuntimeController,
    single_instance::SingleInstance,
};

fn main() {
    if let Err(error) = run() {
        let _ = nwg::init();
        nwg::error_message(APP_NAME, &error.to_string());
    }
}

fn run() -> anyhow::Result<()> {
    let Some(_instance) = SingleInstance::acquire()? else {
        nwg::init()?;
        nwg::simple_message(APP_NAME, "Rust AI Bridge 已经在运行，请检查系统托盘。");
        return Ok(());
    };

    let paths = AppPaths::discover()?;
    paths.ensure()?;
    let config = load_config(&paths.config_file)?;
    save_config(&paths.config_file, &config)?;
    let logger = AppLogger::new(paths.log_dir.clone(), config.log_level)?;
    let runtime = RuntimeController::spawn(logger.clone())?;

    nwg::init()?;
    nwg::Font::set_global_family("Microsoft YaHei UI")?;
    ui::BridgeApp::run(config, paths, logger, runtime)?;
    Ok(())
}
