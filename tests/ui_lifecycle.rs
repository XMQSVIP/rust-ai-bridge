#![cfg(windows)]

use std::{
    process::{Child, Command},
    ptr, thread,
    time::{Duration, Instant},
};

use winapi::{
    shared::{
        minwindef::{BOOL, FALSE, LPARAM, TRUE},
        windef::HWND,
    },
    um::winuser::{
        BM_CLICK, EnumChildWindows, EnumWindows, GetWindowTextLengthW, GetWindowTextW,
        GetWindowThreadProcessId, IsWindow, IsWindowVisible, PostMessageW, SendMessageW, WM_CLOSE,
        WM_LBUTTONUP, WM_USER,
    },
};

const WINDOW_TITLE: &str = "Rust AI Bridge";
const EDITOR_TITLE: &str = "新增上游";
const ADD_BUTTON_TEXT: &str = "新增";
const SAVE_SETTINGS_BUTTON_TEXT: &str = "保存设置";
const NWG_TRAY: u32 = WM_USER + 102;

struct ChildGuard(Child);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct WindowSearch {
    process_id: Option<u32>,
    expected_title: &'static str,
    excluded_hwnd: HWND,
    hwnd: HWND,
}

unsafe extern "system" fn find_process_window(hwnd: HWND, data: LPARAM) -> BOOL {
    let search = unsafe { &mut *(data as *mut WindowSearch) };
    let mut process_id = 0;
    unsafe {
        GetWindowThreadProcessId(hwnd, &mut process_id);
    }
    if search
        .process_id
        .is_some_and(|expected_process_id| process_id != expected_process_id)
        || hwnd == search.excluded_hwnd
    {
        return TRUE;
    }

    let text_length = unsafe { GetWindowTextLengthW(hwnd) };
    if text_length <= 0 {
        return TRUE;
    }
    let mut title = vec![0u16; text_length as usize + 1];
    let copied = unsafe { GetWindowTextW(hwnd, title.as_mut_ptr(), title.len() as i32) };
    if copied <= 0 || String::from_utf16_lossy(&title[..copied as usize]) != search.expected_title {
        return TRUE;
    }

    search.hwnd = hwnd;
    FALSE
}

fn find_window(process_id: u32, expected_title: &'static str) -> Option<HWND> {
    let mut search = WindowSearch {
        process_id: Some(process_id),
        expected_title,
        excluded_hwnd: ptr::null_mut(),
        hwnd: ptr::null_mut(),
    };
    unsafe {
        EnumWindows(
            Some(find_process_window),
            &mut search as *mut WindowSearch as LPARAM,
        );
    }
    (!search.hwnd.is_null()).then_some(search.hwnd)
}

fn find_child_window(parent: HWND, expected_title: &'static str) -> Option<HWND> {
    let mut search = WindowSearch {
        process_id: None,
        expected_title,
        excluded_hwnd: ptr::null_mut(),
        hwnd: ptr::null_mut(),
    };
    unsafe {
        EnumChildWindows(
            parent,
            Some(find_process_window),
            &mut search as *mut WindowSearch as LPARAM,
        );
    }
    (!search.hwnd.is_null()).then_some(search.hwnd)
}

fn find_other_window(
    process_id: u32,
    expected_title: &'static str,
    excluded_hwnd: HWND,
) -> Option<HWND> {
    let mut search = WindowSearch {
        process_id: Some(process_id),
        expected_title,
        excluded_hwnd,
        hwnd: ptr::null_mut(),
    };
    unsafe {
        EnumWindows(
            Some(find_process_window),
            &mut search as *mut WindowSearch as LPARAM,
        );
    }
    (!search.hwnd.is_null()).then_some(search.hwnd)
}

unsafe extern "system" fn collect_child_titles(hwnd: HWND, data: LPARAM) -> BOOL {
    let titles = unsafe { &mut *(data as *mut Vec<String>) };
    let text_length = unsafe { GetWindowTextLengthW(hwnd) };
    if text_length > 0 {
        let mut title = vec![0u16; text_length as usize + 1];
        let copied = unsafe { GetWindowTextW(hwnd, title.as_mut_ptr(), title.len() as i32) };
        if copied > 0 {
            titles.push(String::from_utf16_lossy(&title[..copied as usize]));
        }
    }
    TRUE
}

fn child_titles(parent: HWND) -> Vec<String> {
    let mut titles = Vec::new();
    unsafe {
        EnumChildWindows(
            parent,
            Some(collect_child_titles),
            &mut titles as *mut Vec<String> as LPARAM,
        );
    }
    titles
}

fn wait_until(mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(25));
    }
    false
}

#[test]
fn settings_save_does_not_crash_and_window_lifecycle_works() {
    let app_data = tempfile::tempdir().expect("create isolated LocalAppData");
    let child = Command::new(env!("CARGO_BIN_EXE_rust-ai-bridge"))
        .env("LOCALAPPDATA", app_data.path())
        .spawn()
        .expect("launch GUI binary");
    let child = ChildGuard(child);

    let mut hwnd = ptr::null_mut();
    assert!(
        wait_until(|| {
            hwnd = find_window(child.0.id(), WINDOW_TITLE).unwrap_or(ptr::null_mut());
            !hwnd.is_null() && unsafe { IsWindowVisible(hwnd) != 0 }
        }),
        "main window did not become visible"
    );

    let mut save_button = ptr::null_mut();
    assert!(
        wait_until(|| {
            save_button =
                find_child_window(hwnd, SAVE_SETTINGS_BUTTON_TEXT).unwrap_or(ptr::null_mut());
            !save_button.is_null()
        }),
        "find save settings button; child titles: {:?}",
        child_titles(hwnd)
    );
    unsafe {
        PostMessageW(save_button, BM_CLICK, 0, 0);
    }
    let mut saved_dialog = ptr::null_mut();
    assert!(
        wait_until(|| {
            saved_dialog =
                find_other_window(child.0.id(), WINDOW_TITLE, hwnd).unwrap_or(ptr::null_mut());
            !saved_dialog.is_null()
        }),
        "settings save confirmation did not appear; the process may have crashed"
    );
    unsafe {
        SendMessageW(saved_dialog, WM_CLOSE, 0, 0);
    }
    assert!(wait_until(|| unsafe { IsWindow(saved_dialog) == 0 }));

    let mut add_button = ptr::null_mut();
    assert!(
        wait_until(|| {
            add_button = find_child_window(hwnd, ADD_BUTTON_TEXT).unwrap_or(ptr::null_mut());
            !add_button.is_null()
        }),
        "find add upstream button; child titles: {:?}",
        child_titles(hwnd)
    );
    unsafe {
        SendMessageW(add_button, BM_CLICK, 0, 0);
    }
    let mut editor_hwnd = ptr::null_mut();
    assert!(
        wait_until(|| {
            editor_hwnd = find_window(child.0.id(), EDITOR_TITLE).unwrap_or(ptr::null_mut());
            !editor_hwnd.is_null() && unsafe { IsWindowVisible(editor_hwnd) != 0 }
        }),
        "upstream editor did not become visible"
    );
    unsafe {
        SendMessageW(editor_hwnd, WM_CLOSE, 0, 0);
    }
    assert!(wait_until(|| unsafe { IsWindowVisible(editor_hwnd) == 0 }));
    assert_ne!(
        unsafe { IsWindow(editor_hwnd) },
        0,
        "WM_CLOSE destroyed the editor HWND"
    );

    unsafe {
        SendMessageW(hwnd, WM_CLOSE, 0, 0);
    }
    assert!(wait_until(|| unsafe { IsWindowVisible(hwnd) == 0 }));
    assert_ne!(unsafe { IsWindow(hwnd) }, 0, "WM_CLOSE destroyed the HWND");

    unsafe {
        SendMessageW(hwnd, NWG_TRAY, 0, WM_LBUTTONUP as LPARAM);
    }
    assert!(
        wait_until(|| unsafe { IsWindowVisible(hwnd) != 0 }),
        "tray callback did not restore the main window"
    );
}
