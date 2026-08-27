use anyhow::{Context, Result};
use windows::{
    Win32::{
        Foundation::{CloseHandle, ERROR_ALREADY_EXISTS, GetLastError, HANDLE},
        System::Threading::CreateMutexW,
    },
    core::w,
};

pub struct SingleInstance {
    handle: HANDLE,
}

impl SingleInstance {
    pub fn acquire() -> Result<Option<Self>> {
        unsafe {
            let handle = CreateMutexW(
                None,
                true,
                w!("Local\\RustAIBridge-6F4D883D-4A55-43D5-AEE1-96DA881A6E5B"),
            )
            .context("无法创建单实例互斥量")?;
            if GetLastError() == ERROR_ALREADY_EXISTS {
                let _ = CloseHandle(handle);
                return Ok(None);
            }
            Ok(Some(Self { handle }))
        }
    }
}

impl Drop for SingleInstance {
    fn drop(&mut self) {
        unsafe {
            let _ = CloseHandle(self.handle);
        }
    }
}
