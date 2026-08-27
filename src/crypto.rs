use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use windows::{
    Win32::{
        Foundation::{HLOCAL, LocalFree},
        Security::Cryptography::{
            CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData, CryptUnprotectData,
        },
    },
    core::PWSTR,
};

pub fn protect_string(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    let input = CRYPT_INTEGER_BLOB {
        cbData: bytes.len() as u32,
        pbData: bytes.as_ptr() as *mut u8,
    };
    let mut output = CRYPT_INTEGER_BLOB::default();
    unsafe {
        CryptProtectData(
            &input,
            None,
            None,
            None,
            None,
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output,
        )
        .context("Windows DPAPI 加密失败")?;
        let encrypted = std::slice::from_raw_parts(output.pbData, output.cbData as usize);
        let encoded = STANDARD.encode(encrypted);
        let _ = LocalFree(Some(HLOCAL(output.pbData.cast())));
        Ok(encoded)
    }
}

pub fn unprotect_string(value: &str) -> Result<String> {
    let mut encrypted = STANDARD
        .decode(value)
        .context("密钥数据不是有效的 Base64")?;
    let input = CRYPT_INTEGER_BLOB {
        cbData: encrypted.len() as u32,
        pbData: encrypted.as_mut_ptr(),
    };
    let mut output = CRYPT_INTEGER_BLOB::default();
    let mut description = PWSTR::null();
    unsafe {
        CryptUnprotectData(
            &input,
            Some(&mut description),
            None,
            None,
            None,
            CRYPTPROTECT_UI_FORBIDDEN,
            &mut output,
        )
        .context("Windows DPAPI 解密失败")?;
        let plain = std::slice::from_raw_parts(output.pbData, output.cbData as usize);
        let result = String::from_utf8(plain.to_vec()).context("解密后的密钥不是 UTF-8")?;
        if !description.is_null() {
            let _ = LocalFree(Some(HLOCAL(description.0.cast())));
        }
        let _ = LocalFree(Some(HLOCAL(output.pbData.cast())));
        if result.is_empty() {
            bail!("解密后的密钥为空");
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dpapi_round_trip() {
        let secret = "rab_test_secret_123";
        let encrypted = protect_string(secret).unwrap();
        assert_ne!(encrypted, secret);
        assert_eq!(unprotect_string(&encrypted).unwrap(), secret);
    }
}
