use std::{env, fs, io, path::PathBuf};

fn main() -> io::Result<()> {
    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is set"));
    let icon_path = out_dir.join("rust-ai-bridge.ico");
    write_icon(&icon_path)?;

    let mut resource = winresource::WindowsResource::new();
    resource
        .set_icon(icon_path.to_str().expect("icon path is UTF-8"))
        .set("ProductName", "Rust AI Bridge")
        .set("FileDescription", "OpenAI-compatible streaming relay for Windows")
        .set("LegalCopyright", "Copyright (c) 2026")
        .set_manifest(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<assembly xmlns="urn:schemas-microsoft-com:asm.v1" manifestVersion="1.0">
  <dependency>
    <dependentAssembly>
      <assemblyIdentity type="win32" name="Microsoft.Windows.Common-Controls" version="6.0.0.0" processorArchitecture="*" publicKeyToken="6595b64144ccf1df" language="*"/>
    </dependentAssembly>
  </dependency>
  <application xmlns="urn:schemas-microsoft-com:asm.v3">
    <windowsSettings>
      <dpiAware xmlns="http://schemas.microsoft.com/SMI/2005/WindowsSettings">true/pm</dpiAware>
      <dpiAwareness xmlns="http://schemas.microsoft.com/SMI/2016/WindowsSettings">PerMonitorV2</dpiAwareness>
      <longPathAware xmlns="http://schemas.microsoft.com/SMI/2016/WindowsSettings">true</longPathAware>
    </windowsSettings>
  </application>
</assembly>"#,
        );
    resource.compile()?;
    Ok(())
}

fn write_icon(path: &PathBuf) -> io::Result<()> {
    const SIZE: usize = 32;
    let xor_size = SIZE * SIZE * 4;
    let mask_row = SIZE.div_ceil(32) * 4;
    let mask_size = mask_row * SIZE;
    let image_size = 40 + xor_size + mask_size;

    let mut data = Vec::with_capacity(22 + image_size);
    data.extend_from_slice(&0u16.to_le_bytes());
    data.extend_from_slice(&1u16.to_le_bytes());
    data.extend_from_slice(&1u16.to_le_bytes());
    data.extend_from_slice(&[SIZE as u8, SIZE as u8, 0, 0]);
    data.extend_from_slice(&1u16.to_le_bytes());
    data.extend_from_slice(&32u16.to_le_bytes());
    data.extend_from_slice(&(image_size as u32).to_le_bytes());
    data.extend_from_slice(&22u32.to_le_bytes());

    data.extend_from_slice(&40u32.to_le_bytes());
    data.extend_from_slice(&(SIZE as i32).to_le_bytes());
    data.extend_from_slice(&((SIZE * 2) as i32).to_le_bytes());
    data.extend_from_slice(&1u16.to_le_bytes());
    data.extend_from_slice(&32u16.to_le_bytes());
    data.extend_from_slice(&0u32.to_le_bytes());
    data.extend_from_slice(&(xor_size as u32).to_le_bytes());
    data.extend_from_slice(&0i32.to_le_bytes());
    data.extend_from_slice(&0i32.to_le_bytes());
    data.extend_from_slice(&0u32.to_le_bytes());
    data.extend_from_slice(&0u32.to_le_bytes());

    for y in (0..SIZE).rev() {
        for x in 0..SIZE {
            let dx = x as i32 - 16;
            let dy = y as i32 - 16;
            let inside = dx * dx + dy * dy <= 225;
            let bridge = (8..=23).contains(&x)
                && ((10..=13).contains(&y)
                    || (19..=22).contains(&y)
                    || ((12..=20).contains(&x) && (13..=19).contains(&y)));
            let (r, g, b, a) = if bridge {
                (245, 250, 255, 255)
            } else if inside {
                (24, 105, 180, 255)
            } else {
                (0, 0, 0, 0)
            };
            data.extend_from_slice(&[b, g, r, a]);
        }
    }
    data.resize(data.len() + mask_size, 0);
    fs::write(path, data)
}
