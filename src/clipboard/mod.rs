use thiserror::Error;

#[derive(Error, Debug)]
pub enum CopyError {
    #[error("剪贴板不可用")]
    Unavailable,

    #[error("复制失败: {0}")]
    Failed(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CopyResult {
    Osc52,
    System,
}

pub struct ClipboardService;

impl ClipboardService {
    pub fn new() -> Self {
        Self
    }

    pub fn copy(&self, text: &str) -> Result<CopyResult, CopyError> {
        if try_osc52(text) {
            return Ok(CopyResult::Osc52);
        }

        match arboard::Clipboard::new() {
            Ok(mut cb) => {
                cb.set_text(text.to_string())
                    .map_err(|e| CopyError::Failed(e.to_string()))?;
                Ok(CopyResult::System)
            }
            Err(_) => Err(CopyError::Unavailable),
        }
    }
}

pub fn try_osc52(text: &str) -> bool {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let encoded = STANDARD.encode(text.as_bytes());
    let seq = format!("\x1b]52;c;{encoded}\x07");
    print!("{seq}");
    use std::io::Write;
    std::io::stdout().flush().is_ok()
}
