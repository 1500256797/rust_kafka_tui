use std::io::{self, Write};

use crossterm::terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen};
use crossterm::ExecutableCommand;

/// Enable click/scroll mouse reporting only (no motion tracking).
/// Crossterm's `EnableMouseCapture` also enables mode 1003 which floods the
/// input queue and can corrupt the terminal if the UI panics.
pub fn enable_mouse_click_only(out: &mut impl Write) -> io::Result<()> {
    write!(out, "\x1b[?1000h\x1b[?1006h\x1b[?1007h")?;
    out.flush()
}

pub fn disable_mouse(out: &mut impl Write) -> io::Result<()> {
    write!(out, "\x1b[?1007l\x1b[?1006l\x1b[?1003l\x1b[?1002l\x1b[?1000l")?;
    out.flush()
}

pub fn enter_terminal(out: &mut impl Write) -> io::Result<()> {
    enable_raw_mode()?;
    out.execute(EnterAlternateScreen)?;
    enable_mouse_click_only(out)
}

pub fn leave_terminal(out: &mut impl Write) -> io::Result<()> {
    let _ = disable_mouse(out);
    let _ = out.execute(LeaveAlternateScreen);
    disable_raw_mode()
}

pub fn install_panic_hook() {
    let original = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let mut out = io::stdout();
        let _ = leave_terminal(&mut out);
        original(info);
    }));
}
