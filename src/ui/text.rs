/// Strip ANSI escapes and non-printable control chars before rendering in the TUI.
pub fn sanitize_display(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\x1b' {
            skip_ansi_sequence(&mut chars);
            continue;
        }
        if c.is_control() && c != '\n' && c != '\r' && c != '\t' {
            out.push('·');
            continue;
        }
        out.push(c);
    }

    out
}

/// Human-readable page number; `u64::MAX` is used internally as the "last page" sentinel.
pub fn format_page_index(page_index: u64) -> String {
    if page_index == u64::MAX {
        "末".to_string()
    } else {
        page_index.saturating_add(1).to_string()
    }
}

fn skip_ansi_sequence<I>(chars: &mut std::iter::Peekable<I>)
where
    I: Iterator<Item = char>,
{
    match chars.peek() {
        Some('[') => {
            chars.next();
            while let Some(c) = chars.next() {
                if ('@'..='~').contains(&c) {
                    break;
                }
            }
        }
        Some(']') => {
            // OSC sequence: ESC ] ... BEL or ESC \
            chars.next();
            while let Some(c) = chars.next() {
                if c == '\x07' {
                    break;
                }
                if c == '\x1b' {
                    if chars.peek() == Some(&'\\') {
                        chars.next();
                        break;
                    }
                }
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_page_index_last_page() {
        assert_eq!(format_page_index(u64::MAX), "末");
        assert_eq!(format_page_index(0), "1");
    }

    #[test]
    fn strips_ansi_color_codes() {
        let raw = "\x1b[31mhello\x1b[0m";
        assert_eq!(sanitize_display(raw), "hello");
    }

    #[test]
    fn replaces_control_chars() {
        let raw = "a\x00b";
        assert_eq!(sanitize_display(raw), "a·b");
    }
}
