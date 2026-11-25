//! A redis-rs command to the words pgkv takes, and those words to a statement parameter.

use redis::{Arg, Cmd, ErrorKind, RedisError, RedisResult};

/// The words of `cmd` as text.
///
/// pgkv stores text: an argument that is not valid UTF-8, or that contains a NUL byte (which
/// PostgreSQL text cannot hold), is rejected here instead of being altered.
pub(crate) fn argv(cmd: &Cmd) -> RedisResult<Vec<String>> {
    let mut words: Vec<&[u8]> = Vec::new();
    let mut simple = true;
    for arg in cmd.args_iter() {
        match arg {
            Arg::Simple(bytes) => words.push(bytes),
            // A cursor (or a kind of argument added later) has no public accessor.
            _ => simple = false,
        }
    }
    let packed;
    if !simple {
        // Fall back to the bytes redis-rs would send, which include the cursor's value.
        packed = cmd.get_packed_command();
        words = unpack(&packed).ok_or_else(|| {
            RedisError::from((
                ErrorKind::Client,
                "pgkv: could not read the command's arguments",
            ))
        })?;
    }
    if words.is_empty() {
        return Err(RedisError::from((ErrorKind::Client, "empty command")));
    }
    words
        .iter()
        .enumerate()
        .map(|(index, word)| text(word, index, words[0]))
        .collect()
}

/// The words of one command as a PostgreSQL `text[]` literal: every element quoted, so that
/// braces, commas, spaces, `NULL` and the empty string are taken literally.
pub(crate) fn array_literal(words: &[String]) -> String {
    let mut literal = String::with_capacity(words.iter().map(|w| w.len() + 3).sum::<usize>() + 2);
    literal.push('{');
    for (index, word) in words.iter().enumerate() {
        if index > 0 {
            literal.push(',');
        }
        literal.push('"');
        for character in word.chars() {
            if matches!(character, '"' | '\\') {
                literal.push('\\');
            }
            literal.push(character);
        }
        literal.push('"');
    }
    literal.push('}');
    literal
}

/// The commands of a pipeline as a JSON array of arrays of strings.
pub(crate) fn json(commands: &[Vec<String>]) -> String {
    serde_json::Value::from(commands.to_vec()).to_string()
}

fn text(word: &[u8], index: usize, command: &[u8]) -> RedisResult<String> {
    let reject = |what: &'static str| {
        RedisError::from((
            ErrorKind::Client,
            what,
            format!(
                "argument {index} of {}",
                String::from_utf8_lossy(command).to_uppercase()
            ),
        ))
    };
    let word = std::str::from_utf8(word)
        .map_err(|_| reject("pgkv: argument is not valid UTF-8 (pgkv values are text)"))?;
    if word.contains('\0') {
        return Err(reject(
            "pgkv: argument contains a NUL byte (pgkv values are text)",
        ));
    }
    Ok(word.to_owned())
}

/// Splits one RESP command (`*<n>\r\n` then n times `$<len>\r\n<bytes>\r\n`) into its arguments.
fn unpack(mut packed: &[u8]) -> Option<Vec<&[u8]>> {
    let count = header(&mut packed, b'*')?;
    // Every argument takes at least "$0\r\n\r\n".
    if count > packed.len() / 6 {
        return None;
    }
    let mut words = Vec::with_capacity(count);
    for _ in 0..count {
        let len = header(&mut packed, b'$')?;
        let word = packed.get(..len)?;
        if packed.get(len..len.checked_add(2)?)? != b"\r\n" {
            return None;
        }
        words.push(word);
        packed = &packed[len + 2..];
    }
    packed.is_empty().then_some(words)
}

/// Reads `<marker><decimal>\r\n` from the front of `input`.
fn header(input: &mut &[u8], marker: u8) -> Option<usize> {
    let (&first, rest) = input.split_first()?;
    if first != marker {
        return None;
    }
    let end = rest.iter().position(|&b| b == b'\r')?;
    if rest.get(end + 1) != Some(&b'\n') {
        return None;
    }
    let number = std::str::from_utf8(&rest[..end]).ok()?.parse().ok()?;
    *input = &rest[end + 2..];
    Some(number)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_literal_quotes_every_word() {
        let words = |words: &[&str]| words.iter().map(|w| (*w).to_owned()).collect::<Vec<_>>();
        assert_eq!(array_literal(&words(&["PING"])), r#"{"PING"}"#);
        assert_eq!(
            array_literal(&words(&["SET", "", "NULL", "a,b {c} 'd'\n"])),
            "{\"SET\",\"\",\"NULL\",\"a,b {c} 'd'\n\"}"
        );
        assert_eq!(
            array_literal(&words(&["k", r#"say "hi" \ é"#])),
            r#"{"k","say \"hi\" \\ é"}"#
        );
    }

    #[test]
    fn json_is_an_array_of_arrays_of_strings() {
        let commands = vec![
            vec![
                "SET".to_owned(),
                "k".to_owned(),
                "a\"b\\c\n\u{1}é".to_owned(),
            ],
            vec!["PING".to_owned()],
        ];
        let text = json(&commands);
        assert_eq!(text, r#"[["SET","k","a\"b\\c\n\u0001é"],["PING"]]"#);
        let back: Vec<Vec<String>> = serde_json::from_str::<serde_json::Value>(&text)
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                c.as_array()
                    .unwrap()
                    .iter()
                    .map(|w| w.as_str().unwrap().to_owned())
                    .collect()
            })
            .collect();
        assert_eq!(back, commands);
        assert_eq!(json(&[]), "[]");
    }

    #[test]
    fn words_in_order() {
        let mut cmd = redis::cmd("SET");
        cmd.arg("k").arg("v").arg("EX").arg(10);
        assert_eq!(argv(&cmd).unwrap(), ["SET", "k", "v", "EX", "10"]);
    }

    #[test]
    fn typed_arguments_are_formatted_by_redis_rs() {
        let mut cmd = redis::cmd("ZADD");
        cmd.arg("z")
            .arg(1.5f64)
            .arg("m")
            .arg(-3i64)
            .arg("héllo 😀")
            .arg("");
        assert_eq!(
            argv(&cmd).unwrap(),
            ["ZADD", "z", "1.5", "m", "-3", "héllo 😀", ""]
        );
    }

    #[test]
    fn cursor_argument_takes_the_cursor_value() {
        let mut cmd = redis::cmd("SCAN");
        cmd.cursor_arg(0).arg("MATCH").arg("a\r\n*");
        assert_eq!(argv(&cmd).unwrap(), ["SCAN", "0", "MATCH", "a\r\n*"]);
        let mut cmd = redis::cmd("HSCAN");
        cmd.arg("h").cursor_arg(18446744073709551615);
        assert_eq!(argv(&cmd).unwrap(), ["HSCAN", "h", "18446744073709551615"]);
    }

    #[test]
    fn non_utf8_is_rejected() {
        let mut cmd = redis::cmd("SET");
        cmd.arg("k").arg(&[0xff_u8, 0xfe][..]);
        let err = argv(&cmd).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Client);
        assert!(err.to_string().contains("not valid UTF-8"), "{err}");
        assert_eq!(err.detail(), Some("argument 2 of SET"));
    }

    #[test]
    fn nul_is_rejected() {
        let mut cmd = redis::cmd("set");
        cmd.arg("k\0ey").arg("v");
        let err = argv(&cmd).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Client);
        assert!(err.to_string().contains("NUL byte"), "{err}");
        assert_eq!(err.detail(), Some("argument 1 of SET"));
    }

    #[test]
    fn invalid_argument_after_a_cursor_is_rejected() {
        let mut cmd = redis::cmd("SCAN");
        cmd.cursor_arg(0).arg("MATCH").arg(&[0xc3_u8][..]);
        assert_eq!(argv(&cmd).unwrap_err().kind(), ErrorKind::Client);
    }

    #[test]
    fn empty_command_is_rejected() {
        let err = argv(&redis::Cmd::new()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Client);
    }

    #[test]
    fn unpack_rejects_malformed_input() {
        assert_eq!(
            unpack(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").unwrap(),
            [&b"GET"[..], &b"k"[..]]
        );
        for bad in [
            &b""[..],
            b"*1\r\n",
            b"*1\r\n$3\r\nGE\r\n",
            b"*1\r\n$3\r\nGETxx",
            b"*2\r\n$3\r\nGET\r\n",
            b"*1\r\n$3\r\nGET\r\nextra",
            b"*x\r\n",
            b"$3\r\nGET\r\n",
            b"*1\r\n$18446744073709551615\r\nGET\r\n",
            b"*18446744073709551615\r\n",
        ] {
            assert!(unpack(bad).is_none(), "{bad:?}");
        }
    }
}
