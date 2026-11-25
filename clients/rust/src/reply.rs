//! JSON reply of `pgkv.try_call` and `pgkv.pipeline` to [`redis::Value`].
//!
//! No command's reply is a JSON object, so an object is always a status, an error, or (in place
//! of a pipeline's replies) a discarded transaction.

use redis::{ErrorKind, RedisError, RedisResult, Value};
use serde_json::Value as Json;

/// Turns the JSON text pgkv returned into the value redis-rs reads off a RESP2 connection.
///
/// number -> `Int`, string -> `BulkString`, null -> `Nil`, array -> `Array`,
/// `{"status": "OK"}` -> `Okay`, `{"status": other}` -> `SimpleString`,
/// `{"error": line}` -> `ServerError`.
/// Anything else is not a reply pgkv produces and is an error.
pub(crate) fn parse(json: &str) -> RedisResult<Value> {
    let parsed: Json = serde_json::from_str(json).map_err(|e| malformed(e.to_string()))?;
    convert(parsed).map_err(malformed)
}

/// What `pgkv.pipeline` answered.
#[derive(Debug, PartialEq)]
pub(crate) enum Pipeline {
    /// One reply per command.
    Replies(Vec<Value>),
    /// The atomic pipeline was discarded and nothing was applied.
    Discarded(Discarded),
}

/// A discarded atomic pipeline: the reply to EXEC and the commands refused when queued.
#[derive(Debug, PartialEq)]
pub(crate) struct Discarded {
    /// `EXECABORT Transaction discarded because of previous errors.`
    pub(crate) line: String,
    /// Zero-based index in the pipeline and error line of every refused command.
    pub(crate) refused: Vec<(usize, String)>,
}

/// Turns the JSON text `pgkv.pipeline` returned for `commands` commands into their replies.
///
/// It is an array with one reply per command, or for a discarded atomic pipeline
/// `{"error": line, "previous": [{"index": n, "error": line}, ...]}`.
pub(crate) fn parse_pipeline(json: &str, commands: usize) -> RedisResult<Pipeline> {
    let parsed: Json = serde_json::from_str(json).map_err(|e| malformed(e.to_string()))?;
    match parsed {
        Json::Array(items) if items.len() == commands => items
            .into_iter()
            .map(convert)
            .collect::<Result<_, _>>()
            .map(Pipeline::Replies)
            .map_err(malformed),
        Json::Array(_) => Err(malformed("not one reply per command".to_owned())),
        Json::Object(map) => discarded(map)
            .map(Pipeline::Discarded)
            .ok_or_else(|| malformed("object is not a discarded pipeline".to_owned())),
        _ => Err(malformed("not an array of replies".to_owned())),
    }
}

fn discarded(mut map: serde_json::Map<String, Json>) -> Option<Discarded> {
    let (Some(Json::String(line)), Some(Json::Array(previous))) =
        (map.remove("error"), map.remove("previous"))
    else {
        return None;
    };
    if !map.is_empty() {
        return None;
    }
    let refused = previous
        .into_iter()
        .map(|entry| match entry {
            Json::Object(mut entry) => {
                let index = usize::try_from(entry.remove("index")?.as_u64()?).ok()?;
                match (entry.remove("error")?, entry.is_empty()) {
                    (Json::String(line), true) => Some((index, line)),
                    _ => None,
                }
            }
            _ => None,
        })
        .collect::<Option<_>>()?;
    Some(Discarded { line, refused })
}

pub(crate) fn malformed(detail: String) -> RedisError {
    RedisError::from((ErrorKind::Parse, "pgkv: malformed reply", detail))
}

fn convert(json: Json) -> Result<Value, String> {
    match json {
        Json::Null => Ok(Value::Nil),
        Json::String(s) => Ok(Value::BulkString(s.into_bytes())),
        Json::Number(n) => n
            .as_i64()
            .map(Value::Int)
            .ok_or_else(|| format!("number {n} is not a 64-bit integer")),
        Json::Array(items) => items
            .into_iter()
            .map(convert)
            .collect::<Result<_, _>>()
            .map(Value::Array),
        Json::Object(map) => {
            let mut members = map.into_iter();
            match (members.next(), members.next()) {
                (Some((name, Json::String(text))), None) if name == "status" => {
                    Ok(match text.as_str() {
                        "OK" => Value::Okay,
                        _ => Value::SimpleString(text),
                    })
                }
                (Some((name, Json::String(line))), None) if name == "error" => {
                    Ok(crate::error::server_error(&line))
                }
                _ => Err("object is not {\"status\": string} or {\"error\": string}".to_owned()),
            }
        }
        Json::Bool(b) => Err(format!("unexpected boolean {b}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bulk(s: &str) -> Value {
        Value::BulkString(s.as_bytes().to_vec())
    }

    #[test]
    fn scalars() {
        assert_eq!(parse("null").unwrap(), Value::Nil);
        assert_eq!(parse("42").unwrap(), Value::Int(42));
        assert_eq!(parse("-1").unwrap(), Value::Int(-1));
        assert_eq!(parse("9223372036854775807").unwrap(), Value::Int(i64::MAX));
        assert_eq!(parse("-9223372036854775808").unwrap(), Value::Int(i64::MIN));
        assert_eq!(parse("\"hello\"").unwrap(), bulk("hello"));
        assert_eq!(parse("\"\"").unwrap(), bulk(""));
        assert_eq!(parse("\"OK\"").unwrap(), bulk("OK"));
        assert_eq!(parse("\"1.5\"").unwrap(), bulk("1.5"));
        assert_eq!(parse("\"h\\u00e9 \\ud83d\\ude00\"").unwrap(), bulk("hé 😀"));
    }

    #[test]
    fn status() {
        assert_eq!(parse(r#"{"status": "OK"}"#).unwrap(), Value::Okay);
        assert_eq!(
            parse(r#"{"status": "PONG"}"#).unwrap(),
            Value::SimpleString("PONG".into())
        );
        assert_eq!(
            parse(r#"{"status": "string"}"#).unwrap(),
            Value::SimpleString("string".into())
        );
    }

    #[test]
    fn errors() {
        let wrongtype = "WRONGTYPE Operation against a key holding the wrong kind of value";
        let on_the_wire =
            |line: &str| redis::parse_redis_value(format!("-{line}\r\n").as_bytes()).unwrap();
        let value = parse(&format!(r#"{{"error": "{wrongtype}"}}"#)).unwrap();
        assert_eq!(value, on_the_wire(wrongtype));
        let error = value.extract_error().unwrap_err();
        assert_eq!(error.code(), Some("WRONGTYPE"));

        // In place in a pipeline's replies.
        assert_eq!(
            parse(r#"[{"status": "OK"}, {"error": "ERR syntax error"}, 1]"#).unwrap(),
            Value::Array(vec![
                Value::Okay,
                on_the_wire("ERR syntax error"),
                Value::Int(1)
            ])
        );
    }

    #[test]
    fn arrays() {
        assert_eq!(parse("[]").unwrap(), Value::Array(vec![]));
        assert_eq!(
            parse(r#"["0", ["k1", "k2"]]"#).unwrap(),
            Value::Array(vec![bulk("0"), Value::Array(vec![bulk("k1"), bulk("k2")])])
        );
        assert_eq!(
            parse(r#"["key", [["m", "1"]]]"#).unwrap(),
            Value::Array(vec![
                bulk("key"),
                Value::Array(vec![Value::Array(vec![bulk("m"), bulk("1")])])
            ])
        );
        assert_eq!(
            parse(r#"[1, null, "a", {"status": "OK"}]"#).unwrap(),
            Value::Array(vec![Value::Int(1), Value::Nil, bulk("a"), Value::Okay])
        );
    }

    #[test]
    fn malformed_replies_are_errors() {
        for bad in [
            "",
            "{",
            "[1,",
            "nul",
            "true",
            "false",
            "1.5",
            "1e400",
            "9223372036854775808",
            "{}",
            r#"{"status": 1}"#,
            r#"{"status": null}"#,
            r#"{"status": "OK", "extra": 1}"#,
            r#"{"other": "OK"}"#,
            r#"{"error": 1}"#,
            r#"{"error": "ERR x", "status": "OK"}"#,
            r#"[{"a": 1}]"#,
            r#"[[true]]"#,
            "1 2",
        ] {
            let err = parse(bad).expect_err(bad);
            assert_eq!(err.kind(), ErrorKind::Parse, "{bad}");
        }
    }

    #[test]
    fn pipelines() {
        assert_eq!(
            parse_pipeline(r#"[{"status": "OK"}, null, [1]]"#, 3).unwrap(),
            Pipeline::Replies(vec![
                Value::Okay,
                Value::Nil,
                Value::Array(vec![Value::Int(1)])
            ])
        );
        assert_eq!(parse_pipeline("[]", 0).unwrap(), Pipeline::Replies(vec![]));
        assert_eq!(
            parse_pipeline(
                r#"{"error": "EXECABORT x", "previous": [{"error": "ERR a", "index": 0}, {"index": 2, "error": "ERR b"}]}"#,
                3
            )
            .unwrap(),
            Pipeline::Discarded(Discarded {
                line: "EXECABORT x".to_owned(),
                refused: vec![(0, "ERR a".to_owned()), (2, "ERR b".to_owned())],
            })
        );
        for bad in [
            "[1]",
            "[1, 2, 3]",
            "1",
            "null",
            "not json",
            r#"[1, {"a": 1}]"#,
            r#"{"error": "EXECABORT x"}"#,
            r#"{"status": "OK"}"#,
            r#"{"error": "EXECABORT x", "previous": 1}"#,
            r#"{"error": 1, "previous": []}"#,
            r#"{"error": "EXECABORT x", "previous": [], "more": 1}"#,
            r#"{"error": "EXECABORT x", "previous": [1]}"#,
            r#"{"error": "EXECABORT x", "previous": [{"index": -1, "error": "ERR a"}]}"#,
            r#"{"error": "EXECABORT x", "previous": [{"index": 0}]}"#,
            r#"{"error": "EXECABORT x", "previous": [{"index": 0, "error": 1}]}"#,
            r#"{"error": "EXECABORT x", "previous": [{"index": 0, "error": "ERR a", "more": 1}]}"#,
        ] {
            let err = parse_pipeline(bad, 2).expect_err(bad);
            assert_eq!(err.kind(), ErrorKind::Parse, "{bad}");
        }
    }

    #[test]
    fn deep_nesting_is_an_error_not_a_crash() {
        let deep = "[".repeat(100_000) + &"]".repeat(100_000);
        assert_eq!(parse(&deep).unwrap_err().kind(), ErrorKind::Parse);
    }
}
