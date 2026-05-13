//! Internal helpers.

#![allow(unreachable_pub)]

/// UTF-8-boundary-safe truncation. Returns the longest prefix of `s` whose
/// byte length is `<= max`. Multi-byte codepoints crossing the boundary are
/// dropped, not split.
#[must_use]
#[allow(dead_code)]
pub fn truncate_utf8(s: &str, max: usize) -> &str {
    if max >= s.len() {
        s
    } else {
        // Find the longest valid char boundary <= max
        let mut end = max;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        &s[..end]
    }
}

/// True iff `e` is a Postgres database error with SQLSTATE class `23`
/// (integrity constraint violation). Mirrors `pg_work_queue`'s classification
/// for retry-vs-abort decisions in the worker wrapper.
#[allow(dead_code)]
pub fn is_pg_constraint_violation(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db) = e
        && let Some(code) = db.code()
    {
        code.starts_with("23")
    } else {
        false
    }
}

/// Convert a `serde_json::Value` into a header `Map`. Non-object inputs
/// (arrays, scalars, null) collapse to an empty map — the DB CHECK on
/// `outbox.events.headers` should prevent these reaching us, but defense
/// in depth.
#[allow(dead_code)]
pub fn parse_headers(
    v: serde_json::Value,
) -> serde_json::Map<String, serde_json::Value> {
    match v {
        serde_json::Value::Object(m) => m,
        _ => serde_json::Map::new(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn truncate_utf8_ascii() {
        assert_eq!(truncate_utf8("abcdef", 3), "abc");
    }

    #[test]
    fn truncate_utf8_multibyte_does_not_split_codepoint() {
        // "🦀" is 4 bytes in UTF-8.
        let s = "ab🦀cd";
        let t = truncate_utf8(s, 3);
        assert_eq!(t, "ab", "should drop the crab rather than slice it");
    }

    #[test]
    fn truncate_utf8_max_larger_than_len_returns_whole() {
        assert_eq!(truncate_utf8("abc", 100), "abc");
    }

    #[test]
    fn truncate_utf8_zero_max() {
        assert_eq!(truncate_utf8("abc", 0), "");
    }

    #[test]
    fn is_constraint_violation_types_compile() {
        // Behavioral test for SQLSTATE 23 is in tests/schema_invariants.rs;
        // here just exercise the type signature compile-time.
        fn _types_compile(e: &sqlx::Error) -> bool {
            is_pg_constraint_violation(e)
        }
        let _ = _types_compile as fn(&sqlx::Error) -> bool;
    }

    #[test]
    fn parse_headers_object_passthrough() {
        let v = serde_json::json!({"a": 1, "b": "x"});
        let m = parse_headers(v);
        assert_eq!(m.get("a"), Some(&serde_json::json!(1)));
    }

    #[test]
    fn parse_headers_non_object_returns_empty() {
        let v = serde_json::json!([1, 2, 3]);
        let m = parse_headers(v);
        assert!(m.is_empty());
    }
}
