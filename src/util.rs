//! Internal helpers.

#![allow(unreachable_pub)]

/// Replace Unicode control characters (incl. NUL, ESC, CR, LF, BEL, DEL and
/// the C1 range) with `'?'`. Two reasons:
///
/// 1. Postgres `TEXT` columns reject NUL (`U+0000`) with SQLSTATE `22021`.
///    If a handler returns `HandlerError::retry("..\0..")`, the `mark_*_fenced`
///    UPDATE would fail, surface as `JobError::retry`, and loop until
///    `max_attempts` for purely cosmetic reasons.
/// 2. ANSI escape sequences in operator logs / `psql` are a log-injection
///    vector; sanitizing here defends every downstream consumer of
///    `handler_deliveries.last_error` and `pgwq.jobs.last_error`.
///
/// We replace rather than strip so byte-length budget downstream
/// ([`truncate_utf8`]) is preserved 1:1.
#[must_use]
#[allow(dead_code)]
pub fn sanitize_reason(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .collect()
}

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
/// (integrity constraint violation). Narrow predicate used for "did THIS
/// constraint violation fire" reasoning in dispatch idempotency paths.
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

/// True iff `e` represents a deterministic, no-point-retrying failure.
///
/// Covers four SQLSTATE classes — class `22` (data exception), `23`
/// (integrity constraint), `28` (invalid authorization), `42` (syntax / access
/// rule) — plus a small set of `sqlx::Error` non-database variants
/// (`Protocol`, `TypeNotFound`, `ColumnDecode`, `Encode`, `Decode`,
/// `Configuration`) that cannot improve under retry.
///
/// Excludes classes `08` (connection exception), `40` (transaction rollback /
/// serialization failure), `53` (insufficient resources), `57` (operator
/// intervention), and pool/IO/TLS variants — all retriable.
#[allow(dead_code)]
pub fn is_sqlx_deterministic(e: &sqlx::Error) -> bool {
    if let sqlx::Error::Database(db) = e {
        if let Some(code) = db.code() {
            let c = code.as_ref();
            return c.starts_with("22")
                || c.starts_with("23")
                || c.starts_with("28")
                || c.starts_with("42");
        }
        return false;
    }
    matches!(
        e,
        sqlx::Error::Protocol(_)
            | sqlx::Error::TypeNotFound { .. }
            | sqlx::Error::ColumnDecode { .. }
            | sqlx::Error::Encode(_)
            | sqlx::Error::Decode(_)
            | sqlx::Error::Configuration(_)
    )
}

/// Format a [`sqlx::Error`] for inclusion in `pg_work_queue::JobError` messages
/// (which land in `pgwq.jobs.last_error` and operator logs). Strips Postgres
/// DETAIL lines so user-supplied PII (e.g. values of unique-constraint columns
/// like `idempotency_key`, `tenant_id`) cannot leak via fenced/transient error
/// paths. Pairs with [`is_pg_constraint_violation`] — same SQLSTATE classification.
#[allow(dead_code)]
pub fn redact_db_error(e: &sqlx::Error) -> String {
    match e {
        sqlx::Error::Database(db) => {
            let code = db.code();
            let code = code.as_deref().unwrap_or("unknown");
            format!("db_error code={code}")
        }
        sqlx::Error::Io(_) => "db_error kind=io".into(),
        sqlx::Error::PoolTimedOut => "db_error kind=pool_timed_out".into(),
        sqlx::Error::PoolClosed => "db_error kind=pool_closed".into(),
        sqlx::Error::RowNotFound => "db_error kind=row_not_found".into(),
        sqlx::Error::Tls(_) => "db_error kind=tls".into(),
        sqlx::Error::Configuration(_) => "db_error kind=configuration".into(),
        _ => "db_error kind=other".into(),
    }
}

/// Best-effort string from a `Box<dyn Any + Send>` panic payload returned by
/// `FutureExt::catch_unwind`. Mirrors what `std::panic::PanicHookInfo::payload`
/// would surface for typical `panic!(...)` invocations.
#[allow(dead_code)]
pub fn panic_payload_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        return (*s).to_owned();
    }
    if let Some(s) = payload.downcast_ref::<String>() {
        return s.clone();
    }
    "panic with non-string payload".to_owned()
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
    fn sanitize_reason_strips_nul() {
        let out = sanitize_reason("err\0msg");
        assert_eq!(out, "err?msg");
        assert!(!out.contains('\0'));
    }

    #[test]
    fn sanitize_reason_strips_ansi_escape() {
        let out = sanitize_reason("\x1b[31mred\x1b[0m");
        assert_eq!(out, "?[31mred?[0m");
        assert!(!out.contains('\x1b'));
    }

    #[test]
    fn sanitize_reason_strips_cr_lf_bel_del() {
        assert_eq!(sanitize_reason("a\rb\nc\x07d\x7fe"), "a?b?c?d?e");
    }

    #[test]
    fn sanitize_reason_passes_through_printable_utf8() {
        assert_eq!(sanitize_reason("łódź 🦀 OK"), "łódź 🦀 OK");
    }

    #[test]
    fn sanitize_reason_preserves_byte_count_per_char() {
        // Replacing 1-byte controls with '?' (also 1 byte) preserves
        // downstream truncate_utf8 budget calculations.
        let len_in: usize = "\0\x07\x1b".len();
        let out = sanitize_reason("\0\x07\x1b");
        assert_eq!(out.len(), len_in);
    }

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

    #[test]
    fn redact_db_error_pool_closed() {
        let msg = redact_db_error(&sqlx::Error::PoolClosed);
        assert_eq!(msg, "db_error kind=pool_closed");
    }

    #[test]
    fn redact_db_error_pool_timed_out() {
        let msg = redact_db_error(&sqlx::Error::PoolTimedOut);
        assert_eq!(msg, "db_error kind=pool_timed_out");
    }

    #[test]
    fn redact_db_error_row_not_found() {
        let msg = redact_db_error(&sqlx::Error::RowNotFound);
        assert_eq!(msg, "db_error kind=row_not_found");
    }

    #[test]
    fn redact_db_error_starts_with_prefix() {
        // Operators rely on the "db_error " prefix to distinguish redacted-DB-errors
        // from handler-emitted reasons in pgwq.jobs.last_error.
        for e in [
            sqlx::Error::PoolClosed,
            sqlx::Error::PoolTimedOut,
            sqlx::Error::RowNotFound,
        ] {
            assert!(redact_db_error(&e).starts_with("db_error "));
        }
    }
}
