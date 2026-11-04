//! Works out which session a call belongs to.

use http::request::Parts;
use rmcp::ErrorData;

/// Names a session explicitly, and lets it outlive any one connection or gateway copy.
pub const SESSION_HEADER: &str = "x-ledger-session";
/// The same as [`SESSION_HEADER`], for clients that can configure a URL but not headers.
pub const SESSION_QUERY_PARAM: &str = "session";
const TRANSPORT_SESSION_HEADER: &str = "mcp-session-id";

const MAX_SESSION_ID_LEN: usize = 128;

/// An explicit id wins. Without one, the MCP transport session stands in, so an agent
/// that knows nothing about Ledger still gets one log per connection.
pub fn resolve_session(parts: &Parts) -> Result<String, ErrorData> {
    let header = |name: &str| {
        parts
            .headers
            .get(name)
            .map(|value| value.to_str().unwrap_or_default())
    };
    let query = || {
        parts.uri.query()?.split('&').find_map(|pair| {
            let (key, value) = pair.split_once('=')?;
            (key == SESSION_QUERY_PARAM).then_some(value)
        })
    };

    let id = header(SESSION_HEADER)
        .or_else(query)
        .or_else(|| header(TRANSPORT_SESSION_HEADER))
        .ok_or_else(|| {
            ErrorData::invalid_request(
                format!(
                    "no session: send the {SESSION_HEADER} header or the \
                     ?{SESSION_QUERY_PARAM}= URL parameter"
                ),
                None,
            )
        })?;
    validate(id)?;
    Ok(id.to_owned())
}

fn validate(id: &str) -> Result<(), ErrorData> {
    let valid = !id.is_empty()
        && id.len() <= MAX_SESSION_ID_LEN
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'));
    if valid {
        Ok(())
    } else {
        Err(ErrorData::invalid_request(
            format!(
                "invalid session id: use 1 to {MAX_SESSION_ID_LEN} characters from \
                 A-Z a-z 0-9 - _ . :"
            ),
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(uri: &str, headers: &[(&str, &[u8])]) -> Parts {
        let mut request = http::Request::builder().uri(uri);
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        request.body(()).unwrap().into_parts().0
    }

    #[test]
    fn header_beats_query_beats_transport_session() {
        let all = parts(
            "/mcp?session=from-query",
            &[
                (SESSION_HEADER, b"from-header"),
                (TRANSPORT_SESSION_HEADER, b"from-transport"),
            ],
        );
        assert_eq!(resolve_session(&all).unwrap(), "from-header");

        let no_header = parts(
            "/mcp?a=1&session=from-query&b=2",
            &[(TRANSPORT_SESSION_HEADER, b"from-transport")],
        );
        assert_eq!(resolve_session(&no_header).unwrap(), "from-query");

        let transport_only = parts("/mcp", &[(TRANSPORT_SESSION_HEADER, b"from-transport")]);
        assert_eq!(resolve_session(&transport_only).unwrap(), "from-transport");
    }

    #[test]
    fn a_call_with_no_session_is_refused() {
        let err = resolve_session(&parts("/mcp?other=1", &[])).unwrap_err();
        assert!(err.message.contains(SESSION_HEADER));
    }

    #[test]
    fn malformed_ids_are_refused() {
        let too_long = "a".repeat(MAX_SESSION_ID_LEN + 1);
        for bad in [
            "",
            "has space",
            "semi;colon",
            "slash/slash",
            too_long.as_str(),
        ] {
            let request = parts("/mcp", &[(SESSION_HEADER, bad.as_bytes())]);
            assert!(resolve_session(&request).is_err(), "{bad:?}");
        }
        let query = parts("/mcp?session=a%20b", &[]);
        assert!(resolve_session(&query).is_err());

        let not_ascii = parts("/mcp", &[(SESSION_HEADER, b"caf\xc3\xa9")]);
        assert!(resolve_session(&not_ascii).is_err());
    }

    #[test]
    fn the_longest_allowed_id_is_accepted() {
        let longest = "a".repeat(MAX_SESSION_ID_LEN);
        let request = parts("/mcp", &[(SESSION_HEADER, longest.as_bytes())]);
        assert_eq!(resolve_session(&request).unwrap(), longest);
    }
}
