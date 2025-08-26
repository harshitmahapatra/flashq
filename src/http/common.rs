//! Common utilities shared between HTTP client and server

use std::collections::HashMap;

/// Utility function to parse header strings into HashMap
pub fn parse_headers(header_strings: Option<Vec<String>>) -> Option<HashMap<String, String>> {
    header_strings.map(|headers| {
        headers
            .iter()
            .filter_map(|h| {
                let mut split = h.splitn(2, '=');
                match (split.next(), split.next()) {
                    (Some(key), Some(value)) => Some((key.to_string(), value.to_string())),
                    _ => {
                        eprintln!("Warning: Invalid header format '{h}', expected KEY=VALUE");
                        None
                    }
                }
            })
            .collect()
    })
}