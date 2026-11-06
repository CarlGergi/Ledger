use std::collections::HashSet;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Used when the `DATABASE_URL` environment variable is not set.
    pub database_url: Option<String>,
    /// How long a call waits for its tool, or for a connection to the database.
    #[serde(default = "default_call_timeout_secs")]
    pub call_timeout_secs: u64,
    /// How long a tool server has to accept a new connection. Separate because listing
    /// tools waits for every server, so one dead server should not cost a whole call's limit.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,
    /// Extra `Host` values to accept. Empty means loopback only.
    #[serde(default)]
    pub allowed_hosts: Vec<String>,
    #[serde(rename = "upstream", default)]
    pub upstreams: Vec<UpstreamConfig>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamConfig {
    pub name: String,
    pub url: String,
}

fn default_listen() -> String {
    "127.0.0.1:7100".to_owned()
}

fn default_call_timeout_secs() -> u64 {
    60
}

fn default_connect_timeout_secs() -> u64 {
    5
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("in {}", path.display()))
    }

    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let config: Self = toml::from_str(text)?;
        config.validate()?;
        Ok(config)
    }

    pub fn call_timeout(&self) -> Duration {
        Duration::from_secs(self.call_timeout_secs)
    }

    pub fn connect_timeout(&self) -> Duration {
        Duration::from_secs(self.connect_timeout_secs)
    }

    fn validate(&self) -> anyhow::Result<()> {
        ensure!(
            self.call_timeout_secs > 0,
            "call_timeout_secs must be positive"
        );
        ensure!(
            self.connect_timeout_secs > 0,
            "connect_timeout_secs must be positive"
        );
        ensure!(
            !self.upstreams.is_empty(),
            "at least one [[upstream]] is required"
        );
        let mut names = HashSet::new();
        for upstream in &self.upstreams {
            let name = upstream.name.as_str();
            let valid_chars = name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'));
            // A trailing `_` would run into the separator and route to the wrong place.
            if name.is_empty() || !valid_chars || name.contains("__") || name.ends_with('_') {
                bail!(
                    "invalid upstream name {name:?}: use letters, digits, - and _, \
                     without \"__\" or a trailing \"_\""
                );
            }
            ensure!(names.insert(name), "duplicate upstream name {name:?}");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const UPSTREAMS: &str = r#"
        [[upstream]]
        name = "email"
        url = "http://127.0.0.1:7101/mcp"
    "#;

    #[test]
    fn defaults_apply() {
        let config = Config::parse(UPSTREAMS).unwrap();
        assert_eq!(config.listen, "127.0.0.1:7100");
        assert_eq!(config.call_timeout(), Duration::from_secs(60));
        assert_eq!(config.connect_timeout(), Duration::from_secs(5));
        assert_eq!(config.database_url, None);
        assert!(config.allowed_hosts.is_empty());
        assert_eq!(config.upstreams[0].name, "email");
    }

    #[test]
    fn every_field_can_be_set() {
        let text = format!(
            r#"
            listen = "0.0.0.0:9000"
            database_url = "postgres://db/ledger"
            call_timeout_secs = 5
            connect_timeout_secs = 7
            allowed_hosts = ["ledger.example.com"]
            {UPSTREAMS}
            "#
        );
        let config = Config::parse(&text).unwrap();
        assert_eq!(config.listen, "0.0.0.0:9000");
        assert_eq!(config.database_url.as_deref(), Some("postgres://db/ledger"));
        assert_eq!(config.call_timeout(), Duration::from_secs(5));
        assert_eq!(config.connect_timeout(), Duration::from_secs(7));
        assert_eq!(config.allowed_hosts, ["ledger.example.com"]);
    }

    #[test]
    fn unusable_configs_are_refused() {
        let upstream = |name: &str| format!("[[upstream]]\nname = {name:?}\nurl = \"http://x\"\n");
        let twice = upstream("a") + &upstream("a");
        let zero_timeout = format!("call_timeout_secs = 0\n{UPSTREAMS}");
        let zero_connect = format!("connect_timeout_secs = 0\n{UPSTREAMS}");
        let unknown_field = format!("listn = \"oops\"\n{UPSTREAMS}");
        for text in [
            String::new(),
            twice,
            zero_timeout,
            zero_connect,
            unknown_field,
            upstream(""),
            upstream("a__b"),
            upstream("trailing_"),
            upstream("has space"),
        ] {
            assert!(Config::parse(&text).is_err(), "should refuse: {text}");
        }
    }

    #[test]
    fn a_missing_file_names_the_path() {
        let err = Config::load(Path::new("/nonexistent/ledger.toml")).unwrap_err();
        assert!(format!("{err:#}").contains("/nonexistent/ledger.toml"));
    }
}
