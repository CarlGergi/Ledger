//! Decides how safely each tool's calls can be repeated.
//!
//! The class comes from the tool's MCP annotations, read with MCP's own defaults, which
//! assume the worst about an unannotated tool. Annotations are only claims made by the
//! tool server, so an override file can correct them, and it always wins.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, bail, ensure};
use rmcp::model::Tool;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::gateway::TOOL_SEPARATOR;

/// The input a tool declares to show that it takes an idempotency key.
const DEFAULT_KEY_PARAM: &str = "idempotency_key";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectClass {
    /// Changes nothing, so running it again is harmless.
    ReadOnly,
    /// Running it again with the same arguments has no further effect.
    Idempotent,
    /// Takes an idempotency key, and runs with the same key take effect once.
    Keyed,
    /// Nothing makes a second run safe, so it runs at most once.
    AtMostOnce,
}

/// Where a tool's class came from, recorded so that a decision can be explained later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectSource {
    /// The override file.
    Override,
    /// The tool's annotations and input schema, as listed by its server.
    Listing,
    /// The server did not list the tool, or could not be asked, so nothing is known.
    Unlisted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Effect {
    pub class: EffectClass,
    pub source: EffectSource,
    /// For a keyed tool, the input that carries the key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_param: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct Override {
    class: EffectClass,
    key_param: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct OverrideFile {
    #[serde(default)]
    tool: HashMap<String, Override>,
}

/// The override file, plus the rules for reading annotations.
#[derive(Debug, Clone, Default)]
pub struct EffectPolicy {
    overrides: HashMap<String, Override>,
}

impl EffectPolicy {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("in {}", path.display()))
    }

    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let file: OverrideFile = toml::from_str(text)?;
        for (name, entry) in &file.tool {
            ensure!(
                name.split_once(TOOL_SEPARATOR)
                    .is_some_and(|(upstream, tool)| !upstream.is_empty() && !tool.is_empty()),
                "{name:?} is not a tool name; they look like <server>{TOOL_SEPARATOR}<tool>"
            );
            match (&entry.key_param, entry.class) {
                (Some(param), EffectClass::Keyed) if param.is_empty() => {
                    bail!("{name}: key_param must not be empty")
                }
                (Some(_), class) if class != EffectClass::Keyed => {
                    bail!("{name}: key_param only applies to the keyed class")
                }
                _ => {}
            }
        }
        Ok(Self {
            overrides: file.tool,
        })
    }

    /// A misspelled server name would leave the tool it meant to correct uncorrected, so it
    /// is refused at startup. A misspelled tool name can only be noticed against a listing.
    pub fn check_upstreams(&self, upstreams: &[&str]) -> anyhow::Result<()> {
        for name in self.overrides.keys() {
            let (upstream, _) = name
                .split_once(TOOL_SEPARATOR)
                .expect("checked when parsed");
            ensure!(
                upstreams.contains(&upstream),
                "the override for {name} names no configured upstream"
            );
        }
        Ok(())
    }

    pub(crate) fn has_override(&self, name: &str) -> bool {
        self.overrides.contains_key(name)
    }

    /// `listed` is the tool as its server describes it, if its server does.
    pub(crate) fn effect_of(&self, name: &str, listed: Option<&Tool>) -> Effect {
        if let Some(entry) = self.overrides.get(name) {
            let key_param = (entry.class == EffectClass::Keyed).then(|| {
                entry
                    .key_param
                    .clone()
                    .unwrap_or_else(|| DEFAULT_KEY_PARAM.to_owned())
            });
            return Effect {
                class: entry.class,
                source: EffectSource::Override,
                key_param,
            };
        }
        match listed {
            Some(tool) => {
                let class = class_from_listing(tool);
                Effect {
                    class,
                    source: EffectSource::Listing,
                    key_param: (class == EffectClass::Keyed).then(|| DEFAULT_KEY_PARAM.to_owned()),
                }
            }
            None => Effect {
                class: EffectClass::AtMostOnce,
                source: EffectSource::Unlisted,
                key_param: None,
            },
        }
    }
}

fn class_from_listing(tool: &Tool) -> EffectClass {
    let annotations = tool.annotations.as_ref();
    // A tool that claims to be both is contradicting itself, so believe the unsafe half.
    let read_only = annotations
        .is_some_and(|a| a.read_only_hint == Some(true) && a.destructive_hint != Some(true));
    let idempotent = annotations.is_some_and(|a| a.idempotent_hint == Some(true));
    let takes_key = tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .is_some_and(|properties| properties.contains_key(DEFAULT_KEY_PARAM));

    if read_only {
        EffectClass::ReadOnly
    } else if idempotent {
        EffectClass::Idempotent
    } else if takes_key {
        EffectClass::Keyed
    } else {
        EffectClass::AtMostOnce
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rmcp::model::ToolAnnotations;
    use serde_json::json;

    use super::*;

    fn tool(properties: Value) -> Tool {
        let schema = json!({"type": "object", "properties": properties});
        Tool::new("t", "a tool", Arc::new(schema.as_object().unwrap().clone()))
    }

    fn annotated(
        read_only: Option<bool>,
        destructive: Option<bool>,
        idempotent: Option<bool>,
    ) -> Tool {
        let mut annotations = ToolAnnotations::new();
        annotations.read_only_hint = read_only;
        annotations.destructive_hint = destructive;
        annotations.idempotent_hint = idempotent;
        tool(json!({})).with_annotations(annotations)
    }

    fn class_of(tool: &Tool) -> EffectClass {
        EffectPolicy::default().effect_of("s__t", Some(tool)).class
    }

    fn with_key() -> Value {
        json!({DEFAULT_KEY_PARAM: {"type": "string"}})
    }

    #[test]
    fn an_unannotated_tool_gets_the_safest_class() {
        assert_eq!(class_of(&tool(json!({}))), EffectClass::AtMostOnce);
        assert_eq!(
            class_of(&annotated(None, None, None)),
            EffectClass::AtMostOnce
        );
    }

    #[test]
    fn hints_set_to_false_claim_nothing() {
        assert_eq!(
            class_of(&annotated(Some(false), Some(false), Some(false))),
            EffectClass::AtMostOnce
        );
    }

    #[test]
    fn annotations_decide_the_class() {
        assert_eq!(
            class_of(&annotated(Some(true), None, None)),
            EffectClass::ReadOnly
        );
        assert_eq!(
            class_of(&annotated(Some(false), Some(true), Some(true))),
            EffectClass::Idempotent
        );
    }

    #[test]
    fn a_read_only_tool_that_is_also_destructive_is_not_read_only() {
        assert_eq!(
            class_of(&annotated(Some(true), Some(true), None)),
            EffectClass::AtMostOnce
        );
    }

    #[test]
    fn declaring_a_key_input_makes_a_tool_keyed() {
        let effect = EffectPolicy::default().effect_of("s__t", Some(&tool(with_key())));
        assert_eq!(effect.class, EffectClass::Keyed);
        assert_eq!(effect.key_param.as_deref(), Some(DEFAULT_KEY_PARAM));
    }

    #[test]
    fn a_tool_safe_to_repeat_as_it_is_needs_no_key() {
        let hints = annotated(Some(true), None, None).annotations.unwrap();
        let read_only = tool(with_key()).with_annotations(hints);
        let effect = EffectPolicy::default().effect_of("s__t", Some(&read_only));
        assert_eq!(effect.class, EffectClass::ReadOnly);
        assert_eq!(effect.key_param, None);
    }

    #[test]
    fn a_tool_nothing_is_known_about_gets_the_safest_class() {
        assert_eq!(
            EffectPolicy::default().effect_of("s__t", None),
            Effect {
                class: EffectClass::AtMostOnce,
                source: EffectSource::Unlisted,
                key_param: None,
            }
        );
    }

    #[test]
    fn an_override_beats_a_wrong_annotation_in_either_direction() {
        let policy = EffectPolicy::parse(
            r#"
            [tool.pay__charge]
            class = "at_most_once"
            [tool.search__find]
            class = "read_only"
            "#,
        )
        .unwrap();

        let claims_read_only = annotated(Some(true), None, None);
        let charge = policy.effect_of("pay__charge", Some(&claims_read_only));
        assert_eq!(charge.class, EffectClass::AtMostOnce);
        assert_eq!(charge.source, EffectSource::Override);

        let find = policy.effect_of("search__find", Some(&tool(json!({}))));
        assert_eq!(find.class, EffectClass::ReadOnly);
        assert_eq!(find.source, EffectSource::Override);

        // Other tools are left to their annotations.
        let other = policy.effect_of("pay__status", Some(&claims_read_only));
        assert_eq!(other.class, EffectClass::ReadOnly);
        assert_eq!(other.source, EffectSource::Listing);
    }

    #[test]
    fn an_override_needs_no_listing() {
        let policy = EffectPolicy::parse("[tool.a__b]\nclass = \"idempotent\"").unwrap();
        assert_eq!(
            policy.effect_of("a__b", None).class,
            EffectClass::Idempotent
        );
    }

    #[test]
    fn a_keyed_override_can_name_its_input() {
        let policy = EffectPolicy::parse(
            r#"
            [tool.pay__charge]
            class = "keyed"
            key_param = "request_id"
            [tool.pay__refund]
            class = "keyed"
            "#,
        )
        .unwrap();
        assert_eq!(
            policy.effect_of("pay__charge", None).key_param.as_deref(),
            Some("request_id")
        );
        assert_eq!(
            policy.effect_of("pay__refund", None).key_param.as_deref(),
            Some(DEFAULT_KEY_PARAM)
        );
    }

    #[test]
    fn unusable_override_files_are_refused() {
        for text in [
            "[tool.a__b]\nclass = \"sometimes\"",
            "[tool.a__b]\nclass = \"idempotent\"\ntypo = 1",
            "[tool.a__b]",
            "[tools.a__b]\nclass = \"idempotent\"",
            "[tool.no_separator]\nclass = \"idempotent\"",
            "[tool.__b]\nclass = \"idempotent\"",
            "[tool.a__]\nclass = \"idempotent\"",
            "[tool.a__b]\nclass = \"idempotent\"\nkey_param = \"k\"",
            "[tool.a__b]\nclass = \"keyed\"\nkey_param = \"\"",
        ] {
            assert!(EffectPolicy::parse(text).is_err(), "should refuse: {text}");
        }
    }

    #[test]
    fn an_empty_override_file_is_fine() {
        assert!(EffectPolicy::parse("").unwrap().overrides.is_empty());
    }

    #[test]
    fn a_missing_file_names_the_path() {
        let error = EffectPolicy::load(Path::new("/nonexistent/effects.toml")).unwrap_err();
        assert!(format!("{error:#}").contains("/nonexistent/effects.toml"));
    }

    #[test]
    fn an_override_for_an_unknown_upstream_is_refused() {
        let policy = EffectPolicy::parse("[tool.pay__charge]\nclass = \"keyed\"").unwrap();
        assert!(policy.check_upstreams(&["pay", "mail"]).is_ok());
        let error = policy.check_upstreams(&["mail"]).unwrap_err();
        assert!(error.to_string().contains("pay__charge"), "{error}");
    }
}
