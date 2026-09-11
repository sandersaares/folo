use ohno::AppError;
use serde::{Deserialize, Serialize};

/// Carries cargo-mutants options used to run an empty shard's unmutated baseline.
///
/// Unknown fields belong to cargo-mutants, not this baseline protocol. Serde validates only
/// the fields we consume, without restricting unrelated configuration.
/// Ref: .github/workflows/implementation.md, "Immutable execution".
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default)]
struct MutationConfig {
    // Defaults match the pinned cargo-mutants options implementation:
    // https://github.com/sourcefrog/cargo-mutants/blob/v27.1.0/src/options.rs
    additional_cargo_args: Vec<String>,
    additional_cargo_test_args: Vec<String>,
    all_features: bool,
    cap_lints: bool,
    features: Vec<String>,
    no_default_features: bool,
    profile: Option<String>,
    test_tool: TestTool,
}

/// Selects the baseline command family accepted by pinned cargo-mutants.
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
enum TestTool {
    #[default]
    Cargo,
    Nextest,
}

pub(crate) fn decode(text: &str) -> Result<String, AppError> {
    let config: MutationConfig =
        toml::from_str(text).map_err(DecodeConfigurationError::caused_by)?;
    serde_json::to_string(&config)
        .map_err(EncodeConfigurationError::caused_by)
        .map_err(Into::into)
}

/// Identifies malformed TOML or a baseline option with an unsupported value.
#[ohno::error]
#[display("cannot decode mutation configuration")]
struct DecodeConfigurationError;

/// Identifies failure to encode a validated baseline configuration.
#[ohno::error]
#[display("cannot encode mutation configuration")]
struct EncodeConfigurationError;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn decoded(text: &str) -> Value {
        serde_json::from_str(&decode(text).unwrap()).unwrap()
    }

    #[test]
    fn absent_options_have_pinned_defaults_and_json_types() {
        assert_eq!(
            decoded(""),
            json!({
                "additional_cargo_args": [],
                "additional_cargo_test_args": [],
                "all_features": false,
                "cap_lints": false,
                "features": [],
                "no_default_features": false,
                "profile": null,
                "test_tool": "cargo",
            })
        );
    }

    #[test]
    fn decodes_multiline_arrays_escaped_strings_and_literal_strings() {
        let text = r#"
            additional_cargo_args = [
                "--locked", # Cargo options may have inline comments.
                "--config=build.rustflags=\"--cfg custom\"",
            ]
            additional_cargo_test_args = ['--tests', "line\nbreak", 'C:\workspace']
            features = [
                """feature\
                   _name""",
                "caf\u00e9",
            ]
            all_features = true
            cap_lints = true
            no_default_features = true
            profile = 'mutants'
            test_tool = 'nextest'
        "#;
        assert_eq!(
            decoded(text),
            json!({
                "additional_cargo_args": ["--locked", "--config=build.rustflags=\"--cfg custom\""],
                "additional_cargo_test_args": ["--tests", "line\nbreak", r"C:\workspace"],
                "features": ["feature_name", "café"],
                "all_features": true,
                "cap_lints": true,
                "no_default_features": true,
                "profile": "mutants",
                "test_tool": "nextest",
            })
        );
    }

    #[test]
    fn ignores_options_outside_baseline_scope() {
        assert_eq!(
            decoded(
                "
                exclude_re = ['ignored']
                timeout_multiplier = 2.0
                unrelated_date = 2026-09-08
                [unrelated]
                nested = { values = [1, true, 'anything'] }
                "
            ),
            decoded("")
        );
    }

    #[test]
    fn rejects_wrong_field_types_without_losing_parser_context() {
        for field in [
            "additional_cargo_args",
            "additional_cargo_test_args",
            "features",
        ] {
            for value in [
                "true",
                "1",
                "'not an array'",
                "[1]",
                "['valid', false]",
                "{}",
            ] {
                assert_invalid(&format!("{field} = {value}"));
            }
        }
        for field in ["all_features", "cap_lints", "no_default_features"] {
            for value in ["0", "1", "'true'", "[]", "{}"] {
                assert_invalid(&format!("{field} = {value}"));
            }
        }
        for field in ["profile", "test_tool"] {
            for value in ["1", "true", "[]", "{}", "2026-09-08"] {
                assert_invalid(&format!("{field} = {value}"));
            }
        }
    }

    #[test]
    fn accepts_only_supported_case_sensitive_test_tools() {
        for tool in ["cargo", "nextest"] {
            assert_eq!(
                decoded(&format!("test_tool = '{tool}'"))
                    .get("test_tool")
                    .unwrap(),
                tool
            );
        }
        for tool in ["", "Cargo", "Nextest", "other"] {
            assert_invalid(&format!("test_tool = '{tool}'"));
        }
    }

    #[test]
    fn rejects_invalid_toml_even_for_unrelated_fields() {
        for text in [
            "features = [",
            "all_features = true\nall_features = false",
            "unrelated = 'unterminated",
            "[",
            "profile = null",
        ] {
            assert_invalid(text);
        }
    }

    fn assert_invalid(text: &str) {
        let error = decode(text).unwrap_err();
        _ = error.find_source::<DecodeConfigurationError>().unwrap();
        _ = error.find_source::<toml::de::Error>().unwrap();
    }
}
