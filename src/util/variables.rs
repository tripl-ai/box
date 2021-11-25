use std::collections::HashMap;

use crate::util::*;

use regex::Regex;

pub fn substitute_variables(
    input: String,
    params: &HashMap<String, String>,
    allow_missing_placeholders: bool,
    allow_missing_parameters: bool,
) -> Result<String> {
    lazy_static! {
        // this will find any remaining parameters which have not been replaced
        static ref RE: Regex = Regex::new("[$][{](\\w*)(?:=[^}]+)?[}]").unwrap();
    }

    // iterate over the parameters, create a new regex for each and use it to replace any matches
    let output = params.iter().try_fold(input, |sql, (key, val)| {
        let re = Regex::new(
            vec![
                "[$][{](".to_string(),
                key.to_owned(),
                ")(?:=[^}]+)?[}]".to_string(),
            ]
            .join("")
            .as_str(),
        )?;

        if !allow_missing_placeholders && !re.is_match(sql.as_str()) {
            Err(BoxError::new(format!(
                "No placeholder found for parameter: '{}'.",
                key
            )))
        } else {
            Ok(re.replace_all(sql.as_str(), val.as_str()).to_string())
        }
    })?;

    if !allow_missing_parameters && RE.is_match(output.as_str()) {
        let mut placeholders = RE
            .find_iter(output.as_str())
            .map(|m| m.as_str())
            .collect::<Vec<&str>>();
        placeholders.dedup();
        placeholders.sort_unstable();
        Err(BoxError::new(format!(
            "No parameter value found for placeholders: [{}].",
            placeholders.join(", ")
        )))
    } else {
        Ok(output)
    }
}

#[allow(dead_code)]
pub fn replace_hocon_parameters(input: &str) -> String {
    lazy_static! {
        // find '/abc"${VARIABLE}"/def'
        static ref BOTH_VAR_RE: Regex = Regex::new("\"\\s*\\$\\{(\\w*)(?:=[^}]+)?}\\s*\"").unwrap();

        // find '/abc"${VARIABLE}'
        static ref LEFT_VAR_RE: Regex = Regex::new("\"\\s*\\$\\{(\\w*)(?:=[^}]+)?}").unwrap();

        // find '${VARIABLE}"/def'
        static ref RIGHT_VAR_RE: Regex = Regex::new("\\$\\{(\\w*)(?:=[^}]+)?}\\s*\"([^}]+)").unwrap();
    }

    let output = BOTH_VAR_RE.replace_all(input, "$${${1}}");
    let output = LEFT_VAR_RE.replace_all(&output, "$${${1}}\"");
    let output = RIGHT_VAR_RE.replace_all(&output, "\"$${${1}}${2}");

    output.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::error::Result;
    use serde_json::Value;

    struct TestCase<'a> {
        input: &'a str,
        output: &'a str,
    }

    #[test]
    fn test_replace_parameters() -> Result<()> {
        let cases: Vec<TestCase> = vec![
            TestCase {
                input: r#"{"inputURI": "s3://"${VARIABLE_ONE}"/green_tripdata_2013-08.csv*"}"#,
                output: r#"{"inputURI": "s3://${VARIABLE_ONE}/green_tripdata_2013-08.csv*"}"#,
            },
            TestCase {
                input: r#"{"inputURI": ${VARIABLE_ONE}"/green_tripdata_2013-08.csv*"}"#,
                output: r#"{"inputURI": "${VARIABLE_ONE}/green_tripdata_2013-08.csv*"}"#,
            },
            TestCase {
                input: r#"{"inputURI": "s3://"${VARIABLE_ONE}}"#,
                output: r#"{"inputURI": "s3://${VARIABLE_ONE}"}"#,
            },
            TestCase {
                input: r#"{"inputURI": "s3://"${VARIABLE_ONE}"/"${VARIABLE_TWO}}"#,
                output: r#"{"inputURI": "s3://${VARIABLE_ONE}/${VARIABLE_TWO}"}"#,
            },
        ];

        cases.iter().for_each(|test| {
            // also ensure the output case is valid
            let _: Value = serde_json::from_str(test.output).unwrap();

            // assert match
            let config = replace_hocon_parameters(test.input);
            assert_eq!(&config, test.output);
        });

        Ok(())
    }
}
