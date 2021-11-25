use std::collections::HashMap;

use serde::Serialize;
use std::env;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Serialize, Clone)]
pub struct BoxContext {
    #[serde(rename = "jobPath")]
    pub job_path: Option<String>,

    version: String,

    #[serde(rename = "commandLineArguments")]
    pub commandline_arguments: Option<HashMap<String, String>>,

    #[serde(rename = "environmentVariables", skip_serializing)]
    pub environment_variables: HashMap<String, String>,
}

impl BoxContext {
    pub fn new(
        job_path: Option<String>,
        commandline_arguments: Option<HashMap<String, String>>,
    ) -> Self {
        let mut environment_variables = HashMap::new();
        for (key, val) in env::vars_os() {
            // Use pattern bindings instead of testing .is_some() followed by .unwrap()
            if let (Ok(k), Ok(v)) = (key.into_string(), val.into_string()) {
                environment_variables.insert(k, v);
            }
        }

        Self {
            job_path,
            version: VERSION.to_owned(),
            commandline_arguments,
            environment_variables,
        }
    }
}
