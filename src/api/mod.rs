mod box_context;

pub use box_context::BoxContext;

use std::sync::Arc;
use std::time::Instant;
use std::{collections::HashMap, str::FromStr};

use datafusion::prelude::*;

use async_trait::async_trait;
use serde::Serialize;
use serde_json::value::to_value;
use serde_json::Value;

use crate::extract::{DelimitedExtract, ParquetExtract};
use crate::transform::SQLTransform;
use crate::util::*;

#[async_trait]
pub trait PipelineStage: Send + Sync {
    fn to_value(&self) -> Value;

    async fn execute(
        &mut self,
        box_ctx: BoxContext,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<Arc<dyn DataFrame>>>;
}

#[derive(Serialize)]
pub struct Event {
    pub event: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub success: Option<bool>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<Value>,

    #[serde(rename = "configuration", skip_serializing_if = "Option::is_none")]
    pub box_ctx: Option<Value>,
}

pub fn parse_config(
    box_ctx: BoxContext,
    config: &str,
    allow_missing_placeholders: bool,
    allow_missing_parameters: bool,
) -> Result<Vec<Box<dyn PipelineStage>>> {
    // prepare params
    let mut params = box_ctx.environment_variables.clone();
    params.extend(
        box_ctx
            .commandline_arguments
            .clone()
            .unwrap_or(HashMap::new()),
    );

    // Parse the string of data into serde_json::Value.
    let v = serde_json::from_str(config)?;
    match v {
        Value::Array(stage) => stage
            .iter()
            .map(|v| match v {
                Value::Object(object) => match v["type"].as_str() {
                    Some("DelimitedExtract") => {
                        let json = variables::substitute_variables(Value::to_string(&to_value(object)?), &params, allow_missing_placeholders, allow_missing_parameters)?;
                        DelimitedExtract::try_new(json)
                        .map(|s| Box::new(s) as Box<dyn PipelineStage>)
                    }
                    Some("ParquetExtract") => {
                        let json = variables::substitute_variables(Value::to_string(&to_value(object)?), &params, allow_missing_placeholders, allow_missing_parameters)?;
                        ParquetExtract::try_new(json)
                        .map(|s| Box::new(s) as Box<dyn PipelineStage>)
                    }
                    Some("SQLTransform") => {
                        // don't try to replace variables within the sql value
                        let obj= object.iter().map(|(key, value)| {
                            if key != "sql" {
                                let value = variables::substitute_variables(Value::to_string(value), &params, allow_missing_placeholders, allow_missing_parameters)?;
                                let value = Value::from_str(&value)?;
                                Ok((key.clone(), value))
                            } else {
                                Ok((key.clone(), value.clone()))
                            }
                        })
                        .collect::<Result<serde_json::Map<_,_>>>()?;

                        let json = Value::to_string(&to_value(obj)?);
                        SQLTransform::try_new(json)
                        .map(|s| Box::new(s) as Box<dyn PipelineStage>)
                    },
                    Some(t) => Err(BoxError::new(format!("Expected field 'type' to be one of ['DelimitedExtract', 'ParquetExtract', 'SQLTransform']. Got '{}'.", t))),
                    None => Err(BoxError::new("Missing required field 'type'.".to_string())),
                },
                v =>Err(BoxError::new(format!("Expected object. Got '{:?}'.", v))),
            })

            .collect::<Result<Vec<_>>>(),
        _ => Err(BoxError::new(format!("Expected array. Got '{:?}'.", v))),
    }
}

pub async fn execute(
    box_ctx: BoxContext,
    execution_ctx: &mut ExecutionContext,
    stages: Vec<Box<dyn PipelineStage>>,
    show_entry_exit: bool,
) -> Result<Option<Arc<dyn DataFrame>>> {
    let mut result: Option<Arc<dyn DataFrame>> = None;
    let job_start = Instant::now();

    if show_entry_exit {
        println!(
            "{}",
            serde_json::to_string(&Event {
                event: "enter".to_string(),
                success: None,
                error: None,
                duration: None,
                stage: None,
                box_ctx: Some(serde_json::to_value(box_ctx.clone()).unwrap()),
            })
            .unwrap()
        );
    }

    for mut stage in stages {
        let stage_start = Instant::now();

        println!(
            "{}",
            serde_json::to_string(&Event {
                event: "enter".to_string(),
                success: None,
                error: None,
                duration: None,
                stage: Some(stage.to_value()),
                box_ctx: None,
            })
            .unwrap()
        );

        result = stage
            .execute(box_ctx.clone(), execution_ctx)
            .await
            .map_err(|err| {
                println!(
                    "{}",
                    serde_json::to_string(&Event {
                        event: "exit".to_string(),
                        duration: Some(job_start.elapsed().as_millis() as usize),
                        stage: Some(stage.to_value()),
                        box_ctx: None,
                        success: Some(false),
                        error: Some(err.to_string()),
                    })
                    .unwrap()
                );
                err
            })?;

        println!(
            "{}",
            serde_json::to_string(&Event {
                event: "exit".to_string(),
                success: None,
                error: None,
                duration: Some(stage_start.elapsed().as_millis() as usize),
                stage: Some(stage.to_value()),
                box_ctx: None,
            })
            .unwrap()
        );
    }

    if show_entry_exit {
        println!(
            "{}",
            serde_json::to_string(&Event {
                event: "exit".to_string(),
                duration: Some(job_start.elapsed().as_millis() as usize),
                stage: None,
                box_ctx: Some(serde_json::to_value(box_ctx.clone()).unwrap()),
                success: Some(true),
                error: None
            })
            .unwrap()
        );
    }

    Ok(result)
}
