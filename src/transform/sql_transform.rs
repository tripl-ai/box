use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{datasource::MemTable, datasource::TableProvider, prelude::*};
use serde::{Deserialize, Serialize};

use crate::api::*;
use crate::util::*;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SQLTransform {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    _type: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,

    sql: String,

    #[serde(rename = "outputView")]
    output_view: String,

    #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
    statistics: Option<Statistics>,

    #[serde(
        rename = "inputViews",
        skip_deserializing,
        skip_serializing_if = "Option::is_none"
    )]
    input_views: Option<Vec<String>>,

    #[serde(rename = "sqlParams", default = "default_sql_params")]
    sql_params: HashMap<String, String>,
}

fn default_sql_params() -> HashMap<String, String> {
    HashMap::new()
}

impl fmt::Display for SQLTransform {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

impl SQLTransform {
    pub fn try_new(json: String) -> Result<SQLTransform> {
        serde_json::from_str::<SQLTransform>(&json).map_err(BoxError::from)
    }
}

#[async_trait]
impl PipelineStage for SQLTransform {
    fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(&self).unwrap()
    }

    async fn execute(
        &mut self,
        _: BoxContext,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<Arc<dyn DataFrame>>> {
        let execution_config = ctx.state.lock().unwrap().config.clone();

        // substitute any variables
        self.sql =
            variables::substitute_variables(self.sql.to_owned(), &self.sql_params, false, false)?;

        // calculate the input views by traversing the plan
        let plan = ctx.create_logical_plan(&self.sql).map_err(BoxError::from)?;
        let mut visitor = lineage_visitor::LineageVisitor::new(false);
        plan.accept(&mut visitor).unwrap();
        self.input_views = Some(visitor.table_scan);

        // run the sql
        let df = ctx.sql(&self.sql).await.map_err(BoxError::from)?;

        let table_provider =
            MemTable::try_new(df.schema().clone().into(), df.collect_partitioned().await?)?;

        // record statistics
        let exec = table_provider
            .scan(&None, execution_config.batch_size, &[], None)
            .await?;
        let output_partitions = Some(exec.output_partitioning().partition_count());
        self.statistics = Statistics::new(
            exec.statistics(),
            Some(Partitions::new(None, output_partitions)),
        );

        ctx.register_table(self.output_view.as_str(), Arc::new(table_provider))?;

        ctx.table(self.output_view.as_str())
            .map(Some)
            .map_err(BoxError::from)
    }
}
