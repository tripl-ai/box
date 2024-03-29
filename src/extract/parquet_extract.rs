use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    datasource::file_format::parquet::ParquetFormat, datasource::listing::*, datasource::MemTable,
    datasource::TableProvider, execution::context::ExecutionContext, prelude::*,
};
use serde::{Deserialize, Serialize};

use crate::api::*;
use crate::util::serde_helpers::default_false;
use crate::util::*;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ParquetExtract {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    _type: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,

    #[serde(rename(serialize = "inputURI", deserialize = "inputURI"))]
    input_uri: String,

    #[serde(rename = "outputView")]
    output_view: String,

    #[serde(default = "default_false")]
    persist: bool,

    #[serde(rename = "numPartitions", skip_serializing_if = "Option::is_none")]
    num_partitions: Option<usize>,

    #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
    statistics: Option<Statistics>,

    #[serde(skip_deserializing, skip_serializing_if = "Option::is_none")]
    partitions: Option<Partitions>,
}

impl fmt::Display for ParquetExtract {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap())
    }
}

impl ParquetExtract {
    pub fn try_new(json: String) -> Result<ParquetExtract> {
        serde_json::from_str::<ParquetExtract>(&json).map_err(BoxError::from)
    }
}

#[async_trait]
impl PipelineStage for ParquetExtract {
    fn to_value(&self) -> serde_json::Value {
        serde_json::to_value(&self).unwrap()
    }

    async fn execute(
        &mut self,
        _: BoxContext,
        ctx: &mut ExecutionContext,
    ) -> Result<Option<Arc<dyn DataFrame>>> {
        let execution_config = ctx.state.lock().unwrap().config.clone();

        let file_format = ParquetFormat::default().with_enable_pruning(true);

        let listing_options = ListingOptions {
            format: Arc::new(file_format),
            collect_stat: true,
            file_extension: ".parquet".to_owned(),
            target_partitions: num_cpus::get(),
            table_partition_cols: vec![],
        };

        let (object_store, _) = ctx.object_store(&self.input_uri)?;

        let resolved_schema = listing_options
            .infer_schema(object_store.clone(), &self.input_uri)
            .await
            .map_err(BoxError::from)?;

        let mut table_provider: Arc<dyn TableProvider + Send + Sync> = Arc::new(ListingTable::new(
            object_store,
            self.input_uri.clone(),
            resolved_schema,
            listing_options,
        ));

        // record statistics
        let exec = table_provider
            .scan(&None, execution_config.batch_size, &[], None)
            .await?;
        let input_partitions = Some(exec.output_partitioning().partition_count());
        self.statistics = Statistics::new(
            exec.statistics(),
            Some(Partitions::new(input_partitions, None)),
        );

        let output_partitions = if self.persist {
            table_provider = Arc::new(
                MemTable::load(
                    table_provider,
                    execution_config.batch_size,
                    self.num_partitions,
                )
                .await?,
            );
            let exec = table_provider
                .scan(&None, execution_config.batch_size, &[], None)
                .await?;
            Some(exec.output_partitioning().partition_count())
        } else {
            None
        };

        self.statistics = Statistics::new(
            exec.statistics(),
            Some(Partitions::new(
                input_partitions,
                output_partitions.or(input_partitions),
            )),
        );

        ctx.register_table(self.output_view.as_str(), table_provider)?;

        ctx.table(self.output_view.as_str())
            .map(Some)
            .map_err(BoxError::from)
    }
}
