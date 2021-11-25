use serde::{Deserialize, Serialize};
#[derive(Deserialize, Serialize, Clone)]
pub struct Statistics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_count: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_byte_size: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub partitions: Option<Partitions>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct Partitions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<usize>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<usize>,
}

impl Statistics {
    pub fn new(
        stat: datafusion::physical_plan::Statistics,
        partitions: Option<Partitions>,
    ) -> Option<Self> {
        stat.num_rows.map(|_| Statistics {
            row_count: stat.num_rows,
            total_byte_size: stat.total_byte_size,
            partitions,
        })
    }
}

impl Partitions {
    pub fn new(input: Option<usize>, output: Option<usize>) -> Self {
        Self { input, output }
    }
}
