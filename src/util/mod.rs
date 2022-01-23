pub mod error;
pub mod lineage_visitor;
pub mod serde_helpers;
pub mod statistics;
pub mod variables;

pub use error::{BoxError, Result};
pub use statistics::Partitions;
pub use statistics::Statistics;

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::display::array_value_to_string;

/// Convert a series of record batches into an html table
#[allow(dead_code)]
pub fn create_html_table(
    results: Vec<RecordBatch>,
    max_rows: Option<usize>,
    table_class_names: &[&str],
) -> Result<String> {
    if results.is_empty() {
        Ok("".to_string())
    } else {
        let schema: Arc<Schema> = results[0].schema();

        let mut html = if table_class_names.is_empty() {
            "<table>".to_string()
        } else {
            format!("<table class=\"{}\">", table_class_names.join(" "))
        };

        // make the header
        html.push_str("<thead><tr>");
        for field in schema.fields() {
            html.push_str(format!("<th>{}</th>", field.name()).as_str());
        }
        html.push_str("</tr></thead>");

        // make the body
        html.push_str("<tbody>");

        let max_rows = max_rows.unwrap_or(usize::MAX);
        let mut num_rows = 0;
        for batch in results {
            if num_rows < max_rows {
                for row in 0..batch.num_rows() {
                    if num_rows < max_rows {
                        html.push_str("<tr>");
                        for col in 0..batch.num_columns() {
                            let column = batch.column(col);
                            html.push_str(
                                format!("<td>{}</td>", &array_value_to_string(column, row)?)
                                    .as_str(),
                            );
                        }
                        html.push_str("</tr>");
                        num_rows += 1;
                    }
                }
            }
        }
        html.push_str("</tbody>");
        html.push_str("</table>");

        Ok(html)
    }
}
