use datafusion::logical_plan::{LogicalPlan, PlanVisitor};
use std::fmt;

pub struct LineageVisitor {
    print: bool,

    /// Holds the table names
    pub table_scan: Vec<String>,
}

impl LineageVisitor {
    pub fn new(print: bool) -> Self {
        Self {
            print,
            table_scan: vec![],
        }
    }
}

impl PlanVisitor for LineageVisitor {
    type Error = fmt::Error;

    fn pre_visit(&mut self, plan: &LogicalPlan) -> std::result::Result<bool, fmt::Error> {
        if let LogicalPlan::TableScan { table_name, .. } = plan {
            let _ = &self.table_scan.push(table_name.to_owned());

            if self.print {
                println!("{:?}", table_name);
            }
        }
        Ok(true)
    }

    fn post_visit(&mut self, _plan: &LogicalPlan) -> std::result::Result<bool, fmt::Error> {
        Ok(true)
    }
}
