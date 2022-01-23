#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate json;

mod api;
mod extract;
mod jupyter;
mod transform;
mod util;

use api::*;
use util::*;

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use datafusion::prelude::*;
use datafusion_objectstore_s3::object_store::aws::AmazonS3FileSystem;
use structopt::StructOpt;

use regex::Regex;

#[allow(unused_imports)]
use datafusion::arrow::util::pretty::print_batches;
#[allow(unused_imports)]
use util::create_html_table;

lazy_static! {
    static ref PARAMETER_RE: Regex = Regex::new("^([a-zA-Z0-9_-]+)=(.+)$").unwrap();
}

#[derive(Debug, PartialEq, StructOpt)]
enum Subcommands {
    // `external_subcommand` tells structopt to put
    // all the extra arguments into this Vec
    #[structopt(external_subcommand)]
    Other(Vec<String>),
}

#[derive(Debug, StructOpt)]
struct ExecuteOpt {
    #[structopt(short, long)]
    job_path: String,

    // `external_subcommand` tells structopt to put
    // all the extra arguments into this Vec
    #[structopt(subcommand)]
    arguments: Option<Subcommands>,
}

#[derive(Debug, StructOpt)]
struct NotebookOpt {
    #[structopt(short, long)]
    connection_file: String,
}

#[derive(Debug, StructOpt)]
struct InstallOpt {}

#[derive(Debug, StructOpt)]
#[structopt(name = "box", about = "arc.tripl.ai")]
enum Opt {
    Execute(ExecuteOpt),
    Notebook(NotebookOpt),
    Install(InstallOpt),
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    match Opt::from_args() {
        Opt::Execute(opt) => execute(opt).await,
        Opt::Notebook(opt) => notebook(opt).await.map(|_| ()),
        Opt::Install(opt) => install(opt).await.map(|_| ()),
    }
}

async fn execute(opt: ExecuteOpt) -> Result<()> {
    // read and validate command line arguments to hashmap
    let commandline_arguments = match opt.arguments {
        Some(Subcommands::Other(subcommands)) => subcommands
            .iter()
            .map(|subcommand| match PARAMETER_RE.captures(subcommand) {
                Some(captures) => Ok((captures[1].to_string(), captures[2].to_string())),
                None => Err(BoxError::new(format!(
                    "Invalid format for argument '{}'. Expected 'key=value' format.",
                    subcommand
                ))),
            })
            .collect::<Result<HashMap<String, String>>>()?,
        _ => HashMap::new(),
    };

    let path = fs::canonicalize(opt.job_path)?;
    let box_ctx = BoxContext::new(
        Some(path.into_os_string().into_string().unwrap()),
        Some(commandline_arguments),
    );

    let execution_config = ExecutionConfig::new().with_batch_size(32768);
    let mut execution_ctx = ExecutionContext::with_config(execution_config);
    execution_ctx.register_object_store(
        "s3",
        Arc::new(AmazonS3FileSystem::new(None, None, None, None, None, None).await),
    );

    let config = fs::read_to_string(Path::new(&box_ctx.clone().job_path.unwrap()))
        .map_err(BoxError::from)?;

    // convert hocon to json
    let config = variables::replace_hocon_parameters(config.as_str());

    let stages = api::parse_config(box_ctx.clone(), config.as_str(), true, false)?;

    if let Ok(result) = api::execute(box_ctx, &mut execution_ctx, stages, true).await {
        print_batches(result.unwrap().collect().await.unwrap().as_ref()).unwrap()
    }

    Ok(())
}

async fn notebook(opt: NotebookOpt) -> Result<()> {
    let connection_file = fs::read_to_string(Path::new(&opt.connection_file))?;
    let connection_file: jupyter::ConnectionFile = serde_json::from_str(connection_file.as_str())?;
    let server = jupyter::Server::start(&connection_file, false)?;
    server.wait_for_shutdown();
    println!("server.wait_for_shutdown()");
    Ok(())
}

async fn install(_: InstallOpt) -> Result<()> {
    jupyter::install()?;
    Ok(())
}
