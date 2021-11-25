# box

This is an _experimental_ repository to perform a proof of concept replacement of the [Apache Spark](https://spark.apache.org/) executor for [Arc](https://arc.tripl.ai) with [Apache DataFusion](https://arrow.apache.org/datafusion/).

This is a very simple proof-of-concept which, with community collaboration, could easily form the basis of much more efficient Arc execution. If you can see value in this approach and would like to get involved please raise an issue. If sufficient demand is reached we can set up a more formal discussion forum.

## How to run

### Clone the repository

This respository has a submodule with the [TPC-H](http://www.tpc.org/tpch/) data in it for easy execution demonstration. So when cloning add the recusive capability:

```bash
git clone --recurse-submodules https://github.com/tripl-ai/box.git
```

### Command Line

To execute a job via the command line execute the provided `./box.sh` file. You will need to have Rust installed (see [rustup](https://rustup.rs/)). This will execute `job.json` and is intended to show the basic functionality.

### Notebook

To execute the notebook functionality execute the provided `./notebook.sh` file. You will not need Docker installed (see [Docker](https://www.docker.com/)). The `box.ipynb` file is a demonstration and is intended to show the basic notebook functionality.

## Licenses

The notebook functionality relies on code copied and modified from the [evcxr](https://github.com/google/evcxr/tree/HEAD/evcxr_jupyter) crate.
