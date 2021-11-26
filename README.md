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

To execute a job via the command line you can use the the provided `./box.sh` file which will execute `job.json` and is intended to show the basic functionality. 

You will need to have Rust installed (see [rustup](https://rustup.rs/)) and then add the `nightly` channel: 

```bash
rustup toolchain install nightly
```

after the initial Rust install. The Rust `nightly` version is currently required for the `simd` support. Some packages may need to be install to compile such as `cmake` but if you check the build output it should indicate any missing packages.

Please note that if running on WSL or Windows you may need to convert the line endings to Unix format (LF) in order to run the script. When checking out the code they may be automatically changed to Windows line endings (`CRLF`) depending on your config. If you would like to git to not convert `CRLF` line endings then you can set core.autocrlf to false:

```bash
 git config --global core.autocrlf false
```

See [Customizing Git](https://git-scm.com/book/en/v2/Customizing-Git-Git-Configuration) for more information.

### Notebook

To execute the notebook functionality execute the provided `./notebook.sh` file. The `box.ipynb` file is a demonstration and is intended to show the basic notebook functionality.  You will need Docker installed (see [Docker](https://www.docker.com/)).


## Licenses

The notebook functionality relies on code copied and modified from the [evcxr](https://github.com/google/evcxr/tree/HEAD/evcxr_jupyter) crate.
