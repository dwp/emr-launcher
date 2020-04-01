# emr-launcher
Lambda based EMR Cluster Launcher

## What does it do?

Given a set of YAML files that specify an EMR cluster configuration, it will
call the EMR API to generate that cluster.

## How do I run it locally?

emr-launcher can easily be run from the command line:

```
pipenv install
EMR_LAUNCHER_CONFIG_DIR=<path to config files> ./emr_launcher.py
```

`EMR_LAUNCHER_CONFIG_DIR` must point to a directory containing YAML files defining the configuration of the desired EMR cluster. See below for details.

## How do I run it as a Lambda function?

The Lambda function needs 2 environment variables set:

`EMR_LAUNCHER_CONFIG_S3_BUCKET` - the bucket that contains your YAML configuration files
`EMR_LAUNCHER_CONFIG_S3_FOLDER` - the S3 folder location containing your YAML configuration files

## How do I write the configuration files

Configuration is via a series of YAML files. The easiest way to get started is
to look at the [examples](docs/examples/).

The only hard requirements are that you **must** have files called
`cluster.yaml` and `instances.yml` in the location specified by
`EMR_LAUNCHER_CONFIG_DIR`.

You can optionally also place a `steps.yaml` file in that same location.

Any other files will be ignored.

## Developing locally

The recommended way to hack on `emr-launcher` locally is within a virtual
environment via `pipenv`. This can easily be done by running the following
from this top-level directory:

```
make bootstrap
pipenv install
```

Remember to run `pipenv shell` to reactivate your virtual environment each time
you enter a new terminal session!
