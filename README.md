# emr-launcher
Lambda based EMR Cluster Launcher

## What does it do?

Given a set of YAML files that specify an EMR cluster configuration, it will
call the EMR API to generate that cluster.

It can also modify the cluster configuration based on the event by supplying `s3_overrides`, `overrides`,
`extend` or `additional_step_args` in the event body.

 * `s3_overrides`
    * The optional s3 location overrides the location for EMR to read its config files, made up of s3 id and s3 folder path to config files.
 * `overrides`
    * Additional cluster configuration to be merged with the existing configuration.
    * It must have the same structure as the yaml configuration.
    * A deep merge is performed, therefore nested objects are preserved. However,
     nested lists are not merged but replaced altogether.
* `extend`
    * Mapping of <string, array> where the keys correspond to a path in the configuration
    and values are items to be added to the list present at that path.
    * The path must have the form `Name1.Name2.Name3 ...` 
* `additional_step_args`
    * Mapping of <string, array> where keys are step names, and the values are
    arguments to be added to the step.


#### Event Body Example
```$json
{
    "s3_overrides": {
        "emr_launcher_config_s3_bucket": "new-s3-bucket-name",
        "emr_launcher_config_s3_folder": "new-s3-folder-path",
    },
    "overrides": {
        "Name": "new-cluster-name",
        "Instances": {
            Ec2SubnetId: "new-subnet-id"
        }
    },
    "extend": {
        "Instances.InstanceFleets": [
            {
                "InstanceFleetType": "CORE",
                "Name": "new-instance-fleet"
            }
        ]
    },
    "additional_step_args": {
        "step-name": ["--flag1", "value1", "--flag2", "value2"]
    }
}
```


## How do I run it locally?

emr-launcher can easily be run from the command line:

```
pipenv install
EMR_LAUNCHER_CONFIG_DIR=<path to config files> python -m emr_launcher
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


## Examples of emr-launcher deployments
#### Typical deployments
 * [dwp/aws-analytical-dataset-generation/emr-launcher.tf](https://github.com/dwp/aws-analytical-dataset-generation/blob/master/emr-launcher.tf)
 * [dwp/dataworks-aws-mongo-latest/emr-launcher.tf](https://github.com/dwp/dataworks-aws-mongo-latest/blob/master/emr-launcher.tf)

#### PDM - custom EMR Launcher implementation
* [dwp/aws-pdm-dataset-generation/emr-launcher.tf](https://github.com/dwp/aws-pdm-dataset-generation/blob/master/emr-launcher.tf)
* [dwp/dataworks-pdm-emr-launcher](https://github.com/dwp/dataworks-pdm-emr-launcher)

## Azkaban
Azkaban targets the emr-launcher when launching clusters.  There are two azkaban emr job types:
 * [EMRStep](https://github.com/dwp/dataworks-hardened-images/blob/master/azkaban-executor/azkaban-emr-jobtype/src/main/java/uk/gov/dwp/dataworks/azkaban/jobtype/EMRStep.java) - targets existing clusters, but will launch a new cluster if required using the NEW handler
 * [EmrLauncherJob](https://github.com/dwp/dataworks-hardened-images/blob/master/azkaban-executor/azkaban-emr-jobtype/src/main/java/uk/gov/dwp/dataworks/azkaban/jobtype/EmrLauncherJob.java) - starts a dedicated cluster each time the job runs, uses the OLD handler

