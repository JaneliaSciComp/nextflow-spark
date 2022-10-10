# Spark on Nextflow

This repo contains a reusable set of Nextflow subworkflows and processes which create transient Spark clusters on any infrastructure where Nextflow runs. The only requirement is a shared filesystem which is accessible by all of the nodes that will be running the Spark processes.




## Building the container

The container for spark-3.0.1 with hadoop-3.2 is already
available at registry.int.janelia.org but if you need to
push this to another registry or if you want to change it
you can rebuild it:
```
docker build -t registry.int.janelia.org/janeliascicomp/spark:3.0.1-hadoop3.2 -t multifish/spark:3.0.1-hadoop3.2 .
```

## Usage

You must have [Nextflow](https://www.nextflow.io) and [Singularity](https://sylabs.io) or [Docker](https://www.docker.com/) installed before running the pipeline .


### Running a spark application

To run a spark application you need to specify a working directory
that must be accessible to the nextflow app as well as to the master and all workers.
Also if the application requires to access files that are not
under the application startup directory, it must mount all directories it needs to access inside the singularity or docker container using `--runtime_opts` flag. If the spark cluster is set up on an LSF grid, you can specify additional submit options using `--lsf_opts` flag. Typically you don't have to specify spark configuration but if there is an option using `--spark_conf` to specify a spark configuration directory that contains your own spark config files (similar to [example](examples/spark-conf) spark conf folder).

Here's an example to run Color Depth Search application on an LSF with 4 workers, each worker having 4 slots. Each core has 15G of memory and the driver is started using client deployment mode using 1 core and 10G of memory. The spark application parameters are passed as a string with all parameters enclosed within double quotes.

```
./main.nf -profile lsf \
        --runtime_opts "-e -B $PWD/local -B $PWD/examples -B /nrs/jacs/jacsData/filestore" \
        --lsf_opts "-P scicompsoft -W 30" \
        --workers 3 \
        --worker_cores 4 \
        --gb_per_core 15 \
        --driver_cores 1 \
        --driver_memory 10g \
        --spark_work_dir "$PWD/local" \
        --app $PWD/local/app.jar \
        --app_main org.janelia.colormipsearch.cmd.SparkMainEntry \
        --app_args \
        "searchFromJSON, \
        -m $PWD/local/testData/mask-2.json, \
        -i $PWD/local/testData/sgal4Targets.json, \
        --mirrorMask, \
        --maskThreshold 100, \
        --dataThreshold 100, \
        --xyShift 2, \
        --pctPositivePixels 2.0, \
        --pixColorFluctuation 2, \
        -od $PWD/local/testData/cdsresults.test"
```

The [examples](examples) folder contains more examples of how to run the application on a local cluster using singularity or docker.
