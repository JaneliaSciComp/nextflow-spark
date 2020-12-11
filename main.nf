#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    spark_cluster;
} from './nextflow-lib/spark'

params.workers = 5

spark_log_dir=file(params.spark_log_dir)
spark_workers = params.workers

workflow {
    spark_cluster(spark_log_dir, spark_workers)
}
