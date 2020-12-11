#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    spark_master; 
    spark_worker;
} from './nextflow-lib/spark'

spark_log_dir=file(params.spark_log_dir)

workflow {
    Channel.from(spark_log_dir) | 
    (spark_master & spark_worker) | 
    mix |
    view 
}
