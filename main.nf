#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    spark_master; 
    spark_worker;
} from './nextflow-lib/spark'

spark_master_log=file(params.spark_master_log)

workflow {
    Channel.from(spark_master_log) | 
    spark_master | 
    view
}
