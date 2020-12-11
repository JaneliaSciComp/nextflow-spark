#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    spark_cluster;
    spark_submit_java;
} from './nextflow-lib/spark'

params.workers = 3
params.app_jar = 'local/app.jar'
params.app_main = ''
params.app_args = ''

spark_log_dir=file(params.spark_log_dir)
spark_workers = params.workers
spark_app_jar = file(params.app_jar)
spark_app_main = params.app_main
spark_app_args = params.app_args?.tokenize(',')


workflow {
    println ("!!!!! APP ARGS ${spark_app_args.size()}" + spark_app_args)
    res = spark_cluster(spark_log_dir, spark_workers)
    res \
    | map {[
        it,
        spark_log_dir,
        spark_app_jar, 
        spark_app_main, 
        spark_app_args]} \
    | spark_submit_java \
    | view
}
