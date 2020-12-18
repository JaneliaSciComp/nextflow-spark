#!/usr/bin/env nextflow

nextflow.enable.dsl=2

params.crepo = 'registry.int.janelia.org/janeliascicomp'
params.spark_version = '3.0.1-hadoop3.2'
params.workers = 3
params.app = 'local/app.jar'
params.app_main = ''
params.app_args = ''
params.spark_conf = ''
params.worker_cores = 1
params.gb_per_core = 15
params.driver_cores = 1
params.driver_memory = '1g'
params.driver_logconfig = ''
params.driver_deploy_mode = ''
params.executor_cores = params.worker_cores

include {
    run_spark_app;
    spark_cluster;
} from './nextflow-lib/spark' addParams(lsf_opts: params.lsf_opts, 
                                        crepo: params.crepo,
                                        spark_version: params.spark_version)

// spark app parameters
spark_app = file(params.app)
spark_app_main = params.app_main
spark_app_args = params.app_args

// spark config
spark_conf = params.spark_conf
spark_work_dir = file(params.spark_work_dir)
spark_workers = params.workers
spark_worker_cores = params.worker_cores
spark_executor_cores = params.executor_cores
gb_per_core = params.gb_per_core
driver_cores = params.driver_cores
driver_memory = params.driver_memory
driver_logconfig = params.driver_logconfig
driver_deploy_mode = params.driver_deploy_mode

workflow {
    run_spark_app(
        spark_app,
        spark_app_main,
        spark_app_args,
        spark_conf,
        spark_work_dir,
        spark_workers,
        spark_worker_cores,
        spark_executor_cores,
        gb_per_core,
        driver_cores,
        driver_memory,
        driver_logconfig,
        driver_deploy_mode
    ) \
    | view
}
