#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    default_spark_params;
} from './lib/param_utils'

final_params = default_spark_params() + params

include {
    run_spark_app;
} from './lib/workflows' addParams(final_params)

// spark app parameters
spark_app = final_params.app
spark_app_main = final_params.app_main
spark_app_args = final_params.app_args
spark_app_log = final_params.app_log

// spark config
spark_conf = final_params.spark_conf
spark_work_dir = final_params.spark_work_dir
spark_workers = final_params.workers
spark_worker_cores = final_params.worker_cores
spark_executor_cores = final_params.executor_cores
gb_per_core = final_params.gb_per_core
driver_cores = final_params.driver_cores
driver_memory = final_params.driver_memory
driver_stack_size = final_params.driver_stack_size
driver_logconfig = final_params.driver_logconfig
driver_deploy_mode = final_params.driver_deploy_mode

workflow {
    run_spark_app(
        spark_app,
        spark_app_main,
        spark_app_args,
        spark_app_log,
        '',
        spark_conf,
        spark_work_dir,
        spark_workers,
        spark_worker_cores,
        spark_executor_cores,
        gb_per_core,
        driver_cores,
        driver_memory,
        driver_stack_size,
        driver_logconfig,
        driver_deploy_mode
    ) \
    | view
}
