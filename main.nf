#!/usr/bin/env nextflow

nextflow.enable.dsl=2

include {
    default_spark_params;
} from './lib/param_utils'

final_params = params + default_spark_params()

include {
    run_spark_app;
} from './lib/workflows' addParams(lsf_opts: final_params.lsf_opts, 
                                   crepo: final_params.crepo,
                                   spark_version: final_params.spark_version)

// spark app parameters
spark_app = file(final_params.app)
spark_app_main = final_params.app_main
spark_app_args = final_params.app_args
spark_app_log = final_params.app_log

// spark config
spark_conf = final_params.spark_conf
spark_work_dir = file(final_params.spark_work_dir)
spark_workers = final_params.workers
spark_worker_cores = final_params.worker_cores
spark_executor_cores = final_params.executor_cores
gb_per_core = final_params.gb_per_core
driver_cores = final_params.driver_cores
driver_memory = final_params.driver_memory
driver_stack_size = final_params.driver_stack_size
driver_logconfig = final_params.driver_logconfig
driver_deploy_mode = final_params.driver_deploy_mode

if( !spark_work_dir.exists() ) {
    spark_work_dir.mkdirs()
}

workflow {
    run_spark_app(
        Channel.of([
            spark_app: spark_app,
            spark_app_entrypoint: spark_app_main,
            spark_app_args: spark_app_args,
            spark_app_log: spark_app_log,
            spark_app_terminate_name: '',
            spark_conf: spark_conf,
            spark_work_dir: spark_work_dir,
            spark_workers: spark_workers,
            spark_worker_cores: spark_worker_cores,
            spark_executor_cores: spark_executor_cores,
            spark_gbmem_per_core: gb_per_core,
            spark_driver_cores: driver_cores,
            spark_driver_memory: driver_memory,
            spark_driver_stack_size: driver_stack_size,
            spark_driver_logconfig: driver_logconfig,
            spark_driver_deploy_mode: driver_deploy_mode
        ])
    ) \
    | view
}
