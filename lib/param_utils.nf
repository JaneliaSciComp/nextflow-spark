def default_spark_params() {
    [
        spark_container_repo : 'registry.int.janelia.org/janeliascicomp',
        spark_container_name : 'spark',
        spark_container_version : '3.0.1-hadoop3.2',
        spark_local_dir : '/tmp',
        workers : 3,
        app : 'local/app.jar',
        app_main : '',
        app_args : '',
        app_log : '',
        spark_conf : '',
        worker_cores : 1,
        gb_per_core : 15,
        driver_cores : 1,
        driver_memory : '1g',
        driver_stack_size :  '',
        driver_logconfig : '',
        driver_deploy_mode : '',
        executor_cores : 1,
        wait_for_spark_timeout_seconds : 600,
        sleep_between_timeout_checks_seconds : 5,
    ]
}