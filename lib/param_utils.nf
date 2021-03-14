def default_spark_params() {
    params = [:]

    params.spark_container_repo = 'registry.int.janelia.org/janeliascicomp'
    params.spark_container_name = 'spark'
    params.spark_container_version = '3.0.1-hadoop3.2'
    params.spark_local_dir = '/tmp'
    params.workers = 3
    params.app = 'local/app.jar'
    params.app_main = ''
    params.app_args = ''
    params.app_log = ''
    params.spark_conf = ''
    params.worker_cores = 1
    params.gb_per_core = 15
    params.driver_cores = 1
    params.driver_memory = '1g'
    params.driver_stack_size =  ''
    params.driver_logconfig = ''
    params.driver_deploy_mode = ''
    params.executor_cores = params.worker_cores

    return params
}