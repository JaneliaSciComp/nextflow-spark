include {
    spark_master;
    spark_worker;
    wait_for_cluster;
    spark_start_app;
    terminate_spark;
    terminate_file_name;
} from './processes'

workflow spark_cluster {
    take:
    spark_cluster_inputs

    main:
    // prepare spark cluster params
    spark_cluster_inputs \
    | map {
        spark_conf = it[0]
        spark_work_dir = it[1]
        nworkers = it[2]
        worker_cores = it[3]

        delete_terminate_file(spark_work_dir)
        workers_list = create_workers_list(nworkers)

        [
            spark_conf,
            spark_work_dir,
            worker_cores,
            nworkers,
            workers_list
        ]
    } \
    | set { all_spark_cluster_inputs }
    // start master
    all_spark_cluster_inputs \
    | map {
        println "Prepare parameters for spark master from ${it}"
        spark_conf = it[0]
        spark_work_dir = it[1]
        [
            spark_conf,
            spark_work_dir
        ]
    } \
    | spark_master
    // start workers
    all_spark_cluster_inputs \
    | transpose \
    | map {
        println "Prepare parameters for spark worker from ${it}"
        spark_conf = it[0]
        spark_work_dir = it[1]
        worker_cores = it[2]
        worker_id = it[4]
        [
            worker_id,
            spark_conf,
            spark_work_dir,
            worker_cores
        ]
    } \
    | spark_worker
    // wait for cluster to start
    all_spark_cluster_inputs \
    | map {
        spark_work_dir = it[1]
        nworkers = it[3]
        [
            spark_work_dir,
            nworkers
        ]
    } \
    | wait_for_cluster \
    | set { spark_uri }

    emit:
    spark_uri

}

workflow run_spark_app {
    take:
    spark_app_inputs

    main:
    spark_app_inputs \
    | map {
        spark_conf = it[4]
        spark_work_dir = it[5]
        nworkers = it[6]
        worker_cores = it[7]
        [
            spark_conf,
            spark_work_dir,
            nworkers,
            worker_cores
        ]
    } \
    | spark_cluster \
    | set { spark_uri_var }

    spark_uri_var \
    | combine(spark_app_inputs) \
    | map {
        println "Prepare spark app inputs from ${it}"
        spark_uri = it[0]
        spark_app = it[1]
        spark_app_entrypoint = it[2]
        spark_app_args = it[3]
        spark_app_log = it[4]
        spark_conf = it[5]
        spark_work_dir = it[6]
        nworkers = it[7]
        worker_cores = it[8]
        executor_cores = it[9]
        memgb_per_core = it[10]
        driver_cores = it[11]
        driver_memory = it[12]
        driver_stack_size = it[13]
        driver_logconfig = it[14]
        driver_deploy_mode = it[15]

        [
            spark_uri,
            spark_app,
            spark_app_entrypoint,
            spark_app_args,
            spark_app_log,
            spark_conf,
            spark_work_dir,
            nworkers,
            executor_cores,
            memgb_per_core,
            driver_cores,
            driver_memory,
            driver_stack_size,
            driver_logconfig,
            driver_deploy_mode
        ]
    } \
    | run_spark_app_on_existing_cluster \
    | map { it[1] } \
    | terminate_spark \
    | set { done }

    emit:
    done
}

workflow run_spark_app_on_existing_cluster {
    take:
    spark_app_inputs

    main:
    spark_app_inputs \
    | map {
        println "Run spark app with inputs: ${it}"
        spark_uri = it[0]
        spark_app = it[1]
        spark_app_entrypoint = it[2]
        spark_app_args_param = it[3]
        spark_app_log = it[4]
        spark_conf = it[5]
        spark_work_dir = it[6]
        nworkers = it[7]
        executor_cores = it[8]
        memgb_per_core = it[9]
        driver_cores = it[10]
        driver_memory = it[11]
        driver_stack_size = it[12]
        driver_logconfig = it[13]
        driver_deploy_mode = it[14]

        spark_app_args = spark_app_args_param instanceof Closure
            ? spark_app_args_param.call()
            : spark_app_args_param

        [
            spark_uri,
            spark_conf,
            spark_work_dir,
            nworkers,
            executor_cores,
            memgb_per_core,
            driver_cores,
            driver_memory,
            driver_stack_size,
            driver_logconfig,
            driver_deploy_mode,
            spark_app,
            spark_app_entrypoint,
            spark_app_args,
            spark_app_log
        ]
    } \
    | spark_start_app \
    | set { done }

    emit:
    done
}

def delete_terminate_file(working_dir) {
    File terminate_file = new File(terminate_file_name(working_dir))
    terminate_file.delete()
    return working_dir
}

def create_workers_list(nworkers) {
    return 1..nworkers
}
