include {
    spark_master;
    spark_worker;
    wait_for_cluster;
    spark_start_app;
    terminate_spark;
    terminate_file_name;
} from './processes'

/**
 * All workflows take a channel of "named" arguments, i.e. map of arguments
 */

/**
 * spark_cluster_inputs is a map containing the following keys:
 * [
 *   spark_conf:,
 *   spark_work_dir:,
 *   spark_workers:
 *   spark_worker_cores:
 * ]
 */
workflow spark_cluster {
    take:
    spark_cluster_inputs

    main:
    // prepare spark cluster params
    all_spark_cluster_inputs = spark_cluster_inputs \
    | map {
        delete_terminate_file(it.spark_work_dir)
        it + [workers_list: create_workers_list(it.spark_workers)]
    }

    // start master
    all_spark_cluster_inputs \
    | map {
        println "Prepare parameters for spark master from ${it}"
        [
            it.spark_conf,
            it.spark_work_dir
        ]
    } \
    | spark_master

    // start workers
    all_spark_cluster_inputs \
    | map {
        println "Prepare parameters for ${it.workers_list.size()} spark workers from ${it}"
        [
            it.workers_list,
            it.spark_conf,
            it.spark_work_dir,
            it.spark_worker_cores,
        ]
    }
    | transpose \
    | spark_worker

    // wait for cluster to start
    spark_uri = all_spark_cluster_inputs \
    | map {
        [
            it.spark_work_dir,
            it.spark_workers
        ]
    } \
    | wait_for_cluster

    emit:
    spark_uri
}

/**
 * spark_app_inputs is a map containing the following keys:
 * [
 *   spark_uri:,
 *   spark_app:,
 *   spark_app_entrypoint:,
 *   spark_app_args:,
 *   spark_app_log:,
 *   spark_conf:,
 *   spark_work_dir:,
 *   spark_worker_cores:,
 *   spark_worker_cores:,
 *   spark_executor_cores:,
 *   memgb_per_core:,
 *   driver_cores:,
 *   driver_memory:,
 *   driver_stack_size:,
 *   driver_logconfig:,
 *   driver_deploy_mode:
 * ]
 */
workflow run_spark_app {
    take:
    spark_app_inputs

    main:
    spark_uri_var = spark_app_inputs | spark_cluster

    done = spark_uri_var \
    | combine(spark_app_inputs) \
    | map {
        uri_and_spark_inputs = [spark_uri: it[0]] + it[1]
        println "Spark app inputs ${uri_and_spark_inputs}"
        return uri_and_spark_inputs
    } \
    | run_spark_app_on_existing_cluster \
    | map {
        // only pass the working dir to terminate_spark process
        it[1]
    } \
    | terminate_spark

    emit:
    done
}

/**
 * spark_app_inputs is a map containing the following keys:
 * [
 *   spark_uri:,
 *   spark_app:,
 *   spark_app_entrypoint:,
 *   spark_app_args:,
 *   spark_app_log:,
 *   spark_conf:,
 *   spark_work_dir:,
 *   spark_executor_cores:,
 *   memgb_per_core:,
 *   driver_cores:,
 *   driver_memory:,
 *   driver_stack_size:,
 *   driver_logconfig:,
 *   driver_deploy_mode:
 * ]
 */
workflow run_spark_app_on_existing_cluster {
    take:
    spark_app_inputs

    main:
    done = spark_app_inputs \
    | map {
        println "Run spark app with inputs: ${it}"
        spark_app_args = it.spark_app_args instanceof Closure
            ? it.spark_app_args.call()
            : it.spark_app_args

        [
            it.spark_uri,
            it.spark_conf,
            it.spark_work_dir,
            it.spark_workers,
            it.spark_executor_cores,
            it.memgb_per_core,
            it.driver_cores,
            it.driver_memory,
            it.driver_stack_size,
            it.driver_logconfig,
            it.driver_deploy_mode,
            it.spark_app,
            it.spark_app_entrypoint,
            spark_app_args,
            it.spark_app_log
        ]
    } \
    | spark_start_app

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