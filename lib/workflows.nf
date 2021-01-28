include {
    prepare_spark_work_dir;
    spark_master;
    spark_worker;
    wait_for_cluster;
    spark_start_app;
    terminate_spark;
    terminate_file_name;
} from './processes'

/**
 * Start a spark cluster
 */
workflow spark_cluster {
    take:
    spark_conf
    spark_work_dir
    spark_workers
    spark_worker_cores
    spark_app_terminate_name

    main:
    // prepare spark cluster params
    workers_list = Channel.fromList(create_workers_list(spark_workers))
    work_dir = prepare_spark_work_dir(spark_work_dir, spark_app_terminate_name)

    // start master
    spark_master(
        spark_conf,
        work_dir,
        spark_app_terminate_name
    )

    // cross product all workers with all work dirs and 
    // then push them to different channels
    // so that we can start all needed spark workers with the proper worker directory
    workers_with_work_dirs = workers_list.combine(work_dir)
        .multiMap {
            all_workers: it[0]
            all_worker_dirs: it[1]
        }

    // start workers
    spark_worker(
        workers_with_work_dirs.all_workers,
        spark_conf,
        workers_with_work_dirs.all_worker_dirs,
        spark_worker_cores,
        spark_app_terminate_name
    )

    // wait for cluster to start
    spark_cluster_res = wait_for_cluster(
        work_dir,
        spark_workers,
        spark_app_terminate_name
    )

    emit:
    spark_cluster_res
}

/**
 * Start a spark cluster then run the given app and when it's done terminate the cluster.
 */
workflow run_spark_app {
    take:
    spark_app
    spark_app_entrypoint
    spark_app_args
    spark_app_log
    spark_app_terminate_name
    spark_conf
    spark_work_dir
    spark_workers
    spark_worker_cores
    spark_executor_cores
    spark_gbmem_per_core
    spark_driver_cores
    spark_driver_memory
    spark_driver_stack_size
    spark_driver_logconfig
    spark_driver_deploy_mode

    main:
    // start the cluster
    spark_cluster_res = spark_cluster(
        spark_conf,
        spark_work_dir,
        spark_workers,
        spark_worker_cores,
        spark_app_terminate_name
    )
    // run the app on the cluster
    spark_uri = spark_cluster_res | map { it[0] }
    spark_app_dir = run_spark_app_on_existing_cluster(
        spark_uri,
        spark_app,
        spark_app_entrypoint,
        spark_app_args,
        spark_app_log,
        spark_app_terminate_name,
        spark_conf,
        spark_work_dir,
        spark_workers,
        spark_executor_cores,
        spark_gbmem_per_core,
        spark_driver_cores,
        spark_driver_memory,
        spark_driver_stack_size,
        spark_driver_logconfig,
        spark_driver_deploy_mode
    )
    // stop the cluster
    done = terminate_spark(spark_app_dir, spark_app_terminate_name)

    emit:
    done
}

/**
 * Run the given app on the given spark cluster identified by spark_uri
 */
workflow run_spark_app_on_existing_cluster {
    take:
    spark_uri
    spark_app
    spark_app_entrypoint
    spark_app_args
    spark_app_log
    spark_app_terminate_name
    spark_conf
    spark_work_dir
    spark_workers
    spark_executor_cores
    spark_gbmem_per_core
    spark_driver_cores
    spark_driver_memory
    spark_driver_stack_size
    spark_driver_logconfig
    spark_driver_deploy_mode

    main:
    done = spark_start_app(
        spark_uri,
        spark_conf,
        spark_work_dir,
        spark_workers,
        spark_executor_cores,
        spark_gbmem_per_core,
        spark_driver_cores,
        spark_driver_memory,
        spark_driver_stack_size,
        spark_driver_logconfig,
        spark_driver_deploy_mode,
        spark_app,
        spark_app_entrypoint,
        spark_app_args,
        spark_app_log
    )

    emit:
    done
}

def create_workers_list(nworkers) {
    println "Prepare $nworkers workers"
    return 1..nworkers
}
