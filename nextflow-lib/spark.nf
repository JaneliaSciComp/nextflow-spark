workflow run_spark_app {
    take:
    spark_app
    spark_app_entrypoint
    spark_app_args
    spark_conf
    spark_work_dir
    nworkers
    worker_cores
    memgb_per_core
    driver_cores
    driver_memory

    main:
    spark_uri = spark_cluster(spark_conf, spark_work_dir, nworkers, worker_cores)
    spark_uri \
    | map {[
        it,
        spark_conf,
        spark_work_dir,
        worker_cores,
        memgb_per_core,
        driver_cores,
        driver_memory,
        spark_app, 
        spark_app_entrypoint, 
        spark_app_args]} \
    | spark_submit_java \
    | map { spark_work_dir } \
    | terminate_spark \
    | set { done }

    emit:
    done
}

workflow spark_cluster {
    take:
    spark_conf
    spark_work_dir
    workers
    worker_cores

    main:
    clean_work_dir = delete_terminate_file(spark_work_dir)

    worker_channels = spark_worker_channels(spark_conf, clean_work_dir, workers, worker_cores)

    Channel.of([spark_conf, clean_work_dir]) | spark_master
    worker_channels | spark_worker

    wait_for_cluster(spark_work_dir, workers) | set { spark_uri }

    emit:
    spark_uri
}

process spark_master {
    container = 'bde2020/spark-master:3.0.1-hadoop3.2'

    input:
    tuple val(spark_conf), path(spark_work_dir)

    output:

    script:
    spark_master_log_file = spark_master_log(spark_work_dir)
    remove_log_file(spark_master_log_file)
    spark_config_name = spark_config_name(spark_conf, spark_work_dir)
    terminate_file_name = terminate_file_name(spark_work_dir)
    def spark_config_env
    def spark_config_arg
    if (spark_config_name != '') {
        create_default_spark_config(spark_config_name)
        spark_config_arg = "--properties-file ${spark_config_name}"
        spark_config_env = ""
    } else {
        spark_config_arg = ""
        spark_config_env = "export SPARK_CONF_DIR=${spark_conf}"
    }

    """
    echo "Starting spark master - logging to ${spark_master_log_file}"

    ${spark_config_env}

    /spark/bin/spark-class \
    org.apache.spark.deploy.master.Master \
    ${spark_config_arg} \
    &> ${spark_master_log_file} &
    spid=\$!
    while true; do
        if [[ -e "${terminate_file_name}" ]] ; then
            kill \$spid
            break
        fi
	    sleep 5
    done
    """
}

process spark_worker {
    container = 'bde2020/spark-worker:3.0.1-hadoop3.2'
    cpus { ncores }

    input:
    tuple val(worker),
          val(spark_conf),
          path(spark_work_dir),
          val(ncores)

    output:
    
    script:
    spark_master_log_file = spark_master_log(spark_work_dir)
    spark_master_uri = wait_for_master(spark_master_log_file)
    spark_worker_log_file = spark_worker_log(worker, spark_work_dir)
    remove_log_file(spark_worker_log_file)
    spark_config_name = spark_config_name(spark_conf, spark_work_dir)
    terminate_file_name = terminate_file_name(spark_work_dir)
    def spark_config_env
    def spark_config_arg
    if (spark_config_name != '') {
        spark_config_arg = "--properties-file ${spark_config_name}"
        spark_config_env = ""
    } else {
        spark_config_arg = ""
        spark_config_env = "export SPARK_CONF_DIR=${spark_conf}"
    }

    """
    echo "Starting spark worker ${worker} - logging to ${spark_worker_log_file}"

    ${spark_config_env}

    /spark/bin/spark-class \
    org.apache.spark.deploy.worker.Worker ${spark_master_uri} \
    -c ${ncores} \
    -d ${spark_work_dir} \
    ${spark_config_arg} \
    &> ${spark_worker_log_file} &
    spid=\$!
    while true; do
        if [[ -e "${terminate_file_name}" ]] ; then
            kill \$spid
            break
        fi
	    sleep 5
    done
    """
}

process wait_for_cluster {
    input:
    path(spark_work_dir)
    val(workers)

    output:
    val(spark_uri)

    exec:
    spark_uri = wait_for_master(spark_master_log(spark_work_dir))
    wait_for_all_workers(spark_work_dir, workers)
}

process spark_submit_java {
    container = 'bde2020/spark-submit:3.0.1-hadoop3.2'
    cpus { driver_cores == 0 ? 1 : driver_cores }

    input:
    tuple val(spark_uri), 
        val(spark_conf), 
        path(spark_work_dir),
        val(worker_cores),
        val(mem_per_core_in_gb),
        val(driver_cores),
        val(driver_memory),
        path(app),  
        val(app_main), 
        val(app_args)

    output:
    stdout
    
    script:
    // prepare submit args
    submit_args_list = ["--master ${spark_uri}"]
    if (app_main != "") {
        submit_args_list.add("--class ${app_main}")
    }
    submit_args_list.add("--conf")
    submit_args_list.add("spark.executor.cores=${worker_cores}")
    executor_memory = calc_executor_memory(worker_cores, mem_per_core_in_gb)
    if (executor_memory > 0) {
        submit_args_list.add("--executor-memory")
        submit_args_list.add("${executor_memory}g")
    }
    if (driver_cores > 0) {
        submit_args_list.add("--conf")
        submit_args_list.add("spark.driver.cores=${driver_cores}")
    }
    if (driver_memory != '') {
        submit_args_list.add("--conf")
        submit_args_list.add("spark.driver.memory=${driver_memory}")
    }
    submit_args_list.add(app)
    submit_args_list.addAll(app_args)
    submit_args = submit_args_list.join(' ')

    """
    # if the next block cannot find a network interface the script should fail
    SPARK_LOCAL_IP=
    for interface in /sys/class/net/{eth*,en*,em*}; do 
        [ -e \$interface ] && \
        [ `cat \$interface/operstate` == "up" ] && \
        SPARK_LOCAL_IP=\$(ifconfig `basename \$interface` | grep "inet " | awk '\$1=="inet" {print \$2; exit}' | sed s/addr://g)
        if [[ "\$SPARK_LOCAL_IP" != "" ]]; then
            echo "Use Spark IP: \$SPARK_LOCAL_IP"
            break
        fi
    done

    echo "\
    --deploy-mode client \
    --conf spark.driver.host=\${SPARK_LOCAL_IP} \
    --conf spark.driver.bindAddress=\${SPARK_LOCAL_IP} \
    ${submit_args} \
    "

    /spark/bin/spark-submit \
    --deploy-mode client \
    --conf spark.driver.host=\${SPARK_LOCAL_IP} \
    --conf spark.driver.bindAddress=\${SPARK_LOCAL_IP} \
    ${submit_args}
    """
}

process terminate_spark {
    input:
    val(spark_work_dir)

    output:
    stdout

    script:
    terminate_file_name = terminate_file_name(spark_work_dir)
    """
    cat > ${terminate_file_name} <<EOF
    DONE
    EOF
    cat ${terminate_file_name}
    """
}

def delete_terminate_file(working_dir) {
    File terminate_file = new File(terminate_file_name(working_dir))
    terminate_file.delete()
    return working_dir
}

def terminate_file_name(working_dir) {
    return "${working_dir}/terminate-spark"
}

def spark_config_name(spark_conf, spark_dir) {
    if (spark_conf == '') {
        return "${spark_dir}/spark-defaults.conf"
    } else {
        return ''
    }
}

def create_default_spark_config(config_name) {
    Properties sparkConfig = new Properties()
    File configFile = new File(config_name)

    sparkConfig.put("spark.rpc.askTimeout", "300s");
    sparkConfig.put("spark.storage.blockManagerHeartBeatMs", "30000");
    sparkConfig.put("spark.rpc.retry.wait", "30s");
    sparkConfig.put("spark.kryoserializer.buffer.max", "1024m");
    sparkConfig.put("spark.core.connection.ack.wait.timeout", "600s");

    sparkConfig.store(configFile.newWriter(), null)
}

def spark_master_log(spark_work_dir) {
    return "${spark_work_dir}/master.log"
}

def spark_worker_log(worker, spark_work_dir) {
    return "${spark_work_dir}/worker-${worker}.log"
}

def remove_log_file(log_file) {
    File f = new File(log_file)
    f.delete()
}

def wait_for_master(spark_master_log_name) {
    def uri;
    while ((uri = search_spark_uri(spark_master_log_name)) == null) {
        sleep(5000)
    }
    return uri
}

def search_spark_uri(spark_master_log_name) {
    File spark_master_log_file = new File(spark_master_log_name)
    if (!spark_master_log_file.exists()) 
        return null

    return spark_master_log_file.withReader { reader ->
        def line = null
        def uri = null
        while ((line = reader.readLine()) != null) {
            def i = line.indexOf("Starting Spark master at spark://");
            if (i == -1) {
                continue
            } else {
                l = "Starting Spark master at ".length()
                uri = line.substring(i + l)
                break
            }
        }
        return uri
    }
}

def wait_for_all_workers(spark_work_dir, workers) {
    Set running_workers = []
    while (running_workers.size() == workers) {
        for (int i = 0; i < workers; i++) {
            def worker_id = i + 1
            if (running_workers.contains(worker_id))
                continue
            spark_worker_log_file = spark_worker_log(worker_id, spark_work_dir)
            if (check_worker_started(spark_worker_log_file))
                running_workers.add(worker_id)
        }
        sleep(1000)
    }
}

def check_worker_started(spark_worker_log_name) {
    File spark_worker_file = new File(spark_worker_log_name)
    if (!spark_worker_file.exists())
        return false

    return spark_worker_file.withReader { reader ->
        def line = null
        def found = false
        while ((line = reader.readLine()) != null) {
            def i = line.indexOf("Worker: Successfully registered with master spark://");
            if (i == -1) {
                continue
            } else {
                found = true
                break
            }
        }
        return found
    }
}

def spark_worker_channels(spark_conf, spark_work_dir, nworkers, worker_cores) {
    def worker_channels = []
    for (int i = 0; i < nworkers; i++) {
        println("Prepare input for worker ${i+1}")
        worker_channels.add([i+1, spark_conf, spark_work_dir, worker_cores])
    }
    return Channel.fromList(worker_channels)
}

def calc_executor_memory(cores, mem_per_core_in_gb) {
    return cores * mem_per_core_in_gb
}
