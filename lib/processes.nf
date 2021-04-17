process prepare_spark_work_dir {
    input:
    val(spark_work_dir)
    val(terminate_name)

    output:
    val(spark_work_dir)

    script:
    terminate_file_name = terminate_file_name(spark_work_dir, terminate_name)
    log.info("Spark work directory: ${spark_work_dir}")
    """
    if [[ ! -d "${spark_work_dir}" ]] ; then
        mkdir -p "${spark_work_dir}"
    else
        rm -f "${terminate_file_name}"
    fi
    """
}

process spark_master {
    container = "${params.spark_container_repo}/${params.spark_container_name}:${params.spark_container_version}"
    def lookup_ip_script = create_lookup_ip_script()

    cpus 1
    memory '1 GB'

    input:
    val(spark_conf)
    val(spark_work_dir)
    val(terminate_name)

    output:

    script:
    spark_master_log_file = spark_master_log(spark_work_dir)
    remove_log_file(spark_master_log_file)
    spark_config_name = spark_config_name(spark_conf, spark_work_dir)
    terminate_file_name = terminate_file_name(spark_work_dir, terminate_name)
    def spark_config_env
    def spark_config_arg
    if (spark_config_name != '') {
        create_default_spark_config(params.spark_local_dir, spark_config_name)
        spark_config_arg = "--properties-file ${spark_config_name}"
        spark_config_env = ""
    } else {
        spark_config_arg = ""
        spark_config_env = "export SPARK_CONF_DIR=${spark_conf}"
    }
    spark_env = create_spark_env(spark_work_dir, spark_config_env, task.ext.sparkLocation)

    """
    echo "Starting spark master - logging to ${spark_master_log_file}"

    ${spark_env}
    ${lookup_ip_script}

    echo "\
    ${task.ext.sparkLocation}/bin/spark-class org.apache.spark.deploy.master.Master \
    -h \$SPARK_LOCAL_IP \
    ${spark_config_arg} \
    "

    ${task.ext.sparkLocation}/bin/spark-class org.apache.spark.deploy.master.Master \
    -h \$SPARK_LOCAL_IP \
    ${spark_config_arg} \
    &> ${spark_master_log_file} &
    spid=\$!
    ${wait_to_terminate('spid', terminate_file_name)}
    """
}

process spark_worker {
    container = "${params.spark_container_repo}/${params.spark_container_name}:${params.spark_container_version}"

    cpus { worker_cores }
    // 1 GB of overhead for the worker itself, the rest for its executors
    memory "${worker_mem_in_gb+1} GB"

    input:
    val(worker)
    val(spark_conf)
    val(spark_work_dir)
    val(worker_cores)
    val(worker_mem_in_gb)
    val(terminate_name)
    
    def lookup_ip_script = create_lookup_ip_script()
    script:
    spark_master_log_file = spark_master_log(spark_work_dir)
    terminate_file_name = terminate_file_name(spark_work_dir, terminate_name)
    spark_master_uri = wait_for_master(spark_master_log_file, terminate_file_name)
    spark_worker_log_file = spark_worker_log(worker, spark_work_dir)
    remove_log_file(spark_worker_log_file)
    spark_config_name = spark_config_name(spark_conf, spark_work_dir)
    def spark_config_env
    def spark_config_arg
    spark_worker_opts='export SPARK_WORKER_OPTS="-Dspark.worker.cleanup.enabled=true -Dspark.worker.cleanup.interval=30 -Dspark.worker.cleanup.appDataTtl=1"'
    if (spark_config_name != '') {
        spark_config_arg = "--properties-file ${spark_config_name}"
        spark_config_env = """
        ${spark_worker_opts}
        """
    } else {
        spark_config_arg = ""
        spark_config_env = """
        ${spark_worker_opts}
        export SPARK_CONF_DIR=${spark_conf}
        """
    }

    spark_env = create_spark_env(spark_work_dir, spark_config_env, task.ext.sparkLocation)
    """
    echo "Starting spark worker ${worker} - logging to ${spark_worker_log_file}"

    ${spark_env}
    ${lookup_ip_script}

    echo "\
    ${task.ext.sparkLocation}/bin/spark-class org.apache.spark.deploy.worker.Worker \
    ${spark_master_uri} \
    -c ${worker_cores} \
    -m ${worker_mem_in_gb}G \
    -d ${spark_work_dir} \
    -h \$SPARK_LOCAL_IP \
    ${spark_config_arg} \
    "

    ${task.ext.sparkLocation}/bin/spark-class org.apache.spark.deploy.worker.Worker \
    ${spark_master_uri} \
    -c ${worker_cores} \
    -m ${worker_mem_in_gb}G \
    -d ${spark_work_dir} \
    -h \$SPARK_LOCAL_IP \
    ${spark_config_arg} \
    &> ${spark_worker_log_file} &
    spid=\$!
    ${wait_to_terminate('spid', terminate_file_name)}
    """
}

process wait_for_cluster {
    executor 'local'

    input:
    val(spark_work_dir)
    val(workers)
    val(terminate_name)

    output:
    tuple val(spark_uri), val(spark_work_dir)

    exec:
    terminate_file_name = terminate_file_name(spark_work_dir, terminate_name)
    spark_uri = wait_for_master(spark_master_log(spark_work_dir), terminate_file_name)
    wait_for_all_workers(spark_work_dir, workers, terminate_file_name)
}

process spark_start_app {
    container = "${params.spark_container_repo}/${params.spark_container_name}:${params.spark_container_version}"
    def lookup_ip_script = create_lookup_ip_script()

    cpus { driver_cores == 0 ? 1 : driver_cores }
    memory { driver_memory.replace('k'," KB").replace('m'," MB").replace('g'," GB").replace('t'," TB") }

    input:
    val(spark_uri)
    val(spark_conf)
    val(spark_work_dir)
    val(workers)
    val(executor_cores)
    val(mem_per_core_in_gb)
    val(driver_cores)
    val(driver_memory)
    val(driver_stack_size)
    val(driver_logconfig)
    val(driver_deploy_mode)
    val(app)
    val(app_main)
    val(app_args)
    val(app_log)

    output:
    tuple val(spark_uri), val(spark_work_dir)
    
    script:
    // prepare submit args
    submit_args_list = ["--master ${spark_uri}"]
    if (app_main != "") {
        submit_args_list.add("--class ${app_main}")
    }
    submit_args_list.add("--conf")
    submit_args_list.add("spark.executor.cores=${executor_cores}")
    parallelism = workers * executor_cores
    if (parallelism > 0) {
        submit_args_list.add("--conf")
        submit_args_list.add("spark.files.openCostInBytes=0")
        submit_args_list.add("--conf")
        submit_args_list.add("spark.default.parallelism=${parallelism}")
    }
    executor_memory = calc_executor_memory(executor_cores, mem_per_core_in_gb)
    if (executor_memory > 0) {
        submit_args_list.add("--executor-memory")
        submit_args_list.add("${executor_memory}g")
    }
    if (driver_cores > 0) {
        submit_args_list.add("--conf")
        submit_args_list.add("spark.driver.cores=${driver_cores}")
    }
    if (driver_memory != '') {
        submit_args_list.add("--driver-memory")
        submit_args_list.add(driver_memory)
    }
    sparkDriverJavaOpts = []
    if (driver_logconfig != null && driver_logconfig != '') {
        submit_args_list.add("--conf")
        submit_args_list.add("spark.executor.extraJavaOptions=-Dlog4j.configuration=file://${driver_logconfig}")
        sparkDriverJavaOpts.add("-Dlog4j.configuration=file://${driver_logconfig}")
    }
    if (driver_stack_size != null && driver_stack_size != '') {
        sparkDriverJavaOpts.add("-Xss${driver_stack_size}")
    }
    if (sparkDriverJavaOpts.size() > 0) {
        submit_args_list.add("--driver-java-options")
        submit_args_list.add('"' + sparkDriverJavaOpts.join(' ') + '"')
    }
    submit_args_list.add(app)
    submit_args_list.add(app_args)
    submit_args = submit_args_list.join(' ')
    deploy_mode_arg = ''
    spark_config_name = spark_config_name(spark_conf, spark_work_dir)
    if (driver_deploy_mode != null && driver_deploy_mode != '') {
        deploy_mode_arg = "--deploy-mode ${driver_deploy_mode}"
    }
    def spark_config_env
    def spark_config_arg
    if (spark_config_name != '') {
        spark_config_arg = "--properties-file ${spark_config_name}"
        spark_config_env = ""
    } else {
        spark_config_arg = ""
        spark_config_env = "export SPARK_CONF_DIR=${spark_conf}"
    }
    spark_driver_log_file = spark_driver_log(spark_work_dir, app_log)
    spark_env = create_spark_env(spark_work_dir, spark_config_env, task.ext.sparkLocation)

    """
    echo "Starting the spark driver"

    ${spark_env}

    ${lookup_ip_script}

    echo "\
    ${task.ext.sparkLocation}/bin/spark-class org.apache.spark.deploy.SparkSubmit \
    ${spark_config_arg} \
    ${deploy_mode_arg} \
    --conf spark.driver.host=\${SPARK_LOCAL_IP} \
    --conf spark.driver.bindAddress=\${SPARK_LOCAL_IP} \
    ${submit_args} \
    "

    ${task.ext.sparkLocation}/bin/spark-class org.apache.spark.deploy.SparkSubmit \
    ${spark_config_arg} \
    ${deploy_mode_arg} \
    --conf spark.driver.host=\${SPARK_LOCAL_IP} \
    --conf spark.driver.bindAddress=\${SPARK_LOCAL_IP} \
    ${submit_args} \
    &> ${spark_driver_log_file}
    """
}

process terminate_spark {
    input:
    val(spark_work_dir)
    val(terminate_name)

    output:
    tuple val(terminate_file_name), val(spark_work_dir)

    script:
    terminate_file_name = terminate_file_name(spark_work_dir, terminate_name)
    """
    cat > ${terminate_file_name} <<EOF
    DONE
    EOF
    cat ${terminate_file_name}
    """
}

def terminate_file_name(working_dir, terminate_name) {
    return terminate_name == null || terminate_name == ''
        ? "${working_dir}/terminate-spark"
        : "${working_dir}/${terminate_name}"
}

def spark_config_name(spark_conf, spark_dir) {
    if (spark_conf == '') {
        return "${spark_dir}/spark-defaults.conf"
    } else {
        return ''
    }
}

def create_spark_env(spark_work_dir, spark_config_env, sparkLocation) {
    return """
    export SPARK_ENV_LOADED=
    export SPARK_HOME=${sparkLocation}
    export PYSPARK_PYTHONPATH_SET=
    export PYTHONPATH="${sparkLocation}/python"
    export SPARK_LOG_DIR="${spark_work_dir}"
    ${spark_config_env}
    . "${sparkLocation}/sbin/spark-config.sh"
    . "${sparkLocation}/bin/load-spark-env.sh"
    """
}

def create_default_spark_config(spark_local_dir, config_name) {
    Properties sparkConfig = new Properties()
    File configFile = new File(config_name)

    sparkConfig.put("spark.rpc.askTimeout", "300s")
    sparkConfig.put("spark.storage.blockManagerHeartBeatMs", "30000")
    sparkConfig.put("spark.rpc.retry.wait", "30s")
    sparkConfig.put("spark.kryoserializer.buffer.max", "1024m")
    sparkConfig.put("spark.core.connection.ack.wait.timeout", "600s")
    sparkConfig.put("spark.driver.maxResultSize", "0")
    sparkConfig.put("spark.worker.cleanup.enabled", "true")
    sparkConfig.put("spark.local.dir", spark_local_dir.toString())

    sparkConfig.store(configFile.newWriter(), null)
}

def spark_master_log(spark_work_dir) {
    return "${spark_work_dir}/sparkmaster.log"
}

def spark_worker_log(worker, spark_work_dir) {
    return "${spark_work_dir}/sparkworker-${worker}.log"
}

def spark_driver_log(spark_work_dir, log_name) {
    def log_file_name = log_name == null || log_name == "" ? "sparkdriver.log" : log_name
    return "${spark_work_dir}/${log_file_name}"
}

def remove_log_file(log_file) {
    try {
        File f = new File(log_file)
        f.delete()
    }
    catch (Throwable t) {
        log.error "Problem deleting log file ${log_file}"
        t.printStackTrace()
    }
}

def create_lookup_ip_script() {
    if (workflow.containerEngine == "docker") {
        return lookup_ip_inside_docker_script()
    } else {
        return lookup_local_ip_script()
    }
}

def lookup_local_ip_script() {
    // Take the last IP that's listed by hostname -i.
    // This hack works on Janelia Cluster and AWS EC2.
    // It won't be necessary at all once we add a local option for Spark apps.
    """
    SPARK_LOCAL_IP=`hostname -i | rev | cut -d' ' -f1 | rev`
    echo "Use Spark IP: \$SPARK_LOCAL_IP"
    """
}

def lookup_ip_inside_docker_script() {
    """
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
    """
}

def wait_for_master(spark_master_log_name, terminate_file_name) {
    def uri;
    File terminate_file = new File(terminate_file_name)
    while ((uri = search_spark_uri(spark_master_log_name)) == null) {
        if (terminate_file.exists()) break

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

def wait_for_all_workers(spark_work_dir, workers, terminate_file_name) {
    Set running_workers = []
    File terminate_file = new File(terminate_file_name)
    while (running_workers.size() == workers) {
        if (terminate_file.exists()) break

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

def wait_to_terminate(pid_var, terminate_file_name) {
    """
    while true; do

        if ! kill -0 \$${pid_var} >/dev/null 2>&1; then
            echo "Process \$${pid_var} died"
            exit 1
        fi

        if [[ -e "${terminate_file_name}" ]] ; then
            kill \$${pid_var}
            break
        fi

	    sleep 1

    done
    """
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

def calc_executor_memory(cores, mem_per_core_in_gb) {
    return cores * mem_per_core_in_gb
}
