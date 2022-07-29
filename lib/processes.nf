process prepare_spark_work_dir {
    container = "${params.spark_container_repo}/${params.spark_container_name}:${params.spark_container_version}"
    label 'small'

    input:
    val(spark_work_dir)
    val(terminate_name)

    output:
    val(spark_work_dir)

    script:
    def terminate_file_name = get_terminate_file_name(spark_work_dir, terminate_name)
    def write_session_id = create_write_session_id_script(spark_work_dir)
    log.debug "Spark local directory: ${params.spark_local_dir}"
    log.debug "Spark work directory: ${spark_work_dir}"
    """
    mkdir -p "${params.spark_local_dir}"
    if [[ ! -d "${spark_work_dir}" ]] ; then
        mkdir -p "${spark_work_dir}"
    else
        rm -f ${spark_work_dir}/* || true
    fi
    ${write_session_id}
    """
}

process spark_master {
    container = "${params.spark_container_repo}/${params.spark_container_name}:${params.spark_container_version}"
    label 'small'

    input:
    val(spark_conf)
    val(spark_work_dir)
    val(terminate_name)

    output:

    script:
    def spark_master_log_file = get_spark_master_log(spark_work_dir)
    def spark_config_name = get_spark_config_name(spark_conf, spark_work_dir)
    def terminate_file_name = get_terminate_file_name(spark_work_dir, terminate_name)
    def create_spark_config
    def spark_config_env
    def spark_config_arg
    if (spark_config_name != '') {
        create_spark_config = create_default_spark_config(spark_config_name)
        spark_config_arg = "--properties-file ${spark_config_name}"
        spark_config_env = ""
    } else {
        create_spark_config = ""
        spark_config_arg = ""
        spark_config_env = "export SPARK_CONF_DIR=${spark_conf}"
    }
    def spark_env = create_spark_env(spark_work_dir, spark_config_env, task.ext.sparkLocation)
    def lookup_ip_script = create_lookup_ip_script()
    def check_session_id = create_check_session_id_script(spark_work_dir)
    """
    echo "Starting spark master - logging to ${spark_master_log_file}"
    ${check_session_id}

    rm -f ${spark_master_log_file} || true

    ${create_spark_config}
    ${spark_env}
    ${lookup_ip_script}

    ${task.ext.sparkLocation}/bin/spark-class org.apache.spark.deploy.master.Master \
    -h \$SPARK_LOCAL_IP \
    ${spark_config_arg} \
    &> ${spark_master_log_file} &
    spid=\$!

    ${wait_to_terminate('spid', terminate_file_name, spark_master_log_file)}
    """
}

process wait_for_master {
    container { "${params.spark_container_repo}/${params.spark_container_name}:${params.spark_container_version}" }
    label 'small'
    errorStrategy { task.exitStatus == 2
        ? 'retry' // retry on a timeout to prevent the case when the waiter is started before the master and master never gets its chance
        : 'terminate' }
    maxRetries 20

    input:
    val(spark_work_dir)
    val(terminate_name)

    output:
    tuple val(spark_work_dir), val(terminate_name), env(spark_uri)

    script:
    def spark_master_log_name = get_spark_master_log(spark_work_dir)
    def terminate_file_name = get_terminate_file_name(spark_work_dir, terminate_name)
    def check_session_id = create_check_session_id_script(spark_work_dir)
    """
    ${check_session_id}

    while true; do

        if [[ -e ${spark_master_log_name} ]]; then
            test_uri=`grep -o "\\(spark://.*\$\\)" ${spark_master_log_name} || true`
            if [[ ! -z \${test_uri} ]]; then
                echo "Spark master started at \${test_uri}"
                break
            fi
        fi

        if [[ -e "${terminate_file_name}" ]]; then
            echo "Terminate file ${terminate_file_name} found"
            exit 1
        fi

        if (( \${SECONDS} > \${MAX_WAIT_SECS} )); then
            echo "Timed out after \${SECONDS} seconds while waiting for spark master <- ${spark_master_log_name}"
            cat ${spark_master_log_name} >&2
            exit 2
        fi

        sleep \${SLEEP_SECS}
        SECONDS=\$(( \${SECONDS} + \${SLEEP_SECS} ))


    done
    spark_uri=\${test_uri}
    """
}

process spark_worker {
    container = "${params.spark_container_repo}/${params.spark_container_name}:${params.spark_container_version}"
    cpus { worker_cores }
    // 1 GB of overhead for the worker itself, the rest for its executors
    memory "${worker_mem_in_gb+1} GB"

    input:
    val(spark_master_uri)
    val(worker_id)
    val(spark_conf)
    val(spark_work_dir)
    val(worker_cores)
    val(worker_mem_in_gb)
    val(terminate_name)
    
    output:

    script:
    def terminate_file_name = get_terminate_file_name(spark_work_dir, terminate_name)
    def spark_worker_log_file = get_spark_worker_log(spark_work_dir, worker_id)
    def spark_config_name = get_spark_config_name(spark_conf, spark_work_dir)
    def spark_config_env
    def spark_config_arg
    def spark_worker_opts_list = []
    spark_worker_opts_list << "spark.worker.cleanup.enabled=true"
    spark_worker_opts_list << "spark.worker.cleanup.interval=30"
    spark_worker_opts_list << "spark.worker.cleanup.appDataTtl=1"
    spark_worker_opts_list << "spark.port.maxRetries=${params.max_connect_retries}"
    def spark_worker_opts_string = spark_worker_opts_list.inject('') {
        arg, item -> "${arg} -D${item}"
    }
    def spark_worker_opts="export SPARK_WORKER_OPTS=\"${spark_worker_opts_string}\""
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

    def spark_env = create_spark_env(spark_work_dir, spark_config_env, task.ext.sparkLocation)
    def lookup_ip_script = create_lookup_ip_script()
    def check_session_id = create_check_session_id_script(spark_work_dir)
    """
    echo "Starting spark worker ${worker_id} - logging to ${spark_worker_log_file}"
    ${check_session_id}

    rm -f ${spark_worker_log_file} || true
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
    ${wait_to_terminate('spid', terminate_file_name, spark_worker_log_file)}
    """
}

process wait_for_worker {
    container = "${params.spark_container_repo}/${params.spark_container_name}:${params.spark_container_version}"
    label 'small'
    errorStrategy { task.exitStatus == 2
        ? 'retry' // retry on a timeout to prevent the case when the waiter is started before the worker and the worker never gets its chance
        : 'terminate' }
    maxRetries 20

    input:
    val(spark_master_uri)
    val(spark_work_dir)
    val(terminate_name)
    val(worker_id)

    output:
    tuple val(spark_master_uri),
          val(spark_work_dir),
          val(terminate_name),
          val(worker_id)

    script:
    def terminate_file_name = get_terminate_file_name(spark_work_dir, terminate_name)
    def spark_worker_log_file = get_spark_worker_log(spark_work_dir, worker_id)
    def check_session_id = create_check_session_id_script(spark_work_dir)
    """
    ${check_session_id}

    while true; do

        if [[ -e "${spark_worker_log_file}" ]]; then
            found=`grep -o "\\(Worker: Successfully registered with master ${spark_master_uri}\\)" ${spark_worker_log_file} || true`

            if [[ ! -z \${found} ]]; then
                echo "\${found}"
                break
            fi
        fi

        if [[ -e "${terminate_file_name}" ]]; then
            echo "Terminate file ${terminate_file_name} found"
            exit 1
        fi

        if (( \${SECONDS} > \${MAX_WAIT_SECS} )); then
            echo "Spark worker ${worker_id} timed out after \${SECONDS} seconds while waiting for master ${spark_master_uri}"
            cat ${spark_worker_log_file} >&2
            exit 2
        fi

        sleep \${SLEEP_SECS}
        SECONDS=\$(( \${SECONDS} + \${SLEEP_SECS} ))

    done
    """
}

process spark_start_app {
    container = "${params.spark_container_repo}/${params.spark_container_name}:${params.spark_container_version}"
    cpus { driver_cores == 0 ? 1 : driver_cores }
    memory { driver_memory.replace('k'," KB").replace('m'," MB").replace('g'," GB").replace('t'," TB") }

    input:
    val(spark_uri)
    val(spark_conf)
    val(spark_work_dir)
    val(workers)
    val(executor_cores_param)
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
    def submit_args_list = []
    submit_args_list << "--master" << spark_uri
    if (app_main != "") {
        submit_args_list << "--class ${app_main}"
    }
    submit_args_list << "--conf"
    def executor_cores = executor_cores_param as int
    submit_args_list.add("spark.executor.cores=${executor_cores}")
    def parallelism = workers * executor_cores
    if (parallelism > 0) {
        submit_args_list << "--conf" << "spark.files.openCostInBytes=0"
        submit_args_list << "--conf" << "spark.default.parallelism=${parallelism}"
    }
    def executor_memory = calc_executor_memory(executor_cores, mem_per_core_in_gb)
    if (executor_memory > 0) {
        submit_args_list << "--executor-memory" << "${executor_memory}g"
    }
    if (driver_cores > 0) {
        submit_args_list << "--conf" << "spark.driver.cores=${driver_cores}"
    }
    if (driver_memory != '') {
        submit_args_list << "--driver-memory" << driver_memory
    }
    def sparkDriverJavaOpts = []
    if (driver_logconfig != null && driver_logconfig != '') {
        submit_args_list << "--conf" << "spark.executor.extraJavaOptions=-Dlog4j.configuration=file://${driver_logconfig}"
        sparkDriverJavaOpts << "-Dlog4j.configuration=file://${driver_logconfig}"
    }
    if (driver_stack_size != null && driver_stack_size != '') {
        sparkDriverJavaOpts << "-Xss${driver_stack_size}"
    }
    if (sparkDriverJavaOpts.size() > 0) {
        submit_args_list << "--driver-java-options"
        submit_args_list << '"' + sparkDriverJavaOpts.join(' ') + '"'
    }
    submit_args_list << app << app_args
    def submit_args = submit_args_list.join(' ')
    def deploy_mode_arg = ''
    def spark_config_name = get_spark_config_name(spark_conf, spark_work_dir)
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
    def spark_driver_log_file = get_spark_driver_log(spark_work_dir, app_log)
    def spark_env = create_spark_env(spark_work_dir, spark_config_env, task.ext.sparkLocation)
    def lookup_ip_script = create_lookup_ip_script()
    def check_session_id = create_check_session_id_script(spark_work_dir)
    """
    echo "Starting the spark driver"
    ${check_session_id}

    ${spark_env}

    ${lookup_ip_script}

    ${task.ext.sparkLocation}/bin/spark-class org.apache.spark.deploy.SparkSubmit \
    ${spark_config_arg} \
    ${deploy_mode_arg} \
    --conf spark.driver.host=\${SPARK_LOCAL_IP} \
    --conf spark.driver.bindAddress=\${SPARK_LOCAL_IP} \
    ${submit_args}
    """
}

process terminate_spark {
    container = "${params.spark_container_repo}/${params.spark_container_name}:${params.spark_container_version}"
    label 'small'

    input:
    val(spark_work_dir)
    val(terminate_name)

    output:
    tuple val(terminate_file_name), val(spark_work_dir)

    script:
    terminate_file_name = get_terminate_file_name(spark_work_dir, terminate_name)
    check_session_id = create_check_session_id_script(spark_work_dir)
    """
    ${check_session_id}

    cat > ${terminate_file_name} <<EOF
    DONE
    EOF
    cat ${terminate_file_name}
    """
}

def get_terminate_file_name(working_dir, terminate_name) {
    return terminate_name == null || terminate_name == ''
        ? "${working_dir}/terminate-spark"
        : "${working_dir}/${terminate_name}"
}

def get_spark_config_name(spark_conf, spark_dir) {
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

def create_default_spark_config(config_name) {
    def config_file = file(config_name)
    return """
        mkdir -p ${config_file.parent}
        cat <<EOF > ${config_name}
        spark.rpc.askTimeout=300s
        spark.storage.blockManagerHeartBeatMs=30000
        spark.rpc.retry.wait=30s
        spark.kryoserializer.buffer.max=1024m
        spark.core.connection.ack.wait.timeout=600s
        spark.driver.maxResultSize=0
        spark.worker.cleanup.enabled=true
        spark.local.dir=${params.spark_local_dir}
        EOF
        """.stripIndent()
}

def get_spark_master_log(spark_work_dir) {
    return "${spark_work_dir}/sparkmaster.log"
}

def get_spark_worker_log(spark_work_dir, worker) {
    return "${spark_work_dir}/sparkworker-${worker}.log"
}

def get_spark_driver_log(spark_work_dir, log_name) {
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
    if [[ -z "\${SPARK_LOCAL_IP}" ]] ; then
        echo "Could not determine local IP: SPARK_LOCAL_IP is empty"
        exit 1
    fi
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
    if [[ -z "\${SPARK_LOCAL_IP}" ]] ; then
        echo "Could not determine local IP: SPARK_LOCAL_IP is empty"
        exit 1
    fi
    """
}

def create_write_session_id_script(spark_work_dir) {
    """
    echo "Writing ${workflow.sessionId} to ${spark_work_dir}/.sessionId"
    echo "${workflow.sessionId}" > "${spark_work_dir}/.sessionId"
    """
}

def create_check_session_id_script(spark_work_dir) {
    """
    SESSION_FILE="$spark_work_dir/.sessionId"   
    echo "Checking for \$SESSION_FILE"
    SLEEP_SECS=${params.sleep_between_timeout_checks_seconds}
    MAX_WAIT_SECS=${params.wait_for_spark_timeout_seconds}
    SECONDS=0

    while ! test -e "\$SESSION_FILE"; do
        sleep \${SLEEP_SECS}
        if (( \${SECONDS} < \${MAX_WAIT_SECS} )); then
            echo "Waiting for \$SESSION_FILE"
            SECONDS=\$(( \${SECONDS} + \${SLEEP_SECS} ))
        else
            echo "-------------------------------------------------------------------------------"
            echo "ERROR: Timed out after \${SECONDS} seconds while waiting for \$SESSION_FILE    "
            echo "Make sure that your --spark_work_dir is accessible to all nodes in the cluster "
            echo "-------------------------------------------------------------------------------"
            exit 1
        fi
    done

    echo "Found \$SESSION_FILE in \$SECONDS s"
    # reset SECONDS for the next wait
    SECONDS=0

    if ! grep -F -x -q "${workflow.sessionId}" \$SESSION_FILE
    then
        echo "------------------------------------------------------------------------------"
        echo "ERROR: session id in \$SESSION_FILE does not match current session            "
        echo "Make sure that your --spark_work_dir is accessible to all nodes in the cluster"
        echo "and that you are not running multiple pipelines with the same --spark_work_dir"
        echo "------------------------------------------------------------------------------"
        exit 1
    fi
    """
}

def wait_to_terminate(pid_var, terminate_file_name, log_file) {
    """
    trap "kill -9 \$${pid_var} &>/dev/null" EXIT

    while true; do

        if ! kill -0 \$${pid_var} >/dev/null 2>&1; then
            echo "Process \$${pid_var} died"
            cat ${log_file} >&2
            exit 1
        fi

        if [[ -e "${terminate_file_name}" ]] ; then
            cat ${log_file}
            break
        fi

        sleep 1
    done
    """
}

def calc_executor_memory(cores, mem_per_core_in_gb) {
    return cores * mem_per_core_in_gb
}
