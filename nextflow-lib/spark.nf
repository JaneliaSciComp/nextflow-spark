process spark_master {
    container = 'bde2020/spark-master:3.0.1-hadoop3.2'

    input:
    path spark_log_dir

    output:

    script:
    spark_master_log_file = spark_master_log(spark_log_dir)
    remove_log_file(spark_master_log_file)
    spark_config_name = spark_config_name(spark_log_dir)
    create_default_spark_config(spark_config_name)

    """
    echo "Spark master log: ${spark_master_log_file}"

    if [[ -e "${spark_log_dir}/terminate-spark" ]] ; then
        rm -f "${spark_log_dir}/terminate-spark"
    fi

    /spark/bin/spark-class \
    org.apache.spark.deploy.master.Master \
    --properties-file ${spark_config_name} \
    &> ${spark_master_log_file} &
    spid=\$!
    while true; do
        if [[ -e "${spark_log_dir}/terminate-spark" ]] ; then
            kill \$spid
            break
        fi
	    sleep 5
    done
    """
}

process spark_worker {
    container = 'bde2020/spark-worker:3.0.1-hadoop3.2'

    input:
    tuple val(worker), path(spark_log_dir)

    output:
    
    script:
    spark_master_log_file = spark_master_log(spark_log_dir)
    spark_master_uri = extract_spark_uri(spark_master_log_file)
    spark_worker_log_file = spark_worker_log(worker, spark_log_dir)
    remove_log_file(spark_worker_log_file)
    spark_config_name = spark_config_name(spark_log_dir)

    """
    /spark/bin/spark-class \
    org.apache.spark.deploy.worker.Worker ${spark_master_uri} \
    -d ${spark_log_dir} \
    --properties-file ${spark_config_name} \
    &> ${spark_worker_log_file} &
    spid=\$!
    while true; do
        if [[ -e "${spark_log_dir}/terminate-spark" ]] ; then
            kill \$spid
            break
        fi
	    sleep 5
    done
    """
}

process check_spark_cluster {
    input:
    path(spark_log_dir)
    val(workers)

    output:
    val(spark_master_uri)

    exec:
    spark_master_log_file = spark_master_log(spark_log_dir)
    wait_for_all_workers(spark_log_dir, workers)
    spark_master_uri = extract_spark_uri(spark_master_log_file)
}

def spark_config_name(spark_dir) {
    return "${spark_dir}/spark-defaults.conf"
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

def spark_master_log(spark_log_dir) {
    return "${spark_log_dir}/master.log"
}

def spark_worker_log(worker, spark_log_dir) {
    return "${spark_log_dir}/worker-${worker}.log"
}

def remove_log_file(log_file) {
    File f = new File(log_file)
    f.delete()
}

def extract_spark_uri(spark_master_log_name) {
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

def wait_for_all_workers(spark_log_dir, workers) {
    Set running_workers = []
    while (running_workers.size() == workers) {
        for (int i = 0; i < workers; i++) {
            def worker_id = i + 1
            if (running_workers.contains(worker_id))
                continue
            spark_worker_log_file = spark_worker_log(worker_id, spark_log_dir)
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

workflow spark_cluster {
    take: 
    spark_log_dir
    workers

    main:
    worker_channels = spark_worker_channels(spark_log_dir, workers)
    spark_master(spark_log_dir)
    worker_channels | spark_worker
    
    check_spark_cluster(spark_log_dir, workers) | set {res}

    emit:
    res
}

def spark_worker_channels(spark_log_dir, nworkers) {
    def worker_channels = []
    for (int i = 0; i < nworkers; i++) {
        println("Prepare input for worker ${i+1}")
        worker_channels.add([i+1, spark_log_dir])
    }
    return Channel.fromList(worker_channels)
}

process spark_submit_java {
    container = 'bde2020/spark-submit:3.0.1-hadoop3.2'

    input:
    tuple val(spark_uri), path(spark_log_dir), path(app_jar),  val(app_main), val(app_args)

    output:
    stdout
    
    script:
    // prepare submit args
    submit_args_list = ["--master ${spark_uri}"]
    if (app_main != "") {
        submit_args_list.add("--class ${app_main}")
    }
    submit_args_list.add("--conf")
    submit_args_list.add("spark.executor.cores=2")
    submit_args_list.add("--executor-memory")
    submit_args_list.add("4g")
    submit_args_list.add("--conf")
    submit_args_list.add("spark.default.parallelism=2")
    submit_args_list.add(app_jar)
    submit_args_list.addAll(app_args)
    submit_args = submit_args_list.join(' ')

    """
    SPARK_LOCAL_IP=\$(ifconfig eth0 | grep inet | awk '\$1=="inet" {print \$2}' | sed s/addr://g)

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
    val(spark_log_dir)

    output:
    stdout

    script:
    """
    cat > ${spark_log_dir}/terminate-spark <<EOF
    DONE
    EOF
    """
}
