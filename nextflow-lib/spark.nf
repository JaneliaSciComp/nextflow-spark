process spark_master {
    container = 'bde2020/spark-master:3.0.1-hadoop3.2'

    input:
    path spark_log_dir

    output:

    script:
    spark_master_log_file = spark_master_log(spark_log_dir)
    remove_log_file(spark_master_log_file)
    """
    echo "Spark master log: ${spark_master_log_file}"
    /spark/bin/spark-class \
    org.apache.spark.deploy.master.Master &> ${spark_master_log_file}
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
    """
    /spark/bin/spark-class \
    org.apache.spark.deploy.worker.Worker ${spark_master_uri} &> ${spark_worker_log_file}
    """
}

process check_spark {
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
    
    check_spark(spark_log_dir, workers) | set {res}

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
