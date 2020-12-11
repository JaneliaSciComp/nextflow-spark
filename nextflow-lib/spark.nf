process spark_master {
    container = 'bde2020/spark-master:3.0.1-hadoop3.2'

    input:
    path spark_log_dir

    output:
    env URI

    script:
    spark_master_log_file = spark_master_log(spark_log_dir)
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
    stdout
    
    script:
    spark_master_log_file = spark_master_log(spark_log_dir)
    spark_master_uri = extract_spark_uri(spark_master_log_file)
    spark_worker_log_file = spark_worker_log(worker, spark_log_dir)
    """
    /spark/bin/spark-class \
    org.apache.spark.deploy.worker.Worker ${spark_master_uri} &> ${spark_worker_log_file}
    """
}

def spark_master_log(spark_log_dir) {
    return "${spark_log_dir}/master.log"
}

def spark_worker_log(worker, spark_log_dir) {
    return "${spark_log_dir}/worker-${worker}.log"
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

def spark_worker_channels(spark_log_dir, nworkers) {
    def worker_channels = []
    for (int i = 0; i < nworkers; i++) {
        println("Prepare input for worker ${i+1}")
        worker_channels.add([i+1, spark_log_dir])
    }
    return Channel.fromList(worker_channels)
}

workflow spark_cluster {
    take: 
    spark_log_dir
    workers

    main:
    worker_channels = spark_worker_channels(spark_log_dir, workers)
    spark_master(spark_log_dir)
    worker_channels | spark_worker 

}
